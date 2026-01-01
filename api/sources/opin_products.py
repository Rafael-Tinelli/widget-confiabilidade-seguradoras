# api/sources/opin_products.py
from __future__ import annotations

import gzip
import json
import re
import concurrent.futures
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# Importação condicional do curl_cffi (já instalado no pipeline)
# Ele é essencial para evitar bloqueios de WAF (Cloudflare/Akamai)
try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    import requests as cffi_requests

# --- Configurações ---
MAX_WORKERS = 10  # Downloads simultâneos
TIMEOUT_SECONDS = 20

# Endpoints diretos (mais confiável que crawling recursivo)
# Estes são os caminhos padrão da especificação Open Insurance Brasil
TARGET_ENDPOINTS = {
    "auto": "/open-insurance/products-services/v1/auto-insurance",
    "life": "/open-insurance/products-services/v1/life-pension",
    "home": "/open-insurance/products-services/v1/home-insurance",
}

# Headers que simulam um navegador real
OPIN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

@dataclass
class OpinProductMeta:
    """
    Classe unificada que carrega Metadados E Dados.
    Substitui a complexidade de OpinMeta + OpinResult do código antigo.
    Compatível com: build_insurers.py
    """
    source: str = "Open Insurance Brasil"
    as_of: str = ""
    products_auto_file: str = ""
    products_life_file: str = ""
    products_home_file: str = ""
    stats: dict[str, int] | None = None
    # Campo essencial: Dicionário {CNPJ: [Lista de Produtos]}
    data: dict[str, list] = field(default_factory=dict) 

def _load_participants(json_path: Path) -> list[dict]:
    """Carrega o JSON de participantes gerado pelo build_json.py."""
    if not json_path.exists():
        print(f"OPIN WARNING: Arquivo não encontrado: {json_path}")
        return []
    
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            return data.get("participants", []) or []
    except Exception as e:
        print(f"OPIN ERROR: Falha ao ler participantes: {e}")
        return []

def _extract_api_base_url(participant: dict) -> str | None:
    """Extrai a URL base (ApiDiscoveryUri) do participante de forma robusta."""
    # Prioridade 1: ApiResources
    for resource in participant.get("ApiResources", []):
        uri = resource.get("ApiDiscoveryUri") or resource.get("ApiFamilyType")
        if uri and isinstance(uri, str) and uri.startswith("http"):
            # Corta a URL antes do sufixo de versão ou endpoint específico
            # Ex: https://api.seguradora.com/open-insurance/discovery/v1 -> https://api.seguradora.com
            parts = uri.split("/open-insurance/")
            if parts:
                return parts[0]
    
    # Prioridade 2: AuthorisationServers
    for server in participant.get("AuthorisationServers", []):
        uri = server.get("OpenIDDiscoveryDocument")
        if uri and isinstance(uri, str) and uri.startswith("http"):
            parts = uri.split("/.well-known")
            return parts[0]
            
    return None

def _format_cnpj(raw: str) -> str:
    """Formata 12345678000199 para 12.345.678/0001-99 (Padrão SUSEP)."""
    nums = re.sub(r"[^0-9]", "", str(raw))
    if len(nums) != 14:
        return raw 
    return f"{nums[:2]}.{nums[2:5]}.{nums[5:8]}/{nums[8:12]}-{nums[12:]}"

def _fetch_single_target(name: str, base_url: str, category: str, endpoint: str) -> list[dict]:
    """Baixa produtos de uma seguradora específica usando impersonate do Chrome."""
    full_url = f"{base_url.rstrip('/')}{endpoint}"
    results = []

    try:
        # Configuração do Request
        kwargs = {"timeout": TIMEOUT_SECONDS, "verify": False, "headers": OPIN_HEADERS}
        
        # Se estivermos usando curl_cffi, ativamos a simulação de Chrome
        if hasattr(cffi_requests, "Session"): 
             kwargs["impersonate"] = "chrome"

        resp = cffi_requests.get(full_url, **kwargs)

        if resp.status_code == 200:
            # Estrutura padrão: data -> brand -> companies -> products
            payload = resp.json().get("data", {})
            brand = payload.get("brand", {})
            companies = brand.get("companies", [])

            for comp in companies:
                # Tenta pegar o CNPJ para vincular com a SUSEP
                cnpj_raw = comp.get("identification") or comp.get("cnpjNumber")
                formatted_cnpj = _format_cnpj(cnpj_raw) if cnpj_raw else None
                
                products = comp.get("products", [])
                for p in products:
                    # Enriquece o produto com metadados
                    p["_source_cnpj"] = formatted_cnpj
                    p["_category"] = category
                    p["_source_participant"] = name
                    results.append(p)
    except Exception:
        # Ignora erros individuais (timeout, 404) para não parar o lote inteiro
        pass

    return results

def extract_open_insurance_products() -> OpinProductMeta:
    """Função principal orquestradora chamada pelo build_insurers.py"""
    # Define caminhos relativos à execução
    # Assume que o script roda da raiz do projeto via 'python -m api.build_insurers'
    root = Path.cwd() 
    
    # Localização do arquivo de participantes (gerado no passo anterior do CI)
    part_file = root / "api" / "v1" / "participants.json"
    
    # Diretório de cache para debug (Raw Data)
    raw_dir = root / "data" / "raw" / "opin"
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"OPIN: Carregando participantes de {part_file}...")
    participants = _load_participants(part_file)
    
    # Monta lista de alvos (Nome, URL Base)
    targets = []
    for p in participants:
        name = p.get("RegisteredName", "Unknown")
        url = _extract_api_base_url(p)
        if url:
            targets.append((name, url))
            
    print(f"OPIN: {len(targets)} seguradoras com URLs de API identificadas.")

    stats = {"auto": 0, "life": 0, "home": 0}
    filenames = {}
    master_data: dict[str, list] = {} # {CNPJ: [produtos]}

    # Download Paralelo por Categoria
    for category, endpoint in TARGET_ENDPOINTS.items():
        print(f"OPIN: Baixando {category}...")
        cat_products = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submete tarefas
            futures = [
                executor.submit(_fetch_single_target, name, url, category, endpoint)
                for name, url in targets
            ]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    prods = future.result()
                    cat_products.extend(prods)
                    
                    # Agrega em memória para o build_insurers
                    for p in prods:
                        cnpj = p.get("_source_cnpj")
                        if cnpj:
                            if cnpj not in master_data:
                                master_data[cnpj] = []
                            master_data[cnpj].append(p)
                except Exception:
                    continue

        # Salva backup em disco (JSON GZIP)
        if cat_products:
            fname = f"products_{category}.json.gz"
            fpath = raw_dir / fname
            with gzip.open(fpath, "wt", encoding="utf-8") as f:
                json.dump(cat_products, f, ensure_ascii=False)
            
            filenames[f"products_{category}_file"] = fname
            count = len(cat_products)
            stats[category] = count
            print(f"OPIN: {count} produtos de {category} salvos.")
        else:
            print(f"OPIN: Nenhum produto de {category} encontrado (Verificar Logs).")

    # Retorna objeto compatível com build_insurers.py
    return OpinProductMeta(
        as_of=datetime.now().strftime("%Y-%m-%d"),
        products_auto_file=filenames.get("products_auto_file", ""),
        products_life_file=filenames.get("products_life_file", ""),
        products_home_file=filenames.get("products_home_file", ""),
        stats=stats,
        data=master_data 
    )
