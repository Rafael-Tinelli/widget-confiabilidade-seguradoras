# api/sources/opin_products.py
from __future__ import annotations

import gzip
import json
import re
import concurrent.futures
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# Tenta usar curl_cffi para evitar bloqueios de WAF (Cloudflare/Akamai)
try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    import requests as cffi_requests

# --- Configurações ---
MAX_WORKERS = 10
TIMEOUT_SECONDS = 20
# URL oficial do diretório (garante dados completos com ApiResources)
OPIN_DIRECTORY_URL = "https://data.directory.opinbrasil.com.br/participants"

# Endpoints diretos
TARGET_ENDPOINTS = {
    "auto": "/open-insurance/products-services/v1/auto-insurance",
    "life": "/open-insurance/products-services/v1/life-pension",
    "home": "/open-insurance/products-services/v1/home-insurance",
}

OPIN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

@dataclass
class OpinProductMeta:
    source: str = "Open Insurance Brasil"
    as_of: str = ""
    products_auto_file: str = ""
    products_life_file: str = ""
    products_home_file: str = ""
    stats: dict[str, int] | None = None
    data: dict[str, list] = field(default_factory=dict) 

def _fetch_participants_full() -> list[dict]:
    """
    Baixa a lista COMPLETA de participantes direto da fonte oficial.
    Isso corrige o erro de ler o arquivo 'SLIM' local que não tem URLs.
    """
    print(f"OPIN: Baixando participantes de {OPIN_DIRECTORY_URL}...")
    try:
        # verify=False pois o certificado do diretório as vezes falha em containers
        resp = cffi_requests.get(OPIN_DIRECTORY_URL, timeout=30, verify=False)
        data = resp.json()
        
        # O endpoint pode retornar lista direta ou dict com chave
        if isinstance(data, list):
            return data
        return data.get("participants", []) or []
    except Exception as e:
        print(f"OPIN ERROR: Falha fatal ao baixar participantes: {e}")
        return []

def _extract_api_base_url(participant: dict) -> str | None:
    """Extrai a URL base (ApiDiscoveryUri) do participante."""
    # Prioridade 1: ApiResources
    for resource in participant.get("ApiResources", []):
        uri = resource.get("ApiDiscoveryUri") or resource.get("ApiFamilyType")
        if uri and isinstance(uri, str) and uri.startswith("http"):
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
    """Formata 12345678000199 para 12.345.678/0001-99."""
    nums = re.sub(r"[^0-9]", "", str(raw))
    if len(nums) != 14:
        return raw 
    return f"{nums[:2]}.{nums[2:5]}.{nums[5:8]}/{nums[8:12]}-{nums[12:]}"

def _fetch_single_target(name: str, base_url: str, category: str, endpoint: str) -> list[dict]:
    """Baixa produtos de uma seguradora específica."""
    full_url = f"{base_url.rstrip('/')}{endpoint}"
    results = []

    try:
        kwargs = {"timeout": TIMEOUT_SECONDS, "verify": False, "headers": OPIN_HEADERS}
        if hasattr(cffi_requests, "Session"): 
             kwargs["impersonate"] = "chrome"

        resp = cffi_requests.get(full_url, **kwargs)

        if resp.status_code == 200:
            payload = resp.json().get("data", {})
            brand = payload.get("brand", {})
            companies = brand.get("companies", [])

            for comp in companies:
                cnpj_raw = comp.get("identification") or comp.get("cnpjNumber")
                formatted_cnpj = _format_cnpj(cnpj_raw) if cnpj_raw else None
                
                products = comp.get("products", [])
                for p in products:
                    p["_source_cnpj"] = formatted_cnpj
                    p["_category"] = category
                    p["_source_participant"] = name
                    results.append(p)
    except Exception:
        pass

    return results

def extract_open_insurance_products() -> OpinProductMeta:
    """Função principal."""
    # Diretório para salvar cache/debug
    root = Path.cwd()
    raw_dir = root / "data" / "raw" / "opin"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # MUDANÇA: Baixa fresco da internet em vez de usar arquivo local 'slim'
    participants = _fetch_participants_full()
    
    targets = []
    for p in participants:
        name = p.get("RegisteredName", "Unknown")
        # Filtra apenas ativos para economizar tempo
        status = str(p.get("Status", "")).lower()
        if status == "active":
            url = _extract_api_base_url(p)
            if url:
                targets.append((name, url))
            
    print(f"OPIN: {len(targets)} seguradoras ativas com URLs identificadas.")

    stats = {"auto": 0, "life": 0, "home": 0}
    filenames = {}
    master_data: dict[str, list] = {}

    for category, endpoint in TARGET_ENDPOINTS.items():
        print(f"OPIN: Baixando {category}...")
        cat_products = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(_fetch_single_target, name, url, category, endpoint)
                for name, url in targets
            ]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    prods = future.result()
                    cat_products.extend(prods)
                    
                    for p in prods:
                        cnpj = p.get("_source_cnpj")
                        if cnpj:
                            if cnpj not in master_data:
                                master_data[cnpj] = []
                            master_data[cnpj].append(p)
                except Exception:
                    continue

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
            print(f"OPIN: Nenhum produto de {category} encontrado.")

    return OpinProductMeta(
        as_of=datetime.now().strftime("%Y-%m-%d"),
        products_auto_file=filenames.get("products_auto_file", ""),
        products_life_file=filenames.get("products_life_file", ""),
        products_home_file=filenames.get("products_home_file", ""),
        stats=stats,
        data=master_data 
    )
