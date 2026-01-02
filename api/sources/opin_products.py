# api/sources/opin_products.py
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Any
import requests

# Configuração
CACHE_DIR = Path("data/raw/opin")
PARTICIPANTS_FILE = Path("api/v1/participants.json")
OUTPUT_FILE = CACHE_DIR / "products_full.json"
TIMEOUT = 10  # segundos

# Mapeamento de interesse (Resource Name -> Tipo Amigável)
# Apenas produtos que queremos mostrar no widget
INTERESTING_RESOURCES = {
    "auto-insurance": "Auto",
    "home-insurance": "Residencial",
    "life-pension": "Vida/Prev",
    "condominium-insurance": "Condomínio",
    "rural-insurance": "Rural",
    "business-insurance": "Empresarial"
}

# Headers padrão para não ser bloqueado por WAFs
HEADERS = {
    "User-Agent": "WidgetSeguradoras/1.0 (Open Source Data Project)",
    "Accept": "application/json"
}

def load_participants() -> List[Dict[str, Any]]:
    """Carrega o arquivo de participantes gerado pelo passo anterior."""
    if not PARTICIPANTS_FILE.exists():
        logging.error(f"Arquivo de participantes não encontrado: {PARTICIPANTS_FILE}")
        return []
    
    try:
        with open(PARTICIPANTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Erro ao ler participantes: {e}")
        return []

def extract_api_endpoints(participant: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Descobre as URLs exatas dos produtos lendo o manifesto da seguradora.
    Retorna lista de {tipo, url, cnpj}.
    """
    endpoints = []
    
    # Tenta encontrar o CNPJ principal
    org_profile = participant.get("OrganisationProfile", {})
    cnpj = "N/A"
    # Tenta pegar do LegalEntity (padrão OPIN)
    if "LegalEntity" in org_profile:
         cnpj = org_profile["LegalEntity"].get("RegistrationNumber", "N/A")
    # Fallback: tenta pegar de chaves antigas se houver
    if cnpj == "N/A":
        cnpj = participant.get("OrganisationId", "N/A")

    servers = participant.get("AuthorisationServers", [])
    
    for server in servers:
        base_url = server.get("ApiBaseUrl", "").rstrip("/")
        if not base_url:
            continue

        resources = server.get("ApiResources", [])
        for resource in resources:
            family = resource.get("ApiFamilyType")
            
            # Só nos interessa a família "products-services"
            if family != "products-services":
                continue
            
            # Verifica se o recurso específico (ex: auto-insurance) está na lista de recursos da API
            # A estrutura do JSON de participantes pode variar, mas geralmente ApiResources lista os endpoints disponíveis.
            # Às vezes, os recursos específicos estão dentro de "ApiResource" ou a lista é genérica.
            # Vamos assumir que precisamos iterar sobre os recursos de interesse e checar se a seguradora suporta.
            
            # Na V1 do diretório, ApiResources tem uma lista de "ApiResource"
            api_resources_list = resource.get("ApiResource", [])
            
            # Se for string, converte para lista (alguns diretórios antigos bugavam)
            if isinstance(api_resources_list, str):
                api_resources_list = [api_resources_list]

            version = resource.get("ApiVersion", "1.0.0")
            
            for res_code in api_resources_list:
                if res_code in INTERESTING_RESOURCES:
                    # CONSTRUÇÃO DA URL (Discovery Pattern)
                    # Padrão: BaseUrl + /open-insurance/products-services/ + Version + / + Resource
                    # Nota: Algumas BaseUrls já trazem o /open-insurance, mas o padrão diz que não deveriam.
                    # Vamos tratar duplicações de barra.
                    
                    path = f"/open-insurance/products-services/{version}/{res_code}"
                    
                    # Remove duplicidade se a BaseUrl já tiver parte do path
                    if "/open-insurance" in base_url:
                         # Caso estranho, mas acontece: ajusta para não duplicar
                         full_url = f"{base_url}/products-services/{version}/{res_code}"
                    else:
                        full_url = f"{base_url}{path}"
                    
                    # Limpeza final de barras duplas (exceto http://)
                    full_url = full_url.replace("///", "/").replace("//open-insurance", "/open-insurance")
                    
                    endpoints.append({
                        "type": INTERESTING_RESOURCES[res_code],
                        "url": full_url,
                        "cnpj": cnpj,
                        "name": participant.get("OrganisationName", "Unknown")
                    })
    
    return endpoints

def fetch_products():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    print("OPIN: Carregando diretório de participantes...")
    participants = load_participants()
    print(f"OPIN: {len(participants)} participantes carregados.")

    # 1. Fase de Discovery (Mapear URLs)
    all_endpoints = []
    for p in participants:
        # Só processa se status for Active
        if p.get("Status") == "Active":
            all_endpoints.extend(extract_api_endpoints(p))
    
    print(f"OPIN: Discovery completo. {len(all_endpoints)} endpoints de produtos encontrados.")
    
    # 2. Fase de Crawling (Baixar Dados)
    products_db = {} # Chave: CNPJ, Valor: Lista de Produtos
    
    success_count = 0
    error_count = 0

    # Agrupa endpoints por tipo para log mais bonito (opcional), ou itera direto
    for i, ep in enumerate(all_endpoints):
        cnpj = ep["cnpj"]
        url = ep["url"]
        p_type = ep["type"]
        
        # Limita logs
        if i % 10 == 0:
            print(f"OPIN: Progresso {i}/{len(all_endpoints)} - Baixando {p_type} de {ep['name']}...")

        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, verify=False) # Verify=False pois muitos certificados gov falham
            
            if resp.status_code == 200:
                data = resp.json()
                # A resposta padrão tem { "data": { "brand": ... } }
                # Vamos simplificar e guardar a estrutura bruta ou levemente limpa
                
                payload = data.get("data", {})
                brand_list = payload.get("brand", [])
                
                count_prods = 0
                
                for brand in brand_list:
                    companies = brand.get("companies", [])
                    for comp in companies:
                        prods = comp.get("products", [])
                        for prod in prods:
                            # Salva o produto vinculado ao CNPJ
                            if cnpj not in products_db:
                                products_db[cnpj] = []
                            
                            products_db[cnpj].append({
                                "type": p_type,
                                "name": prod.get("name", "Produto sem nome"),
                                "code": prod.get("code", "")
                            })
                            count_prods += 1
                
                if count_prods > 0:
                    success_count += 1
            else:
                # 404 ou 500 é normal em ambiente instável
                # print(f"Erro {resp.status_code} em {url}")
                error_count += 1
                
        except Exception:
            # print(f"Exception em {url}: {e}")
            error_count += 1

    print(f"OPIN: Download concluído. Sucessos: {success_count}, Erros: {error_count}.")
    
    # Salva o "Banco de Dados" de produtos
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(products_db, f, ensure_ascii=False, indent=2)
    
    print(f"OPIN: Base de produtos salva em {OUTPUT_FILE} com {len(products_db)} seguradoras listadas.")

if __name__ == "__main__":
    # Desabilita warnings de SSL inseguro (comum no Open Insurance BR)
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    fetch_products()
