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
# OUTPUT_FILE = CACHE_DIR / "products_full.json" # Não usado diretamente na função importada
TIMEOUT = 10  # segundos

# Mapeamento de interesse (Resource Name -> Tipo Amigável)
INTERESTING_RESOURCES = {
    "auto-insurance": "Auto",
    "home-insurance": "Residencial",
    "life-pension": "Vida/Prev",
    "condominium-insurance": "Condomínio",
    "rural-insurance": "Rural",
    "business-insurance": "Empresarial"
}

# Headers padrão
HEADERS = {
    "User-Agent": "WidgetSeguradoras/1.0 (Open Source Data Project)",
    "Accept": "application/json"
}

def load_participants() -> List[Dict[str, Any]]:
    """Carrega a lista de participantes, tratando estruturas de envelope (dict) ou lista direta."""
    if not PARTICIPANTS_FILE.exists():
        logging.error(f"Arquivo de participantes não encontrado: {PARTICIPANTS_FILE}")
        return []
    try:
        with open(PARTICIPANTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            
            # Caso 1: O JSON é um dicionário com uma chave 'data' (Padrão API OPIN)
            if isinstance(data, dict):
                if "data" in data and isinstance(data["data"], list):
                    return data["data"]
                # Se for dict mas não tiver 'data', talvez seja uma estrutura diferente, retorna vazio ou tenta achar lista
                return []
            
            # Caso 2: O JSON já é a lista direta
            if isinstance(data, list):
                return data
                
            return []
    except Exception:
        return []

def extract_api_endpoints(participant: Dict[str, Any]) -> List[Dict[str, str]]:
    endpoints = []
    
    org_profile = participant.get("OrganisationProfile", {})
    cnpj = "N/A"
    if "LegalEntity" in org_profile:
         cnpj = org_profile["LegalEntity"].get("RegistrationNumber", "N/A")
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
            if family != "products-services":
                continue
            
            api_resources_list = resource.get("ApiResource", [])
            if isinstance(api_resources_list, str):
                api_resources_list = [api_resources_list]

            version = resource.get("ApiVersion", "1.0.0")
            
            for res_code in api_resources_list:
                if res_code in INTERESTING_RESOURCES:
                    path = f"/open-insurance/products-services/{version}/{res_code}"
                    
                    # Evita duplicação de path se a BaseURL já incluir
                    if "/open-insurance" in base_url:
                         full_url = f"{base_url}/products-services/{version}/{res_code}"
                    else:
                        full_url = f"{base_url}{path}"
                    
                    full_url = full_url.replace("///", "/").replace("//open-insurance", "/open-insurance")
                    
                    endpoints.append({
                        "type": INTERESTING_RESOURCES[res_code],
                        "url": full_url,
                        "cnpj": cnpj,
                        "name": participant.get("OrganisationName", "Unknown")
                    })
    return endpoints

def extract_open_insurance_products():
    """
    Função principal chamada pelo build_insurers.py.
    Retorna um dicionário {CNPJ: [Lista de Produtos]}.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    print("OPIN: Carregando diretório de participantes...")
    participants = load_participants()
    print(f"OPIN: {len(participants)} participantes carregados.")

    # Discovery
    all_endpoints = []
    for p in participants:
        # Garante que p é um dicionário antes de acessar .get
        if isinstance(p, dict) and p.get("Status") == "Active":
            all_endpoints.extend(extract_api_endpoints(p))
    
    print(f"OPIN: Discovery completo. {len(all_endpoints)} endpoints encontrados.")
    
    products_db = {}
    success_count = 0
    error_count = 0

    # Crawling
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    for i, ep in enumerate(all_endpoints):
        cnpj = ep["cnpj"]
        url = ep["url"]
        p_type = ep["type"]
        
        if i % 10 == 0:
            print(f"OPIN: Progresso {i}/{len(all_endpoints)}...")

        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, verify=False)
            
            if resp.status_code == 200:
                data = resp.json()
                payload = data.get("data", {})
                brand_list = payload.get("brand", [])
                
                count_prods = 0
                for brand in brand_list:
                    companies = brand.get("companies", [])
                    for comp in companies:
                        prods = comp.get("products", [])
                        for prod in prods:
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
                error_count += 1
                
        except Exception:
            error_count += 1

    print(f"OPIN: Download concluído. Sucessos: {success_count}, Erros: {error_count}.")
    return products_db

if __name__ == "__main__":
    extract_open_insurance_products()
