# api/sources/opin_products.py
from __future__ import annotations

import gzip
import json
import concurrent.futures
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# Reutilizamos curl_cffi para consistência e bypass de WAFs eventuais
from curl_cffi import requests as cffi_requests

# Configurações
MAX_WORKERS = 10  # Número de downloads simultâneos
TIMEOUT_SECONDS = 15

# Endpoints definidos no estudo "Inteligência de Mercado Segurador"
TARGET_ENDPOINTS = {
    "auto": "/open-insurance/products-services/v1/auto-insurance",
    "life": "/open-insurance/products-services/v1/life-pension",
    "home": "/open-insurance/products-services/v1/home-insurance",
}

# Headers padrão para Open Insurance
OPIN_HEADERS = {
    "User-Agent": "MarketIntelligenceBot/1.0 (Open Source Research)",
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


def _load_participants(json_path: Path) -> list[dict]:
    """Carrega o diretório de participantes gerado na etapa anterior."""
    if not json_path.exists():
        raise RuntimeError(f"Arquivo de participantes não encontrado: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        if isinstance(data, list):
            return data
        return data.get("participants", []) or []


def _extract_api_base_url(participant: dict) -> str | None:
    """
    Tenta descobrir a URL base do servidor de Produtos e Serviços.
    """
    # Estratégia 1: Procurar em ApiResources
    api_resources = participant.get("ApiResources", [])
    for resource in api_resources:
        base_uri = resource.get("ApiDiscoveryUri") or resource.get("ApiFamilyType")
        if base_uri and str(base_uri).startswith("http"):
            parts = str(base_uri).split("/open-insurance/")
            if len(parts) > 0:
                return parts[0]

    # Estratégia 2: AuthorisationServers
    auth_servers = participant.get("AuthorisationServers", [])
    for server in auth_servers:
        config_url = server.get("OpenIDDiscoveryDocument")
        if config_url and str(config_url).startswith("http"):
            parts = str(config_url).split("/.well-known")
            return parts[0]

    return None


def _fetch_single_product(
    participant_name: str, base_url: str, category: str, endpoint: str
) -> list[dict]:
    """Baixa os produtos de uma única seguradora."""
    full_url = f"{base_url.rstrip('/')}{endpoint}"
    results = []

    try:
        resp = cffi_requests.get(
            full_url, headers=OPIN_HEADERS, timeout=TIMEOUT_SECONDS, verify=False
        )

        if resp.status_code == 200:
            data = resp.json()
            payload = data.get("data", {})
            brand = payload.get("brand", {})
            companies = brand.get("companies", [])

            for company in companies:
                products = company.get("products", [])
                for prod in products:
                    prod["_source_participant"] = participant_name
                    prod["_source_url"] = full_url
                    results.append(prod)
    except Exception:
        # Falhas de conexão pontuais são ignoradas
        pass

    return results


def extract_open_insurance_products() -> OpinProductMeta:
    """
    Orquestra o download paralelo dos produtos de Auto, Vida e Residencial.
    Salva em JSONs comprimidos (GZIP).
    """
    # Caminhos
    root = Path(__file__).resolve().parents[2]
    participants_path = root / "api" / "v1" / "participants.json"
    raw_dir = root / "data" / "raw" / "opin"
    raw_dir.mkdir(parents=True, exist_ok=True)

    print("OPIN: Carregando participantes...")
    participants = _load_participants(participants_path)
    
    # Prepara lista de tarefas (Tuplas: Name, Url)
    targets = []
    for p in participants:
        name = p.get("RegisteredName") or p.get("OrganisationName") or "Unknown"
        url = _extract_api_base_url(p)
        if url:
            targets.append((name, url))

    print(f"OPIN: {len(targets)} seguradoras com URLs de API identificadas.")

    stats = {"auto": 0, "life": 0, "home": 0}
    filenames = {}

    # Executa download por categoria
    for category, endpoint in TARGET_ENDPOINTS.items():
        print(f"OPIN: Baixando produtos de {category.upper()}...")
        all_products = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_target = {
                executor.submit(
                    _fetch_single_product, name, url, category, endpoint
                ): name
                for name, url in targets
            }

            for future in concurrent.futures.as_completed(future_to_target):
                try:
                    prods = future.result()
                    all_products.extend(prods)
                except Exception:
                    continue

        # Salva consolidado comprimido
        filename = f"products_{category}.json.gz"
        filepath = raw_dir / filename

        with gzip.open(filepath, "wt", encoding="utf-8") as f:
            json.dump(all_products, f, ensure_ascii=False)

        print(f"OPIN: {len(all_products)} produtos de {category} salvos.")
        stats[category] = len(all_products)
        filenames[f"products_{category}_file"] = filename

    return OpinProductMeta(
        as_of=datetime.now().strftime("%Y-%m-%d"),
        products_auto_file=filenames.get("products_auto_file", ""),
        products_life_file=filenames.get("products_life_file", ""),
        products_home_file=filenames.get("products_home_file", ""),
        stats=stats,
    )
