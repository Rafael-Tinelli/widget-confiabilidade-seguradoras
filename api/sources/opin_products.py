# api/sources/opin_products.py
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from api.matching.consumidor_gov_match import normalize_cnpj

DEFAULT_PARTICIPANTS_URL = os.getenv(
    "OPIN_PARTICIPANTS_URL",
    "https://data.directory.opinbrasil.com.br/participants",
)

CACHE_DIR = Path("data/raw/opin")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_PARTICIPANTS_FILE = CACHE_DIR / "participants.json"
PARTICIPANTS_FILE = Path("api/v1/participants.json")

REQUEST_TIMEOUT = float(os.getenv("OPIN_HTTP_TIMEOUT", "20"))
MAX_TOTAL_REQUESTS = int(os.getenv("OPIN_MAX_REQUESTS", "20000"))
CACHE_MAX_AGE_HOURS = int(os.getenv("OPIN_PARTICIPANTS_CACHE_MAX_AGE_HOURS", "48"))

INTERESTING_RESOURCES: Dict[str, str] = {
    "auto-insurance": "Auto",
    "home-insurance": "Residencial",
    "business-insurance": "Empresarial",
    "life-pension": "Vida & Previdência",
    "travel-insurance": "Viagem",
    "rural-insurance": "Rural",
    "responsibility-insurance": "Responsabilidade Civil",
    "capitalization-title": "Capitalização",
    "other-products": "Outros",
}


def _ci_get(obj: Any, *keys: str, default: Any = None) -> Any:
    if not isinstance(obj, dict):
        return default
    lower_map = {str(k).lower(): k for k in obj.keys()}
    for k in keys:
        real = lower_map.get(str(k).lower())
        if real is not None:
            return obj.get(real)
    return default


def _build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=4, connect=4, read=4, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": "widget-confiabilidade-seguradoras/1.0", "Accept": "application/json"})
    return s


def _is_cache_fresh(path: Path) -> bool:
    import time
    if not path.exists():
        return False
    return (time.time() - os.path.getmtime(path)) <= (CACHE_MAX_AGE_HOURS * 3600)


def _load_participants() -> List[dict]:
    # Try local cache or repo snapshot first
    for p in [CACHE_PARTICIPANTS_FILE, PARTICIPANTS_FILE]:
        if p.exists() and (p == PARTICIPANTS_FILE or _is_cache_fresh(p)):
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                data = data.get("data", [])
            if isinstance(data, list):
                return data

    # Download
    session = _build_session()
    r = session.get(DEFAULT_PARTICIPANTS_URL, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    payload = r.json()
    participants = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(participants, list):
        raise ValueError("Invalid structure")

    CACHE_PARTICIPANTS_FILE.write_text(json.dumps(participants, ensure_ascii=False, indent=2), encoding="utf-8")
    return participants


def _extract_products_services_endpoints(participant: dict) -> List[Tuple[str, str]]:
    endpoints: List[Tuple[str, str]] = []
    auth_servers = _ci_get(participant, "AuthorisationServers", "authorisationServers", default=[])

    if not isinstance(auth_servers, list):
        return endpoints

    for server in auth_servers:
        resources = _ci_get(server, "ApiResources", "apiResources", default=[])
        if not isinstance(resources, list):
            continue

        for res in resources:
            family = _ci_get(res, "ApiFamilyType", "apiFamilyType")
            if not family or str(family).strip().lower() != "products-services":
                continue

            api_version = str(_ci_get(res, "ApiVersion", "apiVersion", default="1.0.0")).strip()
            discovery = _ci_get(res, "ApiDiscoveryEndpoints", "apiDiscoveryEndpoints", default=[])

            if isinstance(discovery, list) and discovery:
                for d in discovery:
                    ep = _ci_get(d, "ApiEndpoint", "apiEndpoint")
                    if isinstance(ep, str) and ep.strip():
                        endpoints.append((ep.strip().rstrip("/"), api_version))
            else:
                api_base = _ci_get(res, "ApiBaseUrl", "apiBaseUrl") or _ci_get(participant, "ApiBaseUrl")
                if isinstance(api_base, str) and api_base.strip():
                    endpoints.append((api_base.strip().rstrip("/"), api_version))

    return endpoints


def _build_products_url(api_endpoint: str, version: str, resource_code: str) -> str:
    base = api_endpoint.rstrip("/")
    if re.search(r"/v?\d+\.\d+\.\d+/?$", base):
        return f"{base}/{resource_code}"
    if "/products-services" in base:
        return f"{base}/{version}/{resource_code}"
    if "/open-insurance" in base:
        return f"{base}/products-services/{version}/{resource_code}"
    return f"{base}/open-insurance/products-services/{version}/{resource_code}"


def _parse_products_payload(payload: Any, resource_code: str) -> List[dict]:
    if not isinstance(payload, dict):
        return []
    out = []
    brands = payload.get("brand") or payload.get("brands") or []
    if not isinstance(brands, list):
        return []

    for b in brands:
        for comp in (b.get("companies") or []):
            for p in (comp.get("products") or []):
                name = p.get("name") or p.get("productName") or p.get("nome")
                code = p.get("code") or p.get("productCode") or resource_code
                out.append({"type": INTERESTING_RESOURCES.get(resource_code, resource_code), "name": str(name or code), "code": str(code)})
    return out


def extract_open_insurance_products() -> Dict[str, List[dict]]:
    participants = _load_participants()
    products_by_cnpj = {}
    endpoint_jobs = []

    for p in participants:
        status = _ci_get(p, "Status", "status")
        if status and str(status).lower() != "active":
            continue

        reg = _ci_get(p, "RegistrationNumber", "registrationNumber", "cnpj")
        if not reg:
            legal = _ci_get(p, "LegalEntity", "legalEntity")
            if isinstance(legal, dict):
                reg = _ci_get(legal, "RegistrationNumber", "cnpj")

        cnpj = normalize_cnpj(reg)
        if not cnpj:
            continue

        products_by_cnpj.setdefault(cnpj, [])
        for ep, ver in _extract_products_services_endpoints(p):
            endpoint_jobs.append((cnpj, ep, ver))

    if not endpoint_jobs:
        return products_by_cnpj

    session = _build_session()
    seen = {k: set() for k in products_by_cnpj}
    req_count = 0

    for cnpj, ep, ver in endpoint_jobs:
        for res_code in INTERESTING_RESOURCES:
            if req_count >= MAX_TOTAL_REQUESTS:
                return products_by_cnpj
            req_count += 1

            try:
                url = _build_products_url(ep, ver, res_code)
                r = session.get(url, timeout=REQUEST_TIMEOUT)
                if r.status_code < 400:
                    items = _parse_products_payload(r.json(), res_code)
                    for it in items:
                        k = (it["code"], it["name"])
                        if k not in seen[cnpj]:
                            seen[cnpj].add(k)
                            products_by_cnpj[cnpj].append(it)
            except Exception:
                continue

    return products_by_cnpj
