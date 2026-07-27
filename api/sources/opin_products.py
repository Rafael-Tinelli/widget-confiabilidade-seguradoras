"""Collect public Open Insurance product endpoints by participant CNPJ."""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from api.matching.consumidor_gov_match import normalize_cnpj

DEFAULT_PARTICIPANTS_URL = os.getenv(
    "OPIN_PARTICIPANTS_URL",
    "https://data.directory.opinbrasil.com.br/participants",
)

CACHE_DIR = Path("data/raw/opin")
CACHE_PARTICIPANTS_FILE = CACHE_DIR / "participants.json"
PARTICIPANTS_FILE = Path("api/v1/participants.json")

REQUEST_TIMEOUT = float(os.getenv("OPIN_HTTP_TIMEOUT", "20"))
MAX_TOTAL_REQUESTS = int(os.getenv("OPIN_MAX_REQUESTS", "20000"))
CACHE_MAX_AGE_HOURS = int(
    os.getenv("OPIN_PARTICIPANTS_CACHE_MAX_AGE_HOURS", "48")
)

INTERESTING_RESOURCES = {
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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ci_get(obj: Any, *keys: str, default: Any = None) -> Any:
    if not isinstance(obj, dict):
        return default
    lower_map = {str(key).casefold(): key for key in obj}
    for key in keys:
        real_key = lower_map.get(str(key).casefold())
        if real_key is not None:
            return obj.get(real_key)
    return default


def _build_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=20,
        pool_maxsize=20,
    )
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "widget-confiabilidade-seguradoras/1.0",
            "Accept": "application/json",
        }
    )
    return session


def _is_cache_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    age_seconds = time.time() - path.stat().st_mtime
    return age_seconds <= CACHE_MAX_AGE_HOURS * 3600


def _extract_participant_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    for key in ("data", "participants", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _read_participants_file(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return []
    return _extract_participant_list(payload)


def _write_participants_cache(participants: list[dict[str, Any]]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    temporary = CACHE_PARTICIPANTS_FILE.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(participants, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(CACHE_PARTICIPANTS_FILE)


def _load_participants() -> list[dict[str, Any]]:
    candidates = (
        (CACHE_PARTICIPANTS_FILE, _is_cache_fresh(CACHE_PARTICIPANTS_FILE)),
        (PARTICIPANTS_FILE, PARTICIPANTS_FILE.exists()),
    )
    for path, eligible in candidates:
        if not eligible:
            continue
        participants = _read_participants_file(path)
        if participants:
            return participants

    session = _build_session()
    response = session.get(DEFAULT_PARTICIPANTS_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    participants = _extract_participant_list(response.json())
    if not participants:
        return []

    try:
        _write_participants_cache(participants)
    except OSError as exc:
        print(f"WARN: não foi possível gravar cache OPIN: {exc}")
    return participants


def _extract_products_services_endpoints(
    participant: dict[str, Any],
) -> list[tuple[str, str]]:
    endpoints: list[tuple[str, str]] = []
    authorization_servers = _ci_get(
        participant,
        "AuthorisationServers",
        "authorisationServers",
        default=[],
    )
    if not isinstance(authorization_servers, list):
        return endpoints

    for server in authorization_servers:
        resources = _ci_get(server, "ApiResources", "apiResources", default=[])
        if not isinstance(resources, list):
            continue

        for resource in resources:
            family = _ci_get(resource, "ApiFamilyType", "apiFamilyType")
            if str(family or "").strip().casefold() != "products-services":
                continue

            version = str(
                _ci_get(
                    resource,
                    "ApiVersion",
                    "apiVersion",
                    default="1.0.0",
                )
            ).strip()
            discovery = _ci_get(
                resource,
                "ApiDiscoveryEndpoints",
                "apiDiscoveryEndpoints",
                default=[],
            )

            if isinstance(discovery, list) and discovery:
                for item in discovery:
                    endpoint = _ci_get(item, "ApiEndpoint", "apiEndpoint")
                    if isinstance(endpoint, str) and endpoint.strip():
                        endpoints.append((endpoint.strip().rstrip("/"), version))
                continue

            api_base = _ci_get(resource, "ApiBaseUrl", "apiBaseUrl")
            if not api_base:
                api_base = _ci_get(participant, "ApiBaseUrl", "apiBaseUrl")
            if isinstance(api_base, str) and api_base.strip():
                endpoints.append((api_base.strip().rstrip("/"), version))

    return endpoints


def _build_products_url(
    api_endpoint: str,
    version: str,
    resource_code: str,
) -> str:
    base = api_endpoint.rstrip("/")
    if re.search(r"/v?\d+\.\d+\.\d+/?$", base):
        return f"{base}/{resource_code}"
    if "/products-services" in base:
        return f"{base}/{version}/{resource_code}"
    if "/open-insurance" in base:
        return f"{base}/products-services/{version}/{resource_code}"
    return (
        f"{base}/open-insurance/products-services/{version}/{resource_code}"
    )


def _parse_products_payload(
    payload: Any,
    resource_code: str,
) -> list[dict[str, str]]:
    if not isinstance(payload, dict):
        return []
    brands = payload.get("brand") or payload.get("brands") or []
    if not isinstance(brands, list):
        return []

    output: list[dict[str, str]] = []
    for brand in brands:
        if not isinstance(brand, dict):
            continue
        companies = brand.get("companies") or []
        if not isinstance(companies, list):
            continue
        for company in companies:
            if not isinstance(company, dict):
                continue
            products = company.get("products") or []
            if not isinstance(products, list):
                continue
            for product in products:
                if not isinstance(product, dict):
                    continue
                name = (
                    product.get("name")
                    or product.get("productName")
                    or product.get("nome")
                )
                code = (
                    product.get("code")
                    or product.get("productCode")
                    or resource_code
                )
                output.append(
                    {
                        "type": INTERESTING_RESOURCES.get(
                            resource_code,
                            resource_code,
                        ),
                        "name": str(name or code),
                        "code": str(code),
                    }
                )
    return output


def _participant_cnpj(participant: dict[str, Any]) -> str:
    registration = _ci_get(
        participant,
        "RegistrationNumber",
        "registrationNumber",
        "cnpj",
    )
    if not registration:
        legal_entity = _ci_get(
            participant,
            "LegalEntity",
            "legalEntity",
        )
        registration = _ci_get(
            legal_entity,
            "RegistrationNumber",
            "registrationNumber",
            "cnpj",
        )
    return normalize_cnpj(registration)


def extract_open_insurance_products() -> tuple[
    dict[str, Any],
    dict[str, list[dict[str, str]]],
]:
    meta: dict[str, Any] = {
        "source": "open_insurance_apis",
        "generatedAt": _utc_now(),
        "status": "partial",
        "endpoints_scanned": 0,
        "requests": 0,
    }

    try:
        participants = _load_participants()
    except (requests.RequestException, ValueError) as exc:
        meta["status"] = "participants_load_failed"
        meta["error"] = str(exc)
        return meta, {}

    products_by_cnpj: dict[str, list[dict[str, str]]] = {}
    endpoint_jobs: list[tuple[str, str, str]] = []

    for participant in participants:
        status = _ci_get(participant, "Status", "status")
        if status and str(status).casefold() != "active":
            continue

        cnpj = _participant_cnpj(participant)
        if not cnpj:
            continue

        products_by_cnpj.setdefault(cnpj, [])
        for endpoint, version in _extract_products_services_endpoints(participant):
            endpoint_jobs.append((cnpj, endpoint, version))

    meta["participants"] = len(participants)
    meta["endpoints_scanned"] = len(endpoint_jobs)
    if not endpoint_jobs:
        meta["status"] = "no_endpoints"
        return meta, products_by_cnpj

    session = _build_session()
    seen: dict[str, set[tuple[str, str]]] = {
        cnpj: set() for cnpj in products_by_cnpj
    }
    request_count = 0

    for cnpj, endpoint, version in endpoint_jobs:
        for resource_code in INTERESTING_RESOURCES:
            if request_count >= MAX_TOTAL_REQUESTS:
                meta["status"] = "limit_reached"
                meta["requests"] = request_count
                return meta, products_by_cnpj

            request_count += 1
            url = _build_products_url(endpoint, version, resource_code)
            try:
                response = session.get(url, timeout=REQUEST_TIMEOUT)
                if response.status_code >= 400:
                    continue
                items = _parse_products_payload(response.json(), resource_code)
            except (requests.RequestException, ValueError):
                continue

            for item in items:
                key = (item["code"], item["name"])
                if key in seen[cnpj]:
                    continue
                seen[cnpj].add(key)
                products_by_cnpj[cnpj].append(item)

    meta["status"] = "completed"
    meta["requests"] = request_count
    return meta, products_by_cnpj
