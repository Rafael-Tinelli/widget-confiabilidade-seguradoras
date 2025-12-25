from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

DEFAULT_PARTICIPANTS_URL = "https://data.directory.opinbrasil.com.br/participants"
USER_AGENT = "widget-confiabilidade-seguradoras/0.1 (+https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras)"

ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_SNAPSHOTS = ROOT / "data" / "snapshots"
API_V1 = ROOT / "api" / "v1"

# Saídas
API_PARTICIPANTS = API_V1 / "participants.json"              # SLIM (público)
FULL_RAW = DATA_RAW / "opin_participants_full.json.gz"       # FULL (auditoria)
# FULL snapshot diário: data/snapshots/opin_participants_full_YYYY-MM-DD.json.gz


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _pick(d: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _as_strings(values: Iterable[Any]) -> List[str]:
    return [str(v) for v in values if v not in (None, "")]


def _extract_participants(payload: Any) -> List[Dict[str, Any]]:
    """
    Tenta suportar variações comuns:
    - lista direta de participantes
    - dicionário com chaves 'data', 'participants', etc.
    """
    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]

    if isinstance(payload, dict):
        for key in ("participants", "data", "result", "items"):
            v = payload.get(key)
            if isinstance(v, list):
                return [p for p in v if isinstance(p, dict)]

    return []


def _normalize_authorization_servers(auth_servers: List[Any]) -> List[Dict[str, Any]]:
    """
    Produz uma versão SLIM dos Authorization Servers.
    Mantém:
      - id, issuer, openid, status
      - apiResourcesCount (quantidade de ApiResources)
      - apiFamiliesCount e apiFamilies (derivados de ApiFamilyType / ApiFamily / etc)
    """
    normalized: List[Dict[str, Any]] = []

    for raw_as in auth_servers:
        if not isinstance(raw_as, dict):
            continue

        as_id = _pick(raw_as, ["AuthorisationServerId", "AuthorizationServerId", "Id", "id"])
        issuer = _pick(raw_as, ["Issuer", "issuer"])
        openid = _pick(
            raw_as,
            ["OpenIDDiscoveryDocument", "openIDDiscoveryDocument", "openidConfigurationUri", "openId"],
        )
        status = _pick(raw_as, ["Status", "status"])

        api_resources = _as_list(raw_as.get("ApiResources") or raw_as.get("apiResources"))
        api_resources_count = len([r for r in api_resources if isinstance(r, dict)])

        # “Famílias” podem vir como string única ou lista; e em chaves diferentes.
        api_families = {
            str(fam)
            for res in api_resources
            if isinstance(res, dict)
            for fam in _as_list(
                _pick(
                    res,
                    ["ApiFamilyType", "ApiFamily", "FamilyType", "ApiFamilyTypes"],
                )
            )
            if fam not in (None, "")
        }

        as_payload: Dict[str, Any] = {
            "id": str(as_id) if as_id is not None else None,
            "issuer": str(issuer) if issuer is not None else None,
            "openid": str(openid) if openid is not None else None,
            "status": str(status) if status is not None else None,
            "apiResourcesCount": api_resources_count,
            "apiFamiliesCount": len(api_families),
        }

        if api_families:
            as_payload["apiFamilies"] = sorted(api_families)

        normalized.append(as_payload)

    return normalized


def _normalize_participant(p: Dict[str, Any]) -> Dict[str, Any]:
    # Campos mais comuns (tolerante a variações de nome)
    pid = _pick(p, ["OrganisationId", "organizationId", "organisationId", "organisation_id", "id"])
    name = _pick(p, ["OrganisationName", "organizationName", "organisationName", "legalName", "name"])
    reg = _pick(p, ["RegistrationNumber", "registrationNumber", "registration_number", "cnpj", "CNPJ"])
    status = _pick(p, ["Status", "status"])
    roles = _pick(p, ["Roles", "roles"])

    auth_servers = _pick(
        p,
        ["AuthorisationServers", "AuthorizationServers", "authorisationServers", "authorizationServers"],
    )

    return {
        "id": str(pid) if pid is not None else None,
        "name": str(name) if name is not None else None,
        "registrationNumber": str(reg) if reg is not None else None,
        "status": str(status) if status is not None else None,
        "roles": _as_strings(_as_list(roles)),
        "authorizationServers": _normalize_authorization_servers(_as_list(auth_servers)),
    }


def fetch_payload(url: str) -> Any:
    r = requests.get(
        url,
        timeout=30,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        },
    )
    r.raise_for_status()
    return r.json()


def build_slim(payload: Any, url: str, fetched_at: str) -> Dict[str, Any]:
    participants = _extract_participants(payload)
    normalized = [_normalize_participant(p) for p in participants]

    return {
        "source": {"url": url, "fetchedAt": fetched_at},
        "participants": normalized,
        "meta": {"count": len(normalized)},
    }


def write_outputs(url: str) -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    API_V1.mkdir(parents=True, exist_ok=True)

    fetched_at = _now_iso()
    payload = fetch_payload(url)

    slim = build_slim(payload, url, fetched_at)
    full_payload = {
        "source": {"url": url, "fetchedAt": fetched_at},
        "payload": payload,
    }

    # FULL raw compactado (auditoria)
    with gzip.open(FULL_RAW, "wt", encoding="utf-8") as f:
        f.write(json.dumps(full_payload, ensure_ascii=False, sort_keys=True))

    # FULL snapshot diário compactado
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap_path = DATA_SNAPSHOTS / f"opin_participants_full_{day}.json.gz"
    with gzip.open(snap_path, "wt", encoding="utf-8") as f:
        f.write(json.dumps(full_payload, ensure_ascii=False, sort_keys=True))

    # SLIM público (minificado)
    API_PARTICIPANTS.write_text(
        json.dumps(slim, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


if __name__ == "__main__":
    url = os.getenv("OPIN_PARTICIPANTS_URL", DEFAULT_PARTICIPANTS_URL)
    write_outputs(url)
    print("OK: generated api/v1/participants.json")
