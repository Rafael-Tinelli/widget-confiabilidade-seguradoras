from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

DEFAULT_PARTICIPANTS_URL = "https://data.directory.opinbrasil.com.br/participants"
USER_AGENT = "widget-confiabilidade-seguradoras/0.1 (+https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras)"

# Raiz do repositório = pasta que contém README.md (build_json.py fica em /api)
ROOT = Path(__file__).resolve().parents[1]

DATA_RAW = ROOT / "data" / "raw"
DATA_SNAPSHOTS = ROOT / "data" / "snapshots"
API_V1 = ROOT / "api" / "v1"

API_PARTICIPANTS = API_V1 / "participants.json"
FULL_RAW_GZ = DATA_RAW / "opin_participants_full.json.gz"


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
    out: List[str] = []
    for v in values:
        if v in (None, ""):
            continue
        out.append(str(v))
    return out


def _extract_participants(payload: Any) -> List[Dict[str, Any]]:
    """
    Suporta variações comuns:
    - lista direta de participantes
    - dicionário com chaves 'participants', 'data', 'result', 'items'
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
    Normaliza o mínimo necessário, evitando carregar estruturas enormes (ApiResources completo).
    Mantém:
      - id, issuer, openid, status
      - apiResourcesCount (qtde de resources)
      - apiFamiliesCount e apiFamilies (derivado dos resources, sem endpoints)
    """
    normalized: List[Dict[str, Any]] = []

    for raw_as in auth_servers:
        if not isinstance(raw_as, dict):
            continue

        as_id = _pick(raw_as, ["AuthorisationServerId", "AuthorizationServerId", "Id", "id"])
        issuer = _pick(raw_as, ["Issuer", "issuer"])
        openid = _pick(
            raw_as,
            [
                "OpenIDDiscoveryDocument",
                "openIDDiscoveryDocument",
                "openidConfigurationUri",
                "openId",
                "openid",
            ],
        )
        status = _pick(raw_as, ["Status", "status"])

        api_resources = _as_list(raw_as.get("ApiResources") or raw_as.get("apiResources"))
        api_resources_count = len([r for r in api_resources if isinstance(r, dict)])

        api_families_set = set()
        for res in api_resources:
            if not isinstance(res, dict):
                continue
            fam_value = _pick(
                res,
                ["ApiFamilyType", "ApiFamily", "FamilyType", "ApiFamilyTypes", "apiFamilyType", "apiFamily"],
            )
            for fam in _as_list(fam_value):
                if fam not in (None, ""):
                    api_families_set.add(str(fam))

        as_payload: Dict[str, Any] = {
            "id": str(as_id) if as_id is not None else None,
            "issuer": str(issuer) if issuer is not None else None,
            "openid": str(openid) if openid is not None else None,
            "status": str(status) if status is not None else None,
            "apiResourcesCount": api_resources_count,
            "apiFamiliesCount": len(api_families_set),
        }

        if api_families_set:
            as_payload["apiFamilies"] = sorted(api_families_set)

        normalized.append(as_payload)

    return normalized


def _normalize_participant(p: Dict[str, Any]) -> Dict[str, Any]:
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


def _load_json_from_path(path: Path) -> Any:
    obj = json.loads(path.read_text(encoding="utf-8"))
    # Se for um FULL já empacotado em JSON (não gz) com {"payload": ...}, extrai o payload bruto
    if isinstance(obj, dict) and "payload" in obj:
        return obj["payload"]
    return obj


def _resolve_payload(source: str) -> Tuple[Any, str]:
    """
    Retorna (payload_bruto, source_url_string).

    Aceita:
      - URL https://...
      - caminho relativo/absoluto (ex: data/raw/opin_participants.json)
      - file:///.../arquivo.json
    """
    src = (source or "").strip()
    if not src:
        src = DEFAULT_PARTICIPANTS_URL

    # file://
    if src.startswith("file://"):
        # file:///home/... -> /home/...
        local_path = Path(src.replace("file://", "", 1))
        return _load_json_from_path(local_path), src

    # caminho local direto
    p = Path(src)
    if p.exists() and p.is_file():
        return _load_json_from_path(p), src

    # caso contrário, trata como URL remota
    r = requests.get(
        src,
        timeout=30,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    r.raise_for_status()
    return r.json(), src


def build_slim(payload: Any, source_url: str, fetched_at: str) -> Dict[str, Any]:
    participants = _extract_participants(payload)
    normalized = [_normalize_participant(p) for p in participants]

    return {
        "source": {"url": source_url, "fetchedAt": fetched_at},
        "participants": normalized,
        "meta": {"count": len(normalized)},
    }


def write_outputs(source: str) -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    API_V1.mkdir(parents=True, exist_ok=True)

    fetched_at = _now_iso()
    payload, resolved_source = _resolve_payload(source)

    slim = build_slim(payload, resolved_source, fetched_at)
    full_payload = {"source": {"url": resolved_source, "fetchedAt": fetched_at}, "payload": payload}

    # FULL raw compactado
    with gzip.open(FULL_RAW_GZ, "wt", encoding="utf-8") as f:
        f.write(json.dumps(full_payload, ensure_ascii=False, sort_keys=True))

    # snapshot FULL compactado (1 por dia)
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
    src = os.getenv("OPIN_PARTICIPANTS_URL", DEFAULT_PARTICIPANTS_URL)
    write_outputs(src)
    print("OK: generated api/v1/participants.json (SLIM) and FULL archives (.json.gz)")
