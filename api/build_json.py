from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


DEFAULT_PARTICIPANTS_URL = "https://data.directory.opinbrasil.com.br/participants"
USER_AGENT = "widget-confiabilidade-seguradoras/0.1 (+https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras)"


ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_SNAPSHOTS = ROOT / "data" / "snapshots"
API_V1 = ROOT / "api" / "v1"


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

    # fallback: nada reconhecido
    return []


def _normalize_participant(p: Dict[str, Any]) -> Dict[str, Any]:
    # Campos mais comuns (tolerante a variações de nome)
    pid = _pick(p, ["OrganisationId", "organizationId", "organisationId", "organisation_id", "id"])
    name = _pick(p, ["OrganisationName", "organizationName", "organisationName", "legalName", "name"])
    reg = _pick(p, ["RegistrationNumber", "registrationNumber", "registration_number", "cnpj", "CNPJ"])
    status = _pick(p, ["Status", "status"])
    roles = _pick(p, ["Roles", "roles"])

    # Alguns diretórios trazem servidores de autorização/AS em campos variados
    auth_servers = _pick(
        p,
        ["AuthorisationServers", "AuthorizationServers", "authorisationServers", "authorizationServers"],
    )

    return {
        "id": str(pid) if pid is not None else None,
        "name": str(name) if name is not None else None,
        "registrationNumber": str(reg) if reg is not None else None,
        "status": str(status) if status is not None else None,
        "roles": _as_list(roles),
        "authorizationServers": _as_list(auth_servers),
    }


def fetch_participants(url: str) -> Dict[str, Any]:
    r = requests.get(
        url,
        timeout=30,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        },
    )
    r.raise_for_status()
    payload = r.json()

    participants = _extract_participants(payload)
    normalized = [_normalize_participant(p) for p in participants]

    return {
        "source": {
            "url": url,
            "fetchedAt": _now_iso(),
        },
        "participants": normalized,
        # Guarda um “shape hint” (sem despejar tudo) para depuração
        "meta": {
            "count": len(normalized),
        },
    }


def write_outputs(url: str) -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    API_V1.mkdir(parents=True, exist_ok=True)

    data = fetch_participants(url)

    # raw “para auditoria” (mesmo sendo já normalizado, é o que coletamos hoje)
    raw_path = DATA_RAW / "opin_participants.json"
    raw_path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    # snapshot versionado (1 por dia; você pode mudar a granularidade depois)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap_path = DATA_SNAPSHOTS / f"opin_participants_{day}.json"
    snap_path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    # “API pública” do projeto
    api_path = API_V1 / "participants.json"
    api_path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    url = os.getenv("OPIN_PARTICIPANTS_URL", DEFAULT_PARTICIPANTS_URL)
    write_outputs(url)
    print("OK: generated api/v1/participants.json")
