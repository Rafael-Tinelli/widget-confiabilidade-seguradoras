# api/sources/opin_participants.py
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Tuple

import requests

OPIN_DIRECTORY_URL = os.getenv("OPIN_DIRECTORY_URL", "https://data.directory.opinbrasil.com.br/participants")
CACHE_DIR = Path(os.getenv("OPIN_CACHE_DIR", "data/raw/opin"))
CACHE_FILE = CACHE_DIR / "participants.json"

_CNPJ_RE = re.compile(r"\D+")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _clean_cnpj(v: Any) -> str | None:
    d = _CNPJ_RE.sub("", str(v or ""))
    return d if len(d) == 14 else None


def _iter_values(obj: Any) -> Iterable[Any]:
    if isinstance(obj, dict):
        for v in obj.values():
            yield v
            yield from _iter_values(v)
    elif isinstance(obj, list):
        for v in obj:
            yield v
            yield from _iter_values(v)


def extract_opin_participants() -> Tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Baixa (ou carrega do cache) o diretório de participantes do OPIN.
    Retorna (meta, participants_list).
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    meta: dict[str, Any] = {"url": OPIN_DIRECTORY_URL, "status": "unknown", "fetchedAt": None, "count": 0}
    participants: list[dict[str, Any]] = []

    try:
        print(f"OPIN: GET {OPIN_DIRECTORY_URL}")
        r = requests.get(OPIN_DIRECTORY_URL, timeout=30)
        r.raise_for_status()
        data = r.json()

        if isinstance(data, list):
            participants = [p for p in data if isinstance(p, dict)]
        elif isinstance(data, dict):
            ps = data.get("participants")
            if isinstance(ps, list):
                participants = [p for p in ps if isinstance(p, dict)]
            else:
                participants = [data]

        CACHE_FILE.write_text(
            json.dumps(
                {"source": {"url": OPIN_DIRECTORY_URL, "fetchedAt": _utc_now()}, "participants": participants},
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        meta["status"] = "live"
        meta["fetchedAt"] = _utc_now()

    except Exception as e:
        print(f"OPIN WARN: falha no download ({e}). Tentando cache...")
        if CACHE_FILE.exists():
            try:
                cached = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
                if isinstance(cached, dict) and isinstance(cached.get("participants"), list):
                    participants = [p for p in cached["participants"] if isinstance(p, dict)]
                meta["status"] = "cache"
            except Exception as ce:
                print(f"OPIN ERROR: cache inválido ({ce})")
                meta["status"] = "empty"
        else:
            meta["status"] = "empty"

    meta["count"] = len(participants)
    return meta, participants


def load_opin_participant_cnpjs(participants: list[dict[str, Any]] | None = None) -> set[str]:
    """
    Extrai um set de CNPJs (14 dígitos) dos participantes OPIN.
    """
    if participants is None:
        _, participants = extract_opin_participants()

    cnpjs: set[str] = set()

    for p in participants:
        candidates = [
            p.get("registrationNumber"),
            p.get("RegistrationNumber"),
            p.get("cnpj"),
            p.get("Cnpj"),
            p.get("TaxId"),
            p.get("RegistrationId"),
        ]

        org_profile = p.get("OrganisationProfile") or p.get("OrganizationProfile")
        if isinstance(org_profile, dict):
            legal = org_profile.get("LegalEntity")
            if isinstance(legal, dict):
                candidates.append(legal.get("RegistrationNumber"))
                candidates.append(legal.get("RegistrationId"))

        for c in candidates:
            cc = _clean_cnpj(c)
            if cc:
                cnpjs.add(cc)

        if not cnpjs:
            for v in _iter_values(p):
                cc = _clean_cnpj(v)
                if cc:
                    cnpjs.add(cc)

    print(f"OPIN: {len(cnpjs)} CNPJs carregados.")
    return cnpjs
