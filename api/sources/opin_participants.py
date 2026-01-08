# api/sources/opin_participants.py
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from curl_cffi import requests

# ---------------------------------------------------------------------
# OPIN / Open Insurance Directory participants (source)
# ---------------------------------------------------------------------

OPIN_DIRECTORY_URL = os.getenv(
    "OPIN_DIRECTORY_URL",
    "https://data.directory.opinbrasil.com.br/participants",
)

CACHE_DIR = Path(os.getenv("OPIN_CACHE_DIR", "data/raw/opin"))
CACHE_FILE = CACHE_DIR / "participants.json"

TIMEOUT = int(os.getenv("OPIN_TIMEOUT", "30"))
IMPERSONATE = os.getenv("OPIN_IMPERSONATE", "chrome110")

# Accept both raw 14 digits and formatted CNPJ
_CNPJ_DIGITS_RE = re.compile(r"\b(\d{14})\b")
_CNPJ_FMT_RE = re.compile(r"\b(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})\b")

# A few common keys that may hold CNPJ / Tax ID in different schemas
_CNPJ_KEYS = (
    "cnpj",
    "CNPJ",
    "taxId",
    "TaxId",
    "tax_id",
    "TaxID",
    "registrationNumber",
    "RegistrationNumber",
    "registration_id",
    "RegistrationId",
    "document",
    "Documento",
    "documento",
    "cpf_cnpj",
    "CPF/CNPJ",
)

# Some common container keys where the ID may live
_CONTAINER_KEYS = (
    "Organisation",
    "Organization",
    "organisation",
    "organization",
    "company",
    "Company",
    "registrations",
    "Registrations",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _clean_cnpj(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    s = re.sub(r"\D", "", str(raw))
    if len(s) != 14:
        return None
    return s


def _ensure_participants_list(payload: Any) -> List[Dict[str, Any]]:
    """
    Normaliza diferentes schemas possíveis para uma lista de dicts.
    """
    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]

    if isinstance(payload, dict):
        # Common wrappers: {"data":[...]}, {"participants":[...]}, {"result":[...]}, {"items":[...]}
        for key in ("participants", "data", "result", "items"):
            v = payload.get(key)
            if isinstance(v, list):
                return [p for p in v if isinstance(p, dict)]

    return []


def _read_cache() -> Tuple[str, Any]:
    """
    Returns (status, payload).
    status in {"cache", "cache_miss", "cache_error"}.
    """
    if not CACHE_FILE.exists():
        return "cache_miss", None
    try:
        payload = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        return "cache", payload
    except Exception:
        return "cache_error", None


def _write_cache(payload: Any) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        # cache is best-effort
        pass


def extract_opin_participants() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    build_insurers.py espera:
      opin_meta, opin_participants = extract_opin_participants()

    Retorna:
      - meta: dict com informações de coleta/cache
      - participants: list[dict] (cada item = participante bruto)
    """
    meta: Dict[str, Any] = {
        "source": "opin_directory",
        "url": OPIN_DIRECTORY_URL,
        "generatedAt": _utc_now(),
        "status": "unknown",
        "count": 0,
        "cached": False,
    }

    # 1) Try live fetch
    try:
        headers = {
            "Accept": "application/json, text/plain;q=0.9, */*;q=0.1",
            "User-Agent": "Mozilla/5.0 (compatible; SanidaBot/1.0; +https://sanida.com.br)",
        }

        sess = requests.Session(impersonate=IMPERSONATE)
        r = sess.get(OPIN_DIRECTORY_URL, headers=headers, timeout=TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")

        payload = r.json()
        _write_cache(payload)

        participants = _ensure_participants_list(payload)
        meta["status"] = "live"
        meta["count"] = len(participants)
        meta["cached"] = False
        return meta, participants

    except Exception as e:
        meta["status"] = "live_failed"
        meta["error"] = str(e)

    # 2) Fallback to cache
    status, cached_payload = _read_cache()
    if status == "cache":
        participants = _ensure_participants_list(cached_payload)
        meta["status"] = "cache"
        meta["count"] = len(participants)
        meta["cached"] = True
        return meta, participants

    # 3) Total failure -> empty list
    meta["status"] = "empty"
    meta["count"] = 0
    meta["cached"] = False
    return meta, []


def _extract_cnpjs_from_any(obj: Any) -> Set[str]:
    """
    Varredura robusta:
    - tenta campos diretos (cnpj/taxId/etc.)
    - tenta containers comuns
    - faz scan regex no dump JSON do objeto para achar padrões de CNPJ
    """
    found: Set[str] = set()

    # 1) Direct keys and common containers
    if isinstance(obj, dict):
        for k in _CNPJ_KEYS:
            if k in obj:
                c = _clean_cnpj(obj.get(k))
                if c:
                    found.add(c)

        for ck in _CONTAINER_KEYS:
            v = obj.get(ck)
            if isinstance(v, dict):
                found |= _extract_cnpjs_from_any(v)
            elif isinstance(v, list):
                for it in v:
                    found |= _extract_cnpjs_from_any(it)

    elif isinstance(obj, list):
        for it in obj:
            found |= _extract_cnpjs_from_any(it)

    # 2) Regex scan (deep fallback)
    try:
        dump = json.dumps(obj, ensure_ascii=False)
        for m in _CNPJ_FMT_RE.findall(dump):
            c = _clean_cnpj(m)
            if c:
                found.add(c)
        for m in _CNPJ_DIGITS_RE.findall(dump):
            c = _clean_cnpj(m)
            if c:
                found.add(c)
    except Exception:
        pass

    return found


def load_opin_participant_cnpjs(
    participants_list: Optional[List[Dict[str, Any]]] = None,
) -> Set[str]:
    """
    build_insurers.py espera:
      opin_by_cnpj = load_opin_participant_cnpjs(opin_participants)

    Retorna:
      - Set[str] com CNPJs apenas dígitos (14 chars)
    """
    if participants_list is None:
        _meta, participants_list = extract_opin_participants()

    cnpjs: Set[str] = set()

    for p in participants_list or []:
        if not isinstance(p, dict):
            continue

        # 1) fast pass (somente neste participante)
        local: Set[str] = set()
        for k in _CNPJ_KEYS:
            if k in p:
                c = _clean_cnpj(p.get(k))
                if c:
                    local.add(c)

        # 2) deep scan só se este participante não rendeu nada no fast pass
        if not local:
            local |= _extract_cnpjs_from_any(p)

        cnpjs |= local

    # 3) fallback global (schema pode mudar e esconder em wrapper)
    if not cnpjs and participants_list:
        cnpjs |= _extract_cnpjs_from_any(participants_list)

    return cnpjs
