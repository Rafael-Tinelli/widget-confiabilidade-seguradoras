from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2

RECEITA_CNPJ_OPEN_DATA_URL = (
    "https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/"
    "dados-abertos/cadastros/cnpj"
)
RECEITA_CNPJ_LAYOUT_URL = (
    "https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/"
    "convenios-e-transferencias/compartilhamento-de-bases-de-dados-2013-"
    "decreto-no-8-789-2016/leiaute-das-bases/dados-da-base-cnpj"
)

DEFAULT_VERIFIED_SNAPSHOT = Path("data/reference/v2/receita_lifecycle_verified.json")


class ReceitaLifecycleError(ValueError):
    """Raised when a Receita lifecycle record is incomplete or contradictory."""


def _iso_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return text
    if len(text) == 10 and text[2] == "/" and text[5] == "/":
        return f"{text[6:10]}-{text[3:5]}-{text[0:2]}"
    raise ReceitaLifecycleError(f"Unsupported Receita date format: {text}")


def _canonical_status(value: Any) -> str:
    text = str(value or "").strip().casefold()
    aliases = {
        "ativa": "active",
        "active": "active",
        "baixada": "closed",
        "closed": "closed",
        "suspensa": "suspended",
        "suspended": "suspended",
        "inapta": "unfit",
        "unfit": "unfit",
        "nula": "null",
        "null": "null",
    }
    status = aliases.get(text)
    if not status:
        raise ReceitaLifecycleError(f"Unsupported Receita cadastral status: {value}")
    return status


def _canonical_reason(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    folded = text.casefold()
    if folded in {"incorporacao", "incorporação"}:
        return "incorporation"
    return folded.replace(" ", "_")


def normalize_receita_lifecycle_record(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize one official CNPJ lifecycle observation.

    Receita status is a legal/cadastral fact. It must not overwrite SUSEP
    regulatory status; both dimensions are deliberately kept separate in v2.
    """
    cnpj = normalize_cnpj_v2(raw.get("cnpj"))
    if not cnpj:
        raise ReceitaLifecycleError("Receita lifecycle record requires a valid CNPJ")

    legal_name = str(raw.get("legal_name") or "").strip()
    if not legal_name:
        raise ReceitaLifecycleError(f"Receita lifecycle record {cnpj} requires legal_name")

    status = _canonical_status(raw.get("cadastral_status"))
    status_date = _iso_date(raw.get("status_date"))
    reason = _canonical_reason(raw.get("status_reason"))

    if status == "closed" and not status_date:
        raise ReceitaLifecycleError(f"Closed CNPJ {cnpj} requires status_date")

    return {
        "cnpj": cnpj,
        "legal_name": legal_name,
        "cadastral_status": status,
        "status_date": status_date,
        "status_reason": reason,
        "raw_status": str(raw.get("cadastral_status") or "").strip(),
        "raw_reason": str(raw.get("status_reason") or "").strip() or None,
        "source_authority": str(raw.get("source_authority") or "Receita Federal").strip(),
        "source_document": str(
            raw.get("source_document")
            or "Comprovante de Inscrição e de Situação Cadastral"
        ).strip(),
        "observed_at": _iso_date(raw.get("observed_at")),
        "source_mode": str(raw.get("source_mode") or "verified_snapshot").strip(),
    }


def load_verified_lifecycle_snapshot(
    path: Path = DEFAULT_VERIFIED_SNAPSHOT,
) -> list[dict[str, Any]]:
    """Load a small verified snapshot derived from official Receita records.

    This is intentionally separate from the full Receita CNPJ bulk dataset.
    The v2 contract is ready for a future filtered bulk-data collector, while
    the verified snapshot lets us model known lifecycle events without using
    unofficial CNPJ APIs or scraping CAPTCHA-protected consultation pages.
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("records") or []
    if not isinstance(rows, list):
        raise ReceitaLifecycleError("Receita lifecycle snapshot records must be a list")

    normalized = [normalize_receita_lifecycle_record(dict(row)) for row in rows]
    seen: set[str] = set()
    duplicates: list[str] = []
    for row in normalized:
        cnpj = row["cnpj"]
        if cnpj in seen:
            duplicates.append(cnpj)
        seen.add(cnpj)
    if duplicates:
        raise ReceitaLifecycleError(f"Duplicate Receita lifecycle CNPJ: {duplicates[:5]}")
    return normalized
