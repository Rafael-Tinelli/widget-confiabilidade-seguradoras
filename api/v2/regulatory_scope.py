from __future__ import annotations

import re
import unicodedata
from typing import Any

SPECIAL_PURPOSE_INSURER_SUBTYPE = "special_purpose_insurer"


def _normalize_legal_name(value: Any) -> str:
    text = str(value or "").strip().upper()
    text = "".join(
        char
        for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )
    return re.sub(r"[^A-Z0-9]+", " ", text).strip()


def infer_regulatory_subtype(entity: dict[str, Any]) -> str | None:
    """Derive narrow regulatory subtypes from authoritative identity facts.

    SUSEP's licensed-entities source currently groups SSPEs under the generic
    insurer type. CNSP rules require a Sociedade Seguradora de Propósito
    Específico to make that exclusive purpose explicit in its corporate name,
    so the official licensed legal name is a stable, source-backed discriminator
    until SUSEP exposes a dedicated machine-readable subtype.

    This function must never use FIP/CNPJ allowlists or fuzzy brand matching.
    """
    if str(entity.get("entity_type") or "") != "insurer":
        return None

    name = _normalize_legal_name(entity.get("legal_name"))
    if "SEGURADORA DE PROPOSITO ESPECIFICO" in name:
        return SPECIAL_PURPOSE_INSURER_SUBTYPE
    return None


def is_special_purpose_insurer(entity: dict[str, Any]) -> bool:
    subtype = str(entity.get("regulatory_subtype") or "") or infer_regulatory_subtype(entity)
    return subtype == SPECIAL_PURPOSE_INSURER_SUBTYPE


def is_current_ordinary_consumer_insurer(entity: dict[str, Any]) -> bool:
    """Return whether an entity is structurally in the ordinary consumer scope.

    Legal lifecycle and evidence sufficiency are separate gates. This predicate
    only distinguishes current ordinary insurers from supervised entities whose
    regulatory role is outside the comparator's intended insurance market.
    """
    return bool(
        entity.get("entity_type") == "insurer"
        and entity.get("regulatory_regime") == "ordinary"
        and entity.get("regulatory_status") == "active_licensed"
        and not is_special_purpose_insurer(entity)
    )
