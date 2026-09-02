from __future__ import annotations

import re
import unicodedata
from typing import Any

SPECIAL_PURPOSE_INSURER_SUBTYPE = "special_purpose_insurer"
INSURANCE_COOPERATIVE_SUBTYPE = "insurance_cooperative"


def _normalize_legal_name(value: Any) -> str:
    text = str(value or "").strip().upper()
    text = "".join(
        char
        for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )
    return re.sub(r"[^A-Z0-9]+", " ", text).strip()


def _looks_like_insurance_cooperative_legal_name(name: str) -> bool:
    """Return whether an official legal name carries the cooperative-insurance marker.

    The current SUSEP licensed-entities form does not yet expose a dedicated machine-
    readable cooperative type. Until it does, this narrow fallback only accepts the
    corporate-name wording that explicitly combines the cooperative form with the
    insurance purpose. A future official taxonomy change still fails closed in the
    source adapter and must be modeled explicitly rather than silently mapped here.
    """
    if "COOPERATIVA" not in name:
        return False
    return bool(
        re.search(r"\bCOOPERATIVA\b.*\bSEGUROS?\b", name)
        or re.search(r"\bSEGUROS?\b.*\bCOOPERATIVA\b", name)
    )


def infer_regulatory_subtype(entity: dict[str, Any]) -> str | None:
    """Derive narrow regulatory subtypes from authoritative identity facts.

    SUSEP's licensed-entities source currently groups SSPEs under the generic
    insurer type and does not expose a dedicated cooperative-insurance type. The
    official legal name is therefore used only as a narrow, source-backed fallback
    for subtypes whose legal form/purpose is explicit in that name.

    This function must never use FIP/CNPJ allowlists, brand marketing, or fuzzy
    matching. If SUSEP introduces a new machine-readable entity type, the source
    adapter remains responsible for failing closed until that taxonomy is modeled.
    """
    if str(entity.get("entity_type") or "") != "insurer":
        return None

    name = _normalize_legal_name(entity.get("legal_name"))
    if "SEGURADORA DE PROPOSITO ESPECIFICO" in name:
        return SPECIAL_PURPOSE_INSURER_SUBTYPE
    if _looks_like_insurance_cooperative_legal_name(name):
        return INSURANCE_COOPERATIVE_SUBTYPE
    return None


def is_special_purpose_insurer(entity: dict[str, Any]) -> bool:
    subtype = str(entity.get("regulatory_subtype") or "") or infer_regulatory_subtype(entity)
    return subtype == SPECIAL_PURPOSE_INSURER_SUBTYPE


def is_insurance_cooperative(entity: dict[str, Any]) -> bool:
    subtype = str(entity.get("regulatory_subtype") or "") or infer_regulatory_subtype(entity)
    return subtype == INSURANCE_COOPERATIVE_SUBTYPE


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
        and not is_insurance_cooperative(entity)
    )
