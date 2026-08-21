from __future__ import annotations

from collections import defaultdict
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import normalize_name_key
from api.v2.build_consumer_gov_157_experiment import _core_name, _entity_names

SAFE_OUTSIDE_ENTITY_TYPES = {
    "capitalization_company",
    "open_pension_entity",
    "sandbox_participant",
    "local_reinsurer",
    "admitted_reinsurer",
    "occasional_reinsurer",
    "reinsurance_broker",
    "self_regulator",
}


def _unique_index(pairs: list[tuple[str, str]]) -> dict[str, str]:
    raw: dict[str, set[str]] = defaultdict(set)
    for key, entity_id in pairs:
        if key:
            raw[key].add(entity_id)
    return {key: next(iter(ids)) for key, ids in raw.items() if len(ids) == 1}


def build_full_universe_provider_index(entities: list[dict[str, Any]]) -> dict[str, Any]:
    """Build deterministic exact/core/CNPJ indexes across the classified universe."""
    by_id = {str(entity["entity_id"]): entity for entity in entities}
    exact_pairs: list[tuple[str, str]] = []
    core_pairs: list[tuple[str, str]] = []
    cnpj_pairs: list[tuple[str, str]] = []
    for entity in entities:
        entity_id = str(entity["entity_id"])
        cnpj = normalize_cnpj_v2(entity.get("cnpj"))
        if cnpj:
            cnpj_pairs.append((cnpj, entity_id))
        for name in _entity_names(entity):
            exact_pairs.append((normalize_name_key(name), entity_id))
            core_pairs.append((_core_name(name), entity_id))
    return {
        "by_id": by_id,
        "cnpj": _unique_index(cnpj_pairs),
        "exact_name": _unique_index(exact_pairs),
        "core_name": _unique_index(core_pairs),
    }


def _state_for_entity(entity: dict[str, Any]) -> tuple[str, str] | None:
    eligibility = entity.get("eligibility") or {}
    if eligibility.get("regulatory_universe_eligible"):
        return "matched_current_insurer", "canonical_current_insurer"

    entity_type = str(entity.get("entity_type") or "")
    reason_codes = set(eligibility.get("reason_codes") or [])

    if entity_type in SAFE_OUTSIDE_ENTITY_TYPES:
        return "outside_157", f"canonical_outside_{entity_type}"

    if entity_type == "insurer" and "special_regulatory_regime" in reason_codes:
        return "outside_157", "canonical_special_regime_insurer"

    if "historical_legal_entity" in reason_codes:
        return "outside_157", "canonical_historical_legal_entity"

    return None


def _resolution_for_entity(
    entity_id: str,
    entity: dict[str, Any],
    *,
    method: str,
) -> dict[str, Any] | None:
    state = _state_for_entity(entity)
    if state is None:
        return None
    resolution_state, reason_code = state
    return {
        "resolution_state": resolution_state,
        "entity_id": entity_id if resolution_state == "matched_current_insurer" else None,
        "matched_canonical_entity_id": entity_id,
        "entity_type": entity.get("entity_type"),
        "legal_name": entity.get("legal_name"),
        "reason_code": reason_code,
        "match_method": method,
    }


def resolve_cnpj_against_full_universe(
    cnpj: str | None,
    index: dict[str, Any],
) -> dict[str, Any] | None:
    """Resolve an exact Receita/SUSEP legal-entity CNPJ against the canonical universe."""
    normalized = normalize_cnpj_v2(cnpj)
    if not normalized:
        return None
    entity_id = (index.get("cnpj") or {}).get(normalized)
    if not entity_id:
        return None
    entity = (index.get("by_id") or {}).get(entity_id)
    if not isinstance(entity, dict):
        return None
    return _resolution_for_entity(
        str(entity_id),
        entity,
        method="canonical_full_universe_cnpj_exact",
    )


def resolve_provider_against_full_universe(
    provider_name: str,
    index: dict[str, Any],
) -> dict[str, Any] | None:
    """Resolve an exact canonical entity without transferring point-of-sale labels.

    Exact normalized legal/source name is tried first. Then a unique core-name
    key is allowed even when short (for example HDI, 180, ALM, MBM), but only
    because uniqueness is tested against the full classified inventory rather
    than just the 157 eligible insurers.

    Labels that do not identify a supervised/classified entity -- e.g. a bank,
    cooperative, retailer or broker acting only as a sales channel -- remain
    unresolved unless separately curated with source-backed evidence.
    """
    key = normalize_name_key(provider_name)
    entity_id = (index.get("exact_name") or {}).get(key)
    method = "canonical_full_universe_exact"

    if not entity_id:
        core = _core_name(provider_name)
        if len(core) < 3:
            return None
        entity_id = (index.get("core_name") or {}).get(core)
        method = "canonical_full_universe_core"

    if not entity_id:
        return None

    entity = (index.get("by_id") or {}).get(entity_id)
    if not isinstance(entity, dict):
        return None
    return _resolution_for_entity(str(entity_id), entity, method=method)
