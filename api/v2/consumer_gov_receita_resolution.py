from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj_identity import SAFE_OUTSIDE_PRIMARY_CNAES
from api.utils.name_cleaner import normalize_name_key
from api.v2.consumer_gov_universe_resolution import (
    build_full_universe_provider_index,
    resolve_cnpj_against_full_universe,
)

DEFAULT_RECEITA_IDENTITY_SNAPSHOT = Path(
    "data/derived/v2/receita_consumer_gov_identity.json"
)


def load_receita_identity_snapshot(
    path: Path = DEFAULT_RECEITA_IDENTITY_SNAPSHOT,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("Receita identity snapshot must be a JSON object")
    if payload.get("artifact") != "v2_receita_cnpj_identity":
        raise ValueError("unexpected Receita identity artifact")
    return payload


def build_receita_provider_index(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not payload:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in payload.get("provider_matches") or []:
        if not isinstance(row, dict):
            continue
        key = normalize_name_key(str(row.get("provider") or row.get("provider_key") or ""))
        if not key:
            continue
        if key in out:
            raise ValueError(f"duplicate Receita provider identity row: {key}")
        out[key] = row
    return out


def _safe_receita_outside_resolution(candidate: dict[str, Any]) -> dict[str, Any] | None:
    primary = str(candidate.get("primary_cnae_code") or "").strip()
    reason = SAFE_OUTSIDE_PRIMARY_CNAES.get(primary)
    if not reason:
        return None
    return {
        "resolution_state": "outside_157",
        "entity_id": None,
        "matched_canonical_entity_id": None,
        "entity_type": "receita_non_insurer_activity",
        "legal_name": candidate.get("legal_name_receita"),
        "reason_code": reason,
        "match_method": "receita_unique_name_candidate_safe_primary_cnae",
        "receita_candidate": candidate,
    }


def resolve_provider_via_receita(
    provider_name: str,
    receita_provider_index: dict[str, dict[str, Any]],
    full_universe_index: dict[str, Any],
) -> dict[str, Any] | None:
    """Resolve one provider through a filtered Receita identity snapshot.

    Name matching only discovers a legal-entity candidate. Current insurer
    admission is always decided by exact candidate CNPJ against the canonical
    SUSEP-derived universe. If the CNPJ is not a canonical current insurer,
    only a small allow-list of unequivocal primary CNAEs may classify the row
    outside the 157. Everything else remains unresolved.
    """
    row = receita_provider_index.get(normalize_name_key(provider_name))
    if not row or row.get("candidate_state") != "unique_candidate":
        return None
    candidates = list(row.get("candidates") or [])
    if len(candidates) != 1 or not isinstance(candidates[0], dict):
        return None
    candidate = candidates[0]

    canonical = resolve_cnpj_against_full_universe(
        candidate.get("cnpj"),
        full_universe_index,
    )
    if canonical is not None:
        result = dict(canonical)
        result["match_method"] = (
            "receita_unique_name_candidate_cnpj_to_canonical_"
            + str(row.get("match_method") or "unknown")
        )
        result["receita_candidate"] = candidate
        return result

    return _safe_receita_outside_resolution(candidate)


def resolve_provider_via_receita_payload(
    provider_name: str,
    payload: dict[str, Any] | None,
    entities: list[dict[str, Any]],
) -> dict[str, Any] | None:
    return resolve_provider_via_receita(
        provider_name,
        build_receita_provider_index(payload),
        build_full_universe_provider_index(entities),
    )
