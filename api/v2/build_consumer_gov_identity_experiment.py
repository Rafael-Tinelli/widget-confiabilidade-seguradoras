from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import normalize_name_key
from api.v2.build_consumer_gov_157_experiment import (
    ELIGIBILITY_PATH,
    _build_indexes,
    _candidate_suggestions,
    _core_name,
    _entries,
    _load_latest_months,
    _match_provider,
    _safe_int,
    _stats,
)
from api.v2.consumer_gov_identity import (
    load_provider_resolution_registry,
    resolve_curated_provider,
)
from api.v2.consumer_gov_temporal_brand import (
    build_temporal_brand_index,
    resolve_temporal_brand,
)
from api.v2.consumer_gov_universe_resolution import (
    build_full_universe_provider_index,
    resolve_provider_against_full_universe,
)

OUTPUT_PATH = Path("data/derived/v2/consumer_gov_identity_experiment.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _provider_display(provider_key: str, entry: dict[str, Any]) -> str:
    return str(entry.get("display_name") or entry.get("name") or provider_key).strip()


def _build_non_brand_fallback_indexes(
    indexes: dict[str, Any],
    temporal_brand_index: dict[str, Any],
) -> dict[str, Any]:
    """Prevent current brand aliases from leaking through timeless exact/core fallbacks."""
    brand_name_keys = set((temporal_brand_index.get("by_name") or {}).keys())
    brand_core_keys = {
        _core_name(name)
        for name in temporal_brand_index.get("display_names") or []
        if _core_name(name)
    }
    fallback = dict(indexes)
    fallback["verified_brand"] = {}
    fallback["exact_name"] = {
        key: entity_id
        for key, entity_id in (indexes.get("exact_name") or {}).items()
        if key not in brand_name_keys
    }
    fallback["core_name"] = {
        key: entity_id
        for key, entity_id in (indexes.get("core_name") or {}).items()
        if key not in brand_core_keys
    }
    return fallback


def _resolve_provider_temporal_brand(
    provider_key: str,
    display: str,
    month: str,
    temporal_brand_index: dict[str, Any],
) -> dict[str, Any] | None:
    seen: set[str] = set()
    for candidate in (display, provider_key):
        key = normalize_name_key(candidate)
        if not key or key in seen:
            continue
        seen.add(key)
        result = resolve_temporal_brand(candidate, month, temporal_brand_index)
        if result is not None:
            return result
    return None


def _record_entity_match(
    entity_id: str,
    month: str,
    complaints: int,
    matched_complaints: int,
    entity_complaints: Counter[str],
    entity_months: dict[str, set[str]],
) -> int:
    entity_complaints[entity_id] += complaints
    entity_months[entity_id].add(month)
    return matched_complaints + complaints


def build_identity_experiment() -> dict[str, Any]:
    eligibility = json.loads(ELIGIBILITY_PATH.read_text(encoding="utf-8"))
    indexes = _build_indexes(eligibility)
    eligible = indexes["eligible"]
    if len(eligible) != 157:
        raise RuntimeError(f"expected 157 current ordinary insurers, got {len(eligible)}")

    registry = load_provider_resolution_registry()
    temporal_brand_index = build_temporal_brand_index(
        list(eligibility.get("brands") or []),
        set(indexes["entity_by_id"]),
    )
    full_universe_index = build_full_universe_provider_index(
        list(eligibility.get("entities") or [])
    )
    fallback_indexes = _build_non_brand_fallback_indexes(indexes, temporal_brand_index)
    monthly = _load_latest_months()

    complaints_total = 0
    matched_complaints = 0
    outside_157_complaints = 0
    ambiguous_complaints = 0
    unresolved_complaints = 0
    match_methods: Counter[str] = Counter()
    resolution_states: Counter[str] = Counter()
    unmatched_provider_rows: Counter[str] = Counter()
    curated_provider_rows: Counter[str] = Counter()
    outside_provider_rows: Counter[str] = Counter()
    ambiguous_provider_rows: Counter[str] = Counter()
    temporal_matched_provider_rows: Counter[str] = Counter()
    temporal_unresolved_provider_rows: Counter[str] = Counter()
    temporal_ambiguous_provider_rows: Counter[str] = Counter()
    canonical_full_universe_rows: Counter[str] = Counter()
    canonical_full_universe_details: dict[str, dict[str, Any]] = {}
    entity_complaints: Counter[str] = Counter()
    entity_months: dict[str, set[str]] = {str(item["entity_id"]): set() for item in eligible}

    for month, _, root in monthly:
        for provider_key, entry in _entries(root).items():
            if not isinstance(entry, dict):
                continue
            complaints = _safe_int(
                _stats(entry).get("complaintsCount")
                or _stats(entry).get("total_claims")
            )
            if complaints <= 0:
                continue
            complaints_total += complaints
            display = _provider_display(str(provider_key), entry)

            curated = resolve_curated_provider(display, indexes["cnpj"], registry)
            if curated is not None:
                state = str(curated["resolution_state"])
                resolution_states[state] += complaints
                curated_provider_rows[display] += complaints
                if state == "matched_current_insurer":
                    entity_id = str(curated["entity_id"])
                    matched_complaints = _record_entity_match(
                        entity_id,
                        month,
                        complaints,
                        matched_complaints,
                        entity_complaints,
                        entity_months,
                    )
                    match_methods["curated_source_backed_resolution"] += complaints
                    continue
                if state == "outside_157":
                    outside_157_complaints += complaints
                    match_methods["curated_outside_157"] += complaints
                    outside_provider_rows[display] += complaints
                    continue
                if state == "ambiguous":
                    ambiguous_complaints += complaints
                    match_methods["curated_ambiguous"] += complaints
                    ambiguous_provider_rows[display] += complaints
                    continue
                raise RuntimeError(f"unexpected curated state: {state}")

            cnpj = normalize_cnpj_v2(entry.get("cnpj"))
            if cnpj and cnpj in indexes["cnpj"]:
                entity_id = str(indexes["cnpj"][cnpj])
                matched_complaints = _record_entity_match(
                    entity_id,
                    month,
                    complaints,
                    matched_complaints,
                    entity_complaints,
                    entity_months,
                )
                resolution_states["matched_current_insurer"] += complaints
                match_methods["cnpj_exact"] += complaints
                continue

            temporal = _resolve_provider_temporal_brand(
                str(provider_key),
                display,
                month,
                temporal_brand_index,
            )
            if temporal is not None:
                state = str(temporal["resolution_state"])
                method = str(temporal["match_method"])
                match_methods[method] += complaints
                if state == "matched_current_insurer":
                    entity_id = str(temporal["entity_id"])
                    matched_complaints = _record_entity_match(
                        entity_id,
                        month,
                        complaints,
                        matched_complaints,
                        entity_complaints,
                        entity_months,
                    )
                    resolution_states["matched_current_insurer"] += complaints
                    temporal_matched_provider_rows[display] += complaints
                    continue
                if state == "ambiguous":
                    ambiguous_complaints += complaints
                    resolution_states["ambiguous"] += complaints
                    ambiguous_provider_rows[display] += complaints
                    temporal_ambiguous_provider_rows[display] += complaints
                    continue
                if state == "unresolved":
                    unresolved_complaints += complaints
                    resolution_states["unresolved"] += complaints
                    temporal_unresolved_provider_rows[display] += complaints
                    if display:
                        unmatched_provider_rows[display] += complaints
                    continue
                raise RuntimeError(f"unexpected temporal brand state: {state}")

            canonical = resolve_provider_against_full_universe(
                display,
                full_universe_index,
            )
            if canonical is None and normalize_name_key(display) != normalize_name_key(
                str(provider_key)
            ):
                canonical = resolve_provider_against_full_universe(
                    str(provider_key),
                    full_universe_index,
                )
            if canonical is not None:
                state = str(canonical["resolution_state"])
                method = str(canonical["match_method"])
                match_methods[method] += complaints
                resolution_states[state] += complaints
                canonical_full_universe_rows[display] += complaints
                canonical_full_universe_details[display] = {
                    "resolution_state": state,
                    "matched_canonical_entity_id": canonical.get(
                        "matched_canonical_entity_id"
                    ),
                    "entity_id": canonical.get("entity_id"),
                    "entity_type": canonical.get("entity_type"),
                    "legal_name": canonical.get("legal_name"),
                    "reason_code": canonical.get("reason_code"),
                    "match_method": method,
                }
                if state == "matched_current_insurer":
                    entity_id = str(canonical["entity_id"])
                    matched_complaints = _record_entity_match(
                        entity_id,
                        month,
                        complaints,
                        matched_complaints,
                        entity_complaints,
                        entity_months,
                    )
                    continue
                if state == "outside_157":
                    outside_157_complaints += complaints
                    outside_provider_rows[display] += complaints
                    continue
                raise RuntimeError(f"unexpected canonical universe state: {state}")

            entity_id, method = _match_provider(
                str(provider_key),
                entry,
                fallback_indexes,
            )
            match_methods[method] += complaints
            if entity_id:
                matched_complaints = _record_entity_match(
                    str(entity_id),
                    month,
                    complaints,
                    matched_complaints,
                    entity_complaints,
                    entity_months,
                )
                resolution_states["matched_current_insurer"] += complaints
            else:
                unresolved_complaints += complaints
                resolution_states["unresolved"] += complaints
                if display:
                    unmatched_provider_rows[display] += complaints

    reconciled = (
        matched_complaints
        + outside_157_complaints
        + ambiguous_complaints
        + unresolved_complaints
    )
    if reconciled != complaints_total:
        raise RuntimeError(
            f"complaint-state accounting mismatch: {reconciled} != {complaints_total}"
        )

    observed_entities = [
        entity_id for entity_id, complaints in entity_complaints.items() if complaints > 0
    ]
    unresolved_all = []
    for provider, complaints in unmatched_provider_rows.most_common():
        unresolved_all.append(
            {
                "provider": provider,
                "provider_key": normalize_name_key(provider),
                "complaints": complaints,
                "candidate_suggestions_non_authoritative": _candidate_suggestions(
                    provider, indexes
                ),
            }
        )

    return {
        "artifact": "v2_consumer_gov_identity_experiment",
        "generated_at": _utc_now(),
        "status": "experimental",
        "consumer_complaint_semantics": {
            "meaning": (
                "A Consumer.gov complaint is an observed consumer allegation/friction event, "
                "not an adjudicated finding that the insurer acted abusively, denied a claim "
                "wrongfully, or breached the contract."
            ),
            "methodological_consequence": (
                "Identity resolution may improve evidence coverage, but no complaint count or "
                "individual complaint changes score, assessment eligibility or ranking eligibility. "
                "Later conduct metrics must combine normalized incidence, problem type, consumer "
                "outcome, persistence and sample confidence."
            ),
        },
        "identity_policy": {
            "accepted_resolution_order": [
                "curated source-backed exact provider resolution",
                "structured CNPJ exact when present",
                "exact verified brand/risk-carrier relationship valid for the full source month",
                "deterministic exact/core identity across the complete classified regulatory universe",
                "exact non-brand legal/current entity name",
                "unique exact non-brand core-name key",
            ],
            "full_universe_exclusion_rule": (
                "A provider that deterministically identifies a classified non-157 entity is "
                "recorded outside_157 instead of generic unresolved. Capitalization, open-pension, "
                "Sandbox, reinsurance and historical legal entities are never transferred to a "
                "same-group insurer. Names that merely identify a sales channel, broker, retailer "
                "or cooperative remain unresolved unless source-backed curation proves their own "
                "identity; distribution relationships never transfer complaints to carriers."
            ),
            "pension_name_rule": (
                "The word previdencia is not an exclusion rule. Some SUSEP-licensed current "
                "insurers also operate life/open-pension business. Exclusion follows canonical "
                "entity_type and the 157 regulatory gate, never a keyword in the provider name."
            ),
            "brand_temporal_rule": (
                "A brand/risk-carrier relationship may attribute a monthly aggregate only when "
                "its effective window covers the entire month. A mid-month start/end remains "
                "unresolved because the monthly source cannot be split safely."
            ),
            "brand_temporal_fallback_rule": (
                "Known brand aliases are removed from timeless exact/core fallbacks so a current "
                "carrier cannot be applied retroactively outside the verified relationship window."
            ),
            "fuzzy_similarity_role": "diagnostic_only_never_authoritative",
            "ambiguous_provider_behavior": "preserve_ambiguity_do_not_assign",
            "outside_157_behavior": "classify_outside_universe_do_not_transfer_complaints",
        },
        "source": {
            "dataset": "reclamacoes-do-consumidor-gov-br",
            "months": [month for month, _, _ in monthly],
            "provider_resolution_registry": (
                "data/reference/v2/consumer_gov_provider_resolutions.json"
            ),
            "canonical_brand_registry": "data/reference/v2/verified_relationships.json",
            "canonical_regulatory_inventory": str(ELIGIBILITY_PATH),
        },
        "universe": {
            "eligible_insurers": len(eligible),
            "observed_insurers": len(observed_entities),
            "unobserved_insurers": len(eligible) - len(observed_entities),
            "coverage_ratio": len(observed_entities) / len(eligible),
            "insurers_with_complaints_in_all_12_months": sum(
                len(entity_months[entity_id]) == 12 for entity_id in entity_months
            ),
            "insurers_with_complaints_in_at_least_9_months": sum(
                len(entity_months[entity_id]) >= 9 for entity_id in entity_months
            ),
            "insurers_with_complaints_in_at_least_6_months": sum(
                len(entity_months[entity_id]) >= 6 for entity_id in entity_months
            ),
        },
        "rows": {
            "complaints_in_insurance_segment_snapshots": complaints_total,
            "matched_current_insurer_complaints": matched_complaints,
            "matched_current_insurer_ratio": (
                matched_complaints / complaints_total if complaints_total else None
            ),
            "outside_157_complaints": outside_157_complaints,
            "ambiguous_complaints": ambiguous_complaints,
            "unresolved_complaints": unresolved_complaints,
            "unresolved_ratio": (
                unresolved_complaints / complaints_total if complaints_total else None
            ),
            "resolution_states_weighted_by_complaints": dict(resolution_states),
            "match_methods_weighted_by_complaints": dict(match_methods),
            "curated_provider_names_applied": len(curated_provider_rows),
            "distinct_unresolved_provider_names": len(unmatched_provider_rows),
            "temporal_brand_matched_complaints": sum(temporal_matched_provider_rows.values()),
            "temporal_brand_unresolved_complaints": sum(
                temporal_unresolved_provider_rows.values()
            ),
            "temporal_brand_ambiguous_complaints": sum(
                temporal_ambiguous_provider_rows.values()
            ),
            "canonical_full_universe_complaints": sum(
                canonical_full_universe_rows.values()
            ),
        },
        "curated_provider_resolutions_applied": [
            {"provider": provider, "complaints": complaints}
            for provider, complaints in curated_provider_rows.most_common()
        ],
        "temporal_brand_matches": [
            {"provider": provider, "complaints": complaints}
            for provider, complaints in temporal_matched_provider_rows.most_common()
        ],
        "temporal_brand_unresolved_providers": [
            {"provider": provider, "complaints": complaints}
            for provider, complaints in temporal_unresolved_provider_rows.most_common()
        ],
        "temporal_brand_ambiguous_providers": [
            {"provider": provider, "complaints": complaints}
            for provider, complaints in temporal_ambiguous_provider_rows.most_common()
        ],
        "canonical_full_universe_resolutions": [
            {
                "provider": provider,
                "complaints": complaints,
                **canonical_full_universe_details[provider],
            }
            for provider, complaints in canonical_full_universe_rows.most_common()
        ],
        "outside_157_providers": [
            {"provider": provider, "complaints": complaints}
            for provider, complaints in outside_provider_rows.most_common()
        ],
        "ambiguous_providers": [
            {"provider": provider, "complaints": complaints}
            for provider, complaints in ambiguous_provider_rows.most_common()
        ],
        "unresolved_providers": unresolved_all,
        "top_unresolved_providers": unresolved_all[:50],
        "entities": [
            {
                "entity_id": entity_id,
                "complaints": entity_complaints[entity_id],
                "months_with_complaints": len(entity_months[entity_id]),
            }
            for entity_id in sorted(
                entity_complaints,
                key=lambda item: (-entity_complaints[item], item),
            )
            if entity_complaints[entity_id] > 0
        ],
    }


def main() -> None:
    payload = build_identity_experiment()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp = OUTPUT_PATH.with_suffix(OUTPUT_PATH.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(OUTPUT_PATH)
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "universe": payload["universe"],
                "rows": payload["rows"],
                "temporal_brand_unresolved_providers": payload[
                    "temporal_brand_unresolved_providers"
                ],
                "temporal_brand_ambiguous_providers": payload[
                    "temporal_brand_ambiguous_providers"
                ],
                "canonical_full_universe_resolutions": payload[
                    "canonical_full_universe_resolutions"
                ],
                "ambiguous_providers": payload["ambiguous_providers"],
                "outside_157_providers": payload["outside_157_providers"],
                "unresolved_providers": payload["unresolved_providers"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
