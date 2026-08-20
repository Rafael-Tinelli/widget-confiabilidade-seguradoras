from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.utils.name_cleaner import normalize_name_key
from api.v2.build_consumer_gov_157_experiment import (
    ELIGIBILITY_PATH,
    _build_indexes,
    _candidate_suggestions,
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

OUTPUT_PATH = Path("data/derived/v2/consumer_gov_identity_experiment.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _provider_display(provider_key: str, entry: dict[str, Any]) -> str:
    return str(entry.get("display_name") or entry.get("name") or provider_key).strip()


def build_identity_experiment() -> dict[str, Any]:
    eligibility = json.loads(ELIGIBILITY_PATH.read_text(encoding="utf-8"))
    indexes = _build_indexes(eligibility)
    eligible = indexes["eligible"]
    if len(eligible) != 157:
        raise RuntimeError(f"expected 157 current ordinary insurers, got {len(eligible)}")

    registry = load_provider_resolution_registry()
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
                    matched_complaints += complaints
                    match_methods["curated_source_backed_resolution"] += complaints
                    entity_complaints[entity_id] += complaints
                    entity_months[entity_id].add(month)
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

            entity_id, method = _match_provider(str(provider_key), entry, indexes)
            match_methods[method] += complaints
            if entity_id:
                matched_complaints += complaints
                resolution_states["matched_current_insurer"] += complaints
                entity_complaints[entity_id] += complaints
                entity_months[entity_id].add(month)
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
                "exact legal/current entity name",
                "exact verified brand",
                "unique exact core-name key",
            ],
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
        },
        "curated_provider_resolutions_applied": [
            {"provider": provider, "complaints": complaints}
            for provider, complaints in curated_provider_rows.most_common()
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
