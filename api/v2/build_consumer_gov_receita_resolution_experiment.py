from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.utils.name_cleaner import normalize_name_key
from api.v2.build_consumer_gov_157_experiment import ELIGIBILITY_PATH
from api.v2.build_consumer_gov_identity_experiment import OUTPUT_PATH as BASE_IDENTITY_PATH
from api.v2.consumer_gov_receita_resolution import (
    DEFAULT_RECEITA_IDENTITY_SNAPSHOT,
    build_receita_provider_index,
    load_receita_identity_snapshot,
    resolve_provider_via_receita,
)
from api.v2.consumer_gov_universe_resolution import build_full_universe_provider_index

OUTPUT_PATH = Path("data/derived/v2/consumer_gov_receita_resolution_experiment.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_experiment() -> dict[str, Any]:
    base = json.loads(BASE_IDENTITY_PATH.read_text(encoding="utf-8"))
    eligibility = json.loads(ELIGIBILITY_PATH.read_text(encoding="utf-8"))
    receita = load_receita_identity_snapshot(DEFAULT_RECEITA_IDENTITY_SNAPSHOT)
    if receita is None:
        raise RuntimeError("Receita identity snapshot is required")

    entities = list(eligibility.get("entities") or [])
    full_index = build_full_universe_provider_index(entities)
    receita_index = build_receita_provider_index(receita)

    deltas: Counter[str] = Counter()
    resolved_rows: list[dict[str, Any]] = []
    still_unresolved: list[dict[str, Any]] = []

    for row in base.get("unresolved_providers") or []:
        provider = str(row.get("provider") or "").strip()
        complaints = int(row.get("complaints") or 0)
        if not provider or complaints <= 0:
            continue
        result = resolve_provider_via_receita(provider, receita_index, full_index)
        receita_match = receita_index.get(normalize_name_key(provider)) or {}
        if result is None:
            still_unresolved.append(
                {
                    "provider": provider,
                    "complaints": complaints,
                    "receita_candidate_state": receita_match.get("candidate_state"),
                    "receita_match_method": receita_match.get("match_method"),
                    "receita_candidates": receita_match.get("candidates") or [],
                }
            )
            deltas["unresolved"] += complaints
            continue
        state = str(result["resolution_state"])
        if state not in {"matched_current_insurer", "outside_157"}:
            raise RuntimeError(f"unexpected Receita resolution state: {state}")
        deltas[state] += complaints
        resolved_rows.append(
            {
                "provider": provider,
                "complaints": complaints,
                "resolution_state": state,
                "entity_id": result.get("entity_id"),
                "matched_canonical_entity_id": result.get("matched_canonical_entity_id"),
                "entity_type": result.get("entity_type"),
                "legal_name": result.get("legal_name"),
                "reason_code": result.get("reason_code"),
                "match_method": result.get("match_method"),
                "receita_candidate": result.get("receita_candidate"),
            }
        )

    base_rows = base["rows"]
    base_unresolved = int(base_rows["unresolved_complaints"])
    if sum(deltas.values()) != base_unresolved:
        raise RuntimeError(
            f"Receita delta accounting mismatch: {sum(deltas.values())} != {base_unresolved}"
        )

    matched_total = int(base_rows["matched_current_insurer_complaints"]) + deltas[
        "matched_current_insurer"
    ]
    outside_total = int(base_rows["outside_157_complaints"]) + deltas["outside_157"]
    ambiguous_total = int(base_rows["ambiguous_complaints"])
    unresolved_total = deltas["unresolved"]
    source_total = int(base_rows["complaints_in_insurance_segment_snapshots"])
    if matched_total + outside_total + ambiguous_total + unresolved_total != source_total:
        raise RuntimeError("post-Receita complaint states do not reconcile to source total")

    return {
        "artifact": "v2_consumer_gov_receita_resolution_experiment",
        "generated_at": _utc_now(),
        "status": "experimental",
        "source": {
            "consumer_identity_artifact": str(BASE_IDENTITY_PATH),
            "receita_identity_artifact": str(DEFAULT_RECEITA_IDENTITY_SNAPSHOT),
            "receita_reference_period": (receita.get("source") or {}).get(
                "reference_period"
            ),
        },
        "methodology": {
            "name_role": "discover_unique_receita_legal_entity_candidate",
            "insurer_admission": "exact_candidate_cnpj_must_match_current_susep_157_entity",
            "safe_exclusion": "only_canonical_non157_or_explicit_primary_cnae_allowlist",
            "insurance_cnae_without_susep": "never_admits_insurer",
            "fuzzy_matching": "not_used",
            "cooperative_or_sales_channel_transfer": "forbidden",
        },
        "base": {
            "matched_current_insurer_complaints": int(
                base_rows["matched_current_insurer_complaints"]
            ),
            "outside_157_complaints": int(base_rows["outside_157_complaints"]),
            "ambiguous_complaints": int(base_rows["ambiguous_complaints"]),
            "unresolved_complaints": base_unresolved,
        },
        "receita_delta": dict(deltas),
        "post_receita": {
            "complaints_total": source_total,
            "matched_current_insurer_complaints": matched_total,
            "matched_current_insurer_ratio": matched_total / source_total,
            "outside_157_complaints": outside_total,
            "ambiguous_complaints": ambiguous_total,
            "unresolved_complaints": unresolved_total,
            "unresolved_ratio": unresolved_total / source_total,
            "distinct_unresolved_provider_names": len(still_unresolved),
        },
        "receita_resolved_providers": sorted(
            resolved_rows,
            key=lambda row: (-int(row["complaints"]), str(row["provider"])),
        ),
        "unresolved_providers": sorted(
            still_unresolved,
            key=lambda row: (-int(row["complaints"]), str(row["provider"])),
        ),
    }


def main() -> None:
    payload = build_experiment()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp = OUTPUT_PATH.with_suffix(OUTPUT_PATH.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(OUTPUT_PATH)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
