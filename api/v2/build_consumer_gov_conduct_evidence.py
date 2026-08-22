from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.build_consumidor_gov import (
    CG_MIN_MONTH_BYTES,
    RAW_DIR,
    TARGET_SEGMENT,
    _download,
    _iter_rows,
    _list_basecompleta_resources,
)
from api.utils.name_cleaner import normalize_name_key
from api.v2.build_consumer_gov_157_experiment import (
    ELIGIBILITY_PATH,
    _build_indexes,
    _match_provider,
)
from api.v2.build_consumer_gov_identity_experiment import (
    OUTPUT_PATH as BASE_IDENTITY_PATH,
    _build_non_brand_fallback_indexes,
    _resolve_provider_temporal_brand,
)
from api.v2.consumer_gov_conduct import (
    accumulate_row,
    build_conduct_film,
    finalize_month_evidence,
    new_month_evidence,
    normalize_text,
    row_value,
    TAXONOMY_COLUMNS,
)
from api.v2.consumer_gov_identity import (
    load_provider_resolution_registry,
    resolve_curated_provider,
)
from api.v2.consumer_gov_receita_resolution import (
    DEFAULT_RECEITA_IDENTITY_SNAPSHOT,
    load_receita_identity_snapshot,
    load_verified_receita_provider_hints,
    resolve_provider_via_receita_payload,
)
from api.v2.consumer_gov_temporal_brand import build_temporal_brand_index
from api.v2.consumer_gov_universe_resolution import (
    build_full_universe_provider_index,
    resolve_provider_against_full_universe,
)

OUTPUT_PATH = Path("data/derived/v2/consumer_gov_conduct_evidence.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _provider_name(row: dict[str, Any]) -> str:
    return str(
        row.get("Nome Fantasia")
        or row.get("Empresa")
        or row.get("Fornecedor")
        or ""
    ).strip()


def _eligible_entities(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        entity
        for entity in payload.get("entities") or []
        if (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
    ]


def _resolve_provider(
    provider: str,
    month: str,
    indexes: dict[str, Any],
    registry: dict[str, Any],
    temporal_brand_index: dict[str, Any],
    full_universe_index: dict[str, Any],
    fallback_indexes: dict[str, Any],
    receita_payload: dict[str, Any] | None,
    receita_hints: dict[str, dict[str, Any]],
    all_entities: list[dict[str, Any]],
) -> dict[str, Any]:
    curated = resolve_curated_provider(provider, indexes["cnpj"], registry)
    if curated is not None:
        return {
            "resolution_state": str(curated["resolution_state"]),
            "entity_id": curated.get("entity_id"),
            "match_method": "curated_source_backed_resolution",
        }

    temporal = _resolve_provider_temporal_brand(
        provider,
        provider,
        month,
        temporal_brand_index,
    )
    if temporal is not None:
        return {
            "resolution_state": str(temporal["resolution_state"]),
            "entity_id": temporal.get("entity_id"),
            "match_method": str(temporal["match_method"]),
        }

    canonical = resolve_provider_against_full_universe(provider, full_universe_index)
    if canonical is not None:
        return {
            "resolution_state": str(canonical["resolution_state"]),
            "entity_id": canonical.get("entity_id"),
            "match_method": str(canonical["match_method"]),
        }

    entry = {"display_name": provider, "name": provider, "cnpj": ""}
    entity_id, method = _match_provider(provider, entry, fallback_indexes)
    if entity_id:
        return {
            "resolution_state": "matched_current_insurer",
            "entity_id": str(entity_id),
            "match_method": method,
        }

    receita = resolve_provider_via_receita_payload(
        provider,
        receita_payload,
        all_entities,
        verified_hints=receita_hints,
    )
    if receita is not None:
        return {
            "resolution_state": str(receita["resolution_state"]),
            "entity_id": receita.get("entity_id"),
            "match_method": str(receita.get("match_method") or "receita"),
        }

    return {
        "resolution_state": "unresolved",
        "entity_id": None,
        "match_method": "unresolved",
    }


def _ensure_raw_csvs(months: list[str]) -> dict[str, Any]:
    resources = _list_basecompleta_resources()
    missing_resources = [month for month in months if month not in resources]
    if missing_resources:
        raise RuntimeError(
            f"Consumer.gov resources missing for months: {missing_resources}"
        )

    result: dict[str, Any] = {}
    for month in months:
        resource = resources[month]
        raw_csv = RAW_DIR / f"basecompleta_{month}.csv"
        if not raw_csv.exists() or raw_csv.stat().st_size < CG_MIN_MONTH_BYTES:
            _download(resource.url, raw_csv)
        if not raw_csv.exists() or raw_csv.stat().st_size < CG_MIN_MONTH_BYTES:
            raise RuntimeError(f"invalid Consumer.gov raw CSV for {month}: {raw_csv}")
        result[month] = {
            "path": raw_csv,
            "resource_url": resource.url,
            "resource_name": resource.name,
            "bytes": raw_csv.stat().st_size,
        }
    return result


def _sum_taxonomy(
    target: dict[str, Counter[str]],
    month_payload: dict[str, Any],
) -> None:
    for key in TAXONOMY_COLUMNS:
        for label, count in (
            (month_payload.get("taxonomy") or {}).get(key) or {}
        ).items():
            target[key][str(label)] += int(count or 0)


def _sorted_counter(counter: Counter[str]) -> dict[str, int]:
    return {
        key: int(value)
        for key, value in sorted(
            counter.items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )
    }


def _entity_totals(months: list[dict[str, Any]]) -> dict[str, Any]:
    complaints = sum(int(item.get("complaints") or 0) for item in months)
    responded = sum(int(item.get("responded") or 0) for item in months)
    finalized = sum(int(item.get("finalized") or 0) for item in months)
    evaluated = sum(int(item.get("evaluated") or 0) for item in months)
    resolved = sum(int(item.get("consumer_resolved") or 0) for item in months)
    not_resolved = sum(
        int(item.get("consumer_not_resolved") or 0) for item in months
    )
    satisfaction_count = sum(
        int(item.get("satisfaction_count") or 0) for item in months
    )
    satisfaction_sum = sum(
        float(item.get("average_satisfaction") or 0.0)
        * int(item.get("satisfaction_count") or 0)
        for item in months
    )
    response_time_count = sum(
        int(item.get("response_time_count") or 0) for item in months
    )
    response_time_sum = sum(
        float(item.get("average_response_time_days") or 0.0)
        * int(item.get("response_time_count") or 0)
        for item in months
    )
    return {
        "complaints": complaints,
        "responded": responded,
        "response_rate": responded / complaints if complaints else None,
        "finalized": finalized,
        "finalized_rate": finalized / complaints if complaints else None,
        "evaluated": evaluated,
        "consumer_resolved": resolved,
        "consumer_not_resolved": not_resolved,
        "consumer_resolved_rate_among_evaluated": (
            resolved / evaluated if evaluated else None
        ),
        "satisfaction_count": satisfaction_count,
        "average_satisfaction": (
            satisfaction_sum / satisfaction_count if satisfaction_count else None
        ),
        "response_time_count": response_time_count,
        "average_response_time_days": (
            response_time_sum / response_time_count if response_time_count else None
        ),
    }


def build_conduct_evidence() -> dict[str, Any]:
    eligibility = json.loads(ELIGIBILITY_PATH.read_text(encoding="utf-8"))
    base_identity = json.loads(BASE_IDENTITY_PATH.read_text(encoding="utf-8"))
    receita_payload = load_receita_identity_snapshot(DEFAULT_RECEITA_IDENTITY_SNAPSHOT)

    months = [
        str(month)
        for month in (base_identity.get("source") or {}).get("months") or []
    ]
    if not months:
        raise RuntimeError("Consumer.gov identity artifact has no months")
    months = sorted(months)

    eligible = _eligible_entities(eligibility)
    eligible_by_id = {str(entity["entity_id"]): entity for entity in eligible}
    indexes = _build_indexes(eligibility)
    registry = load_provider_resolution_registry()
    temporal_brand_index = build_temporal_brand_index(
        list(eligibility.get("brands") or []),
        set(indexes["entity_by_id"]),
    )
    full_universe_index = build_full_universe_provider_index(
        list(eligibility.get("entities") or [])
    )
    fallback_indexes = _build_non_brand_fallback_indexes(
        indexes,
        temporal_brand_index,
    )
    receita_hints = load_verified_receita_provider_hints()
    all_entities = list(eligibility.get("entities") or [])

    raw_files = _ensure_raw_csvs(months)

    entity_month_raw: dict[str, dict[str, dict[str, Any]]] = {
        entity_id: {month: new_month_evidence(month) for month in months}
        for entity_id in eligible_by_id
    }
    market_month_totals: Counter[str] = Counter()
    source_month_totals: Counter[str] = Counter()
    resolution_counts: Counter[str] = Counter()
    resolution_methods: Counter[str] = Counter()
    taxonomy_catalog: dict[str, Counter[str]] = {
        key: Counter() for key in TAXONOMY_COLUMNS
    }
    source_columns: set[str] = set()
    provider_cache: dict[tuple[str, str], dict[str, Any]] = {}
    unresolved_names: Counter[str] = Counter()
    ambiguous_names: Counter[str] = Counter()
    outside_names: Counter[str] = Counter()

    target_segment = normalize_text(TARGET_SEGMENT)

    for month in months:
        path = raw_files[month]["path"]
        for row in _iter_rows(path):
            source_columns.update(str(key) for key in row)
            if normalize_text(row.get("Segmento de Mercado")) != target_segment:
                continue

            situation = normalize_text(row.get("Situação"))
            if "cancelada" in situation:
                continue

            provider = _provider_name(row)
            if not provider:
                continue

            source_month_totals[month] += 1
            cache_key = (month, normalize_name_key(provider))
            resolution = provider_cache.get(cache_key)
            if resolution is None:
                resolution = _resolve_provider(
                    provider,
                    month,
                    indexes,
                    registry,
                    temporal_brand_index,
                    full_universe_index,
                    fallback_indexes,
                    receita_payload,
                    receita_hints,
                    all_entities,
                )
                provider_cache[cache_key] = resolution

            state = str(resolution["resolution_state"])
            method = str(resolution["match_method"])
            resolution_counts[state] += 1
            resolution_methods[method] += 1

            if state != "matched_current_insurer":
                if state == "outside_157":
                    outside_names[provider] += 1
                elif state == "ambiguous":
                    ambiguous_names[provider] += 1
                else:
                    unresolved_names[provider] += 1
                continue

            entity_id = str(resolution.get("entity_id") or "")
            if entity_id not in eligible_by_id:
                raise RuntimeError(
                    "resolved Consumer.gov provider to non-eligible entity: "
                    f"{provider} -> {entity_id}"
                )

            market_month_totals[month] += 1
            month_evidence = entity_month_raw[entity_id][month]
            accumulate_row(month_evidence, row)

            for key, aliases in TAXONOMY_COLUMNS.items():
                label = row_value(row, aliases)
                if label:
                    taxonomy_catalog[key][label] += 1

    entities_out: list[dict[str, Any]] = []
    film_signals: Counter[str] = Counter()
    history_states: Counter[str] = Counter()
    observed_entities = 0

    for entity_id, entity in sorted(
        eligible_by_id.items(),
        key=lambda item: str(item[1].get("legal_name") or item[0]),
    ):
        monthly = [
            finalize_month_evidence(
                entity_month_raw[entity_id][month],
                matched_current_insurer_market_complaints=market_month_totals[month],
            )
            for month in months
        ]
        totals = _entity_totals(monthly)
        if totals["complaints"] > 0:
            observed_entities += 1
        film = build_conduct_film(monthly)
        film_signals[str(film["conduct_signal"])] += 1
        history_states[str(film["history_state"])] += 1

        aggregate_taxonomy: dict[str, Counter[str]] = {
            key: Counter() for key in TAXONOMY_COLUMNS
        }
        for month_payload in monthly:
            _sum_taxonomy(aggregate_taxonomy, month_payload)

        entities_out.append(
            {
                "entity_id": entity_id,
                "fip_code": entity.get("fip_code"),
                "cnpj": entity.get("cnpj"),
                "legal_name": entity.get("legal_name"),
                "display_name": entity.get("display_name"),
                "totals": totals,
                "taxonomy": {
                    key: _sorted_counter(counter)
                    for key, counter in aggregate_taxonomy.items()
                },
                "monthly": monthly,
                "film": film,
            }
        )

    matched_total = int(resolution_counts["matched_current_insurer"])
    if sum(int(item["totals"]["complaints"]) for item in entities_out) != matched_total:
        raise RuntimeError("matched complaint totals do not reconcile to entity evidence")

    source_total = sum(source_month_totals.values())
    if sum(resolution_counts.values()) != source_total:
        raise RuntimeError("resolution states do not reconcile to source complaint total")

    return {
        "artifact": "v2_consumer_gov_conduct_evidence",
        "generated_at": _utc_now(),
        "status": "experimental",
        "assessment_role": "conduct_evidence_and_longitudinal_diagnostic_only",
        "scoring": "forbidden_in_this_artifact",
        "source": {
            "dataset": "reclamacoes-do-consumidor-gov-br",
            "segment": TARGET_SEGMENT,
            "months": months,
            "raw_resources": {
                month: {key: value for key, value in metadata.items() if key != "path"}
                for month, metadata in raw_files.items()
            },
            "source_columns_seen": sorted(source_columns),
            "taxonomy_fields": list(TAXONOMY_COLUMNS),
        },
        "identity": {
            "canonical_regulatory_inventory": str(ELIGIBILITY_PATH),
            "base_consumer_identity_artifact": str(BASE_IDENTITY_PATH),
            "receita_identity_artifact": str(DEFAULT_RECEITA_IDENTITY_SNAPSHOT),
            "policy": (
                "Same deterministic/source-backed resolution order as the completed "
                "Consumer.gov identity stage, with Receita used only after the base "
                "resolver remains unresolved. Fuzzy similarity never assigns evidence."
            ),
        },
        "semantics": {
            "complaint": (
                "Observed consumer allegation/friction event in Consumer.gov; not an "
                "adjudicated finding of abuse, wrongful claim denial or contractual breach."
            ),
            "complaint_share": (
                "Share among matched current-insurer complaints on Consumer.gov. It controls "
                "for platform-wide complaint volume only; it is not normalized by policies, "
                "customers, premiums or insured exposure and therefore is diagnostic only."
            ),
            "consumer_resolved": (
                "Consumer-provided evaluation of whether the complaint was resolved, among "
                "complaints that received that evaluation."
            ),
            "satisfaction": (
                "Consumer score from 1 to 5 among complaints with a recorded score; sample "
                "size is always preserved and small samples are not treated as strong evidence."
            ),
            "taxonomy": (
                "Official Consumer.gov classification preserved as Área → Assunto → "
                "Grupo Problema → Problema, plus acquisition/origin channel context."
            ),
        },
        "universe": {
            "regulatory_eligible_insurers": len(eligible),
            "observed_insurers": observed_entities,
            "coverage_ratio": observed_entities / len(eligible) if eligible else None,
        },
        "resolution_summary": {
            "complaints_total": source_total,
            "matched_current_insurer_complaints": matched_total,
            "matched_current_insurer_ratio": matched_total / source_total if source_total else None,
            "outside_157_complaints": int(resolution_counts["outside_157"]),
            "ambiguous_complaints": int(resolution_counts["ambiguous"]),
            "unresolved_complaints": int(resolution_counts["unresolved"]),
            "resolution_states": dict(resolution_counts),
            "resolution_methods": dict(resolution_methods),
            "top_outside_157_names": outside_names.most_common(25),
            "top_ambiguous_names": ambiguous_names.most_common(25),
            "top_unresolved_names": unresolved_names.most_common(25),
        },
        "market_months": [
            {
                "month": month,
                "segment_complaints_after_cancelled_exclusion": int(source_month_totals[month]),
                "matched_current_insurer_complaints": int(market_month_totals[month]),
                "matched_ratio": (
                    market_month_totals[month] / source_month_totals[month]
                    if source_month_totals[month]
                    else None
                ),
            }
            for month in months
        ],
        "taxonomy_catalog": {
            key: _sorted_counter(counter)
            for key, counter in taxonomy_catalog.items()
        },
        "film_summary": {
            "history_states": dict(history_states),
            "conduct_signals": dict(film_signals),
        },
        "entities": entities_out,
    }


def main() -> None:
    payload = build_conduct_evidence()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp = OUTPUT_PATH.with_suffix(OUTPUT_PATH.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp.replace(OUTPUT_PATH)
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "source": {
                    "months": payload["source"]["months"],
                    "taxonomy_fields": payload["source"]["taxonomy_fields"],
                },
                "universe": payload["universe"],
                "resolution_summary": payload["resolution_summary"],
                "film_summary": payload["film_summary"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
