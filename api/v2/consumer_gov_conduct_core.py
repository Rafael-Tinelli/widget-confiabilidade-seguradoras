from __future__ import annotations

import collections
from collections.abc import Callable
from pathlib import Path
from typing import Any

from api import build_consumidor_gov as consumer_gov_build
from api.sources.consumer_gov_direct import validate_month_csv
from api.utils.name_cleaner import normalize_name_key
from api.v2 import consumer_gov_conduct

CORE_SOURCE_ROLE = "legacy_basecompleta_monthly_aggregate"
TAXONOMY_ROLE = "optional_basecompleta_raw_enrichment"


def _int_stat(stats: dict[str, Any], *names: str) -> int:
    for name in names:
        value = stats.get(name)
        if value not in (None, ""):
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    return 0


def _required_nonnegative_int_stat(
    stats: dict[str, Any],
    *names: str,
    field: str,
) -> int:
    """Read a required preserved count without converting missing evidence to zero."""
    for name in names:
        if name not in stats:
            continue
        value = stats.get(name)
        if value in (None, ""):
            continue
        try:
            number = int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"consumer_gov_core_source_invalid: non-integer {field}: {value!r}"
            ) from exc
        if number < 0:
            raise RuntimeError(
                f"consumer_gov_core_source_invalid: negative {field}: {number}"
            )
        return number
    raise RuntimeError(
        "consumer_gov_core_source_invalid: required preserved statistic missing: "
        f"{field}"
    )


def _float_stat(stats: dict[str, Any], *names: str) -> float:
    for name in names:
        value = stats.get(name)
        if value not in (None, ""):
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return 0.0


def load_monthly_entries(
    month: str,
    *,
    monthly_dir: Path = consumer_gov_build.MONTHLY_DIR,
) -> tuple[dict[str, Any], dict[str, Any], Path]:
    path = monthly_dir / f"consumidor_gov_{month}.json.gz"
    root = consumer_gov_build._load_valid_monthly(path, month)
    if root is None:
        raise RuntimeError(
            "consumer_gov_core_source_unavailable: valid preserved monthly aggregate "
            f"is required for {month}: {path}"
        )
    entries = consumer_gov_build._entries_from_root(root)
    if not entries:
        raise RuntimeError(
            "consumer_gov_core_source_invalid: monthly aggregate has no provider entries "
            f"for {month}: {path}"
        )
    return root, entries, path


def aggregate_entry_to_month(
    month: str,
    entry: dict[str, Any],
    *,
    matched_current_insurer_market_complaints: int,
) -> dict[str, Any]:
    stats = entry.get("statistics") if isinstance(entry.get("statistics"), dict) else {}
    # An internally initialized insurer/month with no matched provider is a
    # legitimate observed zero after the complete source scan. A non-empty
    # provider statistics object without its complaint total is malformed and
    # must fail closed instead of becoming zero.
    complaints = (
        0
        if not stats
        else _required_nonnegative_int_stat(
            stats,
            "complaintsCount",
            "total_claims",
            field="complaints",
        )
    )
    responded = _int_stat(stats, "respondedCount", "responded_claims")
    finalized = _int_stat(stats, "finalizedCount", "finalized_claims")
    resolved = _int_stat(stats, "resolvedCount", "resolved_claims")
    satisfaction_count = _int_stat(stats, "satisfactionCount")
    satisfaction_sum = _float_stat(stats, "scoreSum")

    return {
        "month": month,
        "core_source_role": CORE_SOURCE_ROLE,
        "complaints": complaints,
        "matched_current_insurer_market_complaints": int(
            matched_current_insurer_market_complaints
        ),
        "complaint_share_among_matched_current_insurers": (
            complaints / matched_current_insurer_market_complaints
            if matched_current_insurer_market_complaints > 0
            else None
        ),
        "responded": responded,
        "response_rate": responded / complaints if complaints else None,
        "finalized": finalized,
        "finalized_rate": finalized / complaints if complaints else None,
        "evaluated": 0,
        "consumer_resolved": resolved,
        "consumer_not_resolved": 0,
        "consumer_resolved_rate_among_evaluated": None,
        "consumer_resolution_denominator_state": (
            "not_preserved_in_legacy_monthly_aggregate"
        ),
        "satisfaction_count": satisfaction_count,
        "average_satisfaction": (
            satisfaction_sum / satisfaction_count if satisfaction_count else None
        ),
        "response_time_count": 0,
        "average_response_time_days": None,
        "response_time_state": "not_preserved_in_legacy_monthly_aggregate",
        "consumer_contacted_company_yes": 0,
        "consumer_contacted_company_no": 0,
        "consumer_contacted_company_yes_rate": None,
        "consumer_contacted_company_state": (
            "not_preserved_in_legacy_monthly_aggregate"
        ),
        "taxonomy": {key: {} for key in consumer_gov_conduct.TAXONOMY_COLUMNS},
        "situacao": {},
        "avaliacao_reclamacao": {},
        "analise_recusa": {},
    }


def add_entry_statistics(
    target: dict[str, Any],
    entry: dict[str, Any],
) -> int:
    stats = entry.get("statistics") if isinstance(entry.get("statistics"), dict) else {}
    complaints = _required_nonnegative_int_stat(
        stats,
        "complaintsCount",
        "total_claims",
        field="complaints",
    )
    target["complaintsCount"] = int(target.get("complaintsCount") or 0) + complaints
    target["respondedCount"] = int(target.get("respondedCount") or 0) + _int_stat(
        stats, "respondedCount", "responded_claims"
    )
    target["finalizedCount"] = int(target.get("finalizedCount") or 0) + _int_stat(
        stats, "finalizedCount", "finalized_claims"
    )
    target["resolvedCount"] = int(target.get("resolvedCount") or 0) + _int_stat(
        stats, "resolvedCount", "resolved_claims"
    )
    target["satisfactionCount"] = int(target.get("satisfactionCount") or 0) + _int_stat(
        stats, "satisfactionCount"
    )
    target["scoreSum"] = float(target.get("scoreSum") or 0.0) + _float_stat(
        stats, "scoreSum"
    )
    return complaints


def empty_taxonomy_counters(
    entity_ids: list[str],
    months: list[str],
) -> dict[str, dict[str, dict[str, collections.Counter[str]]]]:
    return {
        entity_id: {
            month: {
                key: collections.Counter()
                for key in consumer_gov_conduct.TAXONOMY_COLUMNS
            }
            for month in months
        }
        for entity_id in entity_ids
    }


def taxonomy_cache_state(
    months: list[str],
    *,
    raw_dir: Path = consumer_gov_build.RAW_DIR,
    min_month_bytes: int = consumer_gov_build.CG_MIN_MONTH_BYTES,
) -> dict[str, Any]:
    available: list[str] = []
    missing: list[str] = []
    files: dict[str, Path] = {}
    for month in months:
        path = raw_dir / f"basecompleta_{month}.csv"
        if path.exists() and path.stat().st_size >= min_month_bytes:
            available.append(month)
            files[month] = path
        else:
            missing.append(month)

    if missing:
        return {
            "state": "source_unavailable",
            "role": TAXONOMY_ROLE,
            "required_for_conduct_core": False,
            "required_for_scoring": False,
            "available_months": available,
            "missing_months": missing,
            "files": files,
        }

    return {
        "state": "candidate_cache_complete",
        "role": TAXONOMY_ROLE,
        "required_for_conduct_core": False,
        "required_for_scoring": False,
        "available_months": available,
        "missing_months": [],
        "files": files,
    }


def build_cached_taxonomy_enrichment(
    months: list[str],
    entity_ids: list[str],
    resolve_provider: Callable[[str, str], dict[str, Any]],
    *,
    core_source_month_totals: dict[str, int],
    core_market_month_totals: dict[str, int],
    raw_dir: Path = consumer_gov_build.RAW_DIR,
    min_month_bytes: int = consumer_gov_build.CG_MIN_MONTH_BYTES,
) -> tuple[
    dict[str, Any],
    dict[str, dict[str, dict[str, collections.Counter[str]]]],
    dict[str, collections.Counter[str]],
]:
    state = taxonomy_cache_state(
        months,
        raw_dir=raw_dir,
        min_month_bytes=min_month_bytes,
    )
    counters = empty_taxonomy_counters(entity_ids, months)
    catalog = {
        key: collections.Counter()
        for key in consumer_gov_conduct.TAXONOMY_COLUMNS
    }
    if state["state"] == "source_unavailable":
        state.pop("files", None)
        return state, counters, catalog

    source_columns: set[str] = set()
    raw_resources: dict[str, Any] = {}
    raw_source_totals: collections.Counter[str] = collections.Counter()
    raw_market_totals: collections.Counter[str] = collections.Counter()
    target_segment = consumer_gov_conduct.normalize_text(
        consumer_gov_build.TARGET_SEGMENT
    )

    for month in months:
        path = state["files"][month]
        validation = validate_month_csv(path, month)

        raw_resources[month] = {
            "path": str(path),
            "bytes": validation.bytes,
            "rows": validation.rows,
            "sha256": validation.sha256,
            "schema_version": validation.schema_version,
        }

        provider_cache: dict[str, dict[str, Any]] = {}
        for row in consumer_gov_build._iter_rows(path):
            source_columns.update(str(key) for key in row)
            if (
                consumer_gov_conduct.normalize_text(row.get("Segmento de Mercado"))
                != target_segment
            ):
                continue
            if "cancelada" in consumer_gov_conduct.normalize_text(row.get("Situação")):
                continue

            provider = str(
                row.get("Nome Fantasia")
                or row.get("Empresa")
                or row.get("Fornecedor")
                or ""
            ).strip()
            if not provider:
                continue

            raw_source_totals[month] += 1
            key = normalize_name_key(provider)
            resolution = provider_cache.get(key)
            if resolution is None:
                resolution = resolve_provider(provider, month)
                provider_cache[key] = resolution

            if str(resolution.get("resolution_state")) != "matched_current_insurer":
                continue
            entity_id = str(resolution.get("entity_id") or "")
            if entity_id not in counters:
                raise RuntimeError(
                    "taxonomy enrichment resolved provider to non-core entity: "
                    f"{provider} -> {entity_id}"
                )
            raw_market_totals[month] += 1

            for taxonomy_key, aliases in consumer_gov_conduct.TAXONOMY_COLUMNS.items():
                label = consumer_gov_conduct.row_value(row, aliases)
                if not label:
                    continue
                counters[entity_id][month][taxonomy_key][label] += 1
                catalog[taxonomy_key][label] += 1

    for month in months:
        if int(raw_source_totals[month]) != int(core_source_month_totals.get(month, 0)):
            raise RuntimeError(
                "taxonomy_raw_population_mismatch: source complaint total differs from "
                f"preserved core for {month}: {raw_source_totals[month]} != "
                f"{core_source_month_totals.get(month, 0)}"
            )
        if int(raw_market_totals[month]) != int(core_market_month_totals.get(month, 0)):
            raise RuntimeError(
                "taxonomy_raw_population_mismatch: matched current-insurer total differs "
                f"from preserved core for {month}: {raw_market_totals[month]} != "
                f"{core_market_month_totals.get(month, 0)}"
            )

    return (
        {
            "state": "available",
            "role": TAXONOMY_ROLE,
            "required_for_conduct_core": False,
            "required_for_scoring": False,
            "available_months": list(months),
            "missing_months": [],
            "raw_resources": raw_resources,
            "source_columns_seen": sorted(source_columns),
            "population_reconciliation": "exact_monthly_match_to_preserved_core",
        },
        counters,
        catalog,
    )
