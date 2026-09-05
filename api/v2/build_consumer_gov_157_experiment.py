from __future__ import annotations

import gzip
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import get_name_tokens, normalize_name_key

ELIGIBILITY_PATH = Path("data/derived/v2/entity_eligibility_inventory.json")
MONTHLY_DIR = Path("data/derived/consumidor_gov/monthly")
OUTPUT_PATH = Path("data/derived/v2/consumer_gov_157_experiment.json")
MONTHS_BACK = 12

GENERIC_NAME_TOKENS = {
    "sa",
    "s",
    "a",
    "ltda",
    "cia",
    "companhia",
    "sociedade",
    "brasil",
    "brasileira",
    "seguro",
    "seguros",
    "seguradora",
    "seguradoras",
    "gerais",
    "de",
    "da",
    "do",
    "das",
    "dos",
    "e",
    "em",
    "para",
    "participacoes",
    "holding",
    "grupo",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json_gz(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rb") as file:
        value = json.loads(file.read().decode("utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} does not contain a JSON object")
    return value


def _entries(root: dict[str, Any]) -> dict[str, Any]:
    value = (
        root.get("by_name_key_raw")
        or root.get("by_name_key")
        or root.get("by_name")
        or {}
    )
    return value if isinstance(value, dict) else {}


def _valid_monthly_snapshot(path: Path) -> tuple[str, dict[str, Any]] | None:
    try:
        root = _read_json_gz(path)
    except (OSError, UnicodeError, ValueError, TypeError):
        return None
    meta = root.get("meta") if isinstance(root.get("meta"), dict) else {}
    if meta.get("invalid") is True or str(meta.get("status") or "").lower() == "invalid":
        return None
    month = str(meta.get("month") or meta.get("ym") or "")
    if not month or not _entries(root):
        return None
    return month, root


def _load_latest_months() -> list[tuple[str, Path, dict[str, Any]]]:
    valid: dict[str, tuple[Path, dict[str, Any]]] = {}
    for path in sorted(MONTHLY_DIR.glob("consumidor_gov_20??-??.json.gz")):
        item = _valid_monthly_snapshot(path)
        if item is None:
            continue
        month, root = item
        valid[month] = (path, root)
    selected = sorted(valid)[-MONTHS_BACK:]
    if len(selected) < MONTHS_BACK:
        raise RuntimeError(
            f"expected at least {MONTHS_BACK} valid monthly snapshots, got {len(selected)}"
        )
    return [(month, valid[month][0], valid[month][1]) for month in selected]


def _core_name(name: str) -> str:
    tokens = [
        token
        for token in normalize_name_key(name).split()
        if token not in GENERIC_NAME_TOKENS and len(token) > 1
    ]
    return " ".join(tokens)


def _build_unique_index(
    pairs: list[tuple[str, str]],
) -> tuple[dict[str, str], dict[str, list[str]]]:
    raw: dict[str, set[str]] = defaultdict(set)
    for key, entity_id in pairs:
        if key:
            raw[key].add(entity_id)
    unique = {key: next(iter(ids)) for key, ids in raw.items() if len(ids) == 1}
    ambiguous = {key: sorted(ids) for key, ids in raw.items() if len(ids) > 1}
    return unique, ambiguous


def _entity_names(entity: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for field in (
        "legal_name",
        "display_name",
        "name",
        "regulatory_name",
        "source_name",
    ):
        value = str(entity.get(field) or "").strip()
        if value:
            names.append(value)

    evidence = entity.get("evidence") or {}
    for source in ("licensed", "ses_identity", "ses"):
        block = evidence.get(source) or {}
        if not isinstance(block, dict):
            continue
        for field in ("legal_name", "name", "source_name"):
            value = str(block.get(field) or "").strip()
            if value:
                names.append(value)
    return sorted(set(names))


def _build_indexes(payload: dict[str, Any]) -> dict[str, Any]:
    eligible = [
        entity
        for entity in payload.get("entities") or []
        if (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
    ]
    entity_by_id = {str(entity["entity_id"]): entity for entity in eligible}
    cnpj_to_entity = {
        normalize_cnpj_v2(entity.get("cnpj")): str(entity["entity_id"])
        for entity in eligible
        if normalize_cnpj_v2(entity.get("cnpj"))
    }

    exact_pairs: list[tuple[str, str]] = []
    core_pairs: list[tuple[str, str]] = []
    all_names_by_entity: dict[str, list[str]] = defaultdict(list)
    for entity in eligible:
        entity_id = str(entity["entity_id"])
        for name in _entity_names(entity):
            all_names_by_entity[entity_id].append(name)
            exact_pairs.append((normalize_name_key(name), entity_id))
            core_pairs.append((_core_name(name), entity_id))

    verified_brand_pairs: list[tuple[str, str]] = []
    for brand in payload.get("brands") or []:
        targets = {
            str(relation.get("target_entity_id"))
            for relation in brand.get("relationships") or []
            if relation.get("target_entity_id") in entity_by_id
            and relation.get("relationship_type") in {"brand_of", "risk_carrier"}
        }
        if len(targets) != 1:
            continue
        entity_id = next(iter(targets))
        for name in [brand.get("name"), *(brand.get("aliases") or [])]:
            value = str(name or "").strip()
            if not value:
                continue
            verified_brand_pairs.append((normalize_name_key(value), entity_id))
            core_pairs.append((_core_name(value), entity_id))
            all_names_by_entity[entity_id].append(value)

    exact, exact_ambiguous = _build_unique_index(exact_pairs)
    brand, brand_ambiguous = _build_unique_index(verified_brand_pairs)
    core, core_ambiguous = _build_unique_index(core_pairs)
    return {
        "eligible": eligible,
        "entity_by_id": entity_by_id,
        "cnpj": cnpj_to_entity,
        "exact_name": exact,
        "verified_brand": brand,
        "core_name": core,
        "all_names_by_entity": {
            key: sorted(set(value)) for key, value in all_names_by_entity.items()
        },
        "ambiguities": {
            "exact_name": exact_ambiguous,
            "verified_brand": brand_ambiguous,
            "core_name": core_ambiguous,
        },
    }


def _match_provider(
    provider_key: str,
    entry: dict[str, Any],
    indexes: dict[str, Any],
) -> tuple[str | None, str]:
    cnpj = normalize_cnpj_v2(entry.get("cnpj"))
    if cnpj and cnpj in indexes["cnpj"]:
        return indexes["cnpj"][cnpj], "cnpj_exact"

    key = normalize_name_key(provider_key)
    display = str(entry.get("display_name") or entry.get("name") or provider_key)
    display_key = normalize_name_key(display)
    for candidate_key in (key, display_key):
        if candidate_key and candidate_key in indexes["exact_name"]:
            return indexes["exact_name"][candidate_key], "legal_name_exact"
        if candidate_key and candidate_key in indexes["verified_brand"]:
            return indexes["verified_brand"][candidate_key], "verified_brand_exact"

    for candidate in (display, provider_key):
        core = _core_name(candidate)
        if core and len(core) >= 4 and core in indexes["core_name"]:
            return indexes["core_name"][core], "unique_core_name"
    return None, "unmatched"


def _candidate_suggestions(
    provider: str,
    indexes: dict[str, Any],
    limit: int = 3,
) -> list[dict[str, Any]]:
    provider_key = normalize_name_key(provider)
    provider_tokens = set(get_name_tokens(provider))
    candidates: list[tuple[float, str, str]] = []
    for entity_id, names in indexes["all_names_by_entity"].items():
        best = 0.0
        best_name = ""
        for name in names:
            name_key = normalize_name_key(name)
            sequence = SequenceMatcher(None, provider_key, name_key).ratio()
            target_tokens = set(get_name_tokens(name))
            union = provider_tokens | target_tokens
            jaccard = (
                len(provider_tokens & target_tokens) / len(union) if union else 0.0
            )
            score = max(sequence, jaccard)
            if score > best:
                best = score
                best_name = name
        candidates.append((best, entity_id, best_name))
    return [
        {
            "entity_id": entity_id,
            "name": name,
            "diagnostic_similarity": round(score, 4),
        }
        for score, entity_id, name in sorted(candidates, reverse=True)[:limit]
        if score >= 0.55
    ]


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _new_entity_stats(entity: dict[str, Any]) -> dict[str, Any]:
    return {
        "entity_id": entity.get("entity_id"),
        "fip_code": entity.get("fip_code"),
        "cnpj": entity.get("cnpj"),
        "legal_name": entity.get("legal_name"),
        "months_with_complaints": 0,
        "complaints": 0,
        "responded": 0,
        "finalized": 0,
        "consumer_resolved": 0,
        "satisfaction_count": 0,
        "satisfaction_sum": 0.0,
        "match_methods": {},
        "by_month": {},
    }


def _stats(entry: dict[str, Any]) -> dict[str, Any]:
    value = entry.get("statistics") or {}
    return value if isinstance(value, dict) else {}


def build_experiment() -> dict[str, Any]:
    eligibility = json.loads(ELIGIBILITY_PATH.read_text(encoding="utf-8"))
    indexes = _build_indexes(eligibility)
    eligible = indexes["eligible"]
    if len(eligible) != 157:
        raise RuntimeError(f"expected 157 regulatory eligible insurers, got {len(eligible)}")

    monthly = _load_latest_months()
    entity_stats = {
        str(entity["entity_id"]): _new_entity_stats(entity) for entity in eligible
    }
    match_method_counts: Counter[str] = Counter()
    unmatched_provider_rows: Counter[str] = Counter()
    monthly_meta: list[dict[str, Any]] = []
    provider_entries_total = 0
    complaints_total = 0
    matched_complaints = 0
    snapshot_has_cnpj = False
    resolution_without_satisfaction = 0

    for month, path, root in monthly:
        entries = _entries(root)
        month_entity_counts: Counter[str] = Counter()
        month_methods: Counter[str] = Counter()
        month_provider_entries = 0
        month_complaints = 0
        month_matched_complaints = 0

        for provider_key, entry in entries.items():
            if not isinstance(entry, dict):
                continue
            month_provider_entries += 1
            provider_entries_total += 1
            stat = _stats(entry)
            complaints = _safe_int(
                stat.get("complaintsCount") or stat.get("total_claims")
            )
            if complaints <= 0:
                continue
            month_complaints += complaints
            complaints_total += complaints

            if normalize_cnpj_v2(entry.get("cnpj")):
                snapshot_has_cnpj = True

            entity_id, method = _match_provider(str(provider_key), entry, indexes)
            match_method_counts[method] += complaints
            month_methods[method] += complaints
            if not entity_id:
                display = str(
                    entry.get("display_name") or entry.get("name") or provider_key
                ).strip()
                if display:
                    unmatched_provider_rows[display] += complaints
                continue

            matched_complaints += complaints
            month_matched_complaints += complaints
            month_entity_counts[entity_id] += complaints
            stats = entity_stats[entity_id]
            stats["complaints"] += complaints
            stats["responded"] += _safe_int(
                stat.get("respondedCount") or stat.get("responded_claims")
            )
            stats["finalized"] += _safe_int(
                stat.get("finalizedCount") or stat.get("finalized_claims")
            )
            stats["consumer_resolved"] += _safe_int(
                stat.get("resolvedCount") or stat.get("resolved_claims")
            )
            satisfaction_count = _safe_int(
                stat.get("satisfactionCount")
                or stat.get("satisfaction_count")
                or stat.get("evaluatedCount")
            )
            stats["satisfaction_count"] += satisfaction_count
            stats["satisfaction_sum"] += _safe_float(stat.get("scoreSum"))
            stats["match_methods"][method] = (
                stats["match_methods"].get(method, 0) + complaints
            )

            if _safe_int(stat.get("resolvedCount")) > satisfaction_count:
                resolution_without_satisfaction += 1

        for entity_id, count in month_entity_counts.items():
            stats = entity_stats[entity_id]
            stats["by_month"][month] = count
            stats["months_with_complaints"] += 1

        meta = root.get("meta") if isinstance(root.get("meta"), dict) else {}
        monthly_meta.append(
            {
                "month": month,
                "snapshot_path": str(path),
                "source_resource_url": meta.get("resource_url"),
                "source_lines_kept": meta.get("lines_kept"),
                "source_companies": meta.get("companies"),
                "provider_entries": month_provider_entries,
                "complaints": month_complaints,
                "matched_complaints": month_matched_complaints,
                "matched_complaint_ratio": (
                    month_matched_complaints / month_complaints
                    if month_complaints
                    else None
                ),
                "observed_insurers": len(month_entity_counts),
                "match_methods": dict(month_methods),
            }
        )

    for stats in entity_stats.values():
        satisfaction_count = int(stats["satisfaction_count"])
        stats["average_satisfaction"] = (
            stats["satisfaction_sum"] / satisfaction_count
            if satisfaction_count > 0
            else None
        )
        stats["resolved_share_of_satisfaction_evaluations"] = (
            stats["consumer_resolved"] / satisfaction_count
            if satisfaction_count > 0
            else None
        )

    observed = [stats for stats in entity_stats.values() if stats["complaints"] > 0]
    complaint_thresholds = [1, 5, 10, 20, 30, 50, 100]
    complaint_threshold_counts = {
        str(threshold): sum(
            stats["complaints"] >= threshold for stats in entity_stats.values()
        )
        for threshold in complaint_thresholds
    }
    evaluation_thresholds = [1, 5, 10, 20, 30, 50]
    evaluation_threshold_counts = {
        str(threshold): sum(
            stats["satisfaction_count"] >= threshold
            for stats in entity_stats.values()
        )
        for threshold in evaluation_thresholds
    }
    month_thresholds = [1, 3, 6, 9, 12]
    month_threshold_counts = {
        str(threshold): sum(
            stats["months_with_complaints"] >= threshold
            for stats in entity_stats.values()
        )
        for threshold in month_thresholds
    }

    top_unmatched: list[dict[str, Any]] = []
    for provider, complaints in unmatched_provider_rows.most_common(50):
        top_unmatched.append(
            {
                "provider": provider,
                "complaints": complaints,
                "candidate_suggestions_non_authoritative": _candidate_suggestions(
                    provider, indexes
                ),
            }
        )

    return {
        "artifact": "v2_consumer_gov_157_experiment",
        "generated_at": _utc_now(),
        "status": "experimental",
        "methodology_note": (
            "Coverage experiment only. It consumes the repository's validated monthly "
            "Consumer.gov snapshots, each originally generated from the official MJSP CKAN "
            "Base Completa resource. No score, assessment eligibility or ranking eligibility "
            "is changed. Accepted identity matching is deterministic: exact structured CNPJ "
            "when present, exact normalized legal/current entity name, exact verified brand, "
            "or a unique exact core-name key. Fuzzy similarity is emitted only as a diagnostic "
            "for unmatched providers. Absence from Consumer.gov is not interpreted as zero "
            "complaints or good conduct because platform coverage is not universal."
        ),
        "source": {
            "provider": "Senacon / Ministerio da Justica e Seguranca Publica",
            "dataset": "reclamacoes-do-consumidor-gov-br",
            "upstream_catalog": "https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br",
            "snapshot_builder": "api.build_consumidor_gov",
            "source_mode": "validated_committed_monthly_snapshots",
            "months": [month for month, _, _ in monthly],
            "structured_cnpj_present_in_snapshots": snapshot_has_cnpj,
            "snapshot_semantics": (
                "Current builder explicitly records empty CNPJ/by_cnpj structures because "
                "the public Base Completa dump does not provide a reliable structured provider CNPJ."
            ),
        },
        "universe": {
            "regulatory_universe": "ordinary_current_insurers",
            "eligible_insurers": len(eligible),
            "observed_insurers": len(observed),
            "unobserved_insurers": len(eligible) - len(observed),
            "coverage_ratio": len(observed) / len(eligible),
            "insurers_by_min_complaints_12m": complaint_threshold_counts,
            "insurers_by_min_satisfaction_evaluations_12m": evaluation_threshold_counts,
            "insurers_by_min_months_with_complaints": month_threshold_counts,
        },
        "rows": {
            "provider_entries": provider_entries_total,
            "complaints_in_insurance_segment_snapshots": complaints_total,
            "matched_complaints": matched_complaints,
            "matched_complaint_ratio": (
                matched_complaints / complaints_total if complaints_total else None
            ),
            "match_methods_weighted_by_complaints": dict(match_method_counts),
            "unmatched_complaints": complaints_total - matched_complaints,
            "distinct_unmatched_provider_names": len(unmatched_provider_rows),
            "resolution_rows_with_resolved_count_above_satisfaction_count": (
                resolution_without_satisfaction
            ),
        },
        "monthly": monthly_meta,
        "entities": sorted(
            entity_stats.values(),
            key=lambda item: (-item["complaints"], str(item["entity_id"])),
        ),
        "top_unmatched_providers": top_unmatched,
        "matching_ambiguities": indexes["ambiguities"],
    }


def main() -> None:
    payload = build_experiment()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp = OUTPUT_PATH.with_suffix(OUTPUT_PATH.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    temp.replace(OUTPUT_PATH)
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "source": payload["source"],
                "universe": payload["universe"],
                "rows": payload["rows"],
                "top_unmatched_providers": payload["top_unmatched_providers"][:15],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
