from __future__ import annotations

import argparse
import gzip
import json
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Any

from api.build_consumidor_gov import _aggregate_basecompleta
from api.sources.consumer_gov_direct import (
    discover_publications,
    ensure_months,
    validate_month_csv,
)

DEFAULT_MONTH = "2026-03"
DEFAULT_EXPECTED_ROWS = 342_527
EXTERNAL_ROW_COUNT_REFERENCE = (
    "https://github.com/mateusdiasc137/consumer-gov-resolution-ml/"
    "blob/dd37c92a48a9fe921a8378b2b5e0e4a328ea204b/"
    "data/processed/catalogo_processamento.csv"
)
OUTPUT_PATH = Path("data/derived/v2/consumer_gov_source_compatibility.json")
HISTORICAL_MONTHLY_DIR = Path("data/derived/consumidor_gov/monthly")


def _read_json_gz(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise TypeError(f"{path} does not contain a JSON object")
    return payload


def _entries(payload: dict[str, Any]) -> dict[str, Any]:
    value = (
        payload.get("by_name_key_raw")
        or payload.get("by_name_key")
        or payload.get("by_name")
        or {}
    )
    return value if isinstance(value, dict) else {}


def _metric(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 8)
    return value


def _canonical_entry_payload(payload: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, entry in sorted(_entries(payload).items()):
        if not isinstance(entry, dict):
            continue
        stats = entry.get("statistics") or {}
        out[str(key)] = {
            field: _metric(stats.get(field))
            for field in (
                "complaintsCount",
                "respondedCount",
                "resolvedCount",
                "finalizedCount",
                "scoreSum",
                "satisfactionCount",
                "total_claims",
                "responded_claims",
                "resolved_claims",
                "finalized_claims",
                "averageScore",
            )
        }
    return out


def _complaint_total(entries: dict[str, dict[str, Any]]) -> int:
    return sum(int(item.get("complaintsCount") or 0) for item in entries.values())


def compare_month(
    month: str = DEFAULT_MONTH,
    *,
    expected_rows: int | None = DEFAULT_EXPECTED_ROWS,
) -> dict[str, Any]:
    historical_path = (
        HISTORICAL_MONTHLY_DIR / f"consumidor_gov_{month}.json.gz"
    )
    if not historical_path.exists():
        raise FileNotFoundError(
            "historical CKAN-derived monthly artifact is missing: "
            f"{historical_path}"
        )
    historical = _read_json_gz(historical_path)

    candidates = [
        asdict(item)
        for item in discover_publications({month})
        if item.month == month
    ]

    with tempfile.TemporaryDirectory(
        prefix="consumer-gov-direct-compat-"
    ) as tmp:
        temp_dir = Path(tmp)
        manifest_path = temp_dir / "manifest.json"
        acquired = ensure_months(
            [month],
            raw_dir=temp_dir,
            manifest_path=manifest_path,
        )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        selected = dict((manifest.get("months") or {}).get(month) or {})
        direct_path = Path(acquired[month]["path"])
        validation = validate_month_csv(direct_path, month)
        direct_aggregate = _aggregate_basecompleta(
            direct_path,
            month=month,
            resource_url=str(acquired[month].get("resource_url") or ""),
        )

    historical_entries = _canonical_entry_payload(historical)
    direct_entries = _canonical_entry_payload(direct_aggregate)
    historical_keys = set(historical_entries)
    direct_keys = set(direct_entries)
    entry_keys_equal = historical_keys == direct_keys
    aggregate_equal = historical_entries == direct_entries
    rows_match_reference = (
        expected_rows is None or validation.rows == expected_rows
    )
    compatible = aggregate_equal and rows_match_reference

    only_historical = sorted(historical_keys - direct_keys)
    only_direct = sorted(direct_keys - historical_keys)

    return {
        "artifact": "consumer_gov_source_compatibility",
        "status": "compatible" if compatible else "incompatible",
        "month": month,
        "source_discovery": {
            "candidate_count": len(candidates),
            "candidates": candidates,
            "selected_publication": {
                key: selected.get(key)
                for key in (
                    "publication_code",
                    "publication_title",
                    "filename",
                    "published_at",
                    "source_url",
                    "discovery_method",
                )
            },
        },
        "comparison": {
            "historical_source_role": "legacy_ckan_derived_monthly_aggregate",
            "direct_source_role": "consumer_gov_direct_dados_abertos",
            "historical_companies": len(historical_entries),
            "direct_companies": len(direct_entries),
            "historical_target_segment_complaints": _complaint_total(
                historical_entries
            ),
            "direct_target_segment_complaints": _complaint_total(direct_entries),
            "entry_keys_equal": entry_keys_equal,
            "aggregate_equal": aggregate_equal,
            "company_keys_only_historical": only_historical,
            "company_keys_only_direct": only_direct,
            "expected_raw_rows_external_crosscheck": expected_rows,
            "external_row_count_reference": EXTERNAL_ROW_COUNT_REFERENCE,
            "direct_raw_rows": validation.rows,
            "raw_rows_match_external_crosscheck": rows_match_reference,
            "schema_version": validation.schema_version,
            "required_schema_fields": validation.field_map,
            "raw_sha256": validation.sha256,
            "raw_bytes": validation.bytes,
            "raw_encoding": validation.encoding,
            "raw_delimiter": validation.delimiter,
            "month_observations": validation.month_observations,
            "month_matches": validation.month_matches,
        },
        "decision": {
            "may_replace_ckan_acquisition": compatible,
            "reason": (
                "direct source reproduces the legacy CKAN-derived monthly "
                "Consumer.gov aggregate and matches the independent raw "
                "row-count cross-check"
                if compatible
                else "compatibility gate failed"
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", default=DEFAULT_MONTH)
    parser.add_argument(
        "--expected-rows",
        type=int,
        default=DEFAULT_EXPECTED_ROWS,
    )
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()

    report = compare_month(args.month, expected_rows=args.expected_rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "compatible" else 1


if __name__ == "__main__":
    raise SystemExit(main())
