from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj import (
    DEFAULT_FILTERED_SNAPSHOT,
    load_filtered_lifecycle_snapshot,
    load_verified_lifecycle_snapshot,
    validate_verified_snapshot_against_bulk,
)
from api.sources.receita_cnpj_bulk import refresh_filtered_lifecycle
from api.sources.ses import extract_ses_master_and_financials
from api.sources.susep_licensed import fetch_licensed_entities
from api.sources.susep_sandbox import fetch_sandbox_participants
from api.sources.susep_special_regimes import fetch_special_regime_records
from api.v2.build_classification_inventory import build_classification_inventory

DEFAULT_MIN_COVERAGE = 0.95


class ReceitaRefreshValidationError(RuntimeError):
    """Raised when a new Receita snapshot is unsafe to publish."""


def _coverage(payload: dict[str, Any]) -> float:
    meta = payload.get("meta") or {}
    target = int(meta.get("target_count") or 0)
    resolved = int(meta.get("resolved_count") or 0)
    return resolved / target if target else 0.0


def validate_refresh_payload(
    payload: dict[str, Any],
    *,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    previous_records: list[dict[str, Any]] | None = None,
) -> None:
    source = payload.get("source") or {}
    meta = payload.get("meta") or {}
    records = list(payload.get("records") or [])

    if source.get("authority") != "Receita Federal do Brasil":
        raise ReceitaRefreshValidationError("unexpected Receita source authority")
    if source.get("ingestion_method") != "official_nextcloud_webdav_bulk_filtered":
        raise ReceitaRefreshValidationError("unexpected Receita ingestion method")
    if not source.get("reference_period"):
        raise ReceitaRefreshValidationError("Receita reference period is missing")

    target = int(meta.get("target_count") or 0)
    resolved = int(meta.get("resolved_count") or 0)
    unresolved = int(meta.get("unresolved_count") or 0)
    if target < 200:
        raise ReceitaRefreshValidationError(
            f"Receita target universe is unexpectedly small: {target}"
        )
    if resolved != len(records):
        raise ReceitaRefreshValidationError(
            f"Receita resolved_count mismatch: meta={resolved} records={len(records)}"
        )
    if target != resolved + unresolved:
        raise ReceitaRefreshValidationError(
            "Receita target count does not equal resolved + unresolved"
        )

    coverage = _coverage(payload)
    if coverage < min_coverage:
        raise ReceitaRefreshValidationError(
            f"Receita lifecycle coverage {coverage:.2%} is below {min_coverage:.2%}"
        )

    cnpjs = [row.get("cnpj") for row in records]
    if len(cnpjs) != len(set(cnpjs)):
        raise ReceitaRefreshValidationError("Receita lifecycle snapshot has duplicate CNPJ")

    verified = load_verified_lifecycle_snapshot()
    validate_verified_snapshot_against_bulk(records, verified)

    if previous_records:
        previous_count = len(previous_records)
        if previous_count and resolved < int(previous_count * 0.95):
            raise ReceitaRefreshValidationError(
                "Receita lifecycle resolved coverage dropped more than 5% from previous snapshot"
            )


def write_snapshot_atomic(
    payload: dict[str, Any],
    output: Path = DEFAULT_FILTERED_SNAPSHOT,
) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp.replace(output)
    return output


def build_current_regulatory_universe() -> list[dict[str, Any]]:
    ses_out = extract_ses_master_and_financials()
    if not isinstance(ses_out, tuple) or len(ses_out) < 2:
        raise RuntimeError(f"Unexpected SES return: {type(ses_out)}")

    classification = build_classification_inventory(
        ses_out[1],
        fetch_licensed_entities(),
        fetch_special_regime_records(),
        fetch_sandbox_participants(),
    )
    return list(classification.get("entities") or [])


def refresh_receita_lifecycle(
    *,
    output: Path = DEFAULT_FILTERED_SNAPSHOT,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
) -> dict[str, Any]:
    entities = build_current_regulatory_universe()
    payload = refresh_filtered_lifecycle(entities)

    previous_records = None
    if output.exists():
        previous_records = load_filtered_lifecycle_snapshot(output)

    validate_refresh_payload(
        payload,
        min_coverage=min_coverage,
        previous_records=previous_records,
    )
    write_snapshot_atomic(payload, output)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh filtered Receita CNPJ lifecycle from official open data."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_FILTERED_SNAPSHOT,
    )
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=DEFAULT_MIN_COVERAGE,
    )
    args = parser.parse_args()

    payload = refresh_receita_lifecycle(
        output=args.output,
        min_coverage=args.min_coverage,
    )
    meta = payload["meta"]
    source = payload["source"]
    print(
        "Receita lifecycle refresh OK: "
        f"period={source['reference_period']} "
        f"resolved={meta['resolved_count']}/{meta['target_count']} "
        f"coverage={_coverage(payload):.2%} "
        f"files={len(meta['files_scanned'])}"
    )


if __name__ == "__main__":
    main()
