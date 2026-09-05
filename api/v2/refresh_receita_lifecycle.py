from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj import (
    DEFAULT_FILTERED_SNAPSHOT,
    load_filtered_lifecycle_snapshot,
    load_verified_lifecycle_snapshot,
    validate_verified_snapshot_against_bulk,
)
from api.sources.receita_cnpj_bulk import (
    ReceitaOpenDataRelease,
    discover_latest_release,
    refresh_filtered_lifecycle,
)
from api.sources.ses import extract_ses_master_and_financials
from api.sources.susep_licensed import fetch_licensed_entities
from api.sources.susep_sandbox import fetch_sandbox_participants
from api.sources.susep_special_regimes import fetch_special_regime_records
from api.v2.build_classification_inventory import build_classification_inventory

DEFAULT_MIN_COVERAGE = 0.95
RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION = "v2-receita-lifecycle-1"


class ReceitaRefreshValidationError(RuntimeError):
    """Raised when a new Receita snapshot is unsafe to publish."""


def _coverage(payload: dict[str, Any]) -> float:
    meta = payload.get("meta") or {}
    target = int(meta.get("target_count") or 0)
    resolved = int(meta.get("resolved_count") or 0)
    return resolved / target if target else 0.0


def _existing_metadata(
    path: Path,
) -> tuple[str | None, str | None, str | None]:
    if not path.exists():
        return None, None, None
    payload = json.loads(path.read_text(encoding="utf-8"))
    period = str((payload.get("source") or {}).get("reference_period") or "").strip() or None
    meta = payload.get("meta") or {}
    target_hash = str(meta.get("target_universe_hash") or "").strip() or None
    contract_version = (
        str(meta.get("gate4_cache_contract_version") or "").strip() or None
    )
    return period, target_hash, contract_version


def regulatory_target_universe_hash(entities: list[dict[str, Any]]) -> str:
    """Hash the exact CNPJ target population used for Receita lifecycle filtering."""
    rows: list[str] = []
    for entity in entities:
        cnpj = str(entity.get("cnpj") or "").strip()
        if not cnpj:
            continue
        rows.append(
            "|".join(
                [
                    str(entity.get("entity_id") or ""),
                    cnpj,
                    str(entity.get("legal_name") or "").strip(),
                    str(entity.get("entity_type") or "unknown"),
                    str(entity.get("regulatory_subtype") or ""),
                    str(entity.get("regulatory_regime") or "unknown"),
                    str(entity.get("regulatory_status") or "unknown"),
                ]
            )
        )
    canonical = "\n".join(sorted(set(rows))).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def validate_refresh_payload(
    payload: dict[str, Any],
    *,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    previous_records: list[dict[str, Any]] | None = None,
    allow_legacy_unversioned: bool = False,
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

    contract_version = str(meta.get("gate4_cache_contract_version") or "").strip()
    if contract_version != RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION:
        legacy_allowed = allow_legacy_unversioned and not contract_version
        if not legacy_allowed:
            raise ReceitaRefreshValidationError(
                "unsupported Receita lifecycle cache contract version: "
                f"{contract_version or 'missing'}"
            )

    target_hash = str(meta.get("target_universe_hash") or "").strip()
    if len(target_hash) != 64:
        raise ReceitaRefreshValidationError("Receita target universe hash is missing or invalid")

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
    release: ReceitaOpenDataRelease | None = None,
    entities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    resolved_release = release or discover_latest_release()
    target_entities = entities if entities is not None else build_current_regulatory_universe()
    target_hash = regulatory_target_universe_hash(target_entities)
    payload = refresh_filtered_lifecycle(target_entities, release=resolved_release)
    meta = payload.setdefault("meta", {})
    meta["target_universe_hash"] = target_hash
    meta["gate4_cache_contract_version"] = RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION

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
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Refresh even when the stored Receita cache-contract version, reference "
            "period and regulatory target-universe hash are current."
        ),
    )
    args = parser.parse_args()

    release = discover_latest_release()
    entities = build_current_regulatory_universe()
    current_target_hash = regulatory_target_universe_hash(entities)
    existing_period, existing_target_hash, existing_contract_version = _existing_metadata(
        args.output
    )
    if (
        not args.force
        and existing_contract_version == RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION
        and existing_period == release.period
        and existing_target_hash == current_target_hash
    ):
        print(
            "Receita lifecycle refresh skipped: "
            f"contract={existing_contract_version} reference_period={release.period} "
            "and target_universe_hash unchanged"
        )
        return

    payload = refresh_receita_lifecycle(
        output=args.output,
        min_coverage=args.min_coverage,
        release=release,
        entities=entities,
    )
    meta = payload["meta"]
    source = payload["source"]
    print(
        "Receita lifecycle refresh OK: "
        f"contract={meta['gate4_cache_contract_version']} "
        f"period={source['reference_period']} "
        f"target_hash={meta['target_universe_hash'][:12]} "
        f"resolved={meta['resolved_count']}/{meta['target_count']} "
        f"coverage={_coverage(payload):.2%} "
        f"files={len(meta['files_scanned'])}"
    )


if __name__ == "__main__":
    main()
