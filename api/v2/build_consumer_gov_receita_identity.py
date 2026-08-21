from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj_bulk import discover_latest_release
from api.sources.receita_cnpj_identity import build_filtered_identity_snapshot
from api.v2.build_consumer_gov_157_experiment import ELIGIBILITY_PATH
from api.v2.consumer_gov_receita_resolution import DEFAULT_RECEITA_IDENTITY_SNAPSHOT

CONSUMER_IDENTITY_PATH = Path("data/derived/v2/consumer_gov_identity_experiment.json")


def _existing_period(path: Path) -> str | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return str((payload.get("source") or {}).get("reference_period") or "").strip() or None


def _load_inputs() -> tuple[list[dict[str, Any]], list[str]]:
    eligibility = json.loads(ELIGIBILITY_PATH.read_text(encoding="utf-8"))
    consumer = json.loads(CONSUMER_IDENTITY_PATH.read_text(encoding="utf-8"))
    entities = list(eligibility.get("entities") or [])
    providers = [
        str(row.get("provider") or "").strip()
        for row in consumer.get("unresolved_providers") or []
        if str(row.get("provider") or "").strip()
    ]
    if not entities:
        raise RuntimeError("eligibility inventory contains no entities")
    if not providers:
        raise RuntimeError("Consumer.gov identity experiment contains no unresolved providers")
    return entities, providers


def write_snapshot(payload: dict[str, Any], output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(output)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve unresolved Consumer.gov provider labels against filtered official "
            "Receita CNPJ identity/activity data."
        )
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_RECEITA_IDENTITY_SNAPSHOT)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    release = discover_latest_release()
    existing = _existing_period(args.output)
    if not args.force and existing == release.period:
        print(
            "Receita Consumer.gov identity build skipped: "
            f"stored snapshot already uses reference_period={release.period}"
        )
        return

    entities, providers = _load_inputs()
    payload = build_filtered_identity_snapshot(
        entities,
        providers,
        release=release,
    )
    write_snapshot(payload, args.output)
    meta = payload["meta"]
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "reference_period": payload["source"]["reference_period"],
                "target_cnpjs": meta["target_cnpjs"],
                "target_cnpjs_enriched": meta["target_cnpjs_enriched"],
                "provider_queries": meta["provider_queries"],
                "provider_unique_candidates": meta["provider_unique_candidates"],
                "provider_ambiguous_candidates": meta["provider_ambiguous_candidates"],
                "provider_no_candidate": meta["provider_no_candidate"],
                "provider_matches": payload["provider_matches"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {args.output}")


if __name__ == "__main__":
    main()
