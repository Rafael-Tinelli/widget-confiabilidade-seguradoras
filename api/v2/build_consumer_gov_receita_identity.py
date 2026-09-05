from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj_bulk import discover_latest_release
from api.sources.receita_cnpj_identity import build_filtered_identity_snapshot
from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import normalize_name_key
from api.v2.build_consumer_gov_157_experiment import ELIGIBILITY_PATH
from api.v2.consumer_gov_receita_resolution import DEFAULT_RECEITA_IDENTITY_SNAPSHOT

CONSUMER_IDENTITY_PATH = Path("data/derived/v2/consumer_gov_identity_experiment.json")
CACHE_KEY_VERSION = "v2-consumer-receita-identity-1"


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def target_universe_hash(entities: list[dict[str, Any]]) -> str:
    rows = sorted(
        {
            (
                str(entity.get("entity_id") or entity.get("id") or "").strip(),
                normalize_cnpj_v2(entity.get("cnpj")) or "",
            )
            for entity in entities
            if str(entity.get("entity_id") or entity.get("id") or "").strip()
        }
    )
    if not rows:
        raise RuntimeError("cannot hash an empty Consumer.gov Receita target universe")
    return _stable_hash(rows)


def unresolved_provider_hash(providers: list[str]) -> str:
    names = sorted(
        {
            normalize_name_key(provider)
            for provider in providers
            if normalize_name_key(provider)
        }
    )
    if not names:
        raise RuntimeError("cannot hash an empty unresolved Consumer.gov provider set")
    return _stable_hash(names)


def _cache_key(
    *,
    reference_period: str,
    universe_hash: str,
    provider_hash: str,
) -> dict[str, str]:
    return {
        "version": CACHE_KEY_VERSION,
        "reference_period": reference_period,
        "target_universe_hash": universe_hash,
        "unresolved_provider_hash": provider_hash,
    }


def _existing_cache_key(path: Path) -> dict[str, str] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    meta = payload.get("meta") if isinstance(payload, dict) else None
    if not isinstance(meta, dict):
        return None
    value = meta.get("gate4_cache_key")
    if not isinstance(value, dict):
        return None
    return {
        "version": str(value.get("version") or ""),
        "reference_period": str(value.get("reference_period") or ""),
        "target_universe_hash": str(value.get("target_universe_hash") or ""),
        "unresolved_provider_hash": str(value.get("unresolved_provider_hash") or ""),
    }


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


def build_snapshot(
    *,
    output: Path = DEFAULT_RECEITA_IDENTITY_SNAPSHOT,
    force: bool = False,
) -> dict[str, Any] | None:
    entities, providers = _load_inputs()
    universe_hash = target_universe_hash(entities)
    provider_hash = unresolved_provider_hash(providers)
    release = discover_latest_release()
    expected_key = _cache_key(
        reference_period=release.period,
        universe_hash=universe_hash,
        provider_hash=provider_hash,
    )
    existing_key = _existing_cache_key(output)
    if not force and existing_key == expected_key:
        print(
            "Receita Consumer.gov identity build skipped: stored snapshot cache key "
            "matches current release, regulatory universe and unresolved providers"
        )
        return None

    payload = build_filtered_identity_snapshot(
        entities,
        providers,
        release=release,
    )
    meta = payload.setdefault("meta", {})
    meta["gate4_cache_key"] = expected_key
    write_snapshot(payload, output)
    return payload


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

    payload = build_snapshot(output=args.output, force=args.force)
    if payload is None:
        return

    meta = payload["meta"]
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "reference_period": payload["source"]["reference_period"],
                "gate4_cache_key": meta["gate4_cache_key"],
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
