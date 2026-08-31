from __future__ import annotations

import gzip
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

import requests

from api import build_consumidor_gov as consumer_gov_build
from api.sources.receita_cnpj import RECEITA_CNPJ_OPEN_DATA_URL
from api.v2.build_consumer_gov_identity_experiment import (
    OUTPUT_PATH as CONSUMER_IDENTITY_PATH,
)
from api.v2.build_consumer_gov_identity_experiment import build_identity_experiment
from api.v2.build_consumer_gov_receita_identity import (
    CACHE_KEY_VERSION,
    _existing_cache_key,
    _load_inputs,
    build_snapshot,
    target_universe_hash,
    unresolved_provider_hash,
)
from api.v2.consumer_gov_receita_resolution import (
    DEFAULT_RECEITA_IDENTITY_SNAPSHOT,
)
from api.v2.generation import BuildContext
from api.v2.source_cache import CachedSource, SourceCacheError, utc_now
from api.v2.source_snapshot import SourceObservation, write_source_lineage

DEFAULT_CACHE_DIR = Path("data/cache/v2/conduct")
DEFAULT_LINEAGE = Path("data/derived/v2/conduct_source_lineage.json")
CONSUMER_MANIFEST = "consumer_gov_core_manifest.json"
CONSUMER_MANIFEST_VERSION = 2
CONSUMER_SOURCE_URL = (
    f"{consumer_gov_build.CG_API_BASE}/package_show?id={consumer_gov_build.CG_DATASET_ID}"
)


class ConductSourceSnapshotError(RuntimeError):
    """Raised when Gate 4 cannot materialize compatible conduct sources."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".tmp")
    try:
        shutil.copy2(source, temp)
        temp.replace(destination)
    finally:
        temp.unlink(missing_ok=True)


def _read_gzip_json(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rb") as handle:
        payload = json.loads(handle.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ConductSourceSnapshotError(f"invalid JSON object: {path}")
    return payload


def _valid_monthly_files() -> list[tuple[str, Path, dict[str, Any]]]:
    valid: dict[str, tuple[Path, dict[str, Any]]] = {}
    for path in sorted(
        consumer_gov_build.MONTHLY_DIR.glob("consumidor_gov_20??-??.json.gz")
    ):
        try:
            payload = _read_gzip_json(path)
        except (OSError, UnicodeError, ValueError, ConductSourceSnapshotError):
            continue
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        month = str(meta.get("month") or meta.get("ym") or "").strip()
        entries = consumer_gov_build._entries_from_root(payload)
        if not month or not entries or meta.get("invalid") is True:
            continue
        valid[month] = (path, payload)
    selected = sorted(valid)[-12:]
    if len(selected) != 12:
        raise ConductSourceSnapshotError(
            f"expected 12 valid Consumer.gov monthly snapshots, got {len(selected)}"
        )
    return [(month, *valid[month]) for month in selected]


def _consumer_manifest_payload() -> dict[str, Any]:
    aggregate = consumer_gov_build.DERIVED_DIR / "consumidor_gov_agg.json.gz"
    if consumer_gov_build._load_valid_aggregate(aggregate) is None:
        raise ConductSourceSnapshotError("valid Consumer.gov aggregate is unavailable")
    monthly = _valid_monthly_files()
    return {
        "artifact": "v2_consumer_gov_core_source_manifest",
        "version": CONSUMER_MANIFEST_VERSION,
        "aggregate": {
            "path": str(aggregate),
            "sha256": _sha256(aggregate),
        },
        "months": [
            {
                "month": month,
                "path": str(path),
                "sha256": _sha256(path),
            }
            for month, path, _ in monthly
        ],
    }


def _manifest_content_key(
    payload: dict[str, Any],
) -> tuple[str, tuple[tuple[str, str], ...]]:
    aggregate = payload.get("aggregate") or {}
    months = payload.get("months") or []
    return (
        str(aggregate.get("sha256") or ""),
        tuple(
            (str(item.get("month") or ""), str(item.get("sha256") or ""))
            for item in months
        ),
    )


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temp.replace(path)
    return path


def _cache_object(source: Path, expected_sha: str, cache_dir: Path) -> Path:
    if len(expected_sha) != 64 or _sha256(source) != expected_sha:
        raise ConductSourceSnapshotError(f"invalid Consumer.gov source hash: {source}")
    destination = cache_dir / "consumer_gov_core" / "objects" / expected_sha
    if destination.is_file() and _sha256(destination) == expected_sha:
        return destination
    _atomic_copy(source, destination)
    if _sha256(destination) != expected_sha:
        destination.unlink(missing_ok=True)
        raise ConductSourceSnapshotError(
            f"Consumer.gov cache object verification failed: {destination}"
        )
    return destination


def _cache_consumer_manifest(
    materialized: dict[str, Any],
    *,
    cache_dir: Path,
    fetched_at: str,
) -> dict[str, Any]:
    aggregate = dict(materialized.get("aggregate") or {})
    aggregate_source = Path(str(aggregate.get("path") or ""))
    aggregate_sha = str(aggregate.get("sha256") or "")
    cached_aggregate = _cache_object(aggregate_source, aggregate_sha, cache_dir)

    cached_months: list[dict[str, Any]] = []
    for item in materialized.get("months") or []:
        row = dict(item)
        source = Path(str(row.get("path") or ""))
        expected = str(row.get("sha256") or "")
        cached = _cache_object(source, expected, cache_dir)
        cached_months.append(
            {
                "month": str(row.get("month") or ""),
                "path": str(cached),
                "materialized_path": str(source),
                "sha256": expected,
            }
        )

    return {
        "artifact": "v2_consumer_gov_core_source_manifest",
        "version": CONSUMER_MANIFEST_VERSION,
        "fetched_at": fetched_at,
        "aggregate": {
            "path": str(cached_aggregate),
            "materialized_path": str(aggregate_source),
            "sha256": aggregate_sha,
        },
        "months": cached_months,
    }


def _validate_consumer_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("artifact") != "v2_consumer_gov_core_source_manifest":
        raise ConductSourceSnapshotError("invalid Consumer.gov cache manifest artifact")
    if payload.get("version") != CONSUMER_MANIFEST_VERSION:
        raise ConductSourceSnapshotError("unsupported Consumer.gov cache manifest version")
    fetched_at = str(payload.get("fetched_at") or "").strip()
    if not fetched_at:
        raise ConductSourceSnapshotError("Consumer.gov cache manifest has no fetched_at")
    months = payload.get("months") or []
    if not isinstance(months, list) or len(months) != 12:
        raise ConductSourceSnapshotError("Consumer.gov cache manifest must contain 12 months")
    aggregate = payload.get("aggregate") or {}
    targets = [aggregate, *months]
    for item in targets:
        target = Path(str(item.get("path") or ""))
        materialized = str(item.get("materialized_path") or "").strip()
        expected = str(item.get("sha256") or "")
        if not materialized:
            raise ConductSourceSnapshotError(
                "Consumer.gov cache manifest is missing materialized_path"
            )
        if not target.is_file() or len(expected) != 64 or _sha256(target) != expected:
            raise ConductSourceSnapshotError(
                f"Consumer.gov cached source hash mismatch: {target}"
            )
    return payload


def _materialize_consumer_manifest(payload: dict[str, Any]) -> None:
    aggregate = payload.get("aggregate") or {}
    targets = [aggregate, *(payload.get("months") or [])]
    for item in targets:
        source = Path(str(item.get("path") or ""))
        destination = Path(str(item.get("materialized_path") or ""))
        expected = str(item.get("sha256") or "")
        _atomic_copy(source, destination)
        if _sha256(destination) != expected:
            destination.unlink(missing_ok=True)
            raise ConductSourceSnapshotError(
                f"Consumer.gov materialization hash mismatch: {destination}"
            )


def _consumer_observation(*, cache_dir: Path) -> SourceObservation:
    cache_manifest = cache_dir / CONSUMER_MANIFEST
    current_error: str | None = None
    try:
        resources = consumer_gov_build._list_basecompleta_resources()
        if not resources:
            raise ConductSourceSnapshotError("Consumer.gov returned no Base Completa resources")
        result = consumer_gov_build.main()
        if result != 0:
            raise ConductSourceSnapshotError(f"Consumer.gov builder returned {result}")
        materialized = _consumer_manifest_payload()
        materialized_months = [str(item["month"]) for item in materialized["months"]]
        latest_official = max(resources)
        if latest_official > materialized_months[-1]:
            raise ConductSourceSnapshotError(
                "Consumer.gov current publication is newer than the validated materialized window: "
                f"official={latest_official} materialized={materialized_months[-1]}"
            )

        prior: dict[str, Any] | None = None
        try:
            prior = _validate_consumer_manifest(cache_manifest)
        except (OSError, ValueError, ConductSourceSnapshotError):
            pass
        if prior and _manifest_content_key(prior) == _manifest_content_key(materialized):
            fetched_at = str(prior["fetched_at"])
        else:
            fetched_at = utc_now()
        cached = _cache_consumer_manifest(
            materialized,
            cache_dir=cache_dir,
            fetched_at=fetched_at,
        )
        _write_json(cache_manifest, cached)
        return SourceObservation(
            source_id="consumer_gov_core",
            source_url=CONSUMER_SOURCE_URL,
            snapshot_path=cache_manifest,
            fetched_at=fetched_at,
            current_validation_succeeded=True,
            state_reason="current CKAN publication window validated against materialized core",
        )
    except (
        ConductSourceSnapshotError,
        OSError,
        RuntimeError,
        ValueError,
        requests.RequestException,
    ) as exc:
        current_error = f"{type(exc).__name__}: {exc}"

    try:
        cached = _validate_consumer_manifest(cache_manifest)
        _materialize_consumer_manifest(cached)
    except (OSError, ValueError, ConductSourceSnapshotError) as exc:
        unavailable = cache_dir / "consumer_gov_core_unavailable.json"
        unavailable.unlink(missing_ok=True)
        return SourceObservation(
            source_id="consumer_gov_core",
            source_url=CONSUMER_SOURCE_URL,
            snapshot_path=unavailable,
            fetched_at=utc_now(),
            state_reason=f"{current_error}; cache invalid: {type(exc).__name__}: {exc}",
        )

    first_month = str(cached["months"][0]["month"])
    last_month = str(cached["months"][-1]["month"])
    return SourceObservation(
        source_id="consumer_gov_core",
        source_url=CONSUMER_SOURCE_URL,
        snapshot_path=cache_manifest,
        fetched_at=str(cached["fetched_at"]),
        state_reason=(
            f"{current_error}; using hash-validated Consumer.gov core cache "
            f"for {first_month}..{last_month}"
        ),
    )


def _receita_cache(cache_dir: Path) -> CachedSource:
    return CachedSource(
        source_id="receita_consumer_identity",
        source_url=RECEITA_CNPJ_OPEN_DATA_URL,
        content_path=cache_dir / "receita_consumer_identity.json",
        metadata_path=cache_dir / "receita_consumer_identity.meta.json",
    )


def _current_input_hashes() -> tuple[str, str]:
    entities, providers = _load_inputs()
    return target_universe_hash(entities), unresolved_provider_hash(providers)


def _cache_matches_current_inputs(
    path: Path,
    universe_hash: str,
    provider_hash: str,
) -> bool:
    key = _existing_cache_key(path)
    return bool(
        key
        and key.get("version") == CACHE_KEY_VERSION
        and key.get("target_universe_hash") == universe_hash
        and key.get("unresolved_provider_hash") == provider_hash
    )


def _receita_observation(*, cache_dir: Path) -> SourceObservation:
    cache = _receita_cache(cache_dir)
    universe_hash, provider_hash = _current_input_hashes()
    current_error: str | None = None

    if cache.content_path.is_file() and _cache_matches_current_inputs(
        cache.content_path,
        universe_hash,
        provider_hash,
    ):
        try:
            cache.materialize(DEFAULT_RECEITA_IDENTITY_SNAPSHOT)
        except (OSError, ValueError, SourceCacheError):
            DEFAULT_RECEITA_IDENTITY_SNAPSHOT.unlink(missing_ok=True)

    try:
        payload = build_snapshot(output=DEFAULT_RECEITA_IDENTITY_SNAPSHOT, force=False)
        if payload is not None:
            fetched_at = utc_now()
            cache.store(DEFAULT_RECEITA_IDENTITY_SNAPSHOT, fetched_at=fetched_at)
            return SourceObservation(
                source_id=cache.source_id,
                source_url=cache.source_url,
                snapshot_path=DEFAULT_RECEITA_IDENTITY_SNAPSHOT,
                fetched_at=fetched_at,
                current_fetch_succeeded=True,
                state_reason="official Receita filtered identity snapshot refreshed",
            )

        if not _cache_matches_current_inputs(
            DEFAULT_RECEITA_IDENTITY_SNAPSHOT,
            universe_hash,
            provider_hash,
        ):
            raise ConductSourceSnapshotError(
                "Receita builder skipped without a cache key compatible with current inputs"
            )
        if not cache.content_path.is_file():
            fetched_at = utc_now()
            cache.store(DEFAULT_RECEITA_IDENTITY_SNAPSHOT, fetched_at=fetched_at)
        else:
            fetched_at = cache.materialize(DEFAULT_RECEITA_IDENTITY_SNAPSHOT)
        return SourceObservation(
            source_id=cache.source_id,
            source_url=cache.source_url,
            snapshot_path=DEFAULT_RECEITA_IDENTITY_SNAPSHOT,
            fetched_at=fetched_at,
            current_validation_succeeded=True,
            state_reason="official release and Gate 4 Receita conduct cache key unchanged",
        )
    except Exception as exc:  # noqa: BLE001 - acquisition boundary records arbitrary source failures
        current_error = f"{type(exc).__name__}: {exc}"

    try:
        if not _cache_matches_current_inputs(
            cache.content_path,
            universe_hash,
            provider_hash,
        ):
            raise ConductSourceSnapshotError(
                "cached Receita identity does not match current universe/provider hashes"
            )
        fetched_at = cache.materialize(DEFAULT_RECEITA_IDENTITY_SNAPSHOT)
    except (OSError, ValueError, SourceCacheError, ConductSourceSnapshotError) as exc:
        DEFAULT_RECEITA_IDENTITY_SNAPSHOT.unlink(missing_ok=True)
        return SourceObservation(
            source_id=cache.source_id,
            source_url=cache.source_url,
            snapshot_path=DEFAULT_RECEITA_IDENTITY_SNAPSHOT,
            fetched_at=utc_now(),
            state_reason=f"{current_error}; cache invalid: {type(exc).__name__}: {exc}",
        )

    return SourceObservation(
        source_id=cache.source_id,
        source_url=cache.source_url,
        snapshot_path=DEFAULT_RECEITA_IDENTITY_SNAPSHOT,
        fetched_at=fetched_at,
        state_reason=f"{current_error}; using compatible hash-validated Receita cache",
    )


def build_conduct_source_snapshot(
    *,
    context: BuildContext,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    lineage_path: Path = DEFAULT_LINEAGE,
) -> dict[str, Any]:
    cache_dir.mkdir(parents=True, exist_ok=True)

    consumer = _consumer_observation(cache_dir=cache_dir)
    if consumer.to_lineage(context).state == "unavailable":
        write_source_lineage(lineage_path, [consumer], context)
        raise ConductSourceSnapshotError("Consumer.gov core source is unavailable")

    identity = build_identity_experiment()
    _write_json(CONSUMER_IDENTITY_PATH, identity)

    receita = _receita_observation(cache_dir=cache_dir)
    observations = [consumer, receita]
    write_source_lineage(lineage_path, observations, context)
    states = {
        observation.source_id: observation.to_lineage(context).state
        for observation in observations
    }
    if any(state == "unavailable" for state in states.values()):
        raise ConductSourceSnapshotError(f"conduct sources unavailable: {states}")

    return {
        "build": context.as_dict(),
        "lineage_path": str(lineage_path),
        "consumer_identity_path": str(CONSUMER_IDENTITY_PATH),
        "receita_identity_path": str(DEFAULT_RECEITA_IDENTITY_SNAPSHOT),
        "source_states": states,
    }


def main() -> None:
    context = BuildContext.from_env()
    result = build_conduct_source_snapshot(context=context)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
