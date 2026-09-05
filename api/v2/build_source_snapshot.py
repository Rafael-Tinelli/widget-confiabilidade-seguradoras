from __future__ import annotations

import argparse
import json
import os
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from api.sources.receita_cnpj import (
    RECEITA_CNPJ_OPEN_DATA_URL,
    load_filtered_lifecycle_snapshot,
)
from api.sources.receita_cnpj_bulk import discover_latest_release
from api.sources.ses import (
    SES_LISTAEMPRESAS_URL,
    SES_ZIP_URL,
    SES_ZIP_URL_FALLBACK,
)
from api.sources.susep_licensed import LICENSED_ENTITIES_URL, fetch_licensed_entities
from api.sources.susep_sandbox import (
    SANDBOX_PARTICIPANTS_URL,
    fetch_sandbox_participants,
)
from api.sources.susep_special_regimes import (
    SPECIAL_REGIME_URLS,
    fetch_special_regime_records,
)
from api.v2.build_classification_inventory import (
    build_classification_inventory,
    write_classification_inventory,
)
from api.v2.generation import BuildContext
from api.v2.refresh_receita_lifecycle import (
    RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION,
    ReceitaRefreshValidationError,
    refresh_receita_lifecycle,
    regulatory_target_universe_hash,
    validate_refresh_payload,
    write_snapshot_atomic,
)
from api.v2.source_cache import (
    AcquisitionResult,
    CachedSource,
    SourceCacheError,
    acquire_with_validated_cache,
    utc_now,
)
from api.v2.source_snapshot import SourceObservation, write_source_lineage

DEFAULT_SOURCE_DIR = Path("data/derived/v2/source")
DEFAULT_CACHE_DIR = Path(os.getenv("V2_SOURCE_CACHE_DIR", "data/cache/v2/source"))
DEFAULT_SES_DIR = Path("data/raw/ses")
DEFAULT_RECEITA_FILTERED = Path("data/derived/v2/receita_cnpj_lifecycle.json")
DEFAULT_LINEAGE = Path("data/derived/v2/source_lineage.json")

_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SanidaResearch/2.0; +https://sanida.com.br)",
    "Accept": "*/*",
}


class SourceSnapshotError(RuntimeError):
    """Raised when a critical Gate 4 source cannot be materialized safely."""


def _normalize_fip(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _pick_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    normalized = {column.casefold().strip(): column for column in columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    for candidate in candidates:
        for key, original in normalized.items():
            if candidate in key:
                return original
    return None


def _read_semicolon_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(
        path,
        sep=";",
        encoding="latin1",
        dtype=str,
        on_bad_lines="skip",
    )
    if frame.empty:
        raise SourceSnapshotError(f"CSV source is empty: {path}")
    return frame


def validate_listaempresas(path: Path) -> None:
    frame = _read_semicolon_csv(path)
    columns = list(frame.columns)
    if _pick_column(columns, ("codigofip", "codfip", "coenti")) is None:
        raise SourceSnapshotError("LISTAEMPRESAS missing FIP column")
    if _pick_column(columns, ("cnpj", "cnpjseg", "cnpjcpf")) is None:
        raise SourceSnapshotError("LISTAEMPRESAS missing CNPJ column")


def validate_base_completa(path: Path) -> None:
    if not path.is_file() or not zipfile.is_zipfile(path):
        raise SourceSnapshotError("BaseCompleta is not a valid ZIP")
    with zipfile.ZipFile(path) as archive:
        names = [name.casefold() for name in archive.namelist()]
        if not any(name.endswith("ses_cias.csv") for name in names):
            raise SourceSnapshotError("BaseCompleta missing Ses_cias.csv")
        if not any("pl_margem" in name for name in names):
            raise SourceSnapshotError("BaseCompleta missing prudential capital source")
        if not any("balanco" in name for name in names):
            raise SourceSnapshotError("BaseCompleta missing balance source")


def _download_urls(urls: list[str], destination: Path, *, timeout: int) -> None:
    last_error: Exception | None = None
    verify_ssl = os.getenv("SES_ALLOW_INSECURE_SSL", "0") != "1"
    for url in urls:
        try:
            with requests.get(
                url,
                headers=_HTTP_HEADERS,
                timeout=timeout,
                verify=verify_ssl,
                stream=True,
            ) as response:
                response.raise_for_status()
                destination.parent.mkdir(parents=True, exist_ok=True)
                with destination.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            handle.write(chunk)
            return
        except (requests.RequestException, OSError) as exc:
            last_error = exc
            if destination.exists():
                destination.unlink()
    raise SourceSnapshotError(f"all official download URLs failed: {last_error}")


def load_ses_master_companies(
    *,
    base_completa: Path,
    listaempresas: Path,
) -> dict[str, dict[str, Any]]:
    """Build only the SES identity master needed by Classification from local files."""
    lista = _read_semicolon_csv(listaempresas)
    fip_col = _pick_column(list(lista.columns), ("codigofip", "codfip", "coenti"))
    cnpj_col = _pick_column(list(lista.columns), ("cnpj", "cnpjseg", "cnpjcpf"))
    if fip_col is None or cnpj_col is None:
        raise SourceSnapshotError("LISTAEMPRESAS cannot map FIP to CNPJ")

    cnpj_by_fip: dict[str, str] = {}
    for _, row in lista.iterrows():
        fip = _normalize_fip(row.get(fip_col))
        cnpj = "".join(ch for ch in str(row.get(cnpj_col) or "") if ch.isdigit())
        if fip and len(cnpj) == 14:
            cnpj_by_fip.setdefault(fip, cnpj)

    with zipfile.ZipFile(base_completa) as archive:
        cias_name = next(
            (name for name in archive.namelist() if name.casefold().endswith("ses_cias.csv")),
            None,
        )
        if cias_name is None:
            raise SourceSnapshotError("BaseCompleta missing Ses_cias.csv")
        with archive.open(cias_name) as handle:
            cias = pd.read_csv(
                handle,
                sep=";",
                encoding="latin1",
                dtype=str,
                on_bad_lines="skip",
            )

    fip_source = _pick_column(
        list(cias.columns),
        ("coenti", "co_entidade", "codigo", "codfip"),
    )
    name_source = _pick_column(
        list(cias.columns),
        ("noenti", "nomeentidade", "nome", "razaosocial"),
    )
    if fip_source is None or name_source is None:
        raise SourceSnapshotError("Ses_cias.csv missing identity columns")

    companies: dict[str, dict[str, Any]] = {}
    for _, row in cias.iterrows():
        fip = _normalize_fip(row.get(fip_source))
        if not fip or fip in companies:
            continue
        legal_name = str(row.get(name_source) or "").strip() or fip
        companies[fip] = {
            "id": fip,
            "name": legal_name,
            "cnpj": cnpj_by_fip.get(fip),
            "premiums": 0.0,
            "claims": 0.0,
            "net_worth": 0.0,
            "sources_found": [],
        }
    if len(companies) < 100:
        raise SourceSnapshotError(f"SES master unexpectedly small: {len(companies)}")
    return companies


def _cache_for(
    cache_dir: Path,
    *,
    source_id: str,
    source_url: str,
    suffix: str,
) -> CachedSource:
    return CachedSource(
        source_id=source_id,
        source_url=source_url,
        content_path=cache_dir / f"{source_id}{suffix}",
        metadata_path=cache_dir / f"{source_id}.meta.json",
    )


def _json_records_validator(path: Path) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(records, list) or not records:
        raise SourceSnapshotError(f"record snapshot is empty: {path}")


def _record_fetch_to_path(
    fetcher: Callable[[], list[dict[str, Any]]],
) -> Callable[[Path], None]:
    def fetch(path: Path) -> None:
        records = fetcher()
        if not records:
            raise SourceSnapshotError("official source returned no records")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"records": records}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    return fetch


def _load_record_snapshot(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("records") or []
    if not isinstance(rows, list):
        raise TypeError(f"snapshot records must be a list: {path}")
    return [dict(row) for row in rows]


def _acquire_record_source(
    *,
    source_id: str,
    source_url: str,
    destination: Path,
    cache_dir: Path,
    fetcher: Callable[[], list[dict[str, Any]]],
    context: BuildContext,
) -> AcquisitionResult:
    return acquire_with_validated_cache(
        source_id=source_id,
        source_url=source_url,
        destination=destination,
        cache=_cache_for(
            cache_dir,
            source_id=source_id,
            source_url=source_url,
            suffix=".json",
        ),
        fetch_to_path=_record_fetch_to_path(fetcher),
        validate_path=_json_records_validator,
        context=context,
    )


def _bootstrap_receita_cache(
    cache: CachedSource,
    existing_snapshot: Path,
) -> None:
    if cache.content_path.exists() or not existing_snapshot.is_file():
        return
    payload = json.loads(existing_snapshot.read_text(encoding="utf-8"))
    fetched_at = str(payload.get("generated_at") or "").strip() or utc_now()
    cache.store(existing_snapshot, fetched_at=fetched_at)


def _receita_payload_contract_version(payload: dict[str, Any]) -> str | None:
    value = str(
        (payload.get("meta") or {}).get("gate4_cache_contract_version") or ""
    ).strip()
    return value or None


def _validate_receita_cache_payload(
    payload: dict[str, Any],
    *,
    target_hash: str,
    allow_legacy_unversioned: bool,
) -> None:
    cached_target_hash = str(
        (payload.get("meta") or {}).get("target_universe_hash") or ""
    )
    if cached_target_hash != target_hash:
        raise SourceSnapshotError(
            "Receita lifecycle cache target-universe hash differs from current classification"
        )
    validate_refresh_payload(
        payload,
        allow_legacy_unversioned=allow_legacy_unversioned,
    )


def _materialize_compatible_receita_cache(
    *,
    cache: CachedSource,
    destination: Path,
    target_hash: str,
) -> tuple[str, bool]:
    """Materialize current cache and promote only structurally compatible legacy v0.

    The pre-version Gate 4 cache is accepted only when it passes the complete current
    Receita lifecycle validation and belongs to the exact current target universe.
    Promotion adds only the explicit cache-contract version, preserves the original
    fetched_at timestamp and rewrites the cached SHA-256 accordingly. Unknown non-empty
    versions fail closed instead of being guessed compatible.
    """
    fetched_at = cache.materialize(destination)
    payload = json.loads(destination.read_text(encoding="utf-8"))
    version = _receita_payload_contract_version(payload)
    if version is None:
        _validate_receita_cache_payload(
            payload,
            target_hash=target_hash,
            allow_legacy_unversioned=True,
        )
        payload.setdefault("meta", {})[
            "gate4_cache_contract_version"
        ] = RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION
        validate_refresh_payload(payload)
        write_snapshot_atomic(payload, destination)
        cache.store(destination, fetched_at=fetched_at)
        return fetched_at, True

    if version != RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION:
        raise SourceSnapshotError(
            f"unsupported Receita lifecycle cache contract version: {version}"
        )

    _validate_receita_cache_payload(
        payload,
        target_hash=target_hash,
        allow_legacy_unversioned=False,
    )
    return fetched_at, False


def _receita_snapshot(
    *,
    classification: dict[str, Any],
    destination: Path,
    cache_dir: Path,
    existing_snapshot: Path,
    context: BuildContext,
) -> AcquisitionResult:
    source_id = "receita_cnpj_lifecycle"
    source_url = RECEITA_CNPJ_OPEN_DATA_URL
    cache = _cache_for(
        cache_dir,
        source_id=source_id,
        source_url=source_url,
        suffix=".json",
    )
    _bootstrap_receita_cache(cache, existing_snapshot)
    entities = list(classification.get("entities") or [])
    target_hash = regulatory_target_universe_hash(entities)

    cache_validation_error: str | None = None
    current_error: str | None = None
    try:
        release = discover_latest_release()
        if cache.content_path.is_file():
            cached_payload = json.loads(cache.content_path.read_text(encoding="utf-8"))
            cached_period = str(
                (cached_payload.get("source") or {}).get("reference_period") or ""
            )
            cached_target_hash = str(
                (cached_payload.get("meta") or {}).get("target_universe_hash") or ""
            )
            if cached_period == release.period and cached_target_hash == target_hash:
                try:
                    fetched_at, promoted = _materialize_compatible_receita_cache(
                        cache=cache,
                        destination=destination,
                        target_hash=target_hash,
                    )
                except (
                    SourceCacheError,
                    SourceSnapshotError,
                    ReceitaRefreshValidationError,
                    OSError,
                    ValueError,
                    json.JSONDecodeError,
                ) as exc:
                    cache_validation_error = (
                        "current cache validation failed: "
                        f"{type(exc).__name__}: {exc}"
                    )
                    destination.unlink(missing_ok=True)
                else:
                    observation = SourceObservation(
                        source_id=source_id,
                        source_url=source_url,
                        snapshot_path=destination,
                        fetched_at=fetched_at,
                        current_validation_succeeded=True,
                        state_reason=(
                            "official release period, cache contract and regulatory target "
                            "hash unchanged"
                            + ("; compatible legacy cache promoted" if promoted else "")
                        ),
                    )
                    return AcquisitionResult(observation=observation, used_cache=True)

        temp = destination.with_suffix(destination.suffix + ".incoming")
        if temp.exists():
            temp.unlink()
        payload = refresh_receita_lifecycle(
            output=temp,
            release=release,
            entities=entities,
        )
        if not payload.get("records"):
            raise SourceSnapshotError("Receita lifecycle refresh returned no records")
        temp.replace(destination)
        fetched_at = utc_now()
        cache.store(destination, fetched_at=fetched_at)
        observation = SourceObservation(
            source_id=source_id,
            source_url=source_url,
            snapshot_path=destination,
            fetched_at=fetched_at,
            current_fetch_succeeded=True,
            state_reason="official bulk lifecycle snapshot refreshed",
        )
        return AcquisitionResult(observation=observation, used_cache=False)
    except Exception as exc:  # noqa: BLE001
        refresh_error = f"{type(exc).__name__}: {exc}"
        current_error = (
            f"{cache_validation_error}; {refresh_error}"
            if cache_validation_error
            else refresh_error
        )

    try:
        fetched_at, promoted = _materialize_compatible_receita_cache(
            cache=cache,
            destination=destination,
            target_hash=target_hash,
        )
        load_filtered_lifecycle_snapshot(destination)
    except (
        SourceCacheError,
        SourceSnapshotError,
        ReceitaRefreshValidationError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ):
        if destination.exists():
            destination.unlink()
        observation = SourceObservation(
            source_id=source_id,
            source_url=source_url,
            snapshot_path=destination,
            fetched_at=utc_now(),
            state_reason=current_error,
        )
        return AcquisitionResult(
            observation=observation,
            used_cache=False,
            current_error=current_error,
        )

    observation = SourceObservation(
        source_id=source_id,
        source_url=source_url,
        snapshot_path=destination,
        fetched_at=fetched_at,
        state_reason=(
            f"{current_error}; using hash-validated Receita lifecycle cache"
            + ("; compatible legacy cache promoted" if promoted else "")
        ),
    )
    return AcquisitionResult(
        observation=observation,
        used_cache=True,
        current_error=current_error,
    )


def _write_receita_records(source_path: Path, output_path: Path) -> None:
    records = load_filtered_lifecycle_snapshot(source_path)
    payload = {
        "artifact": "v2_receita_lifecycle_records_snapshot",
        "source_snapshot": str(source_path),
        "records": records,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp = output_path.with_suffix(output_path.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temp.replace(output_path)


def _require_available(results: list[AcquisitionResult], context: BuildContext) -> None:
    unavailable = [
        result.observation.source_id
        for result in results
        if result.observation.to_lineage(context).state == "unavailable"
    ]
    if unavailable:
        raise SourceSnapshotError(f"critical sources unavailable: {sorted(unavailable)}")


def build_source_snapshot(
    *,
    context: BuildContext,
    source_dir: Path = DEFAULT_SOURCE_DIR,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    ses_dir: Path = DEFAULT_SES_DIR,
    existing_receita_snapshot: Path = DEFAULT_RECEITA_FILTERED,
    lineage_path: Path = DEFAULT_LINEAGE,
) -> dict[str, Any]:
    source_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    ses_dir.mkdir(parents=True, exist_ok=True)

    base_path = ses_dir / "BaseCompleta.zip"
    lista_path = ses_dir / "LISTAEMPRESAS.csv"
    base = acquire_with_validated_cache(
        source_id="susep_ses_base_completa",
        source_url=SES_ZIP_URL,
        destination=base_path,
        cache=_cache_for(
            cache_dir,
            source_id="susep_ses_base_completa",
            source_url=SES_ZIP_URL,
            suffix=".zip",
        ),
        fetch_to_path=lambda path: _download_urls(
            [SES_ZIP_URL, SES_ZIP_URL_FALLBACK],
            path,
            timeout=600,
        ),
        validate_path=validate_base_completa,
        context=context,
    )
    lista = acquire_with_validated_cache(
        source_id="susep_listaempresas",
        source_url=SES_LISTAEMPRESAS_URL,
        destination=lista_path,
        cache=_cache_for(
            cache_dir,
            source_id="susep_listaempresas",
            source_url=SES_LISTAEMPRESAS_URL,
            suffix=".csv",
        ),
        fetch_to_path=lambda path: _download_urls(
            [SES_LISTAEMPRESAS_URL],
            path,
            timeout=120,
        ),
        validate_path=validate_listaempresas,
        context=context,
    )

    licensed_path = source_dir / "susep_licensed_entities.json"
    special_path = source_dir / "susep_special_regimes.json"
    sandbox_path = source_dir / "susep_sandbox.json"
    licensed = _acquire_record_source(
        source_id="susep_licensed_entities",
        source_url=LICENSED_ENTITIES_URL,
        destination=licensed_path,
        cache_dir=cache_dir,
        fetcher=fetch_licensed_entities,
        context=context,
    )
    special = _acquire_record_source(
        source_id="susep_special_regimes",
        source_url=";".join(SPECIAL_REGIME_URLS.values()),
        destination=special_path,
        cache_dir=cache_dir,
        fetcher=fetch_special_regime_records,
        context=context,
    )
    sandbox = _acquire_record_source(
        source_id="susep_sandbox",
        source_url=SANDBOX_PARTICIPANTS_URL,
        destination=sandbox_path,
        cache_dir=cache_dir,
        fetcher=fetch_sandbox_participants,
        context=context,
    )

    preliminary = [base, lista, licensed, special, sandbox]
    write_source_lineage(
        lineage_path,
        [result.observation for result in preliminary],
        context,
    )
    _require_available(preliminary, context)

    companies = load_ses_master_companies(
        base_completa=base_path,
        listaempresas=lista_path,
    )
    classification = build_classification_inventory(
        companies,
        _load_record_snapshot(licensed_path),
        _load_record_snapshot(special_path),
        _load_record_snapshot(sandbox_path),
    )
    classification_path = source_dir / "classification_inventory.json"
    write_classification_inventory(classification, classification_path)

    receita_source_path = source_dir / "receita_cnpj_lifecycle.json"
    receita = _receita_snapshot(
        classification=classification,
        destination=receita_source_path,
        cache_dir=cache_dir,
        existing_snapshot=existing_receita_snapshot,
        context=context,
    )
    all_results = [*preliminary, receita]
    write_source_lineage(
        lineage_path,
        [result.observation for result in all_results],
        context,
    )
    _require_available(all_results, context)

    receita_records = source_dir / "receita_lifecycle_records.json"
    _write_receita_records(receita_source_path, receita_records)

    return {
        "build": context.as_dict(),
        "classification_path": str(classification_path),
        "receita_lifecycle_records_path": str(receita_records),
        "base_completa_path": str(base_path),
        "listaempresas_path": str(lista_path),
        "lineage_path": str(lineage_path),
        "source_states": {
            result.observation.source_id: result.observation.to_lineage(context).state
            for result in all_results
        },
        "cache_used": {
            result.observation.source_id: result.used_cache for result in all_results
        },
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the Gate 4 regulatory source snapshot.")
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--ses-dir", type=Path, default=DEFAULT_SES_DIR)
    parser.add_argument(
        "--existing-receita-snapshot",
        type=Path,
        default=DEFAULT_RECEITA_FILTERED,
    )
    parser.add_argument("--lineage", type=Path, default=DEFAULT_LINEAGE)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    context = BuildContext.from_env()
    result = build_source_snapshot(
        context=context,
        source_dir=args.source_dir,
        cache_dir=args.cache_dir,
        ses_dir=args.ses_dir,
        existing_receita_snapshot=args.existing_receita_snapshot,
        lineage_path=args.lineage,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
