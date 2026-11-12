from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .sources import consumer_gov_direct
from .sources.consumer_gov_csv import detect_csv_encoding
from .utils.name_cleaner import normalize_name_key

CG_DATASET_ID = os.getenv("CG_DATASET_ID", "reclamacoes-do-consumidor-gov-br")
CG_API_BASE = os.getenv("CG_CKAN_API_BASE", "https://dados.mj.gov.br/api/3/action")

RAW_DIR = Path(os.getenv("CG_RAW_DIR", "data/raw/consumidor_gov"))
DERIVED_DIR = Path(os.getenv("CG_DERIVED_DIR", "data/derived/consumidor_gov"))
MONTHLY_DIR = Path(os.getenv("CG_DERIVED_MONTHLY_DIR", str(DERIVED_DIR / "monthly")))

MONTHS_BACK = int(os.getenv("CG_MONTHS_BACK", "12"))
FORCE_MONTH = os.getenv("CG_FORCE_MONTH")  # ex.: "2025-12"
FORCE_DOWNLOAD = os.getenv("CG_FORCE_DOWNLOAD", "0").strip() == "1"

CG_MIN_MONTH_BYTES = int(os.getenv("CG_MIN_MONTH_BYTES", "20000000"))
CG_HTTP_RETRIES = int(os.getenv("CG_HTTP_RETRIES", "5"))
CG_HTTP_BACKOFF = float(os.getenv("CG_HTTP_BACKOFF", "1.5"))
CG_CONNECT_TIMEOUT = float(os.getenv("CG_CONNECT_TIMEOUT", "20"))
CG_READ_TIMEOUT = float(os.getenv("CG_READ_TIMEOUT", "120"))
CG_DOWNLOAD_READ_TIMEOUT = float(os.getenv("CG_DOWNLOAD_READ_TIMEOUT", "300"))
CG_MAX_COMPANY_DROP_PCT = float(os.getenv("CG_MAX_COMPANY_DROP_PCT", "0.25"))

TARGET_SEGMENT = os.getenv(
    "CG_TARGET_SEGMENT",
    "Seguros, Capitalização e Previdência",
).strip()


@dataclass(frozen=True)
class ResourceInfo:
    month: str
    name: str
    url: str
    format: str | None = None
    publication: consumer_gov_direct.Publication | None = None


_MONTH_RE = re.compile(r"(20\d{2})[-_/]?(\d{2})")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _build_session() -> requests.Session:
    retry = Retry(
        total=CG_HTTP_RETRIES,
        connect=CG_HTTP_RETRIES,
        read=CG_HTTP_RETRIES,
        status=CG_HTTP_RETRIES,
        backoff_factor=CG_HTTP_BACKOFF,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=8,
        pool_maxsize=8,
    )
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "widget-confiabilidade-seguradoras/1.0",
            "Accept": "application/json,text/csv,*/*;q=0.8",
        }
    )
    return session


HTTP = _build_session()


def _month_to_date(ym: str) -> date:
    year, month = ym.split("-")
    return date(int(year), int(month), 1)


def _add_months(d: date, delta: int) -> date:
    year = d.year + (d.month - 1 + delta) // 12
    month = (d.month - 1 + delta) % 12 + 1
    return date(year, month, 1)


def _ym(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _safe_float(v: Any) -> float:
    try:
        if v in (None, "", "NA", "N/A", "-", "nan"):
            return 0.0
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


def _read_json_gz(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rb") as file:
        root = json.loads(file.read().decode("utf-8"))
    if not isinstance(root, dict):
        raise TypeError(f"{path} não contém um objeto JSON")
    return root


def _entries_from_root(root: dict[str, Any]) -> dict[str, Any]:
    entries = (
        root.get("by_name_key_raw")
        or root.get("by_name_key")
        or root.get("by_name")
        or {}
    )
    return entries if isinstance(entries, dict) else {}


def _root_company_count(root: dict[str, Any]) -> int:
    meta = root.get("meta") if isinstance(root.get("meta"), dict) else {}
    try:
        declared = int(meta.get("companies") or 0)
    except (TypeError, ValueError):
        declared = 0
    return max(declared, len(_entries_from_root(root)))


def _is_valid_monthly_root(
    root: dict[str, Any], expected_month: str | None = None
) -> bool:
    """Check preserved counters, not just the presence of provider names."""
    meta = root.get("meta") if isinstance(root.get("meta"), dict) else {}
    if meta.get("invalid") is True or meta.get("status") not in (None, "ok"):
        return False
    month = str(meta.get("month") or meta.get("ym") or "")
    if not month or (expected_month is not None and month != expected_month):
        return False
    entries = _entries_from_root(root)
    if not entries:
        return False

    count_sum = 0
    for entry in entries.values():
        if not isinstance(entry, dict):
            return False
        stats = entry.get("statistics")
        if not isinstance(stats, dict):
            return False
        count = stats.get("complaintsCount", stats.get("total_claims"))
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            return False
        for key in (
            "respondedCount", "resolvedCount", "finalizedCount",
            "satisfactionCount",
        ):
            value = stats.get(key)
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
                or value > count
            ):
                return False
        count_sum += count

    if "companies" in meta and meta["companies"] != len(entries):
        return False
    if "lines_kept" in meta and meta["lines_kept"] != count_sum:
        return False
    return not (
        "lines_total" in meta
        and (
            not isinstance(meta["lines_total"], int)
            or meta["lines_total"] < count_sum
        )
    )


def _is_valid_aggregate_root(root: dict[str, Any]) -> bool:
    meta = root.get("meta") if isinstance(root.get("meta"), dict) else {}
    if meta.get("invalid") is True or meta.get("status") != "ok":
        return False
    if meta.get("invalid_months"):
        return False
    entries = _entries_from_root(root)
    if not entries:
        return False
    if meta.get("companies") not in (None, len(entries)):
        return False
    for entry in entries.values():
        if not isinstance(entry, dict):
            return False
        stats = entry.get("statistics")
        if not isinstance(stats, dict):
            return False
        count = stats.get("complaintsCount", stats.get("total_claims"))
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            return False
    return True


def _load_valid_monthly(path: Path, expected_month: str) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        root = _read_json_gz(path)
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"CG WARN: cache mensal ilegível {path.name}: {exc}")
        return None
    if not _is_valid_monthly_root(root, expected_month):
        print(f"CG WARN: cache mensal inválido ignorado: {path.name}")
        return None
    return root


def _load_valid_aggregate(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        root = _read_json_gz(path)
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"CG WARN: agregado existente ilegível {path}: {exc}")
        return None
    if not _is_valid_aggregate_root(root):
        print(f"CG WARN: agregado existente não passou na validação: {path}")
        return None
    return root


def _atomic_write_json_gz(obj: dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = Path(f"{out_path}.tmp")
    payload = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    try:
        with gzip.open(tmp_path, "wb") as file:
            file.write(payload)
        tmp_path.replace(out_path)
    except OSError:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError as cleanup_error:
            print(f"CG WARN: falha ao remover temporário {tmp_path}: {cleanup_error}")
        raise


def _ckan_get(action: str, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{CG_API_BASE.rstrip('/')}/{action}"
    response = HTTP.get(
        url,
        params=params,
        timeout=(CG_CONNECT_TIMEOUT, CG_READ_TIMEOUT),
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or not data.get("success"):
        raise RuntimeError(f"CKAN action failed: {action} {data}")
    result = data.get("result")
    if not isinstance(result, dict):
        raise TypeError(f"CKAN retornou result inválido em {action}")
    return result


def _extract_month(text: str) -> str | None:
    """Extrai YYYY-MM de nomes como basecompleta2025-12 ou 2025_12."""
    match = _MONTH_RE.search(_norm(text))
    if not match:
        return None
    year = int(match.group(1))
    month = int(match.group(2))
    if not 1 <= month <= 12:
        return None
    return f"{year:04d}-{month:02d}"


def _list_basecompleta_resources_ckan() -> dict[str, ResourceInfo]:
    """Legacy CKAN discovery retained for diagnostics/manual forensics only."""
    package = _ckan_get("package_show", {"id": CG_DATASET_ID})
    resources = package.get("resources") or []
    out: dict[str, ResourceInfo] = {}

    for resource in resources:
        if not isinstance(resource, dict):
            continue
        name = str(resource.get("name") or resource.get("title") or "")
        url = str(resource.get("url") or "")
        fmt = str(resource.get("format") or "").strip() or None

        haystack = f"{name} {url}".lower()
        if "basecompleta" not in haystack and "base completa" not in haystack:
            continue

        month = _extract_month(haystack)
        if not month:
            continue

        candidate = ResourceInfo(month=month, name=name, url=url, format=fmt)
        previous = out.get(month)
        if not previous:
            out[month] = candidate
            continue

        candidate_is_csv = (fmt or "").lower() == "csv" or url.lower().endswith(".csv")
        previous_is_csv = (
            (previous.format or "").lower() == "csv"
            or previous.url.lower().endswith(".csv")
        )
        if candidate_is_csv and not previous_is_csv:
            out[month] = candidate

    return out


def _is_explicit_basecompleta_publication(
    publication: consumer_gov_direct.Publication,
) -> bool:
    blob = _norm(f"{publication.title} {publication.filename or ''}")
    return "base completa" in blob or "basecompleta" in blob


def _list_basecompleta_resources() -> dict[str, ResourceInfo]:
    """Resolve a Base Completa diretamente na publicação oficial Consumer.gov."""
    publications = consumer_gov_direct.discover_publications(set())
    candidates: dict[str, list[consumer_gov_direct.Publication]] = {}
    for publication in publications:
        if not publication.month or not _is_explicit_basecompleta_publication(publication):
            continue
        candidates.setdefault(publication.month, []).append(publication)

    out: dict[str, ResourceInfo] = {}
    for month, monthly in candidates.items():
        monthly.sort(key=lambda item: item.code, reverse=True)
        selected = monthly[0]
        filename = selected.filename or ""
        suffix = Path(filename).suffix.lstrip(".").upper() or None
        out[month] = ResourceInfo(
            month=month,
            name=selected.title or filename or f"Base Completa {month}",
            url=selected.download_url,
            format=suffix,
            publication=selected,
        )
    return out


def _download(url: str, out_path: Path) -> None:
    """Baixa de forma atômica; download parcial nunca substitui o cache anterior."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    try:
        with HTTP.get(
            url,
            stream=True,
            timeout=(CG_CONNECT_TIMEOUT, CG_DOWNLOAD_READ_TIMEOUT),
        ) as response:
            response.raise_for_status()
            with open(tmp_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=4 * 1024 * 1024):
                    if chunk:
                        file.write(chunk)

        if not tmp_path.exists() or tmp_path.stat().st_size == 0:
            raise RuntimeError(f"download vazio: {url}")
        tmp_path.replace(out_path)
    except (OSError, RuntimeError, requests.RequestException):
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError as cleanup_error:
            print(f"CG WARN: falha ao remover temporário {tmp_path}: {cleanup_error}")
        raise


def _download_resource(resource: ResourceInfo, out_path: Path) -> None:
    if resource.publication is not None:
        consumer_gov_direct.download_publication(
            resource.publication,
            out_path,
            resource.month,
        )
        return
    _download(resource.url, out_path)


def _iter_rows(csv_path: Path) -> Iterable[dict[str, str]]:
    """Itera o CSV sem reiniciar a leitura após emitir registros."""
    encoding = detect_csv_encoding(csv_path)
    with csv_path.open("r", encoding=encoding, newline="") as file:
        first_line = file.readline()
        delimiter = ";" if ";" in first_line else ","
        file.seek(0)
        reader = csv.DictReader(file, delimiter=delimiter)
        for row in reader:
            yield {
                str(key): (value if value is not None else "")
                for key, value in row.items()
                if key is not None
            }


def _aggregate_basecompleta(
    csv_path: Path,
    month: str,
    resource_url: str,
) -> dict[str, Any]:
    """Agrega a Base Completa transacional por nome da empresa."""
    target_norm = _norm(TARGET_SEGMENT)
    entries: dict[str, dict[str, Any]] = {}
    display_count: dict[str, dict[str, int]] = {}
    lines_total = 0
    lines_kept = 0

    for row in _iter_rows(csv_path):
        lines_total += 1
        if _norm(row.get("Segmento de Mercado", "")) != target_norm:
            continue

        name = (
            row.get("Nome Fantasia")
            or row.get("Empresa")
            or row.get("Fornecedor")
            or ""
        ).strip()
        if not name:
            continue

        situation = _norm(row.get("Situação", ""))
        if "cancelada" in situation:
            continue

        key = normalize_name_key(name)
        if not key:
            continue

        lines_kept += 1
        if key not in entries:
            entries[key] = {
                "name": name,
                "display_name": name,
                "cnpj": "",
                "statistics": {
                    "complaintsCount": 0,
                    "respondedCount": 0,
                    "resolvedCount": 0,
                    "finalizedCount": 0,
                    "scoreSum": 0.0,
                    "satisfactionCount": 0,
                    "total_claims": 0,
                    "responded_claims": 0,
                    "resolved_claims": 0,
                    "finalized_claims": 0,
                },
            }
            display_count[key] = {}

        counts = display_count[key]
        counts[name] = counts.get(name, 0) + 1
        current_display = str(entries[key].get("display_name") or "")
        if counts[name] > counts.get(current_display, 0):
            entries[key]["display_name"] = name
            entries[key]["name"] = name

        stats = entries[key]["statistics"]
        stats["complaintsCount"] += 1
        stats["total_claims"] += 1

        if _norm(row.get("Respondida", "")) == "s":
            stats["respondedCount"] += 1
            stats["responded_claims"] += 1

        if "finalizada" in situation or "encerrada" in situation:
            stats["finalizedCount"] += 1
            stats["finalized_claims"] += 1

        if _norm(row.get("Avaliação Reclamação", "")) == "resolvida":
            stats["resolvedCount"] += 1
            stats["resolved_claims"] += 1

        score_raw = (row.get("Nota do Consumidor") or "").strip()
        if score_raw:
            score = _safe_float(score_raw)
            if score > 0:
                stats["scoreSum"] += score
                stats["satisfactionCount"] += 1

    for entry in entries.values():
        stats = entry.get("statistics") or {}
        score_sum = float(stats.get("scoreSum") or 0.0)
        count = int(stats.get("satisfactionCount") or 0)
        stats["averageScore"] = round(score_sum / count, 4) if count > 0 else 0.0

    meta = {
        "status": "ok",
        "dataset": CG_DATASET_ID,
        "month": month,
        "source_file": str(csv_path),
        "resource_url": resource_url,
        "generated_at": _utc_now(),
        "filter_segment": TARGET_SEGMENT,
        "lines_total": lines_total,
        "lines_kept": lines_kept,
        "companies": len(entries),
    }

    return {
        "meta": meta,
        "by_name_key_raw": entries,
        "by_name_key": entries,
        "by_name": entries,
        "by_cnpj_key_raw": {},
        "by_cnpj_key": {},
    }


def _merge_months(
    monthly: list[dict[str, Any]],
    invalid_months: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if invalid_months:
        raise ValueError(f"refusing to merge incomplete monthly window: {invalid_months}")
    merged: dict[str, dict[str, Any]] = {}
    months: list[str] = []

    for root in monthly:
        if not _is_valid_monthly_root(root):
            raise ValueError("refusing invalid monthly evidence")
        meta = root.get("meta") or {}
        month = str(meta.get("month") or meta.get("ym") or "")
        if not month or month in months:
            raise ValueError(f"missing or duplicated month: {month}")
        months.append(month)

        for key, entry in _entries_from_root(root).items():
            if not isinstance(entry, dict):
                continue
            stats = entry.get("statistics") or {}
            if key not in merged:
                merged[key] = {
                    "name": entry.get("name") or entry.get("display_name") or "",
                    "display_name": entry.get("display_name") or entry.get("name") or "",
                    "cnpj": "",
                    "statistics": {
                        "complaintsCount": 0,
                        "respondedCount": 0,
                        "resolvedCount": 0,
                        "finalizedCount": 0,
                        "scoreSum": 0.0,
                        "satisfactionCount": 0,
                        "total_claims": 0,
                        "responded_claims": 0,
                        "resolved_claims": 0,
                        "finalized_claims": 0,
                    },
                }

            merged_stats = merged[key]["statistics"]
            for field in (
                "complaintsCount",
                "respondedCount",
                "resolvedCount",
                "finalizedCount",
                "satisfactionCount",
            ):
                merged_stats[field] += int(stats.get(field) or 0)
            merged_stats["scoreSum"] += float(stats.get("scoreSum") or 0.0)
            for field in (
                "total_claims",
                "responded_claims",
                "resolved_claims",
                "finalized_claims",
            ):
                merged_stats[field] += int(stats.get(field) or 0)

    for entry in merged.values():
        stats = entry["statistics"]
        score_sum = float(stats.get("scoreSum") or 0.0)
        count = int(stats.get("satisfactionCount") or 0)
        stats["averageScore"] = round(score_sum / count, 4) if count > 0 else 0.0

    if len(months) != len(monthly):
        raise ValueError("monthly window lost a declared month")
    meta = {
        "status": "ok",
        "dataset": CG_DATASET_ID,
        "months": sorted(set(months)),
        "generated_at": _utc_now(),
        "filter_segment": TARGET_SEGMENT,
        "companies": len(merged),
        "invalid_months": [],
        "source_family": "base_completa",
        "reference_event": "abertura",
        "semantics": {
            "source_role": "reputation",
            "primary_key": "name_key",
            "name_key_fn": "normalize_name_key",
            "has_reliable_cnpj": False,
            "cnpj_status": "source_does_not_provide_structured_cnpj",
            "matching_strategy_required": "fuzzy_name_match_against_susep_master",
        },
    }

    return {
        "meta": meta,
        "by_name_key_raw": merged,
        "by_name_key": merged,
        "by_name": merged,
        "by_cnpj_key_raw": {},
        "by_cnpj_key": {},
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_immutable_gzip(payload: dict[str, Any], destination: Path) -> str:
    """Store one immutable object; never replace an active generation in place."""
    data = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    compressed = gzip.compress(data, mtime=0)
    digest = hashlib.sha256(compressed).hexdigest()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_file():
        if _sha256(destination) != digest:
            raise RuntimeError(f"immutable Consumer.gov object has changed: {destination}")
        return digest
    with tempfile.NamedTemporaryFile(
        delete=False, suffix=".tmp", dir=str(destination.parent)
    ) as handle:
        temporary = Path(handle.name)
    try:
        temporary.write_bytes(compressed)
        if _sha256(temporary) != digest:
            raise RuntimeError("Consumer.gov object changed during staging")
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    return digest


def _publish_validated_window(
    roots: list[dict[str, Any]],
    aggregate: dict[str, Any],
) -> dict[str, Any]:
    """Activate all months and aggregate with ONE manifest replacement.

    Monthly and aggregate objects are immutable; existing V2 materialization
    and the previously versioned baseline are never overwritten here.
    """
    from api.v2.consumer_gov_window import (
        SOURCE_FAMILY,
        WINDOW_MANIFEST,
        WINDOW_MANIFEST_VERSION,
        ConsumerGovWindowError,
        load_active_window,
    )

    declared = list((aggregate.get("meta") or {}).get("months") or [])
    by_month = {
        str((root.get("meta") or {}).get("month") or ""): root
        for root in roots
    }
    if (
        len(declared) != MONTHS_BACK
        or len(by_month) != MONTHS_BACK
        or sorted(by_month) != declared
        or not _is_valid_aggregate_root(aggregate)
        or any(
            not _is_valid_monthly_root(by_month[month], month)
            for month in declared
        )
    ):
        raise ValueError("candidate Consumer.gov window is incomplete or invalid")

    # Reconcile per provider before writing even immutable candidate objects.
    def provider_counts(root: dict[str, Any]) -> dict[str, int]:
        return {
            str(key): int((entry.get("statistics") or {}).get("complaintsCount") or 0)
            for key, entry in _entries_from_root(root).items()
        }

    total_by_provider: dict[str, int] = {}
    for month in declared:
        for key, count in provider_counts(by_month[month]).items():
            total_by_provider[key] = total_by_provider.get(key, 0) + count
    if total_by_provider != provider_counts(aggregate):
        raise ValueError("candidate monthly/aggregate provider counts disagree")

    manifest_path = DERIVED_DIR / WINDOW_MANIFEST
    if manifest_path.exists():
        try:
            prior = load_active_window(
                DERIVED_DIR, MONTHLY_DIR, required=MONTHS_BACK
            )
        except ConsumerGovWindowError as exc:
            raise ValueError(f"existing active manifest is invalid: {exc}") from exc
        previous_source_hashes = [
            str((root.get("meta") or {}).get("source_sha256") or "")
            for _, _, root in prior.months
        ]
        candidate_source_hashes = [
            str((by_month[month].get("meta") or {}).get("source_sha256") or "")
            for month in declared
        ]
        if (
            [month for month, _, _ in prior.months] == declared
            and previous_source_hashes == candidate_source_hashes
            and all(candidate_source_hashes)
        ):
            return json.loads(manifest_path.read_text(encoding="utf-8"))

    monthly_records = []
    for month in declared:
        root = by_month[month]
        data = json.dumps(
            root, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        digest = hashlib.sha256(gzip.compress(data, mtime=0)).hexdigest()
        object_path = (
            MONTHLY_DIR / "objects" / f"consumidor_gov_{month}-{digest}.json.gz"
        )
        if _write_immutable_gzip(root, object_path) != digest:
            raise RuntimeError(f"{month}: monthly object digest changed")
        monthly_records.append(
            {
                "month": month,
                "sha256": digest,
                "path": object_path.relative_to(DERIVED_DIR).as_posix(),
                "source_sha256": str(
                    (root.get("meta") or {}).get("source_sha256") or ""
                ),
                "rows": int((root.get("meta") or {}).get("lines_total") or 0),
                "complaints": int(
                    (root.get("meta") or {}).get("lines_kept")
                    or sum(provider_counts(root).values())
                ),
            }
        )

    data = json.dumps(
        aggregate, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    agg_sha = hashlib.sha256(gzip.compress(data, mtime=0)).hexdigest()
    aggregate_path = DERIVED_DIR / "objects" / f"{agg_sha}.json.gz"
    if _write_immutable_gzip(aggregate, aggregate_path) != agg_sha:
        raise RuntimeError("candidate aggregate object digest changed")

    manifest = {
        "artifact": "consumer_gov_validated_window",
        "version": WINDOW_MANIFEST_VERSION,
        "state": "complete",
        "source_family": SOURCE_FAMILY,
        "reference_event": "abertura",
        "selected_months": declared,
        "aggregate": {
            "path": aggregate_path.relative_to(DERIVED_DIR).as_posix(),
            "sha256": agg_sha,
        },
        "months": monthly_records,
        "validated_at": _utc_now(),
    }

    # Validate the entire candidate while the active manifest is untouched:
    # the final replace is the only moment the new selection becomes visible.
    with tempfile.NamedTemporaryFile(
        delete=False, suffix=".candidate", dir=str(DERIVED_DIR)
    ) as handle:
        temporary = Path(handle.name)
    try:
        temporary.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        # Check the candidate selection and provider reconciliation before
        # touching the previously active manifest.
        load_active_window(
            DERIVED_DIR,
            MONTHLY_DIR,
            required=MONTHS_BACK,
            manifest_path=temporary,
        )
        temporary.replace(manifest_path)
    finally:
        temporary.unlink(missing_ok=True)

    # Post-activation self-check. The previously active data is not modified.
    load_active_window(DERIVED_DIR, MONTHLY_DIR, required=MONTHS_BACK)
    return manifest


def _preserve_existing_aggregate(
    existing: dict[str, Any] | None,
    reason: str,
) -> int | None:
    if not existing:
        return None
    meta = existing.get("meta") or {}
    print(
        "CG WARN: atualização online não concluída; preservando agregado válido "
        f"existente (companies={_root_company_count(existing)}, "
        f"months={meta.get('months')}). Motivo: {reason}"
    )
    return 0


def _remove_invalid_cache(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        print(f"CG WARN: não consegui remover cache inválido {path}: {exc}")


def main() -> int:
    """Build an all-or-nothing Base Completa candidate without altering V2."""
    from api.v2.consumer_gov_window import (
        WINDOW_MANIFEST,
        ConsumerGovWindowError,
        load_active_window,
    )

    print("\n--- BUILD CONSUMIDOR.GOV (BASE COMPLETA -> VERIFIED WINDOW) ---")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    MONTHLY_DIR.mkdir(parents=True, exist_ok=True)
    DERIVED_DIR.mkdir(parents=True, exist_ok=True)

    legacy_aggregate_path = DERIVED_DIR / "consumidor_gov_agg.json.gz"
    legacy_aggregate = _load_valid_aggregate(legacy_aggregate_path)
    active = None
    try:
        active = load_active_window(
            DERIVED_DIR, MONTHLY_DIR, required=MONTHS_BACK
        )
    except ConsumerGovWindowError as exc:
        if (DERIVED_DIR / WINDOW_MANIFEST).exists():
            print(f"CG FAIL: existing active window manifest invalid: {exc}")
            return 1

    previous = active.aggregate if active else legacy_aggregate

    def unavailable(reason: str) -> int:
        preserved = _preserve_existing_aggregate(previous, reason)
        if preserved is not None:
            return preserved
        print(f"CG FAIL: {reason}; no validated previous aggregate")
        return 1

    if MONTHS_BACK <= 0 or MONTHS_BACK > 24:
        return unavailable(f"invalid CG_MONTHS_BACK={MONTHS_BACK}")

    try:
        resources_by_month = _list_basecompleta_resources()
    except (requests.RequestException, RuntimeError, ValueError) as exc:
        return unavailable(f"official Base Completa unavailable: {exc}")

    if not resources_by_month:
        return unavailable("no explicit Base Completa publications found")

    latest = FORCE_MONTH or max(resources_by_month)
    if latest not in resources_by_month:
        return unavailable(f"requested month {latest} missing from official catalog")

    anchor = _month_to_date(latest)
    required_months = sorted(
        _ym(_add_months(anchor, -index))
        for index in range(MONTHS_BACK)
    )
    missing_publications = [
        month for month in required_months if month not in resources_by_month
    ]
    if missing_publications:
        return unavailable(
            f"incomplete official Base Completa window: missing={missing_publications}"
        )

    print(f"CG: required complete window: {required_months}")
    active_roots = {
        month: root for month, _, root in active.months
    } if active else {}
    roots: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, Any]] = []

    # Temp files do not touch the previously selected raw or monthly objects.
    with tempfile.TemporaryDirectory(
        prefix=".consumer-gov-build-", dir=str(RAW_DIR)
    ) as staging:
        for month in required_months:
            if month in active_roots and not FORCE_DOWNLOAD:
                roots[month] = active_roots[month]
                continue

            resource = resources_by_month[month]
            legacy_raw = RAW_DIR / f"basecompleta_{month}.csv"
            candidate_raw = legacy_raw

            try:
                # A new download is staged, not promoted into the old raw path.
                if FORCE_DOWNLOAD or (
                    not legacy_raw.is_file()
                    or legacy_raw.stat().st_size < CG_MIN_MONTH_BYTES
                ):
                    candidate_raw = Path(staging) / f"basecompleta_{month}.csv"
                    _download_resource(resource, candidate_raw)

                try:
                    validation = consumer_gov_direct.validate_month_csv(
                        candidate_raw, month, min_bytes=CG_MIN_MONTH_BYTES
                    )
                except consumer_gov_direct.ConsumerGovRawError:
                    if candidate_raw != legacy_raw:
                        raise
                    # The existing unverified file cannot be adopted;
                    # retry through the same staged official acquisition.
                    candidate_raw = Path(staging) / f"basecompleta_{month}.csv"
                    _download_resource(resource, candidate_raw)
                    validation = consumer_gov_direct.validate_month_csv(
                        candidate_raw, month, min_bytes=CG_MIN_MONTH_BYTES
                    )

                root = _aggregate_basecompleta(
                    candidate_raw, month=month, resource_url=resource.url
                )
                meta = root["meta"]
                meta.update(
                    {
                        "source_file": f"basecompleta_{month}.csv",
                        "source_family": "base_completa",
                        "reference_event": "abertura",
                        "schema_version": validation.schema_version,
                        "source_sha256": validation.sha256,
                        "source_rows": validation.rows,
                    }
                )
                if not _is_valid_monthly_root(root, month):
                    raise ValueError(f"{month}: invalid monthly counter conservation")
                roots[month] = root
            except (
                OSError, RuntimeError, ValueError, UnicodeError, csv.Error,
                requests.RequestException,
            ) as exc:
                errors.append({"month": month, "reason": type(exc).__name__, "error": str(exc)})
                print(f"CG WARN: rejected month {month}: {exc}")

    if errors or len(roots) != MONTHS_BACK:
        return unavailable(f"candidate window failed closed: {errors}")

    try:
        ordered = [roots[month] for month in required_months]
        aggregate = _merge_months(ordered)
        if not _is_valid_aggregate_root(aggregate):
            raise ValueError("candidate aggregate is empty or invalid")
        if (aggregate.get("meta") or {}).get("months") != required_months:
            raise ValueError("candidate aggregate reference months diverged")

        if previous is not None:
            old_count = _root_company_count(previous)
            new_count = _root_company_count(aggregate)
            min_allowed = int(old_count * (1.0 - CG_MAX_COMPANY_DROP_PCT))
            if old_count > 0 and new_count < min_allowed:
                raise ValueError(
                    f"unexpected provider coverage drop: {old_count} -> {new_count}"
                )

        # No derived or V2 object is selected until the candidate passes
        # per-month and per-provider checks and the single manifest is swapped.
        manifest = _publish_validated_window(ordered, aggregate)
    except (
        OSError, RuntimeError, ValueError, ConsumerGovWindowError
    ) as exc:
        return unavailable(f"candidate publication rejected: {exc}")

    print(
        f"CG: complete Base Completa window activated via manifest "
        f"({manifest['selected_months'][0]}.."
        f"{manifest['selected_months'][-1]}, "
        f"companies={_root_company_count(aggregate)})"
    )
    print("CG: no cross-family stitching, ranking, scoring or CNPJ inference.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
