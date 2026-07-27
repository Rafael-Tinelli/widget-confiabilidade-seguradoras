# api/build_consumidor_gov.py
from __future__ import annotations

import csv
import gzip
import json
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
    root: dict[str, Any],
    expected_month: str | None = None,
) -> bool:
    meta = root.get("meta") if isinstance(root.get("meta"), dict) else {}
    if meta.get("invalid") is True or str(meta.get("status") or "").lower() == "invalid":
        return False

    month = str(meta.get("month") or meta.get("ym") or "")
    if expected_month and month and month != expected_month:
        return False

    return _root_company_count(root) > 0 and bool(_entries_from_root(root))


def _is_valid_aggregate_root(root: dict[str, Any]) -> bool:
    meta = root.get("meta") if isinstance(root.get("meta"), dict) else {}
    if meta.get("invalid") is True or str(meta.get("status") or "").lower() == "invalid":
        return False
    return _root_company_count(root) > 0 and bool(_entries_from_root(root))


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


def _list_basecompleta_resources() -> dict[str, ResourceInfo]:
    """Lê o dataset no CKAN e monta um mapa month -> recurso Base Completa."""
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


def _iter_rows(csv_path: Path) -> Iterable[dict[str, str]]:
    """Itera o CSV com autodetecção simples de encoding e delimitador."""
    encodings = ["utf-8", "utf-8-sig", "latin1"]
    last_error: UnicodeDecodeError | None = None

    for encoding in encodings:
        try:
            with open(csv_path, "r", encoding=encoding, newline="") as file:
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
            return
        except UnicodeDecodeError as exc:
            last_error = exc

    raise RuntimeError(f"Falha ao ler CSV (encoding). Último erro: {last_error}")


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
    merged: dict[str, dict[str, Any]] = {}
    months: list[str] = []

    for root in monthly:
        if not _is_valid_monthly_root(root):
            continue
        meta = root.get("meta") or {}
        month = str(meta.get("month") or meta.get("ym") or "")
        if month:
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

    invalid_months = invalid_months or []
    status = "partial" if invalid_months else "ok"
    meta = {
        "status": status,
        "dataset": CG_DATASET_ID,
        "months": sorted(set(months)),
        "generated_at": _utc_now(),
        "filter_segment": TARGET_SEGMENT,
        "companies": len(merged),
        "invalid_months": invalid_months,
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
    print("\n--- BUILD CONSUMIDOR.GOV (BASE COMPLETA -> AGG) ---")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    MONTHLY_DIR.mkdir(parents=True, exist_ok=True)
    DERIVED_DIR.mkdir(parents=True, exist_ok=True)

    aggregate_path = DERIVED_DIR / "consumidor_gov_agg.json.gz"
    existing_aggregate = _load_valid_aggregate(aggregate_path)

    try:
        resources_by_month = _list_basecompleta_resources()
    except (requests.RequestException, RuntimeError, ValueError) as exc:
        preserved = _preserve_existing_aggregate(
            existing_aggregate,
            f"CKAN indisponível: {exc}",
        )
        if preserved is not None:
            return preserved
        print(f"CG FAIL: CKAN indisponível e nenhum agregado válido existe: {exc}")
        return 1

    if not resources_by_month:
        preserved = _preserve_existing_aggregate(
            existing_aggregate,
            "nenhum recurso 'Base Completa' foi encontrado",
        )
        if preserved is not None:
            return preserved
        print("CG FAIL: nenhum recurso 'Base Completa' encontrado e sem cache válido.")
        return 1

    available_months = sorted(resources_by_month)
    latest_month = available_months[-1]
    if FORCE_MONTH:
        if FORCE_MONTH not in resources_by_month:
            preserved = _preserve_existing_aggregate(
                existing_aggregate,
                f"CG_FORCE_MONTH={FORCE_MONTH} não existe no dataset",
            )
            if preserved is not None:
                return preserved
            print(
                f"CG FAIL: CG_FORCE_MONTH={FORCE_MONTH} não existe. "
                f"Disponíveis: {available_months[-6:]}"
            )
            return 1
        latest_month = FORCE_MONTH

    anchor = _month_to_date(latest_month)
    target_months = [
        month
        for i in range(MONTHS_BACK)
        if (month := _ym(_add_months(anchor, -i))) in resources_by_month
    ]
    target_months = sorted(target_months, reverse=True)

    if not target_months:
        preserved = _preserve_existing_aggregate(
            existing_aggregate,
            "nenhum mês processável após aplicar a janela",
        )
        if preserved is not None:
            return preserved
        print("CG FAIL: nenhum mês processável e sem agregado válido.")
        return 1

    print(f"CG: mês mais recente disponível: {latest_month}")
    print(f"CG: meses a processar (até {MONTHS_BACK}): {target_months}")

    monthly_roots: list[dict[str, Any]] = []
    invalid_months: list[dict[str, Any]] = []

    for month in target_months:
        resource = resources_by_month[month]
        monthly_path = MONTHLY_DIR / f"consumidor_gov_{month}.json.gz"
        cached_monthly = _load_valid_monthly(monthly_path, month)

        if cached_monthly is not None and not FORCE_DOWNLOAD:
            print(f"CG: {month} já possui cache mensal válido; reutilizando.")
            monthly_roots.append(cached_monthly)
            continue

        if monthly_path.exists() and cached_monthly is None:
            _remove_invalid_cache(monthly_path)

        raw_csv = RAW_DIR / f"basecompleta_{month}.csv"
        raw_is_usable = raw_csv.exists() and raw_csv.stat().st_size >= CG_MIN_MONTH_BYTES

        if FORCE_DOWNLOAD or not raw_is_usable:
            print(f"CG: baixando {month}: {resource.name}")
            try:
                _download(resource.url, raw_csv)
            except (OSError, RuntimeError, requests.RequestException) as exc:
                if cached_monthly is not None:
                    print(f"CG WARN: download falhou; usando cache mensal de {month}: {exc}")
                    monthly_roots.append(cached_monthly)
                    continue
                invalid_months.append(
                    {"month": month, "reason": "download_failed", "error": str(exc)}
                )
                print(f"CG WARN: {month} indisponível: {exc}")
                continue

        size = raw_csv.stat().st_size if raw_csv.exists() else 0
        if size < CG_MIN_MONTH_BYTES:
            print(
                f"CG WARN: {month} dump inválido "
                f"(bytes={size} < {CG_MIN_MONTH_BYTES}); descartando."
            )
            _remove_invalid_cache(raw_csv)
            if cached_monthly is not None:
                monthly_roots.append(cached_monthly)
                continue
            invalid_months.append(
                {
                    "month": month,
                    "reason": "bytes_below_threshold",
                    "bytes": size,
                }
            )
            continue

        try:
            root = _aggregate_basecompleta(
                raw_csv,
                month=month,
                resource_url=resource.url,
            )
        except (OSError, UnicodeError, ValueError, RuntimeError, csv.Error) as exc:
            if cached_monthly is not None:
                print(f"CG WARN: agregação falhou; usando cache mensal de {month}: {exc}")
                monthly_roots.append(cached_monthly)
                continue
            invalid_months.append(
                {"month": month, "reason": "aggregation_failed", "error": str(exc)}
            )
            print(f"CG WARN: falha ao agregar {month}: {exc}")
            continue

        if not _is_valid_monthly_root(root, month):
            companies = _root_company_count(root)
            lines_kept = int((root.get("meta") or {}).get("lines_kept") or 0)
            if cached_monthly is not None:
                print(f"CG WARN: agregado mensal vazio; usando cache mensal de {month}.")
                monthly_roots.append(cached_monthly)
                continue
            invalid_months.append(
                {
                    "month": month,
                    "reason": "zero_companies_or_kept",
                    "companies": companies,
                    "kept": lines_kept,
                }
            )
            print(
                f"CG WARN: {month} agregado inválido "
                f"(companies={companies}, kept={lines_kept})."
            )
            continue

        _atomic_write_json_gz(root, monthly_path)
        monthly_roots.append(root)
        meta = root.get("meta") or {}
        print(
            f"CG: OK {month} -> {monthly_path.as_posix()} "
            f"(linhas={meta.get('lines_total')}, kept={meta.get('lines_kept')}, "
            f"empresas={meta.get('companies')})"
        )

    # Se parte da janela falhou e já há agregado bom, não publica uma regressão parcial.
    if invalid_months and existing_aggregate:
        preserved = _preserve_existing_aggregate(
            existing_aggregate,
            f"meses incompletos: {invalid_months}",
        )
        if preserved is not None:
            return preserved

    aggregate = _merge_months(monthly_roots, invalid_months)
    if not _is_valid_aggregate_root(aggregate):
        preserved = _preserve_existing_aggregate(
            existing_aggregate,
            "novo agregado ficou vazio ou inválido",
        )
        if preserved is not None:
            return preserved
        print("CG FAIL: nenhum mês válido produziu um agregado utilizável.")
        return 1

    # Guard rail contra perda abrupta de cobertura por mudança ou falha silenciosa da fonte.
    if existing_aggregate:
        old_count = _root_company_count(existing_aggregate)
        new_count = _root_company_count(aggregate)
        min_allowed = int(old_count * (1.0 - CG_MAX_COMPANY_DROP_PCT))
        if old_count > 0 and new_count < min_allowed:
            preserved = _preserve_existing_aggregate(
                existing_aggregate,
                f"queda anormal de empresas: atual={new_count}, anterior={old_count}, "
                f"mínimo={min_allowed}",
            )
            if preserved is not None:
                return preserved

        old_months = set((existing_aggregate.get("meta") or {}).get("months") or [])
        new_months = set((aggregate.get("meta") or {}).get("months") or [])
        if old_months and len(new_months) < min(len(old_months), MONTHS_BACK):
            preserved = _preserve_existing_aggregate(
                existing_aggregate,
                f"cobertura mensal encolheu: atual={sorted(new_months)}, "
                f"anterior={sorted(old_months)}",
            )
            if preserved is not None:
                return preserved

    _atomic_write_json_gz(aggregate, aggregate_path)
    print(
        f"CG: OK agregado multi-mês -> {aggregate_path.as_posix()} "
        f"(empresas={_root_company_count(aggregate)}, "
        f"status={(aggregate.get('meta') or {}).get('status')})"
    )
    print("CG: CNPJ não confiável na fonte; matching permanece por nome contra SUSEP.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
