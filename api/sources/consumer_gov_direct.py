from __future__ import annotations

import csv
import gzip
import hashlib
import html as html_lib
import json
import os
import re
import shutil
import tempfile
import unicodedata
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CONSUMER_GOV_PAGE = os.getenv(
    "CG_DIRECT_PAGE",
    "https://www.consumidor.gov.br/pages/dadosabertos/externo/",
)
CONSUMER_GOV_BASE = "https://www.consumidor.gov.br"
RAW_DIR = Path(os.getenv("CG_RAW_DIR", "data/raw/consumidor_gov"))
MANIFEST_PATH = Path(os.getenv("CG_RAW_MANIFEST", str(RAW_DIR / "manifest.json")))
MIN_MONTH_BYTES = int(os.getenv("CG_MIN_MONTH_BYTES", "20000000"))
HTTP_RETRIES = int(os.getenv("CG_HTTP_RETRIES", "5"))
HTTP_BACKOFF = float(os.getenv("CG_HTTP_BACKOFF", "1.5"))
CONNECT_TIMEOUT = float(os.getenv("CG_CONNECT_TIMEOUT", "20"))
READ_TIMEOUT = float(os.getenv("CG_READ_TIMEOUT", "120"))
DOWNLOAD_TIMEOUT = float(os.getenv("CG_DOWNLOAD_READ_TIMEOUT", "300"))
HEADLESS_TIMEOUT = int(os.getenv("CG_HEADLESS_TIMEOUT", "120"))
HEADLESS_MAX_PAGES = int(os.getenv("CG_HEADLESS_MAX_PAGES", "100"))
SCHEMA_VERSION = "consumer-gov-monthly-conduct-core-v1-2026-08"

REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "provider": ("Nome Fantasia", "Empresa", "Fornecedor"),
    "segment": ("Segmento de Mercado", "Segmento"),
    "responded": ("Respondida",),
    "situation": ("Situação", "Situacao"),
    "evaluation": ("Avaliação Reclamação", "Avaliacao Reclamacao"),
    "score": ("Nota do Consumidor",),
    "response_time": ("Tempo Resposta", "Tempo de Resposta"),
    "contacted_company": ("Procurou Empresa",),
    "area": ("Área", "Area"),
    "subject": ("Assunto",),
    "problem_group": ("Grupo Problema",),
    "problem": ("Problema",),
    "purchase_channel": ("Como Comprou Contratou", "Como Comprou/Contratou"),
}

_MONTH_NAMES = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}
_CODE_RE = re.compile(r"(?<!\d)(20\d{10,18})(?!\d)")
_DOWNLOAD_CODE_RE = re.compile(
    r"download\s*\(\s*['\"](?P<code>\d{10,24})['\"]\s*\)", re.IGNORECASE
)
_SCRIPT_RE = re.compile(r"<script[^>]+src=['\"]([^'\"]+)['\"]", re.IGNORECASE)
_AJAX_PATTERNS = (
    re.compile(r"['\"]?sAjaxSource['\"]?\s*[:=]\s*['\"]([^'\"]+)", re.IGNORECASE),
    re.compile(r"['\"]?ajax['\"]?\s*:\s*['\"]([^'\"]+)", re.IGNORECASE),
    re.compile(
        r"['\"]?ajax['\"]?\s*:\s*\{.{0,1800}?['\"]?url['\"]?\s*:\s*['\"]([^'\"]+)",
        re.IGNORECASE | re.DOTALL,
    ),
)


class ConsumerGovRawError(RuntimeError):
    code = "consumer_gov_raw_error"

    def __init__(self, message: str) -> None:
        super().__init__(f"{self.code}: {message}")


class ConsumerGovDiscoveryUnavailable(ConsumerGovRawError):
    code = "consumer_gov_discovery_unavailable"


class ConsumerGovPublicationNotFound(ConsumerGovRawError):
    code = "consumer_gov_publication_not_found"


class ConsumerGovDownloadFailed(ConsumerGovRawError):
    code = "consumer_gov_download_failed"


class ConsumerGovArchiveInvalid(ConsumerGovRawError):
    code = "consumer_gov_archive_invalid"


class ConsumerGovSchemaMismatch(ConsumerGovRawError):
    code = "consumer_gov_schema_mismatch"


class ConsumerGovMonthMismatch(ConsumerGovRawError):
    code = "consumer_gov_month_mismatch"


@dataclass(frozen=True)
class Publication:
    code: str
    title: str
    filename: str | None
    published_at: str | None
    month: str | None
    discovery_method: str

    @property
    def download_url(self) -> str:
        return (
            f"{CONSUMER_GOV_BASE}/pages/publicacao/externo/"
            f"{self.code}/download"
        )


@dataclass(frozen=True)
class ValidationResult:
    month: str
    bytes: int
    rows: int
    sha256: str
    encoding: str
    delimiter: str
    columns: tuple[str, ...]
    field_map: dict[str, str]
    month_observations: int
    month_matches: int
    schema_version: str = SCHEMA_VERSION


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _session() -> requests.Session:
    retry = Retry(
        total=HTTP_RETRIES,
        connect=HTTP_RETRIES,
        read=HTTP_RETRIES,
        status=HTTP_RETRIES,
        backoff_factor=HTTP_BACKOFF,
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
            "User-Agent": (
                "widget-confiabilidade-seguradoras/"
                "consumer-gov-direct-1.0"
            ),
            "Accept": "text/html,application/json,text/csv,*/*;q=0.8",
        }
    )
    return session


def _norm(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _extract_month(text: str) -> str | None:
    normalized = _norm(text)
    match = re.search(
        r"(?<!\d)(20\d{2})\D{0,3}(0?[1-9]|1[0-2])(?!\d)",
        normalized,
    )
    if match:
        return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}"

    match = re.search(
        r"(?<!\d)(0?[1-9]|1[0-2])\D{0,3}(20\d{2})(?!\d)",
        normalized,
    )
    if match:
        return f"{int(match.group(2)):04d}-{int(match.group(1)):02d}"

    year_match = re.search(r"\b(20\d{2})\b", normalized)
    if year_match:
        for name, month in _MONTH_NAMES.items():
            if re.search(rf"\b{name}\b", normalized):
                return f"{int(year_match.group(1)):04d}-{month:02d}"
    return None


def _field(record: dict[str, Any], names: tuple[str, ...]) -> str:
    normalized = {_norm(key): value for key, value in record.items()}
    for name in names:
        value = normalized.get(_norm(name))
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _publication_from_record(
    record: dict[str, Any],
    method: str,
) -> Publication | None:
    code = _field(record, ("codigo", "código", "code", "id"))
    if not re.fullmatch(r"\d{10,24}", code):
        blob = json.dumps(record, ensure_ascii=False)
        code_match = _CODE_RE.search(blob)
        code = code_match.group(1) if code_match else ""
    if not code:
        return None

    title = _field(
        record,
        ("texto", "titulo", "título", "descricao", "descrição", "nome"),
    )
    filename = _field(
        record,
        ("nomeArquivo", "nome_arquivo", "arquivo", "filename", "file"),
    ) or None
    published = _field(
        record,
        (
            "dataPublicacao",
            "data_publicacao",
            "data",
            "publishedAt",
            "publicadoEm",
        ),
    ) or None
    month = _extract_month(filename or "") or _extract_month(title)

    return Publication(
        code=code,
        title=title,
        filename=filename,
        published_at=published,
        month=month,
        discovery_method=method,
    )


def _is_monthly_data_publication(publication: Publication) -> bool:
    blob = _norm(f"{publication.title} {publication.filename or ''}")
    return (
        blob.startswith("dados ")
        or "base completa" in blob
        or "basecompleta" in blob
        or "reclamacoes finalizadas" in blob
    )


def _datatables_script_urls(page_html: str) -> list[str]:
    urls: list[str] = []
    for raw in _SCRIPT_RE.findall(page_html):
        src = html_lib.unescape(raw)
        if "datatablesController" not in src or "publicacoesDT" not in src:
            continue
        absolute = urljoin(CONSUMER_GOV_PAGE, src)
        if absolute not in urls:
            urls.append(absolute)
    return urls


def _ajax_urls(script: str, script_url: str) -> list[str]:
    out: list[str] = []
    for pattern in _AJAX_PATTERNS:
        for raw in pattern.findall(script):
            candidate = html_lib.unescape(raw).replace("\\/", "/")
            absolute = urljoin(script_url, candidate)
            if absolute not in out:
                out.append(absolute)
    return out


def _extract_record_list(
    payload: Any,
) -> tuple[list[dict[str, Any]], int | None]:
    if isinstance(payload, list):
        rows = [item for item in payload if isinstance(item, dict)]
        return rows, len(rows)

    if not isinstance(payload, dict):
        return [], None

    for key in ("aaData", "data", "results", "rows", "content"):
        value = payload.get(key)
        if not isinstance(value, list):
            continue
        rows = [item for item in value if isinstance(item, dict)]
        total: int | None = None
        for total_key in (
            "iTotalRecords",
            "recordsTotal",
            "total",
            "totalElements",
            "iTotalDisplayRecords",
        ):
            try:
                if payload.get(total_key) is not None:
                    total = int(payload[total_key])
                    break
            except (TypeError, ValueError):
                continue
        return rows, total
    return [], None


def _with_query(url: str, params: dict[str, Any]) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update({key: str(value) for key, value in params.items()})
    return urlunparse(parsed._replace(query=urlencode(query)))


def _fetch_datatables_payload(
    session: requests.Session,
    ajax_url: str,
    *,
    start: int,
    length: int,
) -> Any:
    variants = (
        {
            "sEcho": 1,
            "iDisplayStart": start,
            "iDisplayLength": length,
            "start": start,
            "length": length,
            "draw": 1,
        },
        {"start": start, "length": length, "draw": 1},
        {"iDisplayStart": start, "iDisplayLength": length, "sEcho": 1},
    )
    errors: list[str] = []
    for params in variants:
        url = _with_query(ajax_url, params)
        try:
            response = session.get(
                url,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
            if response.status_code != 200:
                errors.append(f"{response.status_code} {url}")
                continue
            content_type = response.headers.get("content-type", "").lower()
            if "json" not in content_type and not response.text.lstrip().startswith(
                ("{", "[")
            ):
                errors.append(f"non-json {content_type} {url}")
                continue
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
    raise ConsumerGovDiscoveryUnavailable(
        "structured DataTables endpoint failed: "
        + " | ".join(errors[-3:])
    )


def discover_structured_publications(
    wanted_months: set[str] | None = None,
    *,
    session: requests.Session | None = None,
) -> list[Publication]:
    http = session or _session()
    try:
        page = http.get(
            CONSUMER_GOV_PAGE,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )
        page.raise_for_status()
    except requests.RequestException as exc:
        raise ConsumerGovDiscoveryUnavailable(
            f"official Dados Abertos page unavailable: {exc}"
        ) from exc

    script_urls = _datatables_script_urls(page.text)
    if not script_urls:
        raise ConsumerGovDiscoveryUnavailable(
            "generated publicacoesDT DataTables script was not found"
        )

    ajax_urls: list[str] = []
    for script_url in script_urls:
        try:
            response = http.get(
                script_url,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
            response.raise_for_status()
        except requests.RequestException:
            continue
        for ajax_url in _ajax_urls(response.text, script_url):
            if ajax_url not in ajax_urls:
                ajax_urls.append(ajax_url)

    if not ajax_urls:
        raise ConsumerGovDiscoveryUnavailable(
            "no structured AJAX endpoint could be extracted from publicacoesDT"
        )

    last_error: Exception | None = None
    for ajax_url in ajax_urls:
        try:
            found: dict[str, Publication] = {}
            start = 0
            page_size = 100
            while start < 10000:
                payload = _fetch_datatables_payload(
                    http,
                    ajax_url,
                    start=start,
                    length=page_size,
                )
                rows, total = _extract_record_list(payload)
                if not rows:
                    break
                for row in rows:
                    publication = _publication_from_record(
                        row,
                        "structured_http",
                    )
                    if (
                        publication is None
                        or not _is_monthly_data_publication(publication)
                    ):
                        continue
                    found[publication.code] = publication
                if wanted_months:
                    resolved = {
                        item.month for item in found.values() if item.month
                    }
                    if wanted_months <= resolved:
                        break
                start += len(rows)
                if total is not None and start >= total:
                    break
                if len(rows) < page_size:
                    break

            if found:
                return list(found.values())
        except ConsumerGovRawError as exc:
            last_error = exc
            continue

    if last_error is not None:
        raise ConsumerGovDiscoveryUnavailable(str(last_error)) from last_error
    raise ConsumerGovDiscoveryUnavailable(
        "structured discovery returned no monthly data publications"
    )


def _dom_publication(
    row_text: str,
    onclick: str,
    title_attr: str,
) -> Publication | None:
    match = _DOWNLOAD_CODE_RE.search(onclick or "")
    if not match:
        return None
    code = match.group("code")
    filename_match = re.search(
        r"Download\s+['\"]([^'\"]+)['\"]",
        title_attr or "",
        re.IGNORECASE,
    )
    filename = filename_match.group(1).strip() if filename_match else None
    month = _extract_month(filename or "") or _extract_month(row_text)
    return Publication(
        code=code,
        title=row_text.strip(),
        filename=filename,
        published_at=None,
        month=month,
        discovery_method="headless_dom",
    )


def discover_headless_publications(
    wanted_months: set[str] | None = None,
) -> list[Publication]:
    try:
        from selenium import webdriver
        from selenium.common.exceptions import TimeoutException, WebDriverException
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as ec
        from selenium.webdriver.support.ui import Select, WebDriverWait
    except ImportError as exc:
        raise ConsumerGovDiscoveryUnavailable(
            "headless DOM fallback requires selenium"
        ) from exc

    options = webdriver.ChromeOptions()
    for arg in (
        "--headless=new",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--window-size=1600,1200",
    ):
        options.add_argument(arg)

    driver = None
    try:
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(HEADLESS_TIMEOUT)
        driver.get(CONSUMER_GOV_PAGE)
        wait = WebDriverWait(driver, HEADLESS_TIMEOUT)
        wait.until(
            ec.presence_of_element_located(
                (By.CSS_SELECTOR, "#publicacoesDT tbody tr")
            )
        )

        try:
            select = Select(driver.find_element(By.NAME, "publicacoesDT_length"))
            values = {
                option.get_attribute("value")
                for option in select.options
                if option.get_attribute("value")
            }
            for preferred in ("100", "50", "25"):
                if preferred in values:
                    select.select_by_value(preferred)
                    break
        except WebDriverException:
            pass

        found: dict[str, Publication] = {}
        for _ in range(HEADLESS_MAX_PAGES):
            rows = driver.find_elements(
                By.CSS_SELECTOR,
                "#publicacoesDT tbody tr",
            )
            first_code = ""
            for row in rows:
                buttons = row.find_elements(
                    By.CSS_SELECTOR,
                    "[onclick*='download(']",
                )
                if not buttons:
                    continue
                button = buttons[0]
                onclick = button.get_attribute("onclick") or ""
                title_attr = button.get_attribute("title") or ""
                publication = _dom_publication(
                    row.text,
                    onclick,
                    title_attr,
                )
                if publication is None:
                    continue
                if not first_code:
                    first_code = publication.code
                if _is_monthly_data_publication(publication):
                    found[publication.code] = publication

            if wanted_months:
                resolved = {item.month for item in found.values() if item.month}
                if wanted_months <= resolved:
                    break

            next_nodes = driver.find_elements(
                By.CSS_SELECTOR,
                "#publicacoesDT_next, a.next, button.next",
            )
            if not next_nodes:
                break
            next_node = next_nodes[0]
            classes = next_node.get_attribute("class") or ""
            aria_disabled = (
                next_node.get_attribute("aria-disabled") or ""
            ).lower()
            if "disabled" in classes or aria_disabled == "true":
                break

            next_node.click()
            if first_code:
                try:
                    wait.until(
                        lambda current_driver, code=first_code: code
                        not in current_driver.find_element(
                            By.CSS_SELECTOR,
                            "#publicacoesDT tbody",
                        ).get_attribute("innerHTML")
                    )
                except TimeoutException:
                    pass

        if found:
            return list(found.values())
    except (TimeoutException, WebDriverException) as exc:
        raise ConsumerGovDiscoveryUnavailable(
            f"headless DOM discovery failed: {exc}"
        ) from exc
    finally:
        if driver is not None:
            driver.quit()

    raise ConsumerGovDiscoveryUnavailable(
        "headless DOM discovery returned no monthly data publications"
    )


def discover_publications(wanted_months: set[str]) -> list[Publication]:
    structured_error: Exception | None = None
    structured: list[Publication] = []
    try:
        structured = discover_structured_publications(wanted_months)
    except ConsumerGovRawError as exc:
        structured_error = exc

    by_code = {item.code: item for item in structured}
    resolved = {item.month for item in by_code.values() if item.month}
    if wanted_months <= resolved:
        return list(by_code.values())

    try:
        for publication in discover_headless_publications(
            wanted_months - resolved
        ):
            by_code.setdefault(publication.code, publication)
    except ConsumerGovRawError as dom_error:
        if not by_code:
            detail = f"structured={structured_error}; headless={dom_error}"
            raise ConsumerGovDiscoveryUnavailable(detail) from dom_error

    return list(by_code.values())


def _select_publication(
    publications: list[Publication],
    month: str,
) -> Publication:
    candidates = [item for item in publications if item.month == month]
    if not candidates:
        raise ConsumerGovPublicationNotFound(
            f"no official monthly publication resolved for {month}"
        )
    candidates.sort(
        key=lambda item: (
            "base completa" in _norm(
                f"{item.title} {item.filename or ''}"
            ),
            "basecompleta" in _norm(item.filename or ""),
            item.code,
        ),
        reverse=True,
    )
    return candidates[0]


def _detect_csv_format(path: Path) -> tuple[str, str, tuple[str, ...]]:
    with path.open("rb") as handle:
        sample = handle.read(65536)
    if sample.lstrip().lower().startswith((b"<!doctype html", b"<html")):
        raise ConsumerGovArchiveInvalid("downloaded payload is HTML, not data")

    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
        try:
            text = sample.decode(encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
            continue
        lines = text.splitlines()
        first_line = lines[0] if lines else ""
        delimiter = ";" if first_line.count(";") >= first_line.count(",") else ","
        columns = tuple(part.strip() for part in first_line.split(delimiter))
        if len(columns) >= 5:
            return encoding, delimiter, columns
    raise ConsumerGovSchemaMismatch(
        f"could not decode CSV header: {last_error}"
    )


def _resolve_schema(columns: tuple[str, ...]) -> dict[str, str]:
    normalized = {_norm(column): column for column in columns}
    field_map: dict[str, str] = {}
    missing: list[str] = []
    for semantic, aliases in REQUIRED_FIELDS.items():
        selected = ""
        for alias in aliases:
            selected = normalized.get(_norm(alias), "")
            if selected:
                break
        if selected:
            field_map[semantic] = selected
        else:
            missing.append(semantic)
    if missing:
        raise ConsumerGovSchemaMismatch(
            f"required fields missing: {missing}; columns={list(columns)}"
        )
    return field_map


def _month_from_row(
    row: dict[str, str],
    normalized_keys: dict[str, str],
) -> str | None:
    year_key = normalized_keys.get(_norm("Ano Abertura"))
    month_key = normalized_keys.get(_norm("Mês Abertura")) or normalized_keys.get(
        _norm("Mes Abertura")
    )
    if year_key and month_key:
        try:
            return (
                f"{int(row.get(year_key, '')):04d}-"
                f"{int(row.get(month_key, '')):02d}"
            )
        except (TypeError, ValueError):
            pass

    for alias in (
        "Data Abertura",
        "Data da Reclamação",
        "Data Reclamacao",
        "Data Finalização",
        "Data Finalizacao",
    ):
        key = normalized_keys.get(_norm(alias))
        value = row.get(key, "") if key else ""
        match = re.search(r"(\d{2})[/-](\d{2})[/-](20\d{2})", value)
        if match:
            return f"{int(match.group(3)):04d}-{int(match.group(2)):02d}"
        match = re.search(r"(20\d{2})[/-](\d{2})[/-](\d{2})", value)
        if match:
            return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}"
    return None


def validate_month_csv(path: Path, month: str) -> ValidationResult:
    size = path.stat().st_size if path.exists() else 0
    if size < MIN_MONTH_BYTES:
        raise ConsumerGovArchiveInvalid(
            f"{path.name} has {size} bytes, below minimum {MIN_MONTH_BYTES}"
        )

    encoding, delimiter, columns = _detect_csv_format(path)
    field_map = _resolve_schema(columns)
    sha = hashlib.sha256()
    rows = 0
    observed = 0
    matched = 0
    normalized_keys = {_norm(column): column for column in columns}

    with path.open("rb") as binary:
        for chunk in iter(lambda: binary.read(4 * 1024 * 1024), b""):
            sha.update(chunk)

    try:
        with path.open("r", encoding=encoding, newline="") as handle:
            reader = csv.DictReader(handle, delimiter=delimiter)
            for row in reader:
                rows += 1
                row_month = _month_from_row(row, normalized_keys)
                if row_month is None:
                    continue
                observed += 1
                if row_month == month:
                    matched += 1
    except (OSError, UnicodeError, csv.Error) as exc:
        raise ConsumerGovSchemaMismatch(f"CSV read failed: {exc}") from exc

    if rows <= 0:
        raise ConsumerGovArchiveInvalid("CSV has no data rows")
    if observed > 0 and matched / observed < 0.995:
        raise ConsumerGovMonthMismatch(
            f"{month}: only {matched}/{observed} dated rows belong to expected month"
        )

    return ValidationResult(
        month=month,
        bytes=size,
        rows=rows,
        sha256=sha.hexdigest(),
        encoding=encoding,
        delimiter=delimiter,
        columns=columns,
        field_map=field_map,
        month_observations=observed,
        month_matches=matched,
    )


def _materialize_download(downloaded: Path, destination: Path) -> None:
    with downloaded.open("rb") as handle:
        signature = handle.read(4)
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_csv = destination.with_suffix(destination.suffix + ".tmp")
    tmp_csv.unlink(missing_ok=True)

    try:
        if signature.startswith(b"PK\x03\x04"):
            try:
                with zipfile.ZipFile(downloaded) as archive:
                    candidates = [
                        name
                        for name in archive.namelist()
                        if name.lower().endswith(".csv")
                    ]
                    if not candidates:
                        raise ConsumerGovArchiveInvalid("ZIP contains no CSV")
                    candidates.sort(
                        key=lambda name: (
                            "basecompleta" in _norm(name),
                            "base completa" in _norm(name),
                            archive.getinfo(name).file_size,
                        ),
                        reverse=True,
                    )
                    with (
                        archive.open(candidates[0]) as source,
                        tmp_csv.open("wb") as target,
                    ):
                        shutil.copyfileobj(source, target)
            except zipfile.BadZipFile as exc:
                raise ConsumerGovArchiveInvalid(
                    f"invalid ZIP: {exc}"
                ) from exc
        elif signature.startswith(b"\x1f\x8b"):
            try:
                with (
                    gzip.open(downloaded, "rb") as source,
                    tmp_csv.open("wb") as target,
                ):
                    shutil.copyfileobj(source, target)
            except OSError as exc:
                raise ConsumerGovArchiveInvalid(
                    f"invalid GZIP: {exc}"
                ) from exc
        else:
            shutil.copyfile(downloaded, tmp_csv)

        if not tmp_csv.exists() or tmp_csv.stat().st_size == 0:
            raise ConsumerGovArchiveInvalid("materialized CSV is empty")
        tmp_csv.replace(destination)
    finally:
        tmp_csv.unlink(missing_ok=True)


def download_publication(
    publication: Publication,
    destination: Path,
    month: str,
    *,
    session: requests.Session | None = None,
) -> ValidationResult:
    http = session or _session()
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        delete=False,
        suffix=".download",
        dir=str(destination.parent),
    ) as tmp:
        tmp_path = Path(tmp.name)

    try:
        try:
            with http.get(
                publication.download_url,
                stream=True,
                timeout=(CONNECT_TIMEOUT, DOWNLOAD_TIMEOUT),
                allow_redirects=True,
            ) as response:
                response.raise_for_status()
                with tmp_path.open("wb") as handle:
                    for chunk in response.iter_content(
                        chunk_size=4 * 1024 * 1024
                    ):
                        if chunk:
                            handle.write(chunk)
        except requests.RequestException as exc:
            raise ConsumerGovDownloadFailed(
                f"{month} from {publication.download_url}: {exc}"
            ) from exc

        _materialize_download(tmp_path, destination)
        return validate_month_csv(destination, month)
    except ConsumerGovRawError:
        destination.unlink(missing_ok=True)
        raise
    finally:
        tmp_path.unlink(missing_ok=True)


def _load_manifest(path: Path = MANIFEST_PATH) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "months": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return {"schema_version": 1, "months": {}}
    if not isinstance(payload, dict) or not isinstance(
        payload.get("months"), dict
    ):
        return {"schema_version": 1, "months": {}}
    return payload


def _save_manifest(payload: dict[str, Any], path: Path = MANIFEST_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)


def _manifest_cache_hit(
    path: Path,
    month: str,
    entry: dict[str, Any] | None,
) -> bool:
    if not entry or entry.get("validation") != "passed":
        return False
    if entry.get("schema_version") != SCHEMA_VERSION:
        return False
    if entry.get("month") != month or not path.exists():
        return False
    try:
        return int(entry.get("bytes") or -1) == path.stat().st_size
    except (OSError, TypeError, ValueError):
        return False


def ensure_months(
    months: list[str],
    *,
    raw_dir: Path | None = None,
    manifest_path: Path | None = None,
) -> dict[str, dict[str, Any]]:
    target_dir = raw_dir or RAW_DIR
    target_manifest = manifest_path or (
        MANIFEST_PATH if target_dir == RAW_DIR else target_dir / "manifest.json"
    )
    manifest = _load_manifest(target_manifest)
    month_manifest = manifest.setdefault("months", {})
    result: dict[str, dict[str, Any]] = {}
    missing: list[str] = []

    for month in months:
        path = target_dir / f"basecompleta_{month}.csv"
        entry = month_manifest.get(month)
        if _manifest_cache_hit(path, month, entry):
            result[month] = {
                "path": path,
                "resource_url": entry.get("source_url"),
                "resource_name": entry.get("filename"),
                "bytes": path.stat().st_size,
                "acquisition": "cache_manifest",
                "discovery_method": entry.get("discovery_method"),
            }
            continue

        if path.exists() and path.stat().st_size >= MIN_MONTH_BYTES:
            try:
                validation = validate_month_csv(path, month)
            except ConsumerGovRawError:
                path.unlink(missing_ok=True)
            else:
                entry = {
                    **asdict(validation),
                    "columns": list(validation.columns),
                    "month": month,
                    "source": "existing_cache",
                    "source_url": None,
                    "filename": path.name,
                    "discovery_method": "legacy_cache_validated",
                    "downloaded_at": None,
                    "validated_at": _utc_now(),
                    "validation": "passed",
                }
                month_manifest[month] = entry
                result[month] = {
                    "path": path,
                    "resource_url": None,
                    "resource_name": path.name,
                    "bytes": path.stat().st_size,
                    "acquisition": "cache_schema_validated",
                    "discovery_method": "legacy_cache_validated",
                }
                continue
        missing.append(month)

    if not missing:
        manifest["updated_at"] = _utc_now()
        _save_manifest(manifest, target_manifest)
        return result

    publications = discover_publications(set(missing))
    for month in missing:
        publication = _select_publication(publications, month)
        path = target_dir / f"basecompleta_{month}.csv"
        validation = download_publication(publication, path, month)
        entry = {
            **asdict(validation),
            "columns": list(validation.columns),
            "month": month,
            "source": "consumidor.gov.br",
            "source_url": publication.download_url,
            "publication_code": publication.code,
            "publication_title": publication.title,
            "filename": publication.filename,
            "published_at": publication.published_at,
            "discovery_method": publication.discovery_method,
            "downloaded_at": _utc_now(),
            "validated_at": _utc_now(),
            "validation": "passed",
        }
        month_manifest[month] = entry
        result[month] = {
            "path": path,
            "resource_url": publication.download_url,
            "resource_name": publication.filename or publication.title,
            "bytes": path.stat().st_size,
            "acquisition": "download",
            "discovery_method": publication.discovery_method,
        }

    manifest["updated_at"] = _utc_now()
    _save_manifest(manifest, target_manifest)
    return result
