from __future__ import annotations

import csv
import io
import re
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests

from api.sources.receita_cnpj import normalize_receita_lifecycle_record
from api.utils.identifiers import normalize_cnpj_v2

OFFICIAL_CNPJ_HOST = "https://arquivos.receitafederal.gov.br"
OFFICIAL_CNPJ_SHARE_TOKEN = "YggdBLfdninEJX9"
OFFICIAL_CNPJ_SHARE_URL = (
    f"{OFFICIAL_CNPJ_HOST}/index.php/s/{OFFICIAL_CNPJ_SHARE_TOKEN}"
)
OFFICIAL_CNPJ_DAV_ROOT = (
    f"{OFFICIAL_CNPJ_HOST}/public.php/dav/files/{OFFICIAL_CNPJ_SHARE_TOKEN}/"
)

STATUS_CODE_TO_LABEL = {
    "01": "NULA",
    "02": "ATIVA",
    "03": "SUSPENSA",
    "04": "INAPTA",
    "05": "ATIVA NÃO REGULAR",
    "08": "BAIXADA",
}

ESTABLISHMENT_FILE_RE = re.compile(
    r"Estabelecimentos\d+\.zip$",
    re.IGNORECASE,
)
PERIOD_RE = re.compile(r"20\d{2}-\d{2}$")
DEFAULT_TIMEOUT = (15, 120)
DAV_PROPFIND_BODY = """<?xml version="1.0"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:displayname/>
    <d:getcontentlength/>
    <d:getlastmodified/>
    <d:getetag/>
    <d:resourcetype/>
  </d:prop>
</d:propfind>
"""


class ReceitaOpenDataError(RuntimeError):
    """Raised when the official CNPJ open-data source cannot be used safely."""


@dataclass(frozen=True)
class ReceitaOpenDataRelease:
    base_url: str
    release_url: str
    period: str
    establishment_files: tuple[str, ...]
    reasons_file: str = "Motivos.zip"
    resource_metadata: tuple[dict[str, Any], ...] = ()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _configure_official_session(session: requests.Session) -> None:
    session.headers.setdefault("User-Agent", "Sanida-CNPJ-Lifecycle/2.0")
    session.auth = (OFFICIAL_CNPJ_SHARE_TOKEN, "")


def _dav_resources(xml_payload: bytes) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(xml_payload)
    except ET.ParseError as exc:
        raise ReceitaOpenDataError(f"Invalid Receita WebDAV XML: {exc}") from exc

    ns = {"d": "DAV:"}
    resources: list[dict[str, Any]] = []
    for response in root.findall("d:response", ns):
        propstat = next(
            (
                item
                for item in response.findall("d:propstat", ns)
                if "200" in (item.findtext("d:status", default="", namespaces=ns) or "")
            ),
            None,
        )
        if propstat is None:
            continue
        prop = propstat.find("d:prop", ns)
        if prop is None:
            continue

        name = prop.findtext("d:displayname", default="", namespaces=ns).strip()
        if not name:
            continue
        href = response.findtext("d:href", default="", namespaces=ns).strip()
        size_text = prop.findtext("d:getcontentlength", default="", namespaces=ns).strip()
        resource_type = prop.find("d:resourcetype", ns)
        is_collection = bool(
            resource_type is not None and resource_type.find("d:collection", ns) is not None
        )
        resources.append(
            {
                "name": name,
                "href": href,
                "size": int(size_text) if size_text.isdigit() else None,
                "last_modified": prop.findtext(
                    "d:getlastmodified",
                    default="",
                    namespaces=ns,
                ).strip()
                or None,
                "etag": prop.findtext("d:getetag", default="", namespaces=ns).strip()
                or None,
                "is_collection": is_collection,
            }
        )
    return resources


def _propfind(
    session: requests.Session,
    url: str,
    *,
    retries: int = 5,
) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    headers = {"Depth": "1", "Content-Type": "application/xml"}

    for attempt in range(retries):
        try:
            response = session.request(
                "PROPFIND",
                url,
                headers=headers,
                data=DAV_PROPFIND_BODY.encode("utf-8"),
                timeout=DEFAULT_TIMEOUT,
            )
            if response.status_code != 207:
                raise ReceitaOpenDataError(
                    f"Receita WebDAV PROPFIND returned HTTP {response.status_code}: {url}"
                )
            resources = _dav_resources(response.content)
            if not resources:
                raise ReceitaOpenDataError(
                    f"Receita WebDAV returned no resources for {url}"
                )
            return resources
        except (requests.RequestException, ReceitaOpenDataError) as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(min(2 ** attempt, 8))

    raise ReceitaOpenDataError(
        f"Failed to read official Receita WebDAV manifest {url}: {last_error}"
    )


def _establishment_sort_key(name: str) -> tuple[int, str]:
    match = re.search(r"(\d+)", name)
    return (int(match.group(1)) if match else 10**9, name.casefold())


def discover_latest_release(
    session: requests.Session | None = None,
) -> ReceitaOpenDataRelease:
    """Discover the newest complete official CNPJ release via Receita WebDAV."""
    own_session = session is None
    sess = session or requests.Session()
    _configure_official_session(sess)

    try:
        root_resources = _propfind(sess, OFFICIAL_CNPJ_DAV_ROOT)
        periods = sorted(
            (
                item["name"]
                for item in root_resources
                if item.get("is_collection") and PERIOD_RE.fullmatch(item["name"])
            ),
            reverse=True,
        )
        if not periods:
            raise ReceitaOpenDataError(
                "Official Receita CNPJ WebDAV contains no monthly release directories"
            )

        for period in periods:
            release_url = urljoin(OFFICIAL_CNPJ_DAV_ROOT, period + "/")
            resources = _propfind(sess, release_url)
            files = {
                item["name"]: item
                for item in resources
                if not item.get("is_collection")
            }
            establishments = sorted(
                (
                    name
                    for name in files
                    if ESTABLISHMENT_FILE_RE.fullmatch(name)
                ),
                key=_establishment_sort_key,
            )
            if "Motivos.zip" not in files or not establishments:
                continue

            expected_indexes = list(range(len(establishments)))
            actual_indexes = [
                int(re.search(r"(\d+)", name).group(1))
                for name in establishments
            ]
            if actual_indexes != expected_indexes:
                continue

            metadata_names = {"Motivos.zip", *establishments}
            metadata = tuple(
                {
                    "name": item["name"],
                    "size": item.get("size"),
                    "last_modified": item.get("last_modified"),
                    "etag": item.get("etag"),
                }
                for item in resources
                if item["name"] in metadata_names
            )
            return ReceitaOpenDataRelease(
                base_url=OFFICIAL_CNPJ_SHARE_URL,
                release_url=release_url,
                period=period,
                establishment_files=tuple(establishments),
                resource_metadata=metadata,
            )
    finally:
        if own_session:
            sess.close()

    raise ReceitaOpenDataError(
        "No complete monthly Receita CNPJ release with Motivos and Estabelecimentos found"
    )


def _download_zip(
    session: requests.Session,
    url: str,
    destination: Path,
    *,
    retries: int = 5,
) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".part")
    last_error: Exception | None = None

    for attempt in range(retries):
        try:
            with session.get(
                url,
                timeout=DEFAULT_TIMEOUT,
                stream=True,
                allow_redirects=True,
            ) as response:
                response.raise_for_status()
                with temp.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk:
                            handle.write(chunk)
            if not zipfile.is_zipfile(temp):
                raise ReceitaOpenDataError(f"Downloaded file is not a valid ZIP: {url}")
            temp.replace(destination)
            return destination
        except (requests.RequestException, OSError, ReceitaOpenDataError) as exc:
            last_error = exc
            temp.unlink(missing_ok=True)
            if attempt + 1 < retries:
                time.sleep(min(2 ** attempt, 8))

    raise ReceitaOpenDataError(
        f"Failed to download official Receita ZIP {url}: {last_error}"
    )


def _first_csv_member(archive: zipfile.ZipFile) -> str:
    members = [name for name in archive.namelist() if not name.endswith("/")]
    if not members:
        raise ReceitaOpenDataError("Receita ZIP archive is empty")
    csv_like = [
        name
        for name in members
        if name.lower().endswith((".csv", ".estabele"))
    ]
    return csv_like[0] if csv_like else members[0]


def load_reason_map(zip_path: Path) -> dict[str, str]:
    """Load Receita's official motive-code dictionary from Motivos.zip."""
    if not zipfile.is_zipfile(zip_path):
        raise ReceitaOpenDataError(f"Invalid Motivos ZIP: {zip_path}")
    with zipfile.ZipFile(zip_path) as archive:
        member = _first_csv_member(archive)
        with archive.open(member) as raw:
            text = io.TextIOWrapper(raw, encoding="latin1", newline="")
            reader = csv.reader(text, delimiter=";", quotechar='"')
            out: dict[str, str] = {}
            for row in reader:
                if len(row) < 2:
                    continue
                code = str(row[0]).strip().zfill(2)
                description = str(row[1]).strip()
                if code and description:
                    out[code] = description
            if not out:
                raise ReceitaOpenDataError(
                    "Receita Motivos.zip produced an empty dictionary"
                )
            return out


def extract_target_lifecycle_from_zip(
    zip_path: Path,
    target_legal_names: dict[str, str],
    reason_map: dict[str, str],
    *,
    source_url: str,
    source_period: str,
    observed_at: str | None = None,
) -> list[dict[str, Any]]:
    """Stream one Estabelecimentos ZIP and retain exact target CNPJs only."""
    targets = {
        normalize_cnpj_v2(cnpj): name
        for cnpj, name in target_legal_names.items()
    }
    targets = {cnpj: name for cnpj, name in targets.items() if cnpj}
    if not targets:
        return []
    if not zipfile.is_zipfile(zip_path):
        raise ReceitaOpenDataError(f"Invalid Estabelecimentos ZIP: {zip_path}")

    found: list[dict[str, Any]] = []
    with zipfile.ZipFile(zip_path) as archive:
        member = _first_csv_member(archive)
        with archive.open(member) as raw:
            text = io.TextIOWrapper(raw, encoding="latin1", newline="")
            reader = csv.reader(text, delimiter=";", quotechar='"')
            for row in reader:
                if len(row) < 8:
                    continue
                cnpj = normalize_cnpj_v2(
                    "".join(str(part).strip() for part in row[:3])
                )
                if not cnpj or cnpj not in targets:
                    continue

                status_code = str(row[5]).strip().zfill(2)
                status_label = STATUS_CODE_TO_LABEL.get(status_code)
                if not status_label:
                    raise ReceitaOpenDataError(
                        "Unsupported Receita cadastral-status code "
                        f"{status_code} for {cnpj}"
                    )
                reason_code = str(row[7]).strip().zfill(2)
                reason_label = reason_map.get(reason_code)

                record = normalize_receita_lifecycle_record(
                    {
                        "cnpj": cnpj,
                        "legal_name": targets[cnpj],
                        "cadastral_status": status_label,
                        "status_date": str(row[6]).strip() or None,
                        "status_reason": reason_label,
                        "source_authority": "Receita Federal do Brasil",
                        "source_document": (
                            "Dados Abertos do CNPJ / Estabelecimentos"
                        ),
                        "source_mode": "official_open_data_bulk",
                        "observed_at": observed_at or _today_utc().isoformat(),
                    }
                )
                record.update(
                    {
                        "source_period": source_period,
                        "source_url": source_url,
                        "source_file": Path(source_url).name,
                        "raw_status_code": status_code,
                        "raw_reason_code": reason_code,
                    }
                )
                found.append(record)
    return found


def refresh_filtered_lifecycle(
    target_entities: Iterable[dict[str, Any]],
    *,
    release: ReceitaOpenDataRelease | None = None,
    work_dir: Path | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Refresh lifecycle only for CNPJs in the v2 regulatory universe.

    Bulk partitions are processed one at a time and deleted immediately after
    parsing. This keeps disk usage bounded even though the official monthly
    source is large. The final artifact contains only the target CNPJs.
    """
    target_names: dict[str, str] = {}
    for entity in target_entities:
        cnpj = normalize_cnpj_v2(entity.get("cnpj"))
        name = str(entity.get("legal_name") or "").strip()
        if cnpj and name:
            target_names[cnpj] = name
    if not target_names:
        raise ReceitaOpenDataError("No valid target CNPJs were supplied")

    own_session = session is None
    sess = session or requests.Session()
    _configure_official_session(sess)
    resolved_release = release or discover_latest_release(sess)
    observed_at = _today_utc().isoformat()

    temporary_context = None
    if work_dir is None:
        temporary_context = tempfile.TemporaryDirectory(
            prefix="receita-cnpj-v2-"
        )
        root = Path(temporary_context.name)
    else:
        root = Path(work_dir)
        root.mkdir(parents=True, exist_ok=True)

    try:
        reasons_url = urljoin(
            resolved_release.release_url,
            resolved_release.reasons_file,
        )
        reasons_zip = _download_zip(
            sess,
            reasons_url,
            root / resolved_release.reasons_file,
        )
        reason_map = load_reason_map(reasons_zip)
        reasons_zip.unlink(missing_ok=True)

        records_by_cnpj: dict[str, dict[str, Any]] = {}
        files_scanned: list[str] = []
        for name in resolved_release.establishment_files:
            url = urljoin(resolved_release.release_url, name)
            local = _download_zip(sess, url, root / name)
            try:
                rows = extract_target_lifecycle_from_zip(
                    local,
                    target_names,
                    reason_map,
                    source_url=url,
                    source_period=resolved_release.period,
                    observed_at=observed_at,
                )
                for row in rows:
                    cnpj = row["cnpj"]
                    previous = records_by_cnpj.get(cnpj)
                    if previous and previous != row:
                        raise ReceitaOpenDataError(
                            "Conflicting Receita lifecycle observations for "
                            f"target CNPJ {cnpj}"
                        )
                    records_by_cnpj[cnpj] = row
                files_scanned.append(name)
                if len(records_by_cnpj) == len(target_names):
                    break
            finally:
                local.unlink(missing_ok=True)

        unresolved = sorted(set(target_names) - set(records_by_cnpj))
        return {
            "artifact": "v2_receita_cnpj_lifecycle",
            "generated_at": _utc_now(),
            "status": "ok" if not unresolved else "partial",
            "source": {
                "authority": "Receita Federal do Brasil",
                "dataset": (
                    "Cadastro Nacional da Pessoa Jurídica (CNPJ) - Dados Abertos"
                ),
                "public_share_url": resolved_release.base_url,
                "release_url": resolved_release.release_url,
                "reference_period": resolved_release.period,
                "ingestion_method": "official_nextcloud_webdav_bulk_filtered",
                "retrieved_at": _utc_now(),
                "resource_metadata": list(resolved_release.resource_metadata),
            },
            "meta": {
                "target_count": len(target_names),
                "resolved_count": len(records_by_cnpj),
                "unresolved_count": len(unresolved),
                "files_scanned": files_scanned,
                "available_establishment_files": list(
                    resolved_release.establishment_files
                ),
            },
            "unresolved_cnpjs": unresolved,
            "records": [records_by_cnpj[key] for key in sorted(records_by_cnpj)],
        }
    finally:
        if temporary_context is not None:
            temporary_context.cleanup()
        if own_session:
            sess.close()
