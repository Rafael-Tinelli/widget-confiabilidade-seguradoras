from __future__ import annotations

import csv
import io
import re
import tempfile
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

# Legacy publication paths are kept only as optional discovery candidates.
# They are NOT considered a guaranteed contract: the production refresh must
# validate the resolved resource before downloading any bulk payload.
OFFICIAL_CNPJ_BASE_URLS = (
    "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/",
    "https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/",
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
DEFAULT_TIMEOUT = (15, 60)


class ReceitaOpenDataError(RuntimeError):
    """Raised when the official CNPJ open-data source cannot be used safely."""


@dataclass(frozen=True)
class ReceitaOpenDataRelease:
    base_url: str
    release_url: str
    period: str | None
    establishment_files: tuple[str, ...]
    reasons_file: str = "Motivos.zip"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _month_candidates(today: date | None = None, lookback: int = 8) -> list[str]:
    current = today or _today_utc()
    year = current.year
    month = current.month
    out: list[str] = []
    for delta in range(lookback):
        yy = year
        mm = month - delta
        while mm <= 0:
            yy -= 1
            mm += 12
        out.append(f"{yy:04d}-{mm:02d}")
    return out


def _request_exists(session: requests.Session, url: str) -> bool:
    try:
        response = session.head(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
        if response.status_code == 200:
            return True
        if response.status_code not in {403, 405}:
            return False
    except requests.RequestException:
        pass

    try:
        response = session.get(
            url,
            timeout=DEFAULT_TIMEOUT,
            stream=True,
            allow_redirects=True,
        )
        ok = response.status_code == 200
        response.close()
        return ok
    except requests.RequestException:
        return False


def _directory_zip_files(session: requests.Session, url: str) -> list[str]:
    try:
        response = session.get(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
        if response.status_code != 200:
            return []
        text = response.text
    except requests.RequestException:
        return []

    hrefs = re.findall(
        r'href=["\']([^"\']+\.zip)["\']',
        text,
        flags=re.IGNORECASE,
    )
    names = [Path(href.split("?")[0]).name for href in hrefs]
    return sorted(set(names))


def _probe_establishment_files(
    session: requests.Session,
    release_url: str,
) -> list[str]:
    listed = [
        name
        for name in _directory_zip_files(session, release_url)
        if ESTABLISHMENT_FILE_RE.fullmatch(name)
    ]
    if listed:
        return sorted(listed, key=_establishment_sort_key)

    # Directory indexes are not guaranteed. Probe the conventional numbered
    # partitions without downloading the payload.
    found: list[str] = []
    misses_after_hit = 0
    for index in range(100):
        name = f"Estabelecimentos{index}.zip"
        if _request_exists(session, urljoin(release_url, name)):
            found.append(name)
            misses_after_hit = 0
            continue
        if found:
            misses_after_hit += 1
            if misses_after_hit >= 3:
                break
        elif index >= 12:
            break
    return found


def _establishment_sort_key(name: str) -> tuple[int, str]:
    match = re.search(r"(\d+)", name)
    return (int(match.group(1)) if match else 10**9, name.casefold())


def discover_latest_release(
    session: requests.Session | None = None,
    today: date | None = None,
    lookback: int = 8,
) -> ReceitaOpenDataRelease:
    """Discover a usable official Receita CNPJ open-data release.

    Discovery is fail-closed. A legacy path is accepted only when both the
    establishment partitions and the official motive dictionary are actually
    reachable and structurally present. This avoids treating an old URL
    convention as an enduring source contract.
    """
    own_session = session is None
    sess = session or requests.Session()
    sess.headers.setdefault("User-Agent", "Sanida-CNPJ-Lifecycle/2.0")
    try:
        for base_url in OFFICIAL_CNPJ_BASE_URLS:
            for period in _month_candidates(today=today, lookback=lookback):
                release_url = urljoin(base_url, period + "/")
                sentinel = urljoin(release_url, "Estabelecimentos0.zip")
                if not _request_exists(sess, sentinel):
                    continue
                files = _probe_establishment_files(sess, release_url)
                if files and _request_exists(
                    sess,
                    urljoin(release_url, "Motivos.zip"),
                ):
                    return ReceitaOpenDataRelease(
                        base_url=base_url,
                        release_url=release_url,
                        period=period,
                        establishment_files=tuple(files),
                    )

            files = _probe_establishment_files(sess, base_url)
            if files and _request_exists(sess, urljoin(base_url, "Motivos.zip")):
                return ReceitaOpenDataRelease(
                    base_url=base_url,
                    release_url=base_url,
                    period=None,
                    establishment_files=tuple(files),
                )
    finally:
        if own_session:
            sess.close()

    raise ReceitaOpenDataError("No usable official Receita CNPJ open-data release found")


def _download_zip(
    session: requests.Session,
    url: str,
    destination: Path,
) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".part")
    last_error: Exception | None = None

    for attempt in range(3):
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
            if attempt == 2:
                break

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
    source_period: str | None,
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

    Files are downloaded and processed one at a time, then deleted, so the
    runner never needs to keep the full Receita dataset extracted on disk.
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
    sess.headers.setdefault("User-Agent", "Sanida-CNPJ-Lifecycle/2.0")
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
                "base_url": resolved_release.base_url,
                "release_url": resolved_release.release_url,
                "reference_period": resolved_release.period,
                "ingestion_method": "official_open_data_bulk_filtered",
                "retrieved_at": _utc_now(),
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
