"""Ephemeral HostGator worker for official Receita CNPJ open-data acquisition.

This file is intentionally Python 3.9 compatible because the HostGator account
currently exposes Python 3.9. The worker is uploaded by the exact Gate 4 source
head, runs privately over pinned SSH, reads only the official Receita Federal
Nextcloud share, and writes a small filtered JSON result for the GitHub runner.

HostGator is transport/compute only. Receita Federal remains the data authority.
"""

import argparse
import csv
import fcntl
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests

OFFICIAL_HOST = "https://arquivos.receitafederal.gov.br"
ROOT_URL = OFFICIAL_HOST + "/"
DATASET_PATH = "Dados/Cadastros/CNPJ"
PUBLIC_SHARE_RE = re.compile(r"/index\.php/s/([^/?#]+)")
PERIOD_RE = re.compile(r"20\d{2}-\d{2}$")
ESTABLISHMENT_RE = re.compile(r"Estabelecimentos\d+\.zip$", re.IGNORECASE)
COMPANY_RE = re.compile(r"Empresas\d+\.zip$", re.IGNORECASE)
TIMEOUT = (15, 180)

STATUS_CODE_TO_LABEL = {
    "01": "NULA",
    "02": "ATIVA",
    "03": "SUSPENSA",
    "04": "INAPTA",
    "05": "ATIVA NÃO REGULAR",
    "08": "BAIXADA",
}

BUSINESS_CORE_STOPWORDS = {
    "sa", "s", "a", "ltda", "cia", "companhia", "sociedade",
    "de", "da", "do", "das", "dos", "e", "seguro", "seguros",
    "seguradora", "seguradoras", "vida", "previdencia",
    "previdenciaria", "complementar", "privada", "capitalizacao",
    "corporativo", "corporativos", "corporativa", "corporativas",
}
LEGAL_FORM_SUFFIXES = {"sa", "s", "a", "ltda", "cia", "companhia", "sociedade"}

DAV_BODY = """<?xml version="1.0"?>
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


class WorkerError(RuntimeError):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def today_utc():
    return datetime.now(timezone.utc).date().isoformat()


def digits(value):
    return re.sub(r"\D+", "", str(value or ""))


def normalize_cnpj(value):
    value = digits(value)
    return value if len(value) == 14 else None


def normalize_name_key(name):
    if not name:
        return ""
    value = unicodedata.normalize("NFKD", str(name))
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", " ", value, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", value).strip()


def surface_name(name):
    tokens = normalize_name_key(name).split()
    while tokens and tokens[-1] in LEGAL_FORM_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def business_core(name):
    tokens = [
        token
        for token in normalize_name_key(name).split()
        if token not in BUSINESS_CORE_STOPWORDS and len(token) > 1
    ]
    if not tokens:
        return ""
    if len(tokens) == 1 and len(tokens[0]) < 7:
        return ""
    value = " ".join(tokens)
    return value if len(value.replace(" ", "")) >= 7 else ""


def build_query_indexes(provider_names):
    surface = {}
    core = {}
    display = {}
    for provider in provider_names:
        name = str(provider or "").strip()
        if not name:
            continue
        provider_key = normalize_name_key(name)
        if not provider_key:
            continue
        display.setdefault(provider_key, name)
        s_key = surface_name(name)
        if s_key:
            surface.setdefault(s_key, set()).add(provider_key)
        c_key = business_core(name)
        if c_key:
            core.setdefault(c_key, set()).add(provider_key)
    return {"surface": surface, "core": core, "display": display}


def match_name_to_queries(name, indexes, prefix):
    if not name:
        return []
    out = []
    s_key = surface_name(name)
    for provider_key in indexes["surface"].get(s_key, set()):
        out.append([provider_key, prefix + "_surface_exact"])
    c_key = business_core(name)
    for provider_key in indexes["core"].get(c_key, set()):
        out.append([provider_key, prefix + "_business_core"])
    return out


def configure_session():
    session = requests.Session()
    session.headers.setdefault("User-Agent", "Sanida-CNPJ-RemoteWorker/1.0")
    session.auth = None
    session.cookies.clear()
    return session


def prepare_session(session):
    session.auth = None
    session.cookies.clear()


def dav_resources(payload):
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        raise WorkerError(f"invalid Receita WebDAV XML: {exc}")
    ns = {"d": "DAV:"}
    out = []
    for response in root.findall("d:response", ns):
        propstat = None
        for item in response.findall("d:propstat", ns):
            status = item.findtext("d:status", default="", namespaces=ns) or ""
            if "200" in status:
                propstat = item
                break
        if propstat is None:
            continue
        prop = propstat.find("d:prop", ns)
        if prop is None:
            continue
        name = (prop.findtext("d:displayname", default="", namespaces=ns) or "").strip()
        if not name:
            continue
        href = (response.findtext("d:href", default="", namespaces=ns) or "").strip()
        size_text = (prop.findtext("d:getcontentlength", default="", namespaces=ns) or "").strip()
        resource_type = prop.find("d:resourcetype", ns)
        is_collection = bool(
            resource_type is not None
            and resource_type.find("d:collection", ns) is not None
        )
        out.append(
            {
                "name": name,
                "href": href,
                "size": int(size_text) if size_text.isdigit() else None,
                "last_modified": (
                    prop.findtext("d:getlastmodified", default="", namespaces=ns) or ""
                ).strip() or None,
                "etag": (
                    prop.findtext("d:getetag", default="", namespaces=ns) or ""
                ).strip() or None,
                "is_collection": is_collection,
            }
        )
    return out


def propfind(session, url, retries=5):
    last_error = None
    headers = {
        "Depth": "1",
        "Content-Type": "application/xml",
        "X-Requested-With": "XMLHttpRequest",
    }
    for attempt in range(retries):
        try:
            prepare_session(session)
            response = session.request(
                "PROPFIND",
                url,
                headers=headers,
                data=DAV_BODY.encode("utf-8"),
                timeout=TIMEOUT,
            )
            if response.status_code != 207:
                raise WorkerError(
                    f"Receita WebDAV PROPFIND returned HTTP {response.status_code}: {url}"
                )
            resources = dav_resources(response.content)
            if not resources:
                raise WorkerError(f"Receita WebDAV returned no resources for {url}")
            return resources
        except (requests.RequestException, WorkerError) as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(min(2 ** attempt, 8))
    raise WorkerError(f"failed Receita WebDAV PROPFIND {url}: {last_error}")


def discover_release(session):
    prepare_session(session)
    try:
        response = session.get(ROOT_URL, timeout=TIMEOUT, allow_redirects=True)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise WorkerError(f"failed to resolve Receita public share: {exc}")
    match = PUBLIC_SHARE_RE.search(str(response.url))
    if not match:
        raise WorkerError(
            f"Receita root did not redirect to a recognizable public share: {response.url}"
        )
    token = match.group(1)
    share_url = OFFICIAL_HOST + "/index.php/s/" + token
    dav_root = (
        OFFICIAL_HOST + "/public.php/dav/files/" + token + "/" + DATASET_PATH + "/"
    )

    root_resources = propfind(session, dav_root)
    periods = sorted(
        [
            item["name"]
            for item in root_resources
            if item.get("is_collection") and PERIOD_RE.fullmatch(item["name"])
        ],
        reverse=True,
    )
    if not periods:
        raise WorkerError("official Receita CNPJ share has no monthly releases")

    for period in periods:
        release_url = urljoin(dav_root, period + "/")
        resources = propfind(session, release_url)
        files = {
            item["name"]: item
            for item in resources
            if not item.get("is_collection")
        }
        establishments = sorted(
            [name for name in files if ESTABLISHMENT_RE.fullmatch(name)],
            key=lambda name: int(re.search(r"(\d+)", name).group(1)),
        )
        if "Motivos.zip" not in files or not establishments:
            continue
        expected = list(range(len(establishments)))
        actual = [int(re.search(r"(\d+)", name).group(1)) for name in establishments]
        if actual != expected:
            continue
        return {
            "base_url": share_url,
            "dav_root": dav_root,
            "release_url": release_url,
            "period": period,
            "establishment_files": establishments,
            "resources": resources,
        }
    raise WorkerError("no complete Receita CNPJ monthly release found")


def resource_map(release):
    return {
        item["name"]: item
        for item in release["resources"]
        if not item.get("is_collection")
    }


def first_csv_member(archive):
    members = [name for name in archive.namelist() if not name.endswith("/")]
    if not members:
        raise WorkerError("Receita ZIP archive is empty")
    csv_like = [
        name for name in members if name.lower().endswith((".csv", ".estabele"))
    ]
    return csv_like[0] if csv_like else members[0]


def sanitized_csv_reader(raw):
    """Read Receita CSV lines while tolerating isolated NUL padding bytes."""
    text = io.TextIOWrapper(raw, encoding="latin1", newline="")
    sanitized = (line.replace("\x00", "") for line in text)
    return csv.reader(sanitized, delimiter=";", quotechar='"')


def valid_cached_zip(path, expected_size):
    if not path.is_file():
        return False
    if expected_size is not None and path.stat().st_size != int(expected_size):
        return False
    return zipfile.is_zipfile(str(path))


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cache_manifest_path(destination):
    return destination.parent / ".receita-cache-manifest.json"


def load_cache_manifest(destination):
    path = cache_manifest_path(destination)
    if not path.is_file():
        return {"version": 1, "files": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {"version": 1, "files": {}}
    if not isinstance(payload, dict) or not isinstance(payload.get("files"), dict):
        return {"version": 1, "files": {}}
    return payload


def save_cache_manifest(destination, payload):
    path = cache_manifest_path(destination)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temp.replace(path)


def remote_fingerprint(destination, expected_size, expected_etag, expected_last_modified):
    return {
        "filename": destination.name,
        "size": int(expected_size) if expected_size is not None else None,
        "etag": expected_etag or None,
        "last_modified": expected_last_modified or None,
    }


def download_zip(
    session,
    url,
    destination,
    expected_size=None,
    expected_etag=None,
    expected_last_modified=None,
    retries=5,
):
    destination.parent.mkdir(parents=True, exist_ok=True)
    manifest = load_cache_manifest(destination)
    files = manifest.setdefault("files", {})
    fingerprint = remote_fingerprint(
        destination,
        expected_size,
        expected_etag,
        expected_last_modified,
    )
    entry = files.get(destination.name) if isinstance(files, dict) else None

    if valid_cached_zip(destination, expected_size):
        actual_sha = sha256_file(destination)
        if (
            isinstance(entry, dict)
            and entry.get("remote_fingerprint") == fingerprint
            and entry.get("sha256") == actual_sha
        ):
            print(
                "RECEITA_CACHE reused_unchanged "
                f"file={destination.name} size={destination.stat().st_size}",
                flush=True,
            )
            return destination

        if entry is None:
            # Bootstrap pre-manifest cache created by the same official worker.
            # Current remote metadata + ZIP integrity + SHA-256 become the
            # durable contract for all subsequent reuse decisions.
            files[destination.name] = {
                "remote_fingerprint": fingerprint,
                "sha256": actual_sha,
                "validated_at": utc_now(),
                "validation_mode": "bootstrap_existing_official_cache",
            }
            save_cache_manifest(destination, manifest)
            print(
                "RECEITA_CACHE reused_bootstrapped "
                f"file={destination.name} size={destination.stat().st_size}",
                flush=True,
            )
            return destination

    destination.unlink(missing_ok=True)
    temp = destination.with_suffix(destination.suffix + ".part")
    last_error = None
    for attempt in range(retries):
        try:
            temp.unlink(missing_ok=True)
            prepare_session(session)
            with session.get(
                url,
                timeout=TIMEOUT,
                stream=True,
                allow_redirects=True,
            ) as response:
                response.raise_for_status()
                with temp.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk:
                            handle.write(chunk)
            if expected_size is not None and temp.stat().st_size != int(expected_size):
                raise WorkerError(
                    f"downloaded Receita ZIP size mismatch for {url}: "
                    f"{temp.stat().st_size} != {expected_size}"
                )
            if not zipfile.is_zipfile(str(temp)):
                raise WorkerError(f"downloaded Receita file is not ZIP: {url}")
            temp.replace(destination)
            files[destination.name] = {
                "remote_fingerprint": fingerprint,
                "sha256": sha256_file(destination),
                "validated_at": utc_now(),
                "validation_mode": "downloaded_current_release",
            }
            save_cache_manifest(destination, manifest)
            print(
                "RECEITA_CACHE downloaded "
                f"file={destination.name} size={destination.stat().st_size}",
                flush=True,
            )
            return destination
        except (requests.RequestException, OSError, WorkerError) as exc:
            last_error = exc
            temp.unlink(missing_ok=True)
            if attempt + 1 < retries:
                time.sleep(min(2 ** attempt, 8))
    raise WorkerError(f"failed to download Receita ZIP {url}: {last_error}")


def load_reason_map(path):
    out = {}
    with zipfile.ZipFile(str(path)) as archive:
        member = first_csv_member(archive)
        with archive.open(member) as raw:
            reader = sanitized_csv_reader(raw)
            for row in reader:
                if len(row) < 2:
                    continue
                code = str(row[0]).strip().zfill(2)
                description = str(row[1]).strip()
                if code and description:
                    out[code] = description
    if not out:
        raise WorkerError("Motivos.zip produced an empty map")
    return out


def load_code_map(path):
    out = {}
    with zipfile.ZipFile(str(path)) as archive:
        member = first_csv_member(archive)
        with archive.open(member) as raw:
            reader = sanitized_csv_reader(raw)
            for row in reader:
                if len(row) < 2:
                    continue
                code = digits(row[0])
                label = str(row[1]).strip()
                if code and label:
                    out[code] = label
    if not out:
        raise WorkerError(f"Receita dictionary ZIP produced an empty map: {path}")
    return out


def release_metadata(release, names):
    wanted = set(names)
    return [
        {
            "name": item["name"],
            "size": item.get("size"),
            "last_modified": item.get("last_modified"),
            "etag": item.get("etag"),
        }
        for item in release["resources"]
        if item["name"] in wanted
    ]


def cache_period_root(cache_dir, period):
    root = Path(cache_dir) / period
    root.mkdir(parents=True, exist_ok=True)
    return root


def prune_other_periods(cache_dir, keep_period):
    root = Path(cache_dir)
    if not root.is_dir():
        return
    for child in root.iterdir():
        if child.name == keep_period:
            continue
        if child.is_dir() and PERIOD_RE.fullmatch(child.name):
            shutil.rmtree(str(child), ignore_errors=True)


def require_expected_period(release, expected):
    expected = str(expected or "").strip()
    if expected and release["period"] != expected:
        raise WorkerError(
            f"Receita release changed during Gate 4 generation: "
            f"expected={expected} current={release['period']}"
        )


def lifecycle_extract(session, release, payload, cache_dir):
    require_expected_period(release, payload.get("expected_period"))
    targets = {}
    for item in payload.get("targets") or []:
        cnpj = normalize_cnpj(item.get("cnpj"))
        legal_name = str(item.get("legal_name") or "").strip()
        if cnpj and legal_name:
            targets[cnpj] = legal_name
    if not targets:
        raise WorkerError("remote lifecycle extraction received no target CNPJs")

    files = resource_map(release)
    cache_root = cache_period_root(cache_dir, release["period"])
    reason_meta = files.get("Motivos.zip") or {}
    reasons_zip = download_zip(
        session,
        urljoin(release["release_url"], "Motivos.zip"),
        cache_root / "Motivos.zip",
        reason_meta.get("size"),
        reason_meta.get("etag"),
        reason_meta.get("last_modified"),
    )
    reason_map = load_reason_map(reasons_zip)

    found = {}
    scanned = []
    for name in release["establishment_files"]:
        metadata = files.get(name) or {}
        local = download_zip(
            session,
            urljoin(release["release_url"], name),
            cache_root / name,
            metadata.get("size"),
            metadata.get("etag"),
            metadata.get("last_modified"),
        )
        with zipfile.ZipFile(str(local)) as archive:
            member = first_csv_member(archive)
            with archive.open(member) as raw:
                reader = sanitized_csv_reader(raw)
                for row in reader:
                    if len(row) < 8:
                        continue
                    cnpj = normalize_cnpj("".join(str(part).strip() for part in row[:3]))
                    if not cnpj or cnpj not in targets:
                        continue
                    status_code = str(row[5]).strip().zfill(2)
                    status_label = STATUS_CODE_TO_LABEL.get(status_code)
                    if not status_label:
                        raise WorkerError(
                            f"unsupported Receita status code {status_code} for {cnpj}"
                        )
                    reason_code = str(row[7]).strip().zfill(2)
                    found[cnpj] = {
                        "cnpj": cnpj,
                        "legal_name": targets[cnpj],
                        "cadastral_status": status_label,
                        "status_date": str(row[6]).strip() or None,
                        "status_reason": reason_map.get(reason_code),
                        "source_authority": "Receita Federal do Brasil",
                        "source_document": "Dados Abertos do CNPJ / Estabelecimentos",
                        "source_mode": "official_open_data_bulk",
                        "observed_at": today_utc(),
                        "source_period": release["period"],
                        "source_url": urljoin(release["release_url"], name),
                        "source_file": name,
                        "raw_status_code": status_code,
                        "raw_reason_code": reason_code,
                    }
                    if len(found) == len(targets):
                        break
        scanned.append(name)
        if len(found) == len(targets):
            break

    unresolved = sorted(set(targets) - set(found))
    names = ["Motivos.zip"] + release["establishment_files"]
    return {
        "artifact": "v2_receita_remote_lifecycle_extract",
        "generated_at": utc_now(),
        "source": {
            "authority": "Receita Federal do Brasil",
            "dataset": "Cadastro Nacional da Pessoa Jurídica (CNPJ) - Dados Abertos",
            "public_share_url": release["base_url"],
            "release_url": release["release_url"],
            "reference_period": release["period"],
            "ingestion_method": "official_nextcloud_webdav_bulk_filtered",
            "retrieved_at": utc_now(),
            "resource_metadata": release_metadata(release, names),
        },
        "meta": {
            "target_count": len(targets),
            "resolved_count": len(found),
            "unresolved_count": len(unresolved),
            "files_scanned": scanned,
        },
        "unresolved_cnpjs": unresolved,
        "raw_records": [found[key] for key in sorted(found)],
    }



IDENTITY_CHUNK_BYTES = 256 * 1024 * 1024


def _write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _read_json(path):
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise WorkerError(f"chunk payload is not an object: {path}")
    return payload


def _materialize_zip_chunks(zip_path, scratch_root):
    zip_path = Path(zip_path)
    scratch_root = Path(scratch_root)
    shutil.rmtree(str(scratch_root), ignore_errors=True)
    scratch_root.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(str(zip_path)) as archive:
        member = first_csv_member(archive)

    prefix = str(scratch_root / "part-")
    unzip_process = subprocess.Popen(
        ["unzip", "-p", str(zip_path), member],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        split_process = subprocess.run(
            [
                "split",
                "-C",
                str(IDENTITY_CHUNK_BYTES),
                "-d",
                "-a",
                "4",
                "-",
                prefix,
            ],
            stdin=unzip_process.stdout,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=False,
        )
    finally:
        if unzip_process.stdout is not None:
            unzip_process.stdout.close()

    unzip_stderr = b""
    if unzip_process.stderr is not None:
        unzip_stderr = unzip_process.stderr.read()
    unzip_return = unzip_process.wait()
    if unzip_return != 0:
        raise WorkerError(
            "unzip failed while materializing identity chunks "
            f"for {zip_path.name}: rc={unzip_return} "
            f"stderr={unzip_stderr.decode('utf-8', 'replace')[-2000:]}"
        )
    if split_process.returncode != 0:
        raise WorkerError(
            "split failed while materializing identity chunks "
            f"for {zip_path.name}: rc={split_process.returncode} "
            f"stderr={split_process.stderr.decode('utf-8', 'replace')[-2000:]}"
        )

    chunks = sorted(
        path for path in scratch_root.iterdir()
        if path.is_file() and path.name.startswith("part-")
    )
    if not chunks:
        raise WorkerError(f"identity chunk materialization produced no files: {zip_path}")
    return chunks


def _run_identity_chunk(mode, chunk_file, input_path, output_path):
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--mode",
        mode,
        "--chunk-file",
        str(chunk_file),
        "--input",
        str(input_path),
        "--output",
        str(output_path),
        "--cache-dir",
        str(Path(chunk_file).parent),
    ]
    result = subprocess.run(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise WorkerError(
            f"identity chunk worker failed mode={mode} "
            f"chunk={Path(chunk_file).name} rc={result.returncode}: "
            f"{result.stderr.decode('utf-8', 'replace')[-4000:]}"
        )
    return _read_json(output_path)


def _scan_company_chunk(chunk_file, payload):
    target_bases = {
        str(value)
        for value in payload.get("target_bases") or []
        if len(str(value)) == 8
    }
    indexes = build_query_indexes(payload.get("providers") or [])
    companies_by_base = {}
    legal_matches_by_base = {}

    with Path(chunk_file).open("rb") as raw:
        reader = sanitized_csv_reader(raw)
        for row in reader:
            if len(row) < 3:
                continue
            base = digits(row[0])
            if len(base) != 8:
                continue
            legal_name = str(row[1] or "").strip()
            nature_code = digits(row[2])
            matches = match_name_to_queries(legal_name, indexes, "legal_name")
            if base in target_bases or matches:
                companies_by_base[base] = {
                    "cnpj_base": base,
                    "legal_name_receita": legal_name or None,
                    "legal_nature_code": nature_code or None,
                }
            if matches:
                legal_matches_by_base.setdefault(base, []).extend(matches)

    return {
        "companies_by_base": companies_by_base,
        "legal_matches_by_base": legal_matches_by_base,
    }


def _scan_establishment_chunk(chunk_file, payload):
    targets = {
        str(value)
        for value in payload.get("target_cnpjs") or []
        if len(str(value)) == 14
    }
    indexes = build_query_indexes(payload.get("providers") or [])
    legal_matches_by_base = {
        str(base): list(matches)
        for base, matches in (payload.get("legal_matches_by_base") or {}).items()
    }
    selected = []

    with Path(chunk_file).open("rb") as raw:
        reader = sanitized_csv_reader(raw)
        for row in reader:
            if len(row) < 13:
                continue
            cnpj = normalize_cnpj("".join(str(part).strip() for part in row[:3]))
            if not cnpj:
                continue
            base = cnpj[:8]
            is_head_office = str(row[3]).strip() == "1"
            need_target = cnpj in targets
            legal_matches = legal_matches_by_base.get(base) or []
            trade_matches = []
            if is_head_office:
                trade_matches = match_name_to_queries(
                    str(row[4] or "").strip(),
                    indexes,
                    "trade_name",
                )
            matches = legal_matches + trade_matches
            if not need_target and not (is_head_office and matches):
                continue
            selected.append(
                {
                    "row": row,
                    "provider_matches": matches if is_head_office else [],
                }
            )

    return {"selected_establishments": selected}


def _merge_match_lists(destination, source):
    for base, matches in source.items():
        current = destination.setdefault(base, [])
        seen = {(str(item[0]), str(item[1])) for item in current if len(item) == 2}
        for item in matches:
            if not isinstance(item, list) or len(item) != 2:
                continue
            key = (str(item[0]), str(item[1]))
            if key not in seen:
                current.append([key[0], key[1]])
                seen.add(key)


def _scan_zip_via_chunk_workers(
    *,
    local_zip,
    mode,
    payload,
    scratch_root,
):
    scratch_root = Path(scratch_root)
    chunks_root = scratch_root / "chunks"
    input_path = scratch_root / "chunk-input.json"
    output_path = scratch_root / "chunk-output.json"
    _write_json(input_path, payload)

    chunks = _materialize_zip_chunks(local_zip, chunks_root)
    partials = []
    try:
        for index, chunk_file in enumerate(chunks):
            partial = _run_identity_chunk(
                mode,
                chunk_file,
                input_path,
                output_path,
            )
            partials.append(partial)
            output_path.unlink(missing_ok=True)
            chunk_file.unlink(missing_ok=True)
            print(
                "RECEITA_IDENTITY_CHUNK "
                f"mode={mode} zip={Path(local_zip).name} "
                f"chunk={index + 1}/{len(chunks)}",
                flush=True,
            )
    finally:
        shutil.rmtree(str(scratch_root), ignore_errors=True)
    return partials


def identity_extract(session, release, payload, cache_dir):
    require_expected_period(release, payload.get("expected_period"))
    targets = {}
    for item in payload.get("targets") or []:
        cnpj = normalize_cnpj(item.get("cnpj"))
        if cnpj:
            targets[cnpj] = {
                "entity_id": item.get("entity_id"),
                "project_legal_name": item.get("legal_name"),
            }
    providers = [str(value or "").strip() for value in payload.get("providers") or []]
    providers = [value for value in providers if value]
    if not targets:
        raise WorkerError("remote identity extraction received no target CNPJs")
    if not providers:
        raise WorkerError("remote identity extraction received no provider names")

    target_bases = {cnpj[:8] for cnpj in targets}
    files = resource_map(release)
    companies = sorted(
        [name for name in files if COMPANY_RE.fullmatch(name)],
        key=lambda name: int(re.search(r"(\d+)", name).group(1)),
    )
    if not companies:
        raise WorkerError("Receita release contains no Empresas partitions")
    for required in ("Cnaes.zip", "Naturezas.zip"):
        if required not in files:
            raise WorkerError(f"Receita release missing {required}")

    cache_root = cache_period_root(cache_dir, release["period"])
    cnae_zip = download_zip(
        session,
        urljoin(release["release_url"], "Cnaes.zip"),
        cache_root / "Cnaes.zip",
        files["Cnaes.zip"].get("size"),
        files["Cnaes.zip"].get("etag"),
        files["Cnaes.zip"].get("last_modified"),
    )
    nature_zip = download_zip(
        session,
        urljoin(release["release_url"], "Naturezas.zip"),
        cache_root / "Naturezas.zip",
        files["Naturezas.zip"].get("size"),
        files["Naturezas.zip"].get("etag"),
        files["Naturezas.zip"].get("last_modified"),
    )
    cnae_map = load_code_map(cnae_zip)
    nature_map = load_code_map(nature_zip)

    scratch_base = cache_root / ".identity-work"
    companies_by_base = {}
    legal_matches_by_base = {}
    company_files_scanned = []

    company_payload = {
        "target_bases": sorted(target_bases),
        "providers": providers,
    }
    for name in companies:
        local = download_zip(
            session,
            urljoin(release["release_url"], name),
            cache_root / name,
            files[name].get("size"),
            files[name].get("etag"),
            files[name].get("last_modified"),
        )
        partials = _scan_zip_via_chunk_workers(
            local_zip=local,
            mode="identity_company_chunk",
            payload=company_payload,
            scratch_root=scratch_base / name,
        )
        for partial in partials:
            for base, company in (partial.get("companies_by_base") or {}).items():
                companies_by_base[str(base)] = dict(company)
            _merge_match_lists(
                legal_matches_by_base,
                partial.get("legal_matches_by_base") or {},
            )
        company_files_scanned.append(name)

    for company in companies_by_base.values():
        nature_code = str(company.get("legal_nature_code") or "")
        company["legal_nature"] = nature_map.get(nature_code) if nature_code else None

    selected = []
    establishment_files_scanned = []
    establishment_payload = {
        "target_cnpjs": sorted(targets),
        "providers": providers,
        "legal_matches_by_base": legal_matches_by_base,
    }
    for name in release["establishment_files"]:
        local = download_zip(
            session,
            urljoin(release["release_url"], name),
            cache_root / name,
            files[name].get("size"),
            files[name].get("etag"),
            files[name].get("last_modified"),
        )
        partials = _scan_zip_via_chunk_workers(
            local_zip=local,
            mode="identity_establishment_chunk",
            payload=establishment_payload,
            scratch_root=scratch_base / name,
        )
        for partial in partials:
            selected.extend(partial.get("selected_establishments") or [])
        establishment_files_scanned.append(name)

    return {
        "artifact": "v2_receita_remote_identity_extract",
        "generated_at": utc_now(),
        "source": {
            "authority": "Receita Federal do Brasil",
            "dataset": "Cadastro Nacional da Pessoa Jurídica (CNPJ) - Dados Abertos",
            "public_share_url": release["base_url"],
            "release_url": release["release_url"],
            "reference_period": release["period"],
            "ingestion_method": "official_nextcloud_webdav_bulk_filtered_identity",
            "retrieved_at": utc_now(),
        },
        "targets": targets,
        "providers": providers,
        "cnae_map": cnae_map,
        "nature_map": nature_map,
        "companies_by_base": companies_by_base,
        "selected_establishments": selected,
        "company_files_scanned": company_files_scanned,
        "establishment_files_scanned": establishment_files_scanned,
    }


def discover_payload(release):
    names = ["Motivos.zip"] + release["establishment_files"]
    return {
        "artifact": "v2_receita_remote_release",
        "base_url": release["base_url"],
        "release_url": release["release_url"],
        "period": release["period"],
        "establishment_files": release["establishment_files"],
        "reasons_file": "Motivos.zip",
        "resource_metadata": release_metadata(release, names),
    }


def load_input(path):
    if not path:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise WorkerError("worker input must be a JSON object")
    return payload


def write_output(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(target.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temp.replace(target)


def execute(args):
    if args.mode == "identity_company_chunk":
        return _scan_company_chunk(args.chunk_file, load_input(args.input))
    if args.mode == "identity_establishment_chunk":
        return _scan_establishment_chunk(args.chunk_file, load_input(args.input))

    cache_root = Path(args.cache_dir)
    cache_root.mkdir(parents=True, exist_ok=True)
    lock_path = cache_root / ".worker.lock"

    session = configure_session()
    try:
        with lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            release = discover_release(session)
            if args.mode == "discover":
                result = discover_payload(release)
            else:
                payload = load_input(args.input)
                if args.mode == "lifecycle":
                    result = lifecycle_extract(
                        session,
                        release,
                        payload,
                        args.cache_dir,
                    )
                else:
                    result = identity_extract(
                        session,
                        release,
                        payload,
                        args.cache_dir,
                    )
            write_output(args.output, result)
            prune_other_periods(args.cache_dir, release["period"])
    finally:
        session.close()


def write_exit_marker(path, code):
    if not path:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(target.suffix + ".tmp")
    temp.write_text(str(code) + "\n", encoding="utf-8")
    temp.replace(target)


def write_start_marker(path):
    if not path:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(target.suffix + ".tmp")
    temp.write_text(str(os.getpid()) + "\n", encoding="utf-8")
    temp.replace(target)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=(
            "discover",
            "lifecycle",
            "identity",
            "identity_company_chunk",
            "identity_establishment_chunk",
        ),
        required=True,
    )
    parser.add_argument("--input")
    parser.add_argument("--chunk-file")
    parser.add_argument("--output", required=True)
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--exit-file")
    parser.add_argument("--start-file")
    args = parser.parse_args()

    # HostGator imposes a 120-second CPU limit per process. Identity's heavy
    # scan is orchestrated here but parsed in bounded child processes so no
    # single parser consumes the whole CPU budget.
    write_start_marker(args.start_file)

    exit_code = 0
    try:
        result = execute(args)
        if args.mode in {"identity_company_chunk", "identity_establishment_chunk"}:
            write_output(args.output, result)
    except Exception:  # noqa: BLE001 - worker must always persist an exit marker
        traceback.print_exc()
        exit_code = 1
    finally:
        write_exit_marker(args.exit_file, exit_code)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
