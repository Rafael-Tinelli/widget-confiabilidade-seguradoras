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
import resource
import shutil
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
CACHE_MANIFEST_VERSION = "receita-cnpj-remote-cache-v1"

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
        companies = sorted(
            [name for name in files if COMPANY_RE.fullmatch(name)],
            key=lambda name: int(re.search(r"(\d+)", name).group(1)),
        )
        required = {"Motivos.zip", "Cnaes.zip", "Naturezas.zip"}
        if not required.issubset(files) or not establishments or not companies:
            continue
        expected_establishments = list(range(len(establishments)))
        actual_establishments = [
            int(re.search(r"(\d+)", name).group(1)) for name in establishments
        ]
        expected_companies = list(range(len(companies)))
        actual_companies = [
            int(re.search(r"(\d+)", name).group(1)) for name in companies
        ]
        if actual_establishments != expected_establishments:
            continue
        if actual_companies != expected_companies:
            continue
        return {
            "base_url": share_url,
            "dav_root": dav_root,
            "release_url": release_url,
            "period": period,
            "establishment_files": establishments,
            "company_files": companies,
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
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cache_manifest_path(cache_root):
    return Path(cache_root) / "manifest.json"


def load_cache_manifest(cache_root):
    path = cache_manifest_path(cache_root)
    if not path.is_file():
        return {
            "version": CACHE_MANIFEST_VERSION,
            "files": {},
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise WorkerError(f"invalid Receita cache manifest {path}: {exc}")
    if payload.get("version") != CACHE_MANIFEST_VERSION:
        raise WorkerError(
            f"unsupported Receita cache manifest version: {payload.get('version')}"
        )
    files = payload.get("files")
    if not isinstance(files, dict):
        raise WorkerError("Receita cache manifest files must be an object")
    return payload


def write_cache_manifest(cache_root, payload):
    root = Path(cache_root)
    root.mkdir(parents=True, exist_ok=True)
    target = cache_manifest_path(root)
    temp = target.with_suffix(".json.tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temp.replace(target)


def remote_file_fingerprint(period, name, metadata):
    etag = str(metadata.get("etag") or "").strip()
    size = metadata.get("size")
    if not etag:
        raise WorkerError(f"Receita resource {name} is missing ETag")
    if size is None:
        raise WorkerError(f"Receita resource {name} is missing content length")
    return {
        "period": str(period),
        "filename": str(name),
        "etag": etag,
        "size": int(size),
    }


def release_fingerprint(release):
    files = resource_map(release)
    names = (
        ["Motivos.zip", "Cnaes.zip", "Naturezas.zip"]
        + list(release.get("company_files") or [])
        + list(release.get("establishment_files") or [])
    )
    rows = [
        remote_file_fingerprint(release["period"], name, files[name])
        for name in names
    ]
    encoded = json.dumps(
        sorted(rows, key=lambda row: row["filename"]),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_manifest_cache_entry(path, entry, remote_fingerprint):
    if not isinstance(entry, dict):
        return False, "missing_manifest_entry"
    if entry.get("remote_fingerprint") != remote_fingerprint:
        return False, "remote_fingerprint_changed"
    if not valid_cached_zip(path, remote_fingerprint["size"]):
        if not path.is_file():
            return False, "missing_file"
        if path.stat().st_size != int(remote_fingerprint["size"]):
            return False, "size_mismatch"
        return False, "invalid_zip"
    expected_sha256 = str(entry.get("sha256") or "")
    if len(expected_sha256) != 64:
        return False, "missing_sha256"
    actual_sha256 = sha256_file(path)
    if actual_sha256 != expected_sha256:
        return False, "sha256_mismatch"
    return True, "remote_fingerprint_unchanged_and_local_integrity_valid"


def download_zip(session, url, destination, expected_size=None, retries=5):
    destination.parent.mkdir(parents=True, exist_ok=True)
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
            return destination
        except (requests.RequestException, OSError, WorkerError) as exc:
            last_error = exc
            temp.unlink(missing_ok=True)
            if attempt + 1 < retries:
                time.sleep(min(2 ** attempt, 8))
    raise WorkerError(f"failed to download Receita ZIP {url}: {last_error}")


def acquire_cached_zip(session, release, name, cache_root):
    files = resource_map(release)
    metadata = files.get(name)
    if metadata is None:
        raise WorkerError(f"Receita release metadata missing {name}")

    root = Path(cache_root)
    root.mkdir(parents=True, exist_ok=True)
    destination = root / name
    remote_fingerprint = remote_file_fingerprint(
        release["period"],
        name,
        metadata,
    )
    manifest = load_cache_manifest(root)
    entry = (manifest.get("files") or {}).get(name)
    reusable, reason = validate_manifest_cache_entry(
        destination,
        entry,
        remote_fingerprint,
    )
    if reusable:
        print(
            "RECEITA_CACHE "
            f"status=reused_unchanged file={name} "
            f"size={destination.stat().st_size} etag={remote_fingerprint['etag']}",
            flush=True,
        )
        return destination, "reused_unchanged"

    local = download_zip(
        session,
        urljoin(release["release_url"], name),
        destination,
        remote_fingerprint["size"],
    )
    sha256 = sha256_file(local)
    manifest.setdefault("files", {})[name] = {
        "remote_fingerprint": remote_fingerprint,
        "sha256": sha256,
        "last_modified": metadata.get("last_modified"),
        "downloaded_at": utc_now(),
    }
    manifest["version"] = CACHE_MANIFEST_VERSION
    manifest["reference_period"] = release["period"]
    manifest["release_fingerprint"] = release_fingerprint(release)
    write_cache_manifest(root, manifest)
    print(
        "RECEITA_CACHE "
        f"status=downloaded file={name} reason={reason} "
        f"size={local.stat().st_size} etag={remote_fingerprint['etag']}",
        flush=True,
    )
    return local, "downloaded"


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
    reasons_zip, _ = acquire_cached_zip(
        session,
        release,
        "Motivos.zip",
        cache_root,
    )
    reason_map = load_reason_map(reasons_zip)

    found = {}
    scanned = []
    for name in release["establishment_files"]:
        local, _ = acquire_cached_zip(
            session,
            release,
            name,
            cache_root,
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

    indexes = build_query_indexes(providers)
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
    cnae_zip, _ = acquire_cached_zip(
        session,
        release,
        "Cnaes.zip",
        cache_root,
    )
    nature_zip, _ = acquire_cached_zip(
        session,
        release,
        "Naturezas.zip",
        cache_root,
    )
    cnae_map = load_code_map(cnae_zip)
    nature_map = load_code_map(nature_zip)

    companies_by_base = {}
    legal_matches_by_base = {}
    company_files_scanned = []
    for name in companies:
        local, _ = acquire_cached_zip(
            session,
            release,
            name,
            cache_root,
        )
        with zipfile.ZipFile(str(local)) as archive:
            member = first_csv_member(archive)
            with archive.open(member) as raw:
                reader = sanitized_csv_reader(raw)
                for row in reader:
                    if len(row) < 3:
                        continue
                    base = digits(row[0])
                    if len(base) != 8:
                        continue
                    legal_name = str(row[1] or "").strip()
                    nature_code = digits(row[2])
                    matches = match_name_to_queries(
                        legal_name, indexes, "legal_name"
                    )
                    if base in target_bases or matches:
                        companies_by_base[base] = {
                            "cnpj_base": base,
                            "legal_name_receita": legal_name or None,
                            "legal_nature_code": nature_code or None,
                            "legal_nature": nature_map.get(nature_code)
                            if nature_code
                            else None,
                        }
                    if matches:
                        legal_matches_by_base.setdefault(base, []).extend(matches)
        company_files_scanned.append(name)

    selected = []
    establishment_files_scanned = []
    for name in release["establishment_files"]:
        local, _ = acquire_cached_zip(
            session,
            release,
            name,
            cache_root,
        )
        with zipfile.ZipFile(str(local)) as archive:
            member = first_csv_member(archive)
            with archive.open(member) as raw:
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


def daemonize(stdout_log, stderr_log):
    """Detach completely from the SSH channel before long-running work."""
    first = os.fork()
    if first > 0:
        os._exit(0)

    os.setsid()
    second = os.fork()
    if second > 0:
        os._exit(0)

    os.umask(0o077)

    stdin_fd = os.open(os.devnull, os.O_RDONLY)
    stdout_fd = os.open(str(stdout_log), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    stderr_fd = os.open(str(stderr_log), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.dup2(stdin_fd, 0)
    os.dup2(stdout_fd, 1)
    os.dup2(stderr_fd, 2)

    soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_limit == resource.RLIM_INFINITY or soft_limit > 65536:
        soft_limit = 65536
    os.closerange(3, int(soft_limit))


def execute(args):
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("discover", "lifecycle", "identity"), required=True)
    parser.add_argument("--input")
    parser.add_argument("--output", required=True)
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--daemonize", action="store_true")
    parser.add_argument("--exit-file")
    parser.add_argument("--stdout-log")
    parser.add_argument("--stderr-log")
    args = parser.parse_args()

    if args.daemonize:
        if not args.exit_file or not args.stdout_log or not args.stderr_log:
            parser.error(
                "--daemonize requires --exit-file, --stdout-log and --stderr-log"
            )
        daemonize(args.stdout_log, args.stderr_log)

    exit_code = 0
    try:
        execute(args)
    except Exception:  # noqa: BLE001 - daemon must always persist an exit marker
        traceback.print_exc()
        exit_code = 1
    finally:
        write_exit_marker(args.exit_file, exit_code)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
