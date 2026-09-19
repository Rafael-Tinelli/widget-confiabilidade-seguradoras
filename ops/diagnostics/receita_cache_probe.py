#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import re
import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import unquote

import requests

HOST = "https://arquivos.receitafederal.gov.br"
TOKEN = "gn672Ad4CF8N6TK"
CNPJ_PATH = "Dados/Cadastros/CNPJ"
TARGET_FILE = "Motivos.zip"
NS = {"d": "DAV:"}
TIMEOUT = 30


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def propfind(url: str):
    r = requests.request(
        "PROPFIND",
        url,
        headers={
            "Depth": "1",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "sanida-receita-cache-probe/1.0",
        },
        timeout=TIMEOUT,
    )
    if r.status_code != 207:
        raise RuntimeError(f"PROPFIND {url} returned HTTP {r.status_code}")
    root = ET.fromstring(r.content)
    out = []
    for response in root.findall("d:response", NS):
        href = unquote(response.findtext("d:href", default="", namespaces=NS))
        prop = None
        for ps in response.findall("d:propstat", NS):
            status = ps.findtext("d:status", default="", namespaces=NS) or ""
            if "200" in status:
                prop = ps.find("d:prop", NS)
                break
        if prop is None:
            continue
        rt = prop.find("d:resourcetype", NS)
        is_collection = rt is not None and rt.find("d:collection", NS) is not None
        name = (prop.findtext("d:displayname", default="", namespaces=NS) or "").strip()
        if not name:
            name = href.rstrip("/").rsplit("/", 1)[-1]
        out.append(
            {
                "name": name,
                "href": href,
                "size": int((prop.findtext("d:getcontentlength", default="0", namespaces=NS) or "0").strip() or 0),
                "etag": (prop.findtext("d:getetag", default="", namespaces=NS) or "").strip(),
                "last_modified": (prop.findtext("d:getlastmodified", default="", namespaces=NS) or "").strip(),
                "collection": is_collection,
            }
        )
    return out


def discover_remote():
    root_url = f"{HOST}/public.php/dav/files/{TOKEN}/{CNPJ_PATH}/"
    resources = propfind(root_url)
    periods = sorted(
        r["name"]
        for r in resources
        if r["collection"] and re.fullmatch(r"20\d{2}-\d{2}", r["name"])
    )
    if not periods:
        raise RuntimeError("no CNPJ release period found")
    period = periods[-1]
    period_url = f"{root_url}{period}/"
    files = propfind(period_url)
    target = next((r for r in files if r["name"] == TARGET_FILE and not r["collection"]), None)
    if not target:
        raise RuntimeError(f"{TARGET_FILE} not found in {period}")
    target["period"] = period
    target["url"] = f"{period_url}{TARGET_FILE}"
    return target


def fingerprint(remote):
    return {
        "period": remote["period"],
        "filename": TARGET_FILE,
        "etag": remote["etag"],
        "size": remote["size"],
    }


def load_manifest(path: Path):
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def save_manifest(path: Path, manifest):
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")
    os.replace(tmp, path)


def validate_local(file_path: Path, manifest_entry, remote_fp):
    reasons = []
    if not file_path.exists():
        reasons.append("missing_file")
        return False, reasons
    if not manifest_entry:
        reasons.append("missing_manifest")
        return False, reasons
    if manifest_entry.get("remote_fingerprint") != remote_fp:
        reasons.append("remote_fingerprint_changed")
    actual_size = file_path.stat().st_size
    if actual_size != remote_fp["size"]:
        reasons.append("size_mismatch")
    expected_sha = manifest_entry.get("sha256")
    actual_sha = sha256_file(file_path)
    if not expected_sha or actual_sha != expected_sha:
        reasons.append("sha256_mismatch")
    if not zipfile.is_zipfile(file_path):
        reasons.append("invalid_zip")
    return not reasons, reasons


def download(remote, file_path: Path):
    tmp = file_path.with_suffix(".download")
    with requests.get(
        remote["url"],
        headers={"User-Agent": "sanida-receita-cache-probe/1.0"},
        timeout=TIMEOUT,
        stream=True,
    ) as r:
        if r.status_code != 200:
            raise RuntimeError(f"GET {remote['url']} returned HTTP {r.status_code}")
        with tmp.open("wb") as fh:
            for chunk in r.iter_content(64 * 1024):
                if chunk:
                    fh.write(chunk)
    if tmp.stat().st_size != remote["size"]:
        raise RuntimeError(
            f"downloaded size mismatch: got {tmp.stat().st_size}, expected {remote['size']}"
        )
    if not zipfile.is_zipfile(tmp):
        raise RuntimeError("downloaded file is not a valid ZIP")
    os.replace(tmp, file_path)
    return sha256_file(file_path)


def ensure(cache_dir: Path, expect_status: str | None, expect_reason: str | None):
    cache_dir.mkdir(parents=True, exist_ok=True)
    file_path = cache_dir / TARGET_FILE
    manifest_path = cache_dir / "manifest.json"

    remote = discover_remote()
    remote_fp = fingerprint(remote)
    manifest = load_manifest(manifest_path)
    entry = manifest.get(TARGET_FILE)

    valid, reasons = validate_local(file_path, entry, remote_fp)
    if valid:
        status = "reused_unchanged"
        sha = entry["sha256"]
        reason = "remote_fingerprint_unchanged_and_local_integrity_valid"
    else:
        status = "downloaded"
        reason = ",".join(reasons) if reasons else "cache_miss"
        sha = download(remote, file_path)
        manifest[TARGET_FILE] = {
            "remote_fingerprint": remote_fp,
            "sha256": sha,
            "last_modified": remote["last_modified"],
        }
        save_manifest(manifest_path, manifest)

    result = {
        "status": status,
        "reason": reason,
        "period": remote_fp["period"],
        "filename": TARGET_FILE,
        "etag": remote_fp["etag"],
        "size": remote_fp["size"],
        "sha256": sha,
        "local_size": file_path.stat().st_size,
        "zip_valid": zipfile.is_zipfile(file_path),
    }
    print("CACHE_PROBE_RESULT " + json.dumps(result, ensure_ascii=False, sort_keys=True))

    if expect_status and status != expect_status:
        raise SystemExit(f"expected status {expect_status!r}, got {status!r}")
    if expect_reason and expect_reason not in reason.split(","):
        raise SystemExit(f"expected reason {expect_reason!r}, got {reason!r}")


def tamper(cache_dir: Path):
    file_path = cache_dir / TARGET_FILE
    if not file_path.exists():
        raise SystemExit("cannot tamper: cached file does not exist")
    before_size = file_path.stat().st_size
    before_sha = sha256_file(file_path)
    with file_path.open("r+b") as fh:
        data = bytearray(fh.read())
        if not data:
            raise SystemExit("cannot tamper empty file")
        idx = len(data) // 2
        data[idx] ^= 0xFF
        fh.seek(0)
        fh.write(data)
        fh.truncate()
    after_size = file_path.stat().st_size
    after_sha = sha256_file(file_path)
    if after_size != before_size:
        raise SystemExit("tamper unexpectedly changed file size")
    if after_sha == before_sha:
        raise SystemExit("tamper did not change SHA-256")
    print(
        "CACHE_PROBE_TAMPER "
        + json.dumps(
            {
                "filename": TARGET_FILE,
                "size_unchanged": True,
                "size": after_size,
                "sha256_before": before_sha,
                "sha256_after": after_sha,
            },
            sort_keys=True,
        )
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["ensure", "tamper"])
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--expect-status")
    parser.add_argument("--expect-reason")
    args = parser.parse_args()

    cache_dir = Path(args.cache_dir)
    if args.action == "ensure":
        ensure(cache_dir, args.expect_status, args.expect_reason)
    else:
        tamper(cache_dir)


if __name__ == "__main__":
    main()
