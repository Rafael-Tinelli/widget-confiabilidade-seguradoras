#!/usr/bin/env python3
import csv
import importlib.util
import io
import json
import sys
import time
import zipfile
from pathlib import Path

if len(sys.argv) != 2:
    raise SystemExit("usage: probe_receita_identity_full_cache.py WORKER_PATH")

worker_path = Path(sys.argv[1]).resolve()
spec = importlib.util.spec_from_file_location("receita_remote_worker_probe", worker_path)
if spec is None or spec.loader is None:
    raise SystemExit("cannot load worker")
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)

cache_dir = Path.home() / ".sanida-v2-receita-bridge" / "cache"
session = worker.configure_session()
try:
    release = worker.discover_release(session)
    cache_root = cache_dir / release["period"]
    estab0 = cache_root / "Estabelecimentos0.zip"
    if not estab0.is_file() or not zipfile.is_zipfile(str(estab0)):
        raise SystemExit("Estabelecimentos0.zip cache unavailable")

    target = None
    with zipfile.ZipFile(str(estab0)) as archive:
        member = worker.first_csv_member(archive)
        with archive.open(member) as raw:
            reader = worker.sanitized_csv_reader(raw)
            for row in reader:
                if len(row) < 13:
                    continue
                cnpj = worker.normalize_cnpj(
                    "".join(str(part).strip() for part in row[:3])
                )
                if cnpj:
                    target = cnpj
                    break
    if not target:
        raise SystemExit("could not select deterministic target")

    original_download = worker.download_zip

    def cache_only(
        session,
        url,
        destination,
        expected_size=None,
        retries=5,
        **kwargs,
    ):
        destination = Path(destination)
        if not worker.valid_cached_zip(destination, expected_size):
            raise worker.WorkerError(
                "CACHE_ONLY_MISS "
                f"file={destination.name} expected_size={expected_size}"
            )
        print(
            "IDENTITY_FULL_CACHE_REUSE "
            f"file={destination.name} size={destination.stat().st_size}",
            flush=True,
        )
        return destination

    worker.download_zip = cache_only
    payload = {
        "expected_period": release["period"],
        "targets": [
            {
                "cnpj": target,
                "entity_id": "identity-full-cache-probe",
                "legal_name": "IDENTITY FULL CACHE PROBE TARGET",
            }
        ],
        "providers": ["SANIDA PROVIDER IMPOSSIVEL QZXWVT 987654321"],
    }
    started = time.time()
    result = worker.identity_extract(
        session,
        release,
        payload,
        str(cache_dir),
    )
    elapsed = time.time() - started
    print(
        "IDENTITY_FULL_CACHE_PASS "
        + json.dumps(
            {
                "elapsed_seconds": round(elapsed, 3),
                "period": release["period"],
                "company_files_scanned": result["company_files_scanned"],
                "establishment_files_scanned": result[
                    "establishment_files_scanned"
                ],
                "selected_establishments": len(result["selected_establishments"]),
                "target": target,
            },
            sort_keys=True,
        ),
        flush=True,
    )
finally:
    session.close()
