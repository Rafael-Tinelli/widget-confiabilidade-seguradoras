#!/usr/bin/env python3
import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
WORKER = HERE / "receita_remote_worker.py"

spec = importlib.util.spec_from_file_location("receita_remote_worker", WORKER)
if spec is None or spec.loader is None:
    raise SystemExit("cannot load receita_remote_worker.py")
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)

_original = worker.download_zip

def cache_only_download(session, url, destination, expected_size=None, retries=5):
    destination = Path(destination)
    if not worker.valid_cached_zip(destination, expected_size):
        raise worker.WorkerError(
            "CACHE_ONLY_MISS "
            f"file={destination.name} expected_size={expected_size} "
            f"exists={destination.exists()} "
            f"actual_size={destination.stat().st_size if destination.exists() else None}"
        )
    print(
        "LIFECYCLE_CACHE_REUSE "
        f"file={destination.name} "
        f"size={destination.stat().st_size} "
        f"zip_valid={worker.zipfile.is_zipfile(str(destination))}",
        flush=True,
    )
    return destination

worker.download_zip = cache_only_download
sys.exit(worker.main())
