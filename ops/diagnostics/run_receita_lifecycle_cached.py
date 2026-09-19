#!/usr/bin/env python3
import importlib.util
import json
import sys
from pathlib import Path

if len(sys.argv) != 4:
    raise SystemExit(
        "usage: run_receita_lifecycle_cached.py INPUT OUTPUT CACHE_ROOT"
    )

here = Path(__file__).resolve().parent
worker_path = here / "receita_remote_worker.py"
spec = importlib.util.spec_from_file_location("receita_remote_worker", worker_path)
if spec is None or spec.loader is None:
    raise SystemExit("cannot load receita_remote_worker.py")
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)

input_path = Path(sys.argv[1]).resolve()
output_path = Path(sys.argv[2]).resolve()
cache_root = Path(sys.argv[3]).resolve()
period_root = cache_root / "2026-09"

motivos = period_root / "Motivos.zip"
estab0 = period_root / "Estabelecimentos0.zip"

for path in (motivos, estab0):
    if not path.is_file():
        raise SystemExit(f"CACHE_ONLY_MISSING={path.name}")

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
        f"file={destination.name} size={destination.stat().st_size}",
        flush=True,
    )
    return destination

worker.download_zip = cache_only_download

payload = json.loads(input_path.read_text(encoding="utf-8"))
release_url = (
    "https://arquivos.receitafederal.gov.br/public.php/dav/files/"
    "gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ/2026-09/"
)
release = {
    "base_url": "https://arquivos.receitafederal.gov.br/index.php/s/gn672Ad4CF8N6TK",
    "dav_root": (
        "https://arquivos.receitafederal.gov.br/public.php/dav/files/"
        "gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ/"
    ),
    "release_url": release_url,
    "period": "2026-09",
    "establishment_files": ["Estabelecimentos0.zip"],
    "resources": [
        {
            "name": "Motivos.zip",
            "href": "",
            "size": 1180,
            "last_modified": "Mon, 14 Sep 2026 15:06:40 GMT",
            "etag": '"61c9b2d879ce0e232d4e2cb589d9abba"',
            "is_collection": False,
        },
        {
            "name": "Estabelecimentos0.zip",
            "href": "",
            "size": 2243389254,
            "last_modified": "Mon, 14 Sep 2026 15:02:59 GMT",
            "etag": '"d4efe5ace49c8948c4946b5978a62559"',
            "is_collection": False,
        },
    ],
}

result = worker.lifecycle_extract(
    None,
    release,
    payload,
    str(cache_root),
)
worker.write_output(str(output_path), result)
print(
    "LIFECYCLE_CACHED_EXTRACT_COMPLETE "
    + json.dumps(result["meta"], sort_keys=True),
    flush=True,
)
