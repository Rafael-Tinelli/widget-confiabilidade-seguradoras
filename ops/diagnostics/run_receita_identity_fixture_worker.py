#!/usr/bin/env python3
import importlib.util
import json
import sys
import zipfile
from pathlib import Path

if len(sys.argv) != 4:
    raise SystemExit(
        "usage: run_receita_identity_fixture_worker.py FIXTURE_ROOT INPUT_META OUTPUT_JSON"
    )

fixture_root = Path(sys.argv[1]).resolve()
meta_path = Path(sys.argv[2]).resolve()
output_path = Path(sys.argv[3]).resolve()
here = Path(__file__).resolve().parent
worker_path = here / "receita_remote_worker.py"

spec = importlib.util.spec_from_file_location("receita_remote_worker", worker_path)
if spec is None or spec.loader is None:
    raise SystemExit("cannot load receita_remote_worker.py")
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)

meta = json.loads(meta_path.read_text(encoding="utf-8"))
period = meta["period"]
cache_dir = fixture_root / "cache"
period_root = cache_dir / period

names = ["Cnaes.zip", "Naturezas.zip", "Empresas0.zip", "Estabelecimentos0.zip"]
resources = []
for name in names:
    path = period_root / name
    if not path.is_file() or not zipfile.is_zipfile(str(path)):
        raise SystemExit(f"IDENTITY_FIXTURE_INVALID={name}")
    resources.append(
        {
            "name": name,
            "href": "",
            "size": path.stat().st_size,
            "last_modified": None,
            "etag": None,
            "is_collection": False,
        }
    )

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
        "IDENTITY_CACHE_REUSE "
        f"file={destination.name} size={destination.stat().st_size}",
        flush=True,
    )
    return destination

worker.download_zip = cache_only_download

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
    "period": period,
    "establishment_files": ["Estabelecimentos0.zip"],
    "resources": resources,
}

payload = {
    "expected_period": period,
    "targets": meta["entities"],
    "providers": meta["providers"],
}

result = worker.identity_extract(
    None,
    release,
    payload,
    str(cache_dir),
)
worker.write_output(str(output_path), result)
print(
    "IDENTITY_WORKER_EXTRACT_COMPLETE "
    + json.dumps(
        {
            "company_files_scanned": result["company_files_scanned"],
            "establishment_files_scanned": result["establishment_files_scanned"],
            "selected_establishments": len(result["selected_establishments"]),
        },
        sort_keys=True,
    ),
    flush=True,
)
