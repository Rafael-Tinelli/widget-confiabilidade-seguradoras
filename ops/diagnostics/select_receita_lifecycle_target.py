#!/usr/bin/env python3
import csv
import io
import json
import sys
import zipfile
from pathlib import Path

if len(sys.argv) != 3:
    raise SystemExit("usage: select_receita_lifecycle_target.py REMOTE_DIR CACHE_ROOT")

remote_dir = Path(sys.argv[1]).resolve()
cache_root = Path(sys.argv[2]).resolve()
estab = cache_root / "2026-09" / "Estabelecimentos0.zip"
motivos = cache_root / "2026-09" / "Motivos.zip"

for path in (motivos, estab):
    if not path.is_file() or not zipfile.is_zipfile(str(path)):
        raise SystemExit(f"CACHE_ONLY_REQUIRED_FILE_INVALID={path.name}")

before = {
    path.name: {
        "size": path.stat().st_size,
        "mtime_ns": path.stat().st_mtime_ns,
    }
    for path in (motivos, estab)
}

seed = json.loads((remote_dir / "verified_seed.json").read_text(encoding="utf-8"))
seed_by_cnpj = {row["cnpj"]: row for row in seed["records"]}
wanted = set(seed_by_cnpj)

selected = None
fallback = None
with zipfile.ZipFile(str(estab)) as archive:
    members = [name for name in archive.namelist() if not name.endswith("/")]
    if not members:
        raise SystemExit("ESTABELECIMENTOS0_EMPTY")
    with archive.open(members[0]) as raw:
        text = io.TextIOWrapper(raw, encoding="latin1", newline="")
        reader = csv.reader(
            (line.replace("\x00", "") for line in text),
            delimiter=";",
            quotechar='"',
        )
        for row in reader:
            if len(row) < 8:
                continue
            cnpj = "".join(str(part).strip() for part in row[:3])
            if len(cnpj) != 14 or not cnpj.isdigit():
                continue
            if fallback is None:
                fallback = row
            if cnpj in wanted:
                selected = row
                break

if selected is None:
    selected = fallback
if selected is None:
    raise SystemExit("NO_VALID_ROW_FOUND_IN_ESTABELECIMENTOS0")

cnpj = "".join(str(part).strip() for part in selected[:3])
seed_row = seed_by_cnpj.get(cnpj)
if seed_row:
    legal_name = seed_row["legal_name"]
    target_mode = "verified_seed"
else:
    legal_name = "CACHE ONLY LIFECYCLE PROBE TARGET"
    target_mode = "deterministic_cached_row"

payload = {
    "expected_period": "2026-09",
    "targets": [{"cnpj": cnpj, "legal_name": legal_name}],
}
expected = {
    "target_mode": target_mode,
    "cnpj": cnpj,
    "legal_name": legal_name,
    "status_code": str(selected[5]).strip().zfill(2),
    "status_date": str(selected[6]).strip() or None,
    "reason_code": str(selected[7]).strip().zfill(2),
    "source_file": "Estabelecimentos0.zip",
    "cache_before": before,
}
if seed_row:
    expected["seed_status"] = seed_row["cadastral_status"]
    expected["seed_status_date"] = seed_row["status_date"]

(remote_dir / "input.json").write_text(
    json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
(remote_dir / "expected.json").write_text(
    json.dumps(expected, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
print("LIFECYCLE_TARGET_SELECTED " + json.dumps(expected, ensure_ascii=False, sort_keys=True))
