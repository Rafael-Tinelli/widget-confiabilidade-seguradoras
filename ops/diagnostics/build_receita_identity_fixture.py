#!/usr/bin/env python3
import csv
import io
import json
import shutil
import sys
import zipfile
from pathlib import Path

if len(sys.argv) != 3:
    raise SystemExit("usage: build_receita_identity_fixture.py CACHE_ROOT OUTPUT_ROOT")

cache_root = Path(sys.argv[1]).resolve() / "2026-09"
output_root = Path(sys.argv[2]).resolve()
fixture_period = output_root / "cache" / "2026-09"
fixture_period.mkdir(parents=True, exist_ok=True)

required = ["Empresas0.zip", "Estabelecimentos0.zip", "Cnaes.zip", "Naturezas.zip"]
for name in required:
    path = cache_root / name
    if not path.is_file() or not zipfile.is_zipfile(str(path)):
        raise SystemExit(f"IDENTITY_FIXTURE_REQUIRED_CACHE_INVALID={name}")

cache_before = {
    name: {
        "size": (cache_root / name).stat().st_size,
        "mtime_ns": (cache_root / name).stat().st_mtime_ns,
    }
    for name in required
}

def first_member(archive):
    members = [name for name in archive.namelist() if not name.endswith("/")]
    if not members:
        raise SystemExit("IDENTITY_FIXTURE_EMPTY_ARCHIVE")
    return members[0]

companies = {}
company_rows = {}
with zipfile.ZipFile(str(cache_root / "Empresas0.zip")) as archive:
    with archive.open(first_member(archive)) as raw:
        text = io.TextIOWrapper(raw, encoding="latin1", newline="")
        reader = csv.reader(
            (line.replace("\x00", "") for line in text),
            delimiter=";",
            quotechar='"',
        )
        for row in reader:
            if len(row) < 3:
                continue
            base = "".join(ch for ch in str(row[0] or "") if ch.isdigit())
            if len(base) != 8:
                continue
            companies[base] = {
                "legal_name": str(row[1] or "").strip(),
                "nature_code": "".join(ch for ch in str(row[2] or "") if ch.isdigit()),
            }
            company_rows[base] = row
            if len(companies) >= 50000:
                break

if not companies:
    raise SystemExit("IDENTITY_FIXTURE_NO_COMPANIES")

selected_estab = None
selected_base = None
with zipfile.ZipFile(str(cache_root / "Estabelecimentos0.zip")) as archive:
    with archive.open(first_member(archive)) as raw:
        text = io.TextIOWrapper(raw, encoding="latin1", newline="")
        reader = csv.reader(
            (line.replace("\x00", "") for line in text),
            delimiter=";",
            quotechar='"',
        )
        for row in reader:
            if len(row) < 13:
                continue
            base = "".join(ch for ch in str(row[0] or "") if ch.isdigit())
            order = "".join(ch for ch in str(row[1] or "") if ch.isdigit())
            dv = "".join(ch for ch in str(row[2] or "") if ch.isdigit())
            cnpj = base + order.zfill(4) + dv.zfill(2)
            if len(cnpj) != 14 or not cnpj.isdigit():
                continue
            if str(row[3]).strip() != "1":
                continue
            if base not in companies:
                continue
            selected_estab = row
            selected_base = base
            break

if selected_estab is None or selected_base is None:
    raise SystemExit("IDENTITY_FIXTURE_NO_SHARED_HEAD_OFFICE")

selected_company = company_rows[selected_base]
company = companies[selected_base]
cnpj = (
    "".join(ch for ch in str(selected_estab[0] or "") if ch.isdigit())
    + "".join(ch for ch in str(selected_estab[1] or "") if ch.isdigit()).zfill(4)
    + "".join(ch for ch in str(selected_estab[2] or "") if ch.isdigit()).zfill(2)
)
trade_name = str(selected_estab[4] or "").strip()
provider = trade_name or company["legal_name"]
if not provider:
    raise SystemExit("IDENTITY_FIXTURE_SELECTED_ROW_HAS_NO_PROVIDER_NAME")

def write_single_row_zip(path, member_name, row):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter=";", quotechar='"', lineterminator="\n")
        writer.writerow(row)
        archive.writestr(member_name, buffer.getvalue().encode("latin1"))

write_single_row_zip(
    fixture_period / "Empresas0.zip",
    "K3241.K03200Y0.D60914.EMPRECSV",
    selected_company,
)
write_single_row_zip(
    fixture_period / "Estabelecimentos0.zip",
    "K3241.K03200Y0.D60914.ESTABELE",
    selected_estab,
)
shutil.copy2(cache_root / "Cnaes.zip", fixture_period / "Cnaes.zip")
shutil.copy2(cache_root / "Naturezas.zip", fixture_period / "Naturezas.zip")

meta = {
    "period": "2026-09",
    "entities": [
        {
            "cnpj": cnpj,
            "entity_id": "cache-fixture:" + cnpj,
            "legal_name": company["legal_name"],
        }
    ],
    "providers": [provider],
    "selected": {
        "cnpj": cnpj,
        "cnpj_base": selected_base,
        "legal_name_receita": company["legal_name"],
        "legal_nature_code": company["nature_code"],
        "trade_name": trade_name or None,
        "primary_cnae_code": "".join(
            ch for ch in str(selected_estab[11] or "") if ch.isdigit()
        )
        or None,
        "provider": provider,
        "status_code": str(selected_estab[5] or "").strip().zfill(2),
    },
    "source_cache_before": cache_before,
}
(output_root / "fixture_meta.json").write_text(
    json.dumps(meta, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print("IDENTITY_FIXTURE_READY " + json.dumps(meta["selected"], ensure_ascii=False, sort_keys=True))
