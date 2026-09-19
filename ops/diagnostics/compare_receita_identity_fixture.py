#!/usr/bin/env python3
import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import requests

from api.sources.receita_cnpj_bulk import ReceitaOpenDataRelease
from api.sources import receita_cnpj_identity as canonical
from api.v2 import receita_remote_bridge as bridge

if len(sys.argv) != 3:
    raise SystemExit(
        "usage: compare_receita_identity_fixture.py FIXTURE_ROOT RAW_WORKER_JSON"
    )

fixture_root = Path(sys.argv[1]).resolve()
raw_path = Path(sys.argv[2]).resolve()
meta = json.loads((fixture_root / "fixture_meta.json").read_text(encoding="utf-8"))
raw_worker = json.loads(raw_path.read_text(encoding="utf-8"))
period = meta["period"]
period_root = fixture_root / "cache" / period

release_url = (
    "https://arquivos.receitafederal.gov.br/public.php/dav/files/"
    "gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ/2026-09/"
)
release = ReceitaOpenDataRelease(
    base_url="https://arquivos.receitafederal.gov.br/index.php/s/gn672Ad4CF8N6TK",
    release_url=release_url,
    period=period,
    establishment_files=("Estabelecimentos0.zip",),
    reasons_file="Motivos.zip",
    resource_metadata=(),
)

files = {
    name: {
        "name": name,
        "size": (period_root / name).stat().st_size,
        "is_collection": False,
    }
    for name in ("Cnaes.zip", "Naturezas.zip", "Empresas0.zip", "Estabelecimentos0.zip")
}

def fixture_release_files(session, resolved_release):
    return {
        "companies": ["Empresas0.zip"],
        "cnaes": "Cnaes.zip",
        "legal_natures": "Naturezas.zip",
        "metadata": files,
    }

def fixture_download_zip(session, url, destination):
    name = Path(urlparse(str(url)).path).name
    source = period_root / name
    if not source.is_file():
        raise RuntimeError(f"fixture source missing: {name}")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return destination

canonical._release_files = fixture_release_files
canonical._download_zip = fixture_download_zip

with tempfile.TemporaryDirectory(prefix="receita-identity-expected-") as temp_dir:
    expected = canonical.build_filtered_identity_snapshot(
        meta["entities"],
        meta["providers"],
        release=release,
        work_dir=Path(temp_dir),
        session=requests.Session(),
    )

bridge._remote_call = lambda mode, payload=None: raw_worker
actual = bridge.build_identity_remote(
    meta["entities"],
    meta["providers"],
    release=release,
)

def semantic_projection(payload):
    source = payload.get("source") or {}
    return {
        "artifact": payload.get("artifact"),
        "status": payload.get("status"),
        "source": {
            "authority": source.get("authority"),
            "dataset": source.get("dataset"),
            "public_share_url": source.get("public_share_url"),
            "release_url": source.get("release_url"),
            "reference_period": source.get("reference_period"),
            "ingestion_method": source.get("ingestion_method"),
        },
        "identity_semantics": payload.get("identity_semantics"),
        "meta": payload.get("meta"),
        "safe_outside_primary_cnaes": payload.get("safe_outside_primary_cnaes"),
        "canonical_records": payload.get("canonical_records"),
        "provider_matches": payload.get("provider_matches"),
    }

expected_semantic = semantic_projection(expected)
actual_semantic = semantic_projection(actual)

if actual_semantic != expected_semantic:
    print(
        "IDENTITY_SEMANTIC_MISMATCH "
        + json.dumps(
            {
                "expected": expected_semantic,
                "actual": actual_semantic,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    raise SystemExit(1)

selected = meta["selected"]
records = actual["canonical_records"]
if len(records) != 1:
    raise SystemExit(f"expected one canonical record, got {len(records)}")
record = records[0]
checks = {
    "cnpj": selected["cnpj"],
    "legal_name_receita": selected["legal_name_receita"],
    "legal_nature_code": selected["legal_nature_code"],
    "trade_name": selected["trade_name"],
    "primary_cnae_code": selected["primary_cnae_code"],
}
for key, expected_value in checks.items():
    if record.get(key) != expected_value:
        raise SystemExit(
            f"fixture semantic field mismatch {key}: "
            f"{record.get(key)!r} != {expected_value!r}"
        )

matches = actual["provider_matches"]
if len(matches) != 1:
    raise SystemExit(f"expected one provider match row, got {len(matches)}")
if matches[0]["candidate_state"] != "unique_candidate":
    raise SystemExit(
        f"expected unique_candidate, got {matches[0]['candidate_state']!r}"
    )
if len(matches[0]["candidates"]) != 1:
    raise SystemExit("expected exactly one provider candidate")
if matches[0]["candidates"][0]["cnpj"] != selected["cnpj"]:
    raise SystemExit("provider candidate CNPJ diverged from selected fixture")

encoded = json.dumps(
    actual_semantic,
    ensure_ascii=False,
    sort_keys=True,
    separators=(",", ":"),
).encode("utf-8")
digest = hashlib.sha256(encoded).hexdigest()

print(
    "IDENTITY_SEMANTIC_EQUIVALENCE_PASS "
    + json.dumps(
        {
            "period": period,
            "cnpj": selected["cnpj"],
            "provider": selected["provider"],
            "candidate_state": matches[0]["candidate_state"],
            "match_method": matches[0]["match_method"],
            "target_cnpjs": actual["meta"]["target_cnpjs"],
            "target_cnpjs_enriched": actual["meta"]["target_cnpjs_enriched"],
            "provider_queries": actual["meta"]["provider_queries"],
            "company_files_scanned": actual["meta"]["company_files_scanned"],
            "establishment_files_scanned": actual["meta"]["establishment_files_scanned"],
            "semantic_sha256": digest,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
)
