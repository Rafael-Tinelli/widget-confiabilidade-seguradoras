#!/usr/bin/env python3
import json
import sys
from pathlib import Path

if len(sys.argv) != 3:
    raise SystemExit(
        "usage: verify_receita_identity_source_cache.py FIXTURE_ROOT CACHE_ROOT"
    )

fixture_root = Path(sys.argv[1]).resolve()
cache_root = Path(sys.argv[2]).resolve() / "2026-09"
meta = json.loads((fixture_root / "fixture_meta.json").read_text(encoding="utf-8"))
before = meta["source_cache_before"]
after = {}
for name in sorted(before):
    path = cache_root / name
    after[name] = {
        "size": path.stat().st_size,
        "mtime_ns": path.stat().st_mtime_ns,
    }

if before != after:
    raise SystemExit(
        "IDENTITY_SOURCE_CACHE_CHANGED="
        + json.dumps({"before": before, "after": after}, sort_keys=True)
    )

print("IDENTITY_SOURCE_CACHE_UNCHANGED " + json.dumps(after, sort_keys=True))
