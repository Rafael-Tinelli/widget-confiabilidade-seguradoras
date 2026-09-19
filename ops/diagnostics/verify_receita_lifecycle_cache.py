#!/usr/bin/env python3
import json
import sys
from pathlib import Path

if len(sys.argv) != 3:
    raise SystemExit("usage: verify_receita_lifecycle_cache.py REMOTE_DIR CACHE_ROOT")

remote_dir = Path(sys.argv[1]).resolve()
cache_root = Path(sys.argv[2]).resolve() / "2026-09"
expected = json.loads((remote_dir / "expected.json").read_text(encoding="utf-8"))
before = expected["cache_before"]
after = {}
for name in ("Motivos.zip", "Estabelecimentos0.zip"):
    path = cache_root / name
    after[name] = {
        "size": path.stat().st_size,
        "mtime_ns": path.stat().st_mtime_ns,
    }

if before != after:
    raise SystemExit(
        "CACHE_FILES_CHANGED="
        + json.dumps({"before": before, "after": after}, sort_keys=True)
    )

print("LIFECYCLE_CACHE_UNCHANGED " + json.dumps(after, sort_keys=True))
