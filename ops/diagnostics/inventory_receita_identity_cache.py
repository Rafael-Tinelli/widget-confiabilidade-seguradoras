#!/usr/bin/env python3
import json
import zipfile
from pathlib import Path

root = Path.home() / ".sanida-v2-receita-bridge" / "cache" / "2026-09"
rows = []
for path in sorted(root.glob("*.zip")):
    rows.append(
        {
            "name": path.name,
            "size": path.stat().st_size,
            "zip_valid": zipfile.is_zipfile(str(path)),
            "mtime_ns": path.stat().st_mtime_ns,
        }
    )
print("IDENTITY_CACHE_INVENTORY " + json.dumps(rows, sort_keys=True))
