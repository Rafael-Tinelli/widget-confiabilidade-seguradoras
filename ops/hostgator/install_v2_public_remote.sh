#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 6 ]]; then
  echo "usage: $0 PACKAGE_DIR TARGET_ROOT PUBLIC_PATH EXPECTED_BUILD_ID EXPECTED_PACKAGE_SHA INSTALLER_PY" >&2
  exit 64
fi

PACKAGE_DIR=$1
TARGET_ROOT=$2
PUBLIC_PATH=$3
EXPECTED_BUILD_ID=$4
EXPECTED_PACKAGE_SHA=$5
INSTALLER_PY=$6

for required in "$PACKAGE_DIR" "$TARGET_ROOT" "$PUBLIC_PATH" "$EXPECTED_BUILD_ID" "$EXPECTED_PACKAGE_SHA" "$INSTALLER_PY"; do
  if [[ -z "$required" ]]; then
    echo "publication argument must not be empty" >&2
    exit 64
  fi
done

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required on the HostGator account" >&2
  exit 69
fi

if [[ ! -f "$INSTALLER_PY" ]]; then
  echo "missing installer: $INSTALLER_PY" >&2
  exit 66
fi

if [[ ! -d "$PACKAGE_DIR" ]]; then
  echo "missing incoming package directory: $PACKAGE_DIR" >&2
  exit 66
fi

python3 "$INSTALLER_PY" verify "$PACKAGE_DIR" >/dev/null

python3 - "$PACKAGE_DIR/distribution_manifest.json" "$EXPECTED_BUILD_ID" "$EXPECTED_PACKAGE_SHA" <<'PY'
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
expected_build_id = sys.argv[2]
expected_package_sha = sys.argv[3]
payload = json.loads(manifest_path.read_text(encoding="utf-8"))
actual_build_id = str((payload.get("build") or {}).get("build_id") or "")
actual_package_sha = str((payload.get("public_package") or {}).get("package_sha256") or "")
if actual_build_id != expected_build_id:
    raise SystemExit(
        f"incoming build_id mismatch: {actual_build_id!r} != {expected_build_id!r}"
    )
if actual_package_sha != expected_package_sha:
    raise SystemExit(
        "incoming logical package sha256 does not match the source-run manifest"
    )
PY

mkdir -p "$TARGET_ROOT/generations" "$TARGET_ROOT/incoming"

python3 - "$PUBLIC_PATH" "$TARGET_ROOT/current" <<'PY'
import os
import sys
from pathlib import Path

public_path = Path(sys.argv[1])
current_path = Path(sys.argv[2])
if not public_path.is_symlink():
    raise SystemExit(
        f"public path must be a pre-provisioned symlink: {public_path}"
    )
actual = os.path.realpath(public_path)
expected = os.path.realpath(current_path)
if actual != expected:
    raise SystemExit(
        f"public path must point to the publication current pointer: {actual} != {expected}"
    )
PY

python3 "$INSTALLER_PY" install "$PACKAGE_DIR" "$TARGET_ROOT" > "$TARGET_ROOT/.last-install.json.tmp"
mv -f "$TARGET_ROOT/.last-install.json.tmp" "$TARGET_ROOT/last-install.json"

python3 "$INSTALLER_PY" verify "$TARGET_ROOT/current" >/dev/null

python3 - "$PUBLIC_PATH" "$TARGET_ROOT/current" "$EXPECTED_BUILD_ID" <<'PY'
import json
import os
import sys
from pathlib import Path

public_path = Path(sys.argv[1])
current_path = Path(sys.argv[2])
expected_build_id = sys.argv[3]
actual_public = os.path.realpath(public_path)
actual_current = os.path.realpath(current_path)
if actual_public != actual_current:
    raise SystemExit(
        f"public path no longer resolves to current generation: {actual_public} != {actual_current}"
    )
manifest = json.loads(
    (current_path / "distribution_manifest.json").read_text(encoding="utf-8")
)
actual_build_id = str((manifest.get("build") or {}).get("build_id") or "")
if actual_build_id != expected_build_id:
    raise SystemExit(
        f"published build_id mismatch: {actual_build_id!r} != {expected_build_id!r}"
    )
PY

cat "$TARGET_ROOT/last-install.json"
