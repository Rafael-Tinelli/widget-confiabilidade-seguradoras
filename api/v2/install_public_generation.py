from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
from pathlib import Path
from typing import Any

MANIFEST_NAME = "distribution_manifest.json"
_SAFE_BUILD_ID = re.compile(r"^[A-Za-z0-9._-]+$")


class PublicGenerationInstallError(RuntimeError):
    """Raised when a v2 public generation cannot be verified or installed safely."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_relative_path(value: str) -> Path:
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise PublicGenerationInstallError(f"unsafe manifest path: {value!r}")
    return path


def load_manifest(package_dir: Path) -> dict[str, Any]:
    path = package_dir / MANIFEST_NAME
    if not path.is_file():
        raise PublicGenerationInstallError(f"missing {MANIFEST_NAME}: {package_dir}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("artifact") != "v2_public_distribution_manifest":
        raise PublicGenerationInstallError("unexpected distribution manifest artifact")
    if payload.get("version") != 1:
        raise PublicGenerationInstallError("unsupported distribution manifest version")
    build = payload.get("build") if isinstance(payload.get("build"), dict) else {}
    build_id = str(build.get("build_id") or "")
    if not _SAFE_BUILD_ID.fullmatch(build_id):
        raise PublicGenerationInstallError(f"unsafe or missing build_id: {build_id!r}")
    policy = (
        payload.get("publication_policy")
        if isinstance(payload.get("publication_policy"), dict)
        else {}
    )
    required_policy = {
        "atomic_publish_required": True,
        "partial_publish_forbidden": True,
        "manifest_hash_verification_required": True,
        "retain_previous_generation_for_rollback": True,
    }
    if any(policy.get(key) is not value for key, value in required_policy.items()):
        raise PublicGenerationInstallError("distribution manifest publication policy is incomplete")
    return payload


def verify_package(package_dir: Path) -> dict[str, Any]:
    manifest = load_manifest(package_dir)
    public = (
        manifest.get("public_package")
        if isinstance(manifest.get("public_package"), dict)
        else {}
    )
    rows = public.get("files")
    if not isinstance(rows, list) or not rows:
        raise PublicGenerationInstallError("distribution manifest has no public files")

    expected_paths: set[str] = set()
    package_digest = hashlib.sha256()
    for row in rows:
        if not isinstance(row, dict):
            raise PublicGenerationInstallError("invalid public file record")
        relative = _safe_relative_path(str(row.get("path") or ""))
        relative_text = relative.as_posix()
        if relative_text in expected_paths:
            raise PublicGenerationInstallError(
                f"duplicate public file in manifest: {relative_text}"
            )
        expected_paths.add(relative_text)
        path = package_dir / relative
        expected_sha = str(row.get("sha256") or "")
        expected_bytes = row.get("bytes")
        if not path.is_file():
            raise PublicGenerationInstallError(f"missing public file: {relative_text}")
        if path.suffix != ".json":
            raise PublicGenerationInstallError(f"non-JSON public file: {relative_text}")
        if not isinstance(expected_bytes, int) or expected_bytes < 0:
            raise PublicGenerationInstallError(f"invalid byte size: {relative_text}")
        if path.stat().st_size != expected_bytes:
            raise PublicGenerationInstallError(f"byte size mismatch: {relative_text}")
        digest = _sha256_file(path)
        if len(expected_sha) != 64 or digest != expected_sha:
            raise PublicGenerationInstallError(f"sha256 mismatch: {relative_text}")
        package_digest.update(relative_text.encode("utf-8"))
        package_digest.update(b"\0")
        package_digest.update(digest.encode("ascii"))
        package_digest.update(b"\0")

    actual_paths = {
        path.relative_to(package_dir).as_posix()
        for path in package_dir.rglob("*.json")
        if path.is_file() and path.name != MANIFEST_NAME
    }
    if actual_paths != expected_paths:
        missing = sorted(expected_paths - actual_paths)
        extra = sorted(actual_paths - expected_paths)
        raise PublicGenerationInstallError(
            f"manifest/package file set mismatch: missing={missing}, extra={extra}"
        )

    expected_count = public.get("files_count")
    if expected_count != len(rows):
        raise PublicGenerationInstallError(
            f"files_count mismatch: {expected_count!r} != {len(rows)}"
        )
    expected_package_sha = str(public.get("package_sha256") or "")
    if package_digest.hexdigest() != expected_package_sha:
        raise PublicGenerationInstallError("logical package sha256 mismatch")
    return manifest


def _resolve_link_target(root: Path, link: Path) -> Path | None:
    if not link.is_symlink():
        return None
    raw = Path(os.readlink(link))
    resolved = (link.parent / raw).resolve() if not raw.is_absolute() else raw.resolve()
    generations = (root / "generations").resolve()
    if resolved.parent != generations:
        raise PublicGenerationInstallError(
            f"generation pointer escapes generations directory: {link} -> {raw}"
        )
    return resolved


def _atomic_symlink(link: Path, target: Path) -> None:
    link.parent.mkdir(parents=True, exist_ok=True)
    relative_target = os.path.relpath(target, start=link.parent)
    temp = link.with_name(f".{link.name}.{os.getpid()}.tmp")
    temp.unlink(missing_ok=True)
    os.symlink(relative_target, temp)
    os.replace(temp, link)


def install_generation(package_dir: Path, target_root: Path) -> dict[str, Any]:
    manifest = verify_package(package_dir)
    build_id = str((manifest.get("build") or {}).get("build_id") or "")
    generations = target_root / "generations"
    generations.mkdir(parents=True, exist_ok=True)
    destination = generations / build_id
    staging = generations / f".{build_id}.staging"

    if destination.exists():
        existing = verify_package(destination)
        existing_sha = str(
            ((existing.get("public_package") or {}).get("package_sha256") or "")
        )
        incoming_sha = str(
            ((manifest.get("public_package") or {}).get("package_sha256") or "")
        )
        if existing_sha != incoming_sha:
            raise PublicGenerationInstallError(
                f"existing generation differs from incoming package: {build_id}"
            )
    else:
        if staging.exists() or staging.is_symlink():
            if staging.is_dir() and not staging.is_symlink():
                shutil.rmtree(staging)
            else:
                staging.unlink()
        shutil.copytree(package_dir, staging)
        verify_package(staging)
        os.replace(staging, destination)

    current_link = target_root / "current"
    previous_link = target_root / "previous"
    old_current = _resolve_link_target(target_root, current_link)
    if old_current is not None and old_current != destination:
        verify_package(old_current)
        _atomic_symlink(previous_link, old_current)
    _atomic_symlink(current_link, destination)

    return {
        "action": "install",
        "build_id": build_id,
        "current": str(destination),
        "previous": str(old_current) if old_current and old_current != destination else None,
        "package_sha256": (manifest.get("public_package") or {}).get("package_sha256"),
    }


def rollback_generation(target_root: Path) -> dict[str, Any]:
    current_link = target_root / "current"
    previous_link = target_root / "previous"
    current = _resolve_link_target(target_root, current_link)
    previous = _resolve_link_target(target_root, previous_link)
    if current is None:
        raise PublicGenerationInstallError("current generation pointer is unavailable")
    if previous is None:
        raise PublicGenerationInstallError("previous generation pointer is unavailable")
    current_manifest = verify_package(current)
    previous_manifest = verify_package(previous)

    _atomic_symlink(previous_link, current)
    _atomic_symlink(current_link, previous)
    return {
        "action": "rollback",
        "build_id": str((previous_manifest.get("build") or {}).get("build_id") or ""),
        "current": str(previous),
        "previous": str(current),
        "replaced_build_id": str(
            (current_manifest.get("build") or {}).get("build_id") or ""
        ),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify, atomically install or roll back a Gate 4 v2 public generation."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    verify = subparsers.add_parser("verify")
    verify.add_argument("package_dir", type=Path)

    install = subparsers.add_parser("install")
    install.add_argument("package_dir", type=Path)
    install.add_argument("target_root", type=Path)

    rollback = subparsers.add_parser("rollback")
    rollback.add_argument("target_root", type=Path)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.command == "verify":
        manifest = verify_package(args.package_dir)
        result: dict[str, Any] = {
            "action": "verify",
            "build_id": (manifest.get("build") or {}).get("build_id"),
            "package_sha256": (manifest.get("public_package") or {}).get(
                "package_sha256"
            ),
        }
    elif args.command == "install":
        result = install_generation(args.package_dir, args.target_root)
    else:
        result = rollback_generation(args.target_root)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
