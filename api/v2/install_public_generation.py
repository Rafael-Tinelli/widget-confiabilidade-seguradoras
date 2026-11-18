from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import tarfile
from pathlib import Path, PurePosixPath
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
    # A manifest is a logical POSIX path list, never a filesystem glob.
    # Reject alternate spellings, nested manifests and ambiguous separators.
    path = PurePosixPath(value)
    if (not value or "\\" in value or path.is_absolute()
            or path.as_posix() != value or ".." in path.parts
            or path.name == MANIFEST_NAME):
        raise PublicGenerationInstallError(f"unsafe manifest path: {value!r}")
    return Path(*path.parts)


def _physical_inventory(package_dir: Path) -> tuple[set[str], set[str]]:
    """Inventory EVERY entry without following symlinks, including non-JSON.

    Never trust Path.is_file() or rglob('*.json') alone: both would omit
    unlisted files and can dereference symlinks before validation.
    """
    # current/previous and the public web path intentionally resolve through
    # symlinks into immutable generations/<build_id> directories. Only that
    # trusted *root pointer* is allowed; children are always checked with
    # lstat(follow_symlinks=False) and symlinks inside the package are denied.
    if package_dir.is_symlink():
        resolved = package_dir.resolve()
        if (not resolved.is_dir()
                or resolved.parent.name != "generations"):
            raise PublicGenerationInstallError(
                "public package root symlink is not a generation pointer"
            )
    elif not package_dir.is_dir():
        raise PublicGenerationInstallError("public package root is not a real directory")
    files: set[str] = set()
    directories: set[str] = set()
    pending = [package_dir]
    while pending:
        parent = pending.pop()
        try:
            with os.scandir(parent) as entries:
                for entry in entries:
                    path = Path(entry.path)
                    name = path.relative_to(package_dir).as_posix()
                    metadata = entry.stat(follow_symlinks=False)
                    mode = metadata.st_mode
                    if stat.S_ISLNK(mode):
                        raise PublicGenerationInstallError(
                            f"public package contains symlink: {name}"
                        )
                    if stat.S_ISDIR(mode):
                        directories.add(name)
                        pending.append(path)
                    elif stat.S_ISREG(mode):
                        # A hardlink to a file outside this tree is still a
                        # regular file to lstat; reject multi-linked inodes too.
                        if metadata.st_nlink != 1:
                            raise PublicGenerationInstallError(
                                f"public package contains hardlink: {name}"
                            )
                        files.add(name)
                    else:
                        raise PublicGenerationInstallError(
                            f"public package contains special file: {name}"
                        )
        except OSError as exc:
            raise PublicGenerationInstallError(
                f"cannot inventory public package: {parent}"
            ) from exc
    return files, directories


def _expected_directories(files: set[str]) -> set[str]:
    allowed: set[str] = set()
    for name in files:
        parent = PurePosixPath(name).parent
        while parent != PurePosixPath("."):
            allowed.add(parent.as_posix())
            parent = parent.parent
    return allowed



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
    # Inventory first, including the manifest itself and any hidden files.
    # Every caller (verify/install/reuse/rollback/remote) gets this exact check.
    physical_files, physical_dirs = _physical_inventory(package_dir)
    if MANIFEST_NAME not in physical_files:
        raise PublicGenerationInstallError(f"missing {MANIFEST_NAME}: {package_dir}")
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

    # The only unlisted physical file is the one manifest at package root.
    # This also rejects non-JSON extras (ZIP/PHP), nested manifests, hidden
    # files, unlisted empty directories, symlinks and filesystem special types.
    allowed_files = expected_paths | {MANIFEST_NAME}
    if physical_files != allowed_files:
        missing = sorted(allowed_files - physical_files)
        extra = sorted(physical_files - allowed_files)
        raise PublicGenerationInstallError(
            f"manifest/package file set mismatch: missing={missing}, extra={extra}"
        )
    expected_dirs = _expected_directories(allowed_files)
    if physical_dirs != expected_dirs:
        extra_dirs = sorted(physical_dirs - expected_dirs)
        missing_dirs = sorted(expected_dirs - physical_dirs)
        raise PublicGenerationInstallError(
            f"manifest/package directory set mismatch: missing={missing_dirs}, "
            f"extra={extra_dirs}"
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


def verify_transport_archive(archive_path: Path, package_dir: Path) -> dict[str, Any]:
    """Prove the tar contains exactly the verified package, byte for byte.

    This validates the ACTUAL upload, not just the directory before tar.
    The remote installer repeats verify_package after extraction and before
    touching current/previous. ZIP/PHP, nested manifest, link, special type,
    duplicate and traversal members must fail closed.
    """
    manifest = verify_package(package_dir)
    rows = manifest["public_package"]["files"]
    expected = {
        row["path"]: (row["sha256"], row["bytes"]) for row in rows
    }
    expected[MANIFEST_NAME] = (
        _sha256_file(package_dir / MANIFEST_NAME),
        (package_dir / MANIFEST_NAME).stat().st_size,
    )
    dirs = _expected_directories(set(expected))
    if archive_path.is_symlink() or not archive_path.is_file():
        raise PublicGenerationInstallError("transport archive is not a regular file")
    seen_files: set[str] = set()
    seen_dirs: set[str] = set()
    root_seen = False
    try:
        with tarfile.open(archive_path, mode="r:gz") as archive:
            for member in archive:
                raw = member.name
                if raw == ".":
                    if root_seen or not member.isdir():
                        raise PublicGenerationInstallError("unsafe transport archive root")
                    root_seen = True
                    continue
                name = raw.removeprefix("./")
                # GNU tar sometimes includes trailing '/' on directory
                # headers; compare canonical names without that final slash.
                if member.isdir():
                    name = name.rstrip("/")
                normalized = PurePosixPath(name)
                if (not name or name.startswith("/") or "\\" in name
                        or ".." in normalized.parts
                        or normalized.as_posix() != name):
                    raise PublicGenerationInstallError(
                        f"unsafe transport archive member: {raw}"
                    )
                if member.isdir():
                    if name in seen_dirs or name not in dirs:
                        raise PublicGenerationInstallError(
                            f"unexpected transport archive directory: {raw}"
                        )
                    seen_dirs.add(name)
                elif member.isfile():
                    if name in seen_files or name not in expected:
                        raise PublicGenerationInstallError(
                            f"unexpected transport archive file: {raw}"
                        )
                    seen_files.add(name)
                    digest, expected_bytes = expected[name]
                    if member.size != expected_bytes:
                        raise PublicGenerationInstallError(
                            f"transport archive file size mismatch: {name}"
                        )
                    data = archive.extractfile(member)
                    if data is None:
                        raise PublicGenerationInstallError(
                            f"unreadable transport archive file: {name}"
                        )
                    actual = hashlib.sha256()
                    size = 0
                    with data:
                        for chunk in iter(lambda data=data: data.read(1024 * 1024), b""):
                            size += len(chunk)
                            actual.update(chunk)
                    if size != expected_bytes or actual.hexdigest() != digest:
                        raise PublicGenerationInstallError(
                            f"transport archive content mismatch: {name}"
                        )
                else:
                    raise PublicGenerationInstallError(
                        f"transport archive contains link or special file: {raw}"
                    )
    except (OSError, tarfile.TarError) as exc:
        raise PublicGenerationInstallError("invalid public transport archive") from exc
    if seen_files != set(expected) or seen_dirs != dirs:
        raise PublicGenerationInstallError(
            "transport archive file/directory set mismatch"
        )
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
            (existing.get("public_package") or {}).get("package_sha256") or ""
        )
        incoming_sha = str(
            (manifest.get("public_package") or {}).get("package_sha256") or ""
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

    verify_archive = subparsers.add_parser("verify-archive")
    verify_archive.add_argument("archive_path", type=Path)
    verify_archive.add_argument("package_dir", type=Path)

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
    elif args.command == "verify-archive":
        manifest = verify_transport_archive(args.archive_path, args.package_dir)
        result = {
            "action": "verify-archive",
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
