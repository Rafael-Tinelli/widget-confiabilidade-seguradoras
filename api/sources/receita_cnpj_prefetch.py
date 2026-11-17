"""A09.6 experimental, OFF-BY-DEFAULT Receita partition prefetch.

Only the official Empresas and Estabelecimentos ZIPs may be prefetched.
At most two distinct files may be staged at once; CPU/CSV processing remains
strictly ordered. Every staged file is verified before use, and an error
returns control to the existing serial downloader. A09.4 owns storage of
Estabelecimentos after the ordered consumer adopts a validated staged file.

Set RECEITA_A096_PREFETCH=1 *explicitly* in an isolated proof only.
No workflow enables this for production or changes publication semantics.
"""
from __future__ import annotations

import hashlib
import os
import re
import shutil
import tempfile
import threading
import time
import zipfile
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self
from urllib.parse import urljoin, urlsplit

import requests

from api.sources.receita_cnpj_telemetry import emit, file_category

ENV_NAME = "RECEITA_A096_PREFETCH"
MAX_WORKERS = 2
MAX_STAGED_BYTES = 4 * 1024**3
MIN_FREE_RESERVE_BYTES = 4 * 1024**3
READ_CHUNK = 1024 * 1024
OFFICIAL_HOST = "arquivos.receitafederal.gov.br"
ALLOWED_NAME = re.compile(r"(?:Empresas|Estabelecimentos)(?:0|[1-9]\d*)\.zip\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class Partition:
    name: str
    url: str
    size: int
    etag: str | None = None
    last_modified: str | None = None

    @property
    def index(self) -> int:
        _, index = file_category(self.name)
        if index is None:
            raise ValueError("invalid_partition_index")
        return index


@dataclass(frozen=True)
class Staged:
    path: Path
    sha256: str


class PrefetchRejected(RuntimeError):
    """The staged candidate is not safe to use. Serial path remains authoritative."""


def requested() -> bool:
    return os.environ.get(ENV_NAME) == "1"


def official_partitions(
    release_url: str,
    names: list[str] | tuple[str, ...],
    metadata: dict[str, dict[str, Any]],
) -> list[Partition] | None:
    """Return a fully trusted set or disable speculative transfers entirely."""
    if urlsplit(release_url).scheme != "https" or (
        urlsplit(release_url).hostname != OFFICIAL_HOST
    ):
        return None
    if not names or len(set(names)) != len(names):
        return None
    specs: list[Partition] = []
    for name in names:
        if not isinstance(name, str) or not ALLOWED_NAME.fullmatch(name):
            return None
        item = metadata.get(name)
        if not isinstance(item, dict) or item.get("name") != name:
            return None
        size = item.get("size")
        if type(size) is not int or size <= READ_CHUNK:
            return None
        etag, modified = item.get("etag"), item.get("last_modified")
        if not (isinstance(etag, str) and etag.strip()
                or isinstance(modified, str) and modified.strip()):
            return None
        specs.append(Partition(
            name=name,
            url=urljoin(release_url, name),
            size=size,
            etag=etag if isinstance(etag, str) and etag.strip() else None,
            last_modified=modified if isinstance(modified, str) and modified.strip() else None,
        ))
    groups = {file_category(spec.name)[0] for spec in specs}
    indices = [spec.index for spec in specs]
    if len(groups) != 1 or indices != list(range(len(specs))):
        return None
    return specs


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify(path: Path, spec: Partition, known_digest: str) -> None:
    if (
        path.is_symlink() or not path.is_file()
        or path.stat().st_size != spec.size
        or not SHA256.fullmatch(known_digest)
        or not zipfile.is_zipfile(path)
    ):
        raise PrefetchRejected("incomplete_or_invalid_prefetch_zip")
    with zipfile.ZipFile(path) as archive:
        if not any(not entry.is_dir() for entry in archive.infolist()):
            raise PrefetchRejected("empty_prefetch_zip")
    if _digest(path) != known_digest:
        raise PrefetchRejected("prefetch_sha256_changed")


def _fetch_stage(spec: Partition, root: Path, cancel: threading.Event) -> Staged:
    """Own HTTP Session per worker, no Range/resume, no shared response state.

    Supports cooperative cancellation on exception or lifecycle early exit.
    ZIP CRC for every CSV is NOT decompressed here; parsing remains serial
    downstream, while is_zipfile checks the ZIP central directory.
    """
    from api.sources.receita_cnpj_bulk import _configure_official_session

    if (urlsplit(spec.url).scheme != "https"
            or urlsplit(spec.url).hostname != OFFICIAL_HOST):
        raise PrefetchRejected("nonofficial_prefetch_url")
    destination = root / spec.name
    part = root / (spec.name + ".part")
    started = time.perf_counter()
    size = 0
    digest = hashlib.sha256()
    try:
        if cancel.is_set():
            raise PrefetchRejected("prefetch_cancelled")
        with requests.Session() as session:
            _configure_official_session(session)
            with session.get(
                spec.url, stream=True, allow_redirects=True, timeout=(15, 20),
                headers={"Accept-Encoding": "identity"},
            ) as response:
                if response.status_code != 200:
                    raise PrefetchRejected("prefetch_unexpected_http_status")
                if (urlsplit(response.url).scheme != "https"
                        or urlsplit(response.url).hostname != OFFICIAL_HOST):
                    raise PrefetchRejected("prefetch_redirect_outside_official_host")
                if response.headers.get("Content-Range"):
                    raise PrefetchRejected("prefetch_unexpected_content_range")
                if response.headers.get("Content-Encoding", "identity").strip().lower() not in (
                    "", "identity",
                ):
                    raise PrefetchRejected("prefetch_unexpected_content_encoding")
                length = response.headers.get("Content-Length")
                if length is not None and (not length.isdecimal()
                                           or int(length) != spec.size):
                    raise PrefetchRejected("prefetch_content_length_mismatch")
                etag = response.headers.get("ETag")
                if spec.etag and etag and etag != spec.etag:
                    raise PrefetchRejected("prefetch_etag_mismatch")
                modified = response.headers.get("Last-Modified")
                if spec.last_modified and modified and modified != spec.last_modified:
                    raise PrefetchRejected("prefetch_last_modified_mismatch")
                with part.open("xb") as stream:
                    for chunk in response.iter_content(chunk_size=READ_CHUNK):
                        if cancel.is_set():
                            raise PrefetchRejected("prefetch_cancelled")
                        if not chunk:
                            continue
                        size += len(chunk)
                        if size > spec.size:
                            raise PrefetchRejected("prefetch_exceeded_official_size")
                        digest.update(chunk)
                        stream.write(chunk)
        if cancel.is_set():
            raise PrefetchRejected("prefetch_cancelled")
        if size != spec.size:
            raise PrefetchRejected("prefetch_truncated")
        if not zipfile.is_zipfile(part):
            raise PrefetchRejected("prefetch_invalid_zip")
        part.replace(destination)
        category, index = file_category(spec.name)
        emit(
            "prefetch_ready", category=category, file_index=index,
            expected_bytes=spec.size, received_bytes=size,
            elapsed_download_s=round(time.perf_counter() - started, 3),
            state="verified", zip_valid=True,
        )
        return Staged(destination, digest.hexdigest())
    except Exception:
        part.unlink(missing_ok=True)
        destination.unlink(missing_ok=True)
        raise


class PartitionPrefetch:
    """Context-managed two-file window; yields (partition, staged_path | None).

    None means the caller must use the *existing* serial acquisition.
    The caller, not any worker, performs parsing and A09.4 cache registration.
    """

    def __init__(
        self, specs: list[Partition] | None, *, root: Path, enabled: bool | None = None,
    ):
        self.specs = specs or []
        self.root = Path(root)
        self.enabled = requested() if enabled is None else enabled
        self._cancel = threading.Event()
        self._executor: ThreadPoolExecutor | None = None
        self._temporary: tempfile.TemporaryDirectory[str] | None = None
        self._stage: Path | None = None
        self._pending: dict[int, Future[Staged]] = {}

    def __enter__(self) -> Self:
        if self.enabled and len(self.specs) >= 2:
            try:
                self.root.mkdir(parents=True, exist_ok=True)
                self._temporary = tempfile.TemporaryDirectory(
                    prefix="a096-prefetch-", dir=self.root,
                )
                self._stage = Path(self._temporary.name)
                self._executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
                self._schedule(0)
                self._schedule(1)
            except (OSError, RuntimeError):
                # The optional optimization cannot make a healthy serial
                # acquisition fail during initialization.
                self.__exit__(None, None, None)
                self._executor = None
                self._temporary = None
                self._stage = None
        return self

    def _schedule(self, i: int) -> None:
        if self._executor is None or self._stage is None or i >= len(self.specs):
            return
        spec = self.specs[i]
        pending = sum(self.specs[k].size for k in self._pending)
        required = spec.size + pending
        try:
            affordable = (
                required <= MAX_STAGED_BYTES
                and shutil.disk_usage(self._stage).free
                >= required + MIN_FREE_RESERVE_BYTES
            )
        except OSError:
            affordable = False
        if not affordable:
            category, index = file_category(spec.name)
            emit("prefetch_skipped", category=category, file_index=index, state="disk_guard")
            return
        try:
            self._pending[i] = self._executor.submit(
                _fetch_stage, spec, self._stage, self._cancel,
            )
        except RuntimeError:
            # Executor shutdown is not a reason to fail the original reader.
            return

    def __iter__(self):
        for i, spec in enumerate(self.specs):
            staged: Path | None = None
            future = self._pending.pop(i, None)
            if future is not None:
                try:
                    result = future.result()
                    _verify(result.path, spec, result.sha256)
                    staged = result.path
                except Exception:  # noqa: BLE001 - speculative failure is always serial fallback
                    # Reject and discard the speculative candidate immediately,
                    # rather than reserving its disk space until context exit.
                    if self._stage is not None:
                        try:
                            (self._stage / spec.name).unlink(missing_ok=True)
                        except OSError:
                            pass
                    category, index = file_category(spec.name)
                    emit("prefetch_fallback", category=category, file_index=index,
                         state="serial")
            # The canonical serial reader is responsible for each None.
            yield spec, staged
            # Called only AFTER ordered consumption of the previous partition.
            # Never have more than two speculative files staged concurrently.
            self._schedule(i + MAX_WORKERS)

    def __exit__(self, _type, _value, _traceback) -> None:
        self._cancel.set()
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=True)
        if self._temporary is not None:
            self._temporary.cleanup()


def adopt_staged(staged: Path, destination: Path) -> Path:
    """Promote only a previously checked same-workspace file to the parser."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        destination.unlink()
    staged.replace(destination)
    return destination

def adopt_or_download(
    staged: Path | None,
    destination: Path,
    serial_download: Callable[[], Path],
    *,
    expected: Partition | None = None,
) -> Path:
    """Use a verified staging ZIP, or verify a serial fallback before parsing.

    Keep the original legacy path unchanged when expected is None (flag OFF
    or the official metadata is insufficient to activate the prefetch).
    """
    if staged is not None:
        try:
            return adopt_staged(staged, destination)
        except OSError:
            destination.unlink(missing_ok=True)
    local = serial_download()
    if expected is not None:
        try:
            if (local.is_symlink() or not local.is_file()
                    or local.stat().st_size != expected.size
                    or not zipfile.is_zipfile(local)):
                raise PrefetchRejected("serial_fallback_size_or_zip_invalid")
            # Official content hashes are not published. This is a LOCAL
            # post-transfer digest; A09.4 separately seals and verifies it.
            if not SHA256.fullmatch(_digest(local)):
                raise PrefetchRejected("serial_fallback_digest_invalid")
        except Exception:
            local.unlink(missing_ok=True)
            raise
    return local
