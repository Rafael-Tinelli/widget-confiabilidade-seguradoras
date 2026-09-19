from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj import normalize_receita_lifecycle_record
from api.sources.receita_cnpj_bulk import ReceitaOpenDataRelease
from api.utils.identifiers import normalize_cnpj_v2

WORKER_PATH = Path("ops/hostgator/receita_remote_worker.py")
REMOTE_ROOT_DEFAULT = ".sanida-v2-receita-bridge"
_SAFE_HOST_RE = re.compile(r"^[A-Za-z0-9.-]+$")
_SAFE_USER_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_SAFE_REMOTE_ROOT_RE = re.compile(r"^[A-Za-z0-9._/-]+$")
_SAFE_BUILD_RE = re.compile(r"[^A-Za-z0-9._-]+")
_TRANSIENT_TRANSPORT_PATTERNS = (
    "kex_exchange_identification",
    "ssh_exchange_identification",
    "banner exchange",
    "connection reset by peer",
    "connection closed",
    "connection timed out",
    "connection refused",
    "connection unexpectedly closed",
    "closed by remote host",
    "broken pipe",
    "lost connection",
    "packet_write_wait",
    "client_loop: send disconnect",
    "mux_client_request_session",
    "control socket connect",
    "master refused session request",
)
_TRANSPORT_RETRY_ATTEMPTS = 6
_TRANSPORT_RETRY_BASE_SECONDS = 2
_TRANSPORT_RETRY_MAX_SECONDS = 20
_CONTROL_PERSIST_SECONDS = 600
_REMOTE_DEADLINE_SECONDS = 18_000


class ReceitaRemoteBridgeError(RuntimeError):
    """Raised when the private HostGator acquisition bridge cannot be used safely."""


@dataclass(frozen=True)
class ReceitaRemoteConfig:
    host: str
    user: str
    port: int
    key_file: Path
    known_hosts_file: Path
    remote_root: str


def remote_bridge_enabled() -> bool:
    return os.getenv("V2_RECEITA_REMOTE_ENABLED", "").strip() == "1"


def _config_from_env() -> ReceitaRemoteConfig:
    host = os.getenv("V2_RECEITA_SSH_HOST", "").strip()
    user = os.getenv("V2_RECEITA_SSH_USER", "").strip()
    port_text = os.getenv("V2_RECEITA_SSH_PORT", "").strip() or "22"
    key_file = Path(os.getenv("V2_RECEITA_SSH_KEY_FILE", "").strip())
    known_hosts_file = Path(
        os.getenv("V2_RECEITA_SSH_KNOWN_HOSTS_FILE", "").strip()
    )
    remote_root = (
        os.getenv("V2_RECEITA_REMOTE_ROOT", "").strip() or REMOTE_ROOT_DEFAULT
    )

    if not _SAFE_HOST_RE.fullmatch(host):
        raise ReceitaRemoteBridgeError("invalid Receita SSH host")
    if not _SAFE_USER_RE.fullmatch(user):
        raise ReceitaRemoteBridgeError("invalid Receita SSH user")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ReceitaRemoteBridgeError("invalid Receita SSH port") from exc
    if not 1 <= port <= 65535:
        raise ReceitaRemoteBridgeError("Receita SSH port is outside 1..65535")
    if not key_file.is_file():
        raise ReceitaRemoteBridgeError("Receita SSH private key file is unavailable")
    if not known_hosts_file.is_file():
        raise ReceitaRemoteBridgeError("Receita SSH known_hosts file is unavailable")
    if (
        not _SAFE_REMOTE_ROOT_RE.fullmatch(remote_root)
        or remote_root.startswith("/")
        or ".." in Path(remote_root).parts
    ):
        raise ReceitaRemoteBridgeError("invalid Receita remote root")
    if not WORKER_PATH.is_file():
        raise ReceitaRemoteBridgeError(f"Receita remote worker is missing: {WORKER_PATH}")

    return ReceitaRemoteConfig(
        host=host,
        user=user,
        port=port,
        key_file=key_file,
        known_hosts_file=known_hosts_file,
        remote_root=remote_root.rstrip("/"),
    )


def _control_path(config: ReceitaRemoteConfig) -> Path:
    identity = f"{config.user}@{config.host}:{config.port}".encode()
    digest = hashlib.sha256(identity).hexdigest()[:12]
    return Path(tempfile.gettempdir()) / f"v2r-{os.getpid()}-{digest}.sock"


def _ssh_base(
    config: ReceitaRemoteConfig,
    *,
    control_path: Path | None = None,
) -> list[str]:
    command = [
        "ssh",
        "-T",
        "-i",
        str(config.key_file),
        "-p",
        str(config.port),
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={config.known_hosts_file}",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        "-o",
        "UpdateHostKeys=no",
        "-o",
        "ConnectTimeout=20",
        "-o",
        "ConnectionAttempts=3",
        "-o",
        "TCPKeepAlive=yes",
        "-o",
        "ServerAliveInterval=20",
        "-o",
        "ServerAliveCountMax=9",
    ]
    if control_path is not None:
        command.extend(
            [
                "-o",
                "ControlMaster=auto",
                "-o",
                f"ControlPersist={_CONTROL_PERSIST_SECONDS}",
                "-o",
                f"ControlPath={control_path}",
            ]
        )
    command.append(f"{config.user}@{config.host}")
    return command


def _run(
    command: list[str],
    *,
    timeout: int = 18_000,
    check: bool = True,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            check=check,
            capture_output=True,
            text=True,
            input=input_text,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise ReceitaRemoteBridgeError(
            f"Receita remote command timed out after {timeout}s"
        ) from exc
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()[-4000:]
        stdout = (exc.stdout or "").strip()[-2000:]
        detail = stderr or stdout or f"exit={exc.returncode}"
        raise ReceitaRemoteBridgeError(
            f"Receita remote command failed: {detail}"
        ) from exc


def _is_transient_transport_error(exc: BaseException) -> bool:
    detail = str(exc).casefold()
    return any(pattern in detail for pattern in _TRANSIENT_TRANSPORT_PATTERNS)


def _backoff_seconds(attempt: int) -> int:
    return min(
        _TRANSPORT_RETRY_BASE_SECONDS * (2 ** max(0, attempt - 1)),
        _TRANSPORT_RETRY_MAX_SECONDS,
    )


def _bridge_event(
    *,
    mode: str,
    operation: str,
    phase: str,
    event: str,
    started_at: float | None = None,
    error: BaseException | None = None,
    **details: Any,
) -> None:
    payload: dict[str, Any] = {
        "mode": mode,
        "operation": operation,
        "phase": phase,
        "event": event,
    }
    if started_at is not None:
        payload["duration_seconds"] = round(time.monotonic() - started_at, 3)
    if error is not None:
        payload["error_type"] = type(error).__name__
        payload["error"] = str(error)[-1200:]
    payload.update(details)
    print(
        "RECEITA_BRIDGE_PHASE "
        + json.dumps(payload, ensure_ascii=False, sort_keys=True),
        file=sys.stderr,
        flush=True,
    )


@contextmanager
def _bridge_phase(
    *,
    mode: str,
    operation: str,
    phase: str,
    **details: Any,
):
    started_at = time.monotonic()
    _bridge_event(
        mode=mode,
        operation=operation,
        phase=phase,
        event="start",
        **details,
    )
    try:
        yield
    except BaseException as exc:
        _bridge_event(
            mode=mode,
            operation=operation,
            phase=phase,
            event="error",
            started_at=started_at,
            error=exc,
            **details,
        )
        raise
    else:
        _bridge_event(
            mode=mode,
            operation=operation,
            phase=phase,
            event="end",
            started_at=started_at,
            **details,
        )


def _run_transport(
    command: list[str],
    *,
    timeout: int,
    check: bool = True,
    input_text: str | None = None,
    attempts: int = _TRANSPORT_RETRY_ATTEMPTS,
) -> subprocess.CompletedProcess[str]:
    last_error: ReceitaRemoteBridgeError | None = None
    for attempt in range(1, attempts + 1):
        try:
            return _run(
                command,
                timeout=timeout,
                check=check,
                input_text=input_text,
            )
        except ReceitaRemoteBridgeError as exc:
            last_error = exc
            if not _is_transient_transport_error(exc) or attempt >= attempts:
                raise
            delay = _backoff_seconds(attempt)
            print(
                "RECEITA_SSH_RETRY "
                + json.dumps(
                    {
                        "attempt": attempt,
                        "next_attempt": attempt + 1,
                        "delay_seconds": delay,
                        "error": str(exc)[-600:],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)
    assert last_error is not None
    raise last_error


def _operation_id(mode: str) -> str:
    build_id = os.getenv("V2_BUILD_ID", "").strip() or "manual"
    value = _SAFE_BUILD_RE.sub("-", f"{build_id}-{mode}").strip("-")
    if not value:
        raise ReceitaRemoteBridgeError("cannot derive Receita remote operation id")
    return value[:160]


def _worker_remote_path(config: ReceitaRemoteConfig) -> str:
    digest = hashlib.sha256(WORKER_PATH.read_bytes()).hexdigest()[:20]
    return f"{config.remote_root}/workers/receita_remote_worker-{digest}.py"


def _upload_text_atomic(
    ssh: list[str],
    *,
    remote_path: str,
    payload: str,
) -> None:
    temp_path = f"{remote_path}.part-{os.getpid()}"
    command = (
        "umask 077; "
        f"cat > {shlex.quote(temp_path)} && "
        f"chmod 600 {shlex.quote(temp_path)} && "
        f"mv -f -- {shlex.quote(temp_path)} {shlex.quote(remote_path)}"
    )
    _run_transport(
        [*ssh, command],
        timeout=120,
        input_text=payload,
    )


def _remote_file_exists(
    ssh: list[str],
    remote_path: str,
) -> bool:
    result = _run_transport(
        [
            *ssh,
            (
                f"if test -s {shlex.quote(remote_path)}; "
                "then printf '1\\n'; else printf '0\\n'; fi"
            ),
        ],
        timeout=60,
    )
    return result.stdout.strip().splitlines()[-1:] == ["1"]


def _ensure_remote_worker(
    ssh: list[str],
    *,
    remote_worker: str,
) -> None:
    if _remote_file_exists(ssh, remote_worker):
        return
    _upload_text_atomic(
        ssh,
        remote_path=remote_worker,
        payload=WORKER_PATH.read_text(encoding="utf-8"),
    )
    if not _remote_file_exists(ssh, remote_worker):
        raise ReceitaRemoteBridgeError("Receita remote worker upload did not persist")


def _remote_call(mode: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    if not remote_bridge_enabled():
        raise ReceitaRemoteBridgeError("Receita remote bridge is not enabled")
    if mode not in {"discover", "lifecycle", "identity"}:
        raise ReceitaRemoteBridgeError(f"unsupported Receita remote mode: {mode}")

    config = _config_from_env()
    operation = _operation_id(mode)
    run_dir = f"{config.remote_root}/runs/{operation}"
    cache_dir = f"{config.remote_root}/cache"
    workers_dir = f"{config.remote_root}/workers"
    remote_worker = _worker_remote_path(config)
    remote_input = f"{run_dir}/input.json"
    remote_output = f"{run_dir}/output.json"

    control_path = _control_path(config)
    ssh = _ssh_base(config, control_path=control_path)

    mkdir_command = (
        "umask 077; "
        f"mkdir -p -- {shlex.quote(run_dir)} {shlex.quote(cache_dir)} "
        f"{shlex.quote(workers_dir)}; "
        f"rm -f -- {shlex.quote(remote_output)} {shlex.quote(remote_input)}"
    )
    with _bridge_phase(
        mode=mode,
        operation=operation,
        phase="prepare_remote_run_dir",
    ):
        _run_transport([*ssh, mkdir_command], timeout=60)

    with _bridge_phase(
        mode=mode,
        operation=operation,
        phase="ensure_remote_worker",
    ):
        _ensure_remote_worker(ssh, remote_worker=remote_worker)

    try:
        if payload is not None:
            serialized_payload = (
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                )
                + "\n"
            )
            with _bridge_phase(
                mode=mode,
                operation=operation,
                phase="upload_input",
                payload_bytes=len(serialized_payload.encode()),
            ):
                _upload_text_atomic(
                    ssh,
                    remote_path=remote_input,
                    payload=serialized_payload,
                )

        remote_args = [
            "python3",
            remote_worker,
            "--mode",
            mode,
            "--output",
            remote_output,
            "--cache-dir",
            cache_dir,
        ]
        if payload is not None:
            remote_args.extend(["--input", remote_input])
        remote_command = " ".join(shlex.quote(part) for part in remote_args)

        with _bridge_phase(
            mode=mode,
            operation=operation,
            phase="execute_worker",
        ):
            worker_result = _run_transport(
                [*ssh, remote_command],
                timeout=_REMOTE_DEADLINE_SECONDS,
                attempts=3,
            )

        worker_stdout = worker_result.stdout.strip()
        if worker_stdout:
            print(
                "RECEITA_REMOTE_WORKER_STDOUT "
                + json.dumps(
                    {
                        "mode": mode,
                        "operation": operation,
                        "tail": worker_stdout[-4000:],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )

        with _bridge_phase(
            mode=mode,
            operation=operation,
            phase="read_output",
        ):
            output = _run_transport(
                [*ssh, f"cat -- {shlex.quote(remote_output)}"],
                timeout=60,
                attempts=6,
            ).stdout

        try:
            response = json.loads(output)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ReceitaRemoteBridgeError(
                "Receita remote worker returned invalid JSON"
            ) from exc
        if not isinstance(response, dict):
            raise ReceitaRemoteBridgeError(
                "Receita remote worker output is not a JSON object"
            )
        return response
    finally:
        cleanup = f"rm -rf -- {shlex.quote(run_dir)}"
        try:
            with _bridge_phase(
                mode=mode,
                operation=operation,
                phase="cleanup_remote_run_dir",
            ):
                _run_transport(
                    [*ssh, cleanup],
                    timeout=60,
                    check=False,
                    attempts=3,
                )
        except ReceitaRemoteBridgeError:
            # Ephemeral run directories are namespaced by build/mode. A failed
            # cleanup must not turn a successful acquisition into stale data.
            pass


def discover_release_remote() -> ReceitaOpenDataRelease:
    payload = _remote_call("discover")
    if payload.get("artifact") != "v2_receita_remote_release":
        raise ReceitaRemoteBridgeError("unexpected Receita remote release artifact")
    period = str(payload.get("period") or "").strip()
    base_url = str(payload.get("base_url") or "").strip()
    release_url = str(payload.get("release_url") or "").strip()
    establishments = tuple(
        str(name)
        for name in payload.get("establishment_files") or []
        if str(name).strip()
    )
    if not re.fullmatch(r"20\d{2}-\d{2}", period):
        raise ReceitaRemoteBridgeError("Receita remote release has invalid period")
    if not base_url.startswith("https://arquivos.receitafederal.gov.br/"):
        raise ReceitaRemoteBridgeError("Receita remote release has invalid base URL")
    if not release_url.startswith("https://arquivos.receitafederal.gov.br/"):
        raise ReceitaRemoteBridgeError("Receita remote release has invalid release URL")
    if not establishments:
        raise ReceitaRemoteBridgeError(
            "Receita remote release has no establishment partitions"
        )
    return ReceitaOpenDataRelease(
        base_url=base_url,
        release_url=release_url,
        period=period,
        establishment_files=establishments,
        reasons_file=str(payload.get("reasons_file") or "Motivos.zip"),
        resource_metadata=tuple(payload.get("resource_metadata") or []),
    )


def refresh_lifecycle_remote(
    entities: list[dict[str, Any]],
    *,
    release: ReceitaOpenDataRelease,
) -> dict[str, Any]:
    targets = [
        {
            "cnpj": normalize_cnpj_v2(entity.get("cnpj")),
            "legal_name": str(entity.get("legal_name") or "").strip(),
        }
        for entity in entities
        if normalize_cnpj_v2(entity.get("cnpj"))
        and str(entity.get("legal_name") or "").strip()
    ]
    response = _remote_call(
        "lifecycle",
        {
            "expected_period": release.period,
            "targets": targets,
        },
    )
    if response.get("artifact") != "v2_receita_remote_lifecycle_extract":
        raise ReceitaRemoteBridgeError("unexpected Receita remote lifecycle artifact")
    source = dict(response.get("source") or {})
    if source.get("reference_period") != release.period:
        raise ReceitaRemoteBridgeError("Receita remote lifecycle period mismatch")
    if source.get("authority") != "Receita Federal do Brasil":
        raise ReceitaRemoteBridgeError("Receita remote lifecycle authority mismatch")
    if source.get("ingestion_method") != "official_nextcloud_webdav_bulk_filtered":
        raise ReceitaRemoteBridgeError("Receita remote lifecycle ingestion mismatch")

    records: list[dict[str, Any]] = []
    for raw in response.get("raw_records") or []:
        if not isinstance(raw, dict):
            raise ReceitaRemoteBridgeError("invalid Receita remote lifecycle row")
        record = normalize_receita_lifecycle_record(raw)
        record.update(
            {
                "source_period": str(raw.get("source_period") or release.period),
                "source_url": str(raw.get("source_url") or ""),
                "source_file": str(raw.get("source_file") or ""),
                "raw_status_code": str(raw.get("raw_status_code") or ""),
                "raw_reason_code": str(raw.get("raw_reason_code") or ""),
            }
        )
        records.append(record)

    target_cnpjs = {
        cnpj
        for entity in entities
        if (cnpj := normalize_cnpj_v2(entity.get("cnpj"))) is not None
    }
    resolved_cnpjs = {
        cnpj
        for row in records
        if (cnpj := normalize_cnpj_v2(row.get("cnpj"))) is not None
    }
    unresolved = sorted(target_cnpjs - resolved_cnpjs)
    meta = dict(response.get("meta") or {})
    if int(meta.get("target_count") or 0) != len(target_cnpjs):
        raise ReceitaRemoteBridgeError("Receita remote lifecycle target count mismatch")
    if set(response.get("unresolved_cnpjs") or []) != set(unresolved):
        raise ReceitaRemoteBridgeError("Receita remote lifecycle unresolved set mismatch")

    return {
        "artifact": "v2_receita_cnpj_lifecycle",
        "generated_at": str(response.get("generated_at") or ""),
        "status": "ok" if not unresolved else "partial",
        "source": source,
        "meta": {
            "target_count": len(target_cnpjs),
            "resolved_count": len(records),
            "unresolved_count": len(unresolved),
            "files_scanned": list(meta.get("files_scanned") or []),
        },
        "unresolved_cnpjs": unresolved,
        "records": sorted(records, key=lambda row: str(row.get("cnpj") or "")),
    }


def build_identity_remote(
    entities: list[dict[str, Any]],
    providers: list[str],
    *,
    release: ReceitaOpenDataRelease,
) -> dict[str, Any]:
    targets = [
        {
            "cnpj": normalize_cnpj_v2(entity.get("cnpj")),
            "entity_id": entity.get("entity_id"),
            "legal_name": entity.get("legal_name"),
        }
        for entity in entities
        if normalize_cnpj_v2(entity.get("cnpj"))
    ]
    response = _remote_call(
        "identity",
        {
            "expected_period": release.period,
            "targets": targets,
            "providers": providers,
        },
    )
    if response.get("artifact") != "v2_receita_remote_identity_extract":
        raise ReceitaRemoteBridgeError("unexpected Receita remote identity artifact")
    source = dict(response.get("source") or {})
    if source.get("reference_period") != release.period:
        raise ReceitaRemoteBridgeError("Receita remote identity period mismatch")
    if source.get("authority") != "Receita Federal do Brasil":
        raise ReceitaRemoteBridgeError("Receita remote identity authority mismatch")
    if (
        source.get("ingestion_method")
        != "official_nextcloud_webdav_bulk_filtered_identity"
    ):
        raise ReceitaRemoteBridgeError("Receita remote identity ingestion mismatch")

    from api.sources.receita_cnpj_identity import (
        SAFE_OUTSIDE_PRIMARY_CNAES,
        _build_query_indexes,
        _establishment_record,
        _finalize_provider_matches,
        _match_name_to_queries,
    )

    canonical_targets: dict[str, dict[str, Any]] = {}
    for entity in entities:
        cnpj = normalize_cnpj_v2(entity.get("cnpj"))
        if cnpj:
            canonical_targets[cnpj] = {
                "entity_id": entity.get("entity_id"),
                "project_legal_name": entity.get("legal_name"),
            }

    query_indexes = _build_query_indexes(providers)
    companies_by_base = {
        str(key): dict(value)
        for key, value in (response.get("companies_by_base") or {}).items()
        if isinstance(value, dict)
    }
    cnae_map = {
        str(key): str(value)
        for key, value in (response.get("cnae_map") or {}).items()
    }
    nature_map = {
        str(key): str(value)
        for key, value in (response.get("nature_map") or {}).items()
    }

    canonical_records: dict[str, dict[str, Any]] = {}
    provider_candidates: dict[str, list[dict[str, Any]]] = {}

    for selected in response.get("selected_establishments") or []:
        if not isinstance(selected, dict):
            raise ReceitaRemoteBridgeError("invalid Receita remote identity selection")
        row = selected.get("row")
        if not isinstance(row, list) or len(row) < 13:
            raise ReceitaRemoteBridgeError("invalid Receita remote establishment row")
        cnpj = normalize_cnpj_v2("".join(str(part).strip() for part in row[:3]))
        if not cnpj:
            raise ReceitaRemoteBridgeError("invalid CNPJ in Receita remote identity row")
        base = cnpj[:8]
        record = _establishment_record(
            [str(value or "") for value in row],
            company=companies_by_base.get(base),
            cnae_map=cnae_map,
            nature_map=nature_map,
        )
        if record is None:
            raise ReceitaRemoteBridgeError(
                f"Receita remote identity row could not be canonicalized: {cnpj}"
            )

        if cnpj in canonical_targets:
            target = canonical_targets[cnpj]
            record["entity_id"] = target.get("entity_id")
            record["project_legal_name"] = target.get("project_legal_name")
            canonical_records[cnpj] = dict(record)

        if not record.get("is_head_office"):
            continue

        company = companies_by_base.get(base) or {}
        local_matches = [
            *_match_name_to_queries(
                str(company.get("legal_name_receita") or ""),
                query_indexes,
                prefix="legal_name",
            ),
            *_match_name_to_queries(
                str(record.get("trade_name") or ""),
                query_indexes,
                prefix="trade_name",
            ),
        ]
        remote_matches = {
            (str(item[0]), str(item[1]))
            for item in selected.get("provider_matches") or []
            if isinstance(item, list) and len(item) == 2
        }
        if set(local_matches) != remote_matches:
            raise ReceitaRemoteBridgeError(
                f"Receita remote/local provider matching diverged for {cnpj}"
            )
        for provider_key, method in local_matches:
            candidate = dict(record)
            candidate["match_method"] = method
            provider_candidates.setdefault(provider_key, []).append(candidate)

    provider_matches = _finalize_provider_matches(query_indexes, provider_candidates)
    unique = sum(
        row["candidate_state"] == "unique_candidate" for row in provider_matches
    )
    ambiguous = sum(
        row["candidate_state"] == "ambiguous_candidates" for row in provider_matches
    )
    none = sum(row["candidate_state"] == "no_candidate" for row in provider_matches)

    return {
        "artifact": "v2_receita_cnpj_identity",
        "generated_at": str(response.get("generated_at") or ""),
        "status": "experimental",
        "source": source,
        "identity_semantics": {
            "receita_role": "identify_legal_entity_trade_name_activity_and_legal_nature",
            "susep_role": "authoritative_for_current_insurer_licensing_and_157_membership",
            "cnae_role": "corroborating_or_safe_exclusion_only_never_insurer_admission",
            "fuzzy_matching": "not_used",
            "sales_channel_rule": (
                "never_transfer_complaints_to_a_carrier_from_distribution_relationship_alone"
            ),
        },
        "meta": {
            "target_cnpjs": len(canonical_targets),
            "target_cnpjs_enriched": len(canonical_records),
            "provider_queries": len(query_indexes["display"]),
            "provider_unique_candidates": unique,
            "provider_ambiguous_candidates": ambiguous,
            "provider_no_candidate": none,
            "company_files_scanned": list(
                response.get("company_files_scanned") or []
            ),
            "establishment_files_scanned": list(
                response.get("establishment_files_scanned") or []
            ),
        },
        "safe_outside_primary_cnaes": SAFE_OUTSIDE_PRIMARY_CNAES,
        "canonical_records": [
            canonical_records[key] for key in sorted(canonical_records)
        ],
        "provider_matches": provider_matches,
    }
