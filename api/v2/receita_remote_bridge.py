from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj import normalize_receita_lifecycle_record
from api.sources.receita_cnpj_bulk import ReceitaOpenDataRelease, release_fingerprint
from api.utils.identifiers import normalize_cnpj_v2

WORKER_PATH = Path("ops/hostgator/receita_remote_worker.py")
REMOTE_ROOT_DEFAULT = ".sanida-v2-receita-bridge"
_SAFE_HOST_RE = re.compile(r"^[A-Za-z0-9.-]+$")
_SAFE_USER_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_SAFE_REMOTE_ROOT_RE = re.compile(r"^[A-Za-z0-9._/-]+$")
_SAFE_BUILD_RE = re.compile(r"[^A-Za-z0-9._-]+")


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


def _ssh_base(config: ReceitaRemoteConfig) -> list[str]:
    return [
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
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=6",
        f"{config.user}@{config.host}",
    ]


def _scp_base(config: ReceitaRemoteConfig) -> list[str]:
    return [
        "scp",
        "-q",
        "-i",
        str(config.key_file),
        "-P",
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
    ]


def _run(
    command: list[str],
    *,
    timeout: int = 18_000,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            check=check,
            capture_output=True,
            text=True,
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


def _operation_id(mode: str) -> str:
    build_id = os.getenv("V2_BUILD_ID", "").strip() or "manual"
    value = _SAFE_BUILD_RE.sub("-", f"{build_id}-{mode}").strip("-")
    if not value:
        raise ReceitaRemoteBridgeError("cannot derive Receita remote operation id")
    return value[:160]


def _remote_call(mode: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    if not remote_bridge_enabled():
        raise ReceitaRemoteBridgeError("Receita remote bridge is not enabled")
    if mode not in {"discover", "lifecycle", "identity"}:
        raise ReceitaRemoteBridgeError(f"unsupported Receita remote mode: {mode}")

    config = _config_from_env()
    operation = _operation_id(mode)
    run_dir = f"{config.remote_root}/runs/{operation}"
    cache_dir = f"{config.remote_root}/cache"
    remote_worker = f"{run_dir}/worker.py"
    remote_input = f"{run_dir}/input.json"
    remote_output = f"{run_dir}/output.json"
    remote = f"{config.user}@{config.host}"

    ssh = _ssh_base(config)
    scp = _scp_base(config)

    mkdir_command = (
        f"umask 077; mkdir -p -- {shlex.quote(run_dir)} {shlex.quote(cache_dir)}; "
        f"rm -f -- {shlex.quote(remote_output)} {shlex.quote(remote_input)}"
    )
    _run([*ssh, mkdir_command], timeout=120)

    try:
        _run([*scp, str(WORKER_PATH), f"{remote}:{remote_worker}"], timeout=120)
        with tempfile.TemporaryDirectory(prefix="v2-receita-bridge-") as temp_dir:
            root = Path(temp_dir)
            if payload is not None:
                input_path = root / "input.json"
                input_path.write_text(
                    json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                _run(
                    [*scp, str(input_path), f"{remote}:{remote_input}"],
                    timeout=120,
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

            remote_exit = f"{run_dir}/exit_code"
            remote_stdout = f"{run_dir}/stdout.log"
            remote_stderr = f"{run_dir}/stderr.log"
            remote_args.extend(
                [
                    "--daemonize",
                    "--exit-file",
                    remote_exit,
                    "--stdout-log",
                    remote_stdout,
                    "--stderr-log",
                    remote_stderr,
                ]
            )
            remote_command = " ".join(shlex.quote(part) for part in remote_args)
            _run([*ssh, remote_command], timeout=120)

            deadline_seconds = 18_000
            poll_interval_seconds = 20
            waited = 0
            exit_code: int | None = None
            consecutive_probe_failures = 0
            while waited <= deadline_seconds:
                try:
                    probe = _run(
                        [
                            *ssh,
                            (
                                f"if test -f {shlex.quote(remote_exit)}; then "
                                f"cat {shlex.quote(remote_exit)}; "
                                "else printf 'RUNNING\\n'; fi"
                            ),
                        ],
                        timeout=60,
                    )
                    consecutive_probe_failures = 0
                except ReceitaRemoteBridgeError:
                    consecutive_probe_failures += 1
                    if consecutive_probe_failures >= 5:
                        raise
                    time.sleep(poll_interval_seconds)
                    waited += poll_interval_seconds
                    continue

                status = probe.stdout.strip()
                if status == "RUNNING" or not status:
                    time.sleep(poll_interval_seconds)
                    waited += poll_interval_seconds
                    continue
                try:
                    exit_code = int(status.splitlines()[-1])
                except ValueError as exc:
                    raise ReceitaRemoteBridgeError(
                        f"invalid Receita remote worker status: {status!r}"
                    ) from exc
                break

            if exit_code is None:
                raise ReceitaRemoteBridgeError(
                    "Receita remote worker exceeded the 18000s bridge deadline"
                )
            if exit_code != 0:
                detail = _run(
                    [
                        *ssh,
                        (
                            f"tail -c 6000 {shlex.quote(remote_stderr)} "
                            "2>/dev/null || true"
                        ),
                    ],
                    timeout=120,
                    check=False,
                ).stdout.strip()
                raise ReceitaRemoteBridgeError(
                    "Receita remote worker failed"
                    + (f": {detail}" if detail else f" with exit={exit_code}")
                )

            output_path = root / "output.json"
            _run(
                [*scp, f"{remote}:{remote_output}", str(output_path)],
                timeout=300,
            )
            try:
                response = json.loads(output_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
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
        _run([*ssh, cleanup], timeout=120, check=False)


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
    release = ReceitaOpenDataRelease(
        base_url=base_url,
        release_url=release_url,
        period=period,
        establishment_files=establishments,
        reasons_file=str(payload.get("reasons_file") or "Motivos.zip"),
        resource_metadata=tuple(payload.get("resource_metadata") or []),
    )
    expected_fingerprint = str(payload.get("release_fingerprint") or "").strip()
    actual_fingerprint = release_fingerprint(release)
    if expected_fingerprint and expected_fingerprint != actual_fingerprint:
        raise ReceitaRemoteBridgeError(
            "Receita remote release fingerprint mismatch"
        )
    return release


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
