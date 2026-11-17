"""A09.6 read-only reference recovery and exact-input semantic homologation.

No HTTP, cache mutations, scoring, publication or Full Generation. The
November 2026 proof is pinned to one GitHub Actions run and archive SHA-256.
Do not mistake a metadata fingerprint for a remote ZIP content checksum.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import zipfile
from pathlib import Path
from typing import Any

RUN_ID = 43941197377
RUN_SHA = "0b7c40bb3d9463e7d71680b49e93c90e3c54f46d"
RUN_BUILD = f"v2-gate4-full-{RUN_ID}-a1"
ARTIFACT_ID = 14277894275
ARTIFACT_NAME = f"v2-gate4-full-generation-{RUN_ID}-a1"
ARCHIVE_SHA = "6a8c1e04ef697779129724fb5c5e857d28d3d72bd66036f0f472ca9732c9a068"
PERIOD = "2026-11"
SCHEMA = "a096-receita-november-reference-v1"
REPORT_SCHEMA = "a096-homologation-report-v1"
SHA = re.compile(r"[0-9a-f]{64}\Z")
SOURCES = {
    "receita_cnpj_lifecycle": {
        "lineage": "source_lineage.json",
        "snapshot_path": "data/derived/v2/source/receita_cnpj_lifecycle.json",
        "cache_group": "source",
        "cache_name": "receita_cnpj_lifecycle",
        "artifact": "v2_receita_cnpj_lifecycle",
    },
    "receita_consumer_identity": {
        "lineage": "conduct_source_lineage.json",
        "snapshot_path": "data/derived/v2/receita_consumer_gov_identity.json",
        "cache_group": "conduct",
        "cache_name": "receita_consumer_identity",
        "artifact": "v2_receita_cnpj_identity",
    },
}
PROOF_ENTRIES = (
    "gate4_full_generation_summary.json",
    "source_lineage.json",
    "conduct_source_lineage.json",
    "receita_identity_provenance_a09.json",
    "receita_a09_events.jsonl",
    "public/distribution_manifest.json",
)
VOLATILE_PATHS = frozenset((
    ("generated_at",),
    ("source", "retrieved_at"),
    ("source", "release_checked_at"),
))
ZIP_FAMILIES = ("Empresas", "Estabelecimentos")


class ProofError(ValueError):
    """Invalid evidence must not be accepted as proof of equivalence."""


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(raw: bytes) -> dict[str, Any]:
    obj = json.loads(raw)
    if not isinstance(obj, dict):
        raise ProofError("expected_json_object")
    return obj


def _write_json(path: Path, doc: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, sort_keys=True, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8")


def _get_source(lineage: dict[str, Any], source_id: str) -> dict[str, Any]:
    rows = [x for x in lineage.get("sources", [])
            if isinstance(x, dict) and x.get("source_id") == source_id]
    if len(rows) != 1:
        raise ProofError("missing_or_duplicate_source_lineage")
    row = rows[0]
    if (row.get("state") != "fresh" or row.get("freshness_method") != "current_fetch"
            or row.get("build_id") != RUN_BUILD
            or not SHA.fullmatch(str(row.get("sha256") or ""))):
        raise ProofError("unverified_source_lineage")
    return row


def _validated_run_build(lineage: dict[str, Any]) -> None:
    build = lineage.get("build") or {}
    if (build.get("build_id") != RUN_BUILD
            or str(build.get("workflow_run_id")) != str(RUN_ID)
            or build.get("source_head_sha") != RUN_SHA
            or build.get("workflow_run_attempt") != 1):
        raise ProofError("lineage_run_mismatch")


def reference_from_archive(proof_zip: Path) -> dict[str, Any]:
    """Build a compact manifest from the EXACT validated November proof ZIP."""
    if _hash_file(proof_zip) != ARCHIVE_SHA:
        raise ProofError("unexpected_november_archive_sha256")
    with zipfile.ZipFile(proof_zip) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or any(
            name.startswith("/") or ".." in Path(name).parts for name in names
        ):
            raise ProofError("archive_path_or_duplicate")
        if any(name not in names for name in PROOF_ENTRIES):
            raise ProofError("required_proof_entry_missing")
        entries = {name: archive.read(name) for name in PROOF_ENTRIES}
        summary, regulatory, conduct, provenance, distribution = (
            _json_bytes(entries[name]) for name in (
                "gate4_full_generation_summary.json",
                "source_lineage.json", "conduct_source_lineage.json",
                "receita_identity_provenance_a09.json",
                "public/distribution_manifest.json",
            )
        )
        _validated_run_build(regulatory)
        _validated_run_build(conduct)
        if (summary.get("build_id") != RUN_BUILD
                or summary.get("source_head_sha") != RUN_SHA
                or summary.get("stage_count") != 25):
            raise ProofError("unexpected_generation_summary")
        public = distribution.get("public_package") or {}
        files = public.get("files")
        if (not isinstance(files, list) or len(files) != 816
                or public.get("files_count") != len(files)
                or distribution.get("build", {}).get("build_id") != RUN_BUILD
                or summary.get("package_sha256") != public.get("package_sha256")):
            raise ProofError("public_package_manifest_mismatch")
        visited: set[str] = set()
        for item in files:
            if not isinstance(item, dict):
                raise ProofError("invalid_public_file_record")
            name = item.get("path")
            if (not isinstance(name, str) or not name
                    or name.startswith("/") or ".." in Path(name).parts
                    or name in visited):
                raise ProofError("invalid_or_duplicate_public_file_name")
            visited.add(name)
            full = "public/" + name
            if full not in names:
                raise ProofError("missing_public_package_file")
            blob = archive.read(full)
            if len(blob) != item.get("bytes") or _sha(blob) != item.get("sha256"):
                raise ProofError("public_file_digest_mismatch")
        if (set(x for x in names if x.startswith("public/"))
                != {"public/distribution_manifest.json"} | {"public/" + x for x in visited}):
            raise ProofError("unlisted_public_file_in_proof")

        lifecycle = _get_source(regulatory, "receita_cnpj_lifecycle")
        identity = _get_source(conduct, "receita_consumer_identity")
        if (lifecycle.get("snapshot_path") != SOURCES["receita_cnpj_lifecycle"]["snapshot_path"]
                or identity.get("snapshot_path") != SOURCES["receita_consumer_identity"]["snapshot_path"]):
            raise ProofError("snapshot_lineage_path_mismatch")
        if (provenance.get("build_id") != RUN_BUILD
                or provenance.get("source_id") != "receita_consumer_identity"
                or provenance.get("reference_period") != PERIOD
                or provenance.get("snapshot_sha256") != identity["sha256"]
                or provenance.get("source_state") != "fresh"
                or provenance.get("freshness_method") != "current_fetch"
                or provenance.get("marker_consistency") != "matched"
                or provenance.get("snapshot_contract_consistency") != "matched"
                or not SHA.fullmatch(str(provenance.get("release_fingerprint") or ""))
                or not SHA.fullmatch(str(provenance.get("target_universe_hash") or ""))
                or not SHA.fullmatch(str(provenance.get("unresolved_provider_hash") or ""))):
            raise ProofError("identity_provenance_invalid")
        telemetry = []
        for line in entries["receita_a09_events.jsonl"].decode().splitlines():
            row = _json_bytes(line.encode())
            telemetry.append(row)
        for family in ("empresas", "estabelecimentos"):
            downloads = [
                x for x in telemetry
                if x.get("event") == "zip_download"
                and x.get("category") == family and x.get("state") == "success"
            ]
            if sorted(x.get("file_index") for x in downloads) != list(range(10)):
                raise ProofError("official_partition_downloads_incomplete")
        for state in ("stored", "verified"):
            reused = [x for x in telemetry if x.get("event") == "zip_reuse"
                      and x.get("category") == "estabelecimentos" and x.get("state") == state]
            if sorted(x.get("file_index") for x in reused) != list(range(10)):
                raise ProofError("a094_run_reuse_incomplete")
        disc = [x for x in telemetry if x.get("event") == "release_discovery"]
        if (len(disc) != 1 or disc[0].get("reference_period") != PERIOD
                or disc[0].get("release_fingerprint") != provenance["release_fingerprint"]):
            raise ProofError("release_telemetry_mismatch")

    return {
        "schema": SCHEMA,
        "reference_run": {
            "id": RUN_ID, "attempt": 1, "build_id": RUN_BUILD,
            "source_head_sha": RUN_SHA, "period": PERIOD,
        },
        "proof_artifact": {
            "id": ARTIFACT_ID, "name": ARTIFACT_NAME,
            "archive_sha256": ARCHIVE_SHA,
            "entry_sha256": {name: _sha(entries[name]) for name in PROOF_ENTRIES},
        },
        "public_package": {
            "file_count": len(files), "package_sha256": summary["package_sha256"],
            "files_verified_by_sha256": len(visited),
        },
        "source_snapshots": {
            source: {
                "sha256": row["sha256"],
                "snapshot_path": SOURCES[source]["snapshot_path"],
                "cache_group": SOURCES[source]["cache_group"],
                "cache_name": SOURCES[source]["cache_name"],
                "artifact": SOURCES[source]["artifact"],
                "state": row["state"], "freshness_method": row["freshness_method"],
            }
            for source, row in (
                ("receita_cnpj_lifecycle", lifecycle),
                ("receita_consumer_identity", identity),
            )
        },
        "identity_contract": {
            "release_fingerprint": provenance["release_fingerprint"],
            "cache_contract_version": provenance["cache_contract_version"],
            "target_universe_hash": provenance["target_universe_hash"],
            "unresolved_provider_hash": provenance["unresolved_provider_hash"],
            "marker_consistency": provenance["marker_consistency"],
        },
        "zip_evidence_scope": "observed_downloads_and_a094_verified_not_retained_content_hashes",
        "raw_national_zip_bytes_in_proof": False,
        "filtered_snapshot_bytes_in_proof": False,
    }


def _load_reference(path: Path) -> dict[str, Any]:
    data = _json_bytes(path.read_bytes())
    if (data.get("schema") != SCHEMA
            or data.get("reference_run", {}).get("id") != RUN_ID
            or data.get("reference_run", {}).get("source_head_sha") != RUN_SHA
            or data.get("proof_artifact", {}).get("archive_sha256") != ARCHIVE_SHA):
        raise ProofError("reference_not_pinned_to_november")
    return data


def _read_cached(
    reference: dict[str, Any], source: str, root: Path,
) -> tuple[dict[str, Any] | None, str]:
    """Verify actual bytes, cache receipt and declared source contracts."""
    entry = reference["source_snapshots"][source]
    base = root / entry["cache_group"]
    filename = entry["cache_name"]
    content = base / (filename + ".json")
    receipt = base / (filename + ".meta.json")
    if not content.is_file() or not receipt.is_file():
        return None, "missing_cache_bytes_or_receipt"
    try:
        metadata = _json_bytes(receipt.read_bytes())
        data = _json_bytes(content.read_bytes())
    except (ValueError, OSError, json.JSONDecodeError):
        return None, "unreadable_cache_or_receipt"
    actual_sha = _hash_file(content)
    if (actual_sha != entry["sha256"] or metadata.get("sha256") != actual_sha):
        return None, "cache_sha256_mismatch"
    if (metadata.get("source_id") != source or not metadata.get("fetched_at")
            or not isinstance(metadata.get("source_url"), str)):
        return None, "invalid_cache_contract"
    if data.get("artifact") != entry["artifact"]:
        return None, "unexpected_snapshot_artifact"
    payload_source = data.get("source") or {}
    if payload_source.get("reference_period") != PERIOD:
        return None, "snapshot_period_mismatch"
    if source == "receita_consumer_identity":
        key = (data.get("meta") or {}).get("gate4_cache_key") or {}
        contract = reference["identity_contract"]
        if (payload_source.get("release_fingerprint") != contract["release_fingerprint"]
                or key.get("reference_period") != PERIOD
                or key.get("version") != contract["cache_contract_version"]
                or key.get("target_universe_hash") != contract["target_universe_hash"]
                or key.get("unresolved_provider_hash") != contract["unresolved_provider_hash"]):
            return None, "identity_contract_mismatch"
        if not isinstance(data.get("canonical_records"), list) or not isinstance(
            data.get("provider_matches"), list
        ):
            return None, "identity_semantic_fields_missing"
    elif not isinstance(data.get("records"), list) or not data["records"]:
        return None, "lifecycle_semantic_fields_missing"
    return data, "verified"


def _canonical(item: Any, path: tuple[str, ...] = ()) -> Any:
    """Only THREE precisely named volatile paths; do not drop other fields."""
    if isinstance(item, dict):
        return {k: _canonical(v, (*path, k)) for k, v in sorted(item.items())
                if (*path, k) not in VOLATILE_PATHS}
    if isinstance(item, list):
        return [_canonical(v, (*path, "[]")) for v in item]
    return item


def _semantic_sha(data: dict[str, Any]) -> str:
    return _sha(json.dumps(_canonical(data), ensure_ascii=False, sort_keys=True,
                           separators=(",", ":")).encode("utf-8"))


def _diff_paths(a: Any, b: Any, label: str, limit: int = 12) -> list[str]:
    """Output only JSON field/index locations, never private record values."""
    found: list[str] = []
    def visit(left: Any, right: Any, location: str) -> None:
        if len(found) >= limit:
            return
        if type(left) is not type(right):
            found.append(location); return
        if isinstance(left, dict):
            for key in sorted(set(left) | set(right)):
                if key not in left or key not in right:
                    found.append(location + "." + str(key))
                else:
                    visit(left[key], right[key], location + "." + str(key))
                if len(found) >= limit:
                    break
        elif isinstance(left, list):
            if len(left) != len(right):
                found.append(location + ".length")
            for i, (x, y) in enumerate(zip(left, right)):
                visit(x, y, location + "[" + str(i) + "]")
                if len(found) >= limit:
                    break
        elif left != right:
            found.append(location)
    visit(_canonical(a), _canonical(b), label)
    return found


def audit(
    reference_file: Path, cache_root: Path, *,
    proof_zip: Path | None = None,
    candidate_lifecycle: Path | None = None,
    candidate_identity: Path | None = None,
    local_zip_proof: Path | None = None,
) -> dict[str, Any]:
    """Return exactly equivalent, different or insufficient_evidence."""
    report: dict[str, Any] = {
        "schema": REPORT_SCHEMA, "status": "insufficient_evidence",
        "reason": "not_all_evidence_available",
        "source_checks": {}, "semantic_checks": {},
        "production_activation_authorized": False,
    }
    try:
        reference = _load_reference(reference_file)
        if proof_zip is not None:
            if reference_from_archive(proof_zip) != reference:
                report.update(status="different", reason="reference_archive_mismatch")
                return report
            report["november_archive_verified"] = True
        else:
            report["november_archive_verified"] = False
        baseline = {}
        for source in SOURCES:
            snapshot, state = _read_cached(reference, source, cache_root)
            report["source_checks"][source] = state
            if state == "verified":
                baseline[source] = snapshot
        if len(baseline) != len(SOURCES):
            report["reason"] = "required_snapshot_cache_missing_or_invalid"
            return report
        report["reference_snapshots_verified"] = True
        report["reference_semantic_sha256"] = {
            source: _semantic_sha(data) for source, data in baseline.items()
        }
        if candidate_lifecycle is None or candidate_identity is None or local_zip_proof is None:
            report["reason"] = "candidate_or_same_input_zip_evidence_missing"
            return report
        candidates = {
            "receita_cnpj_lifecycle": _json_bytes(candidate_lifecycle.read_bytes()),
            "receita_consumer_identity": _json_bytes(candidate_identity.read_bytes()),
        }
        # November ZIP hashes were NOT retained. Never claim this candidate
        # shares November's exact bytes just because its period matches.
        proof = _json_bytes(local_zip_proof.read_bytes())
        if (proof.get("schema") != "a096-local-replay-input-v1"
                or proof.get("reference_period") != PERIOD
                or proof.get("source_head_sha") != RUN_SHA
                or proof.get("zip_files_verified") != 20
                or proof.get("input_mode") != "same_exact_local_zip_bytes"
                or proof.get("serial_snapshot_sha256") != {
                    source: _hash_file(path) for source, path in (
                        ("receita_cnpj_lifecycle", cache_root / "source/receita_cnpj_lifecycle.json"),
                        ("receita_consumer_identity", cache_root / "conduct/receita_consumer_identity.json"),
                    )
                }):
            report["reason"] = "same_input_replay_proof_missing_or_invalid"
            return report
        # This is a NOVEMBER comparison. Any new competence must have its own
        # reference derived from the same captured ZIPs, not these old hashes.
        for source, candidate in candidates.items():
            base = baseline[source]
            if (candidate.get("artifact") != base.get("artifact")
                    or (candidate.get("source") or {}).get("reference_period") != PERIOD
                    or (candidate.get("source") or {}).get("release_fingerprint")
                    != (base.get("source") or {}).get("release_fingerprint")):
                report["reason"] = "candidate_period_or_contract_incompatible"
                return report
        differences = {}
        for source, candidate in candidates.items():
            base = baseline[source]
            diffs = _diff_paths(base, candidate, source)
            report["semantic_checks"][source] = {
                "reference_sha256": _semantic_sha(base),
                "candidate_sha256": _semantic_sha(candidate),
                "diff_paths_only": diffs,
            }
            if diffs:
                differences[source] = diffs
        report["status"] = "different" if differences else "equivalent"
        report["reason"] = "semantic_differences" if differences else "same_input_semantic_match"
    except (OSError, KeyError, TypeError, ValueError, zipfile.BadZipFile) as exc:
        # Deliberately do not print exception messages which might include
        # private CNPJ values or signed source URLs.
        report["reason"] = type(exc).__name__ if not isinstance(exc, ProofError) else str(exc)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    generate = sub.add_parser("reference")
    generate.add_argument("--proof-zip", type=Path, required=True)
    generate.add_argument("--output", type=Path, required=True)
    verify = sub.add_parser("audit")
    verify.add_argument("--reference", type=Path, required=True)
    verify.add_argument("--cache-root", type=Path, required=True)
    verify.add_argument("--proof-zip", type=Path)
    verify.add_argument("--candidate-lifecycle", type=Path)
    verify.add_argument("--candidate-identity", type=Path)
    verify.add_argument("--local-zip-proof", type=Path)
    verify.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "reference":
        try:
            _write_json(args.output, reference_from_archive(args.proof_zip))
        except (ProofError, OSError, KeyError, ValueError, zipfile.BadZipFile) as exc:
            print(type(exc).__name__ + ": reference_not_generated", file=sys.stderr)
            raise SystemExit(1) from exc
        print("november_reference_generated_verified")
    else:
        result = audit(
            args.reference, args.cache_root, proof_zip=args.proof_zip,
            candidate_lifecycle=args.candidate_lifecycle,
            candidate_identity=args.candidate_identity,
            local_zip_proof=args.local_zip_proof,
        )
        _write_json(args.output, result)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        # Never approve on an empty pair, incomplete evidence or mismatched results.
        if result["status"] != "equivalent":
            raise SystemExit(1)


if __name__ == "__main__":
    main()
