from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import normalize_name_key

DEFAULT_LIFECYCLE_INPUT = Path(
    "data/derived/v2/entity_lifecycle_relationship_inventory.json"
)
DEFAULT_CONSUMER_IDENTITY_INPUT = Path(
    "data/derived/v2/consumer_gov_identity_experiment.json"
)
DEFAULT_VERIFIED_RELATIONSHIPS = Path(
    "data/reference/v2/verified_relationships.json"
)
DEFAULT_CONDUCT_RELATIONSHIPS = Path(
    "data/reference/v2/conduct_subject_relationships.json"
)
DEFAULT_SANDBOX_RELATIONSHIPS = Path(
    "data/reference/v2/sandbox_brand_relationships.json"
)
DEFAULT_OUTPUT = Path("data/derived/v2/relationship_watchdog.json")

HISTORICAL_TARGET_STATES = {
    "historical_closed_entity",
    "historical_incorporated_entity",
}


class RelationshipWatchdogError(RuntimeError):
    """Raised when an asserted verified relationship has drifted."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _stable_id(kind: str, payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        {"kind": kind, **payload},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    digest = hashlib.sha256(canonical).hexdigest()[:16]
    return f"relationship-watchdog:{kind}:{digest}"


def _candidate(
    kind: str,
    *,
    severity: str,
    blocking: bool,
    subject: dict[str, Any],
    reason: str,
    signals: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fingerprint = {
        "subject": subject,
        "reason": reason,
        "signals": signals or {},
    }
    return {
        "candidate_id": _stable_id(kind, fingerprint),
        "candidate_type": kind,
        "severity": severity,
        "blocking": blocking,
        "assertion_effect": "none",
        "review_state": "registry_drift" if blocking else "review_required",
        "subject": subject,
        "reason": reason,
        "signals": signals or {},
    }


def _observation(
    kind: str,
    *,
    subject: dict[str, Any],
    relationship: dict[str, Any],
    source_authority: str,
) -> dict[str, Any]:
    fingerprint = {
        "subject": subject,
        "relationship": relationship,
        "source_authority": source_authority,
    }
    return {
        "observation_id": _stable_id(kind, fingerprint),
        "observation_type": kind,
        "assertion_state": "verified_or_official_source",
        "candidate_effect": "none",
        "subject": subject,
        "relationship": relationship,
        "source_authority": source_authority,
    }


def _entity_indexes(
    lifecycle_payload: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_id: dict[str, dict[str, Any]] = {}
    by_cnpj: dict[str, dict[str, Any]] = {}
    for entity in lifecycle_payload.get("entities") or []:
        entity_id = str(entity.get("entity_id") or "").strip()
        if entity_id:
            by_id[entity_id] = entity
        cnpj = normalize_cnpj_v2(entity.get("cnpj"))
        if cnpj:
            by_cnpj[cnpj] = entity
    return by_id, by_cnpj


def _scan_closed_without_successor(
    entities: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for entity in entities:
        context = entity.get("query_context") or {}
        if context.get("entity_state") != "historical_closed_entity":
            continue
        candidates.append(
            _candidate(
                "closed_entity_without_verified_successor",
                severity="high",
                blocking=False,
                subject={
                    "entity_id": entity.get("entity_id"),
                    "cnpj": normalize_cnpj_v2(entity.get("cnpj")),
                    "legal_name": entity.get("legal_name"),
                },
                reason=(
                    "Receita identifies a closed legal entity, but no verified corporate "
                    "successor is materialized. Documentary review is requested without "
                    "inferring a successor from names or economic group."
                ),
                signals={
                    "legal_lifecycle": entity.get("legal_lifecycle") or {},
                    "query_context": context,
                },
            )
        )
    return candidates


def _scan_materialized_brand_relationships(
    lifecycle_payload: dict[str, Any],
    entity_by_id: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []

    for brand in lifecycle_payload.get("brands") or []:
        subject = {
            "brand_id": brand.get("brand_id"),
            "name": brand.get("name"),
            "aliases": list(brand.get("aliases") or []),
        }
        for relation in brand.get("relationships") or []:
            target_id = str(relation.get("target_entity_id") or "").strip()
            target = entity_by_id.get(target_id)
            relation_type = str(relation.get("relationship_type") or "").strip()
            status = str(relation.get("status") or "").strip() or None
            target_cnpj = normalize_cnpj_v2(
                relation.get("target_cnpj") or (target or {}).get("cnpj")
            )
            relationship = {
                "relationship_type": relation_type,
                "status": status,
                "target_entity_id": target_id or None,
                "target_cnpj": target_cnpj,
                "target_legal_name": (target or {}).get("legal_name"),
            }
            observations.append(
                _observation(
                    "verified_brand_relationship",
                    subject=subject,
                    relationship=relationship,
                    source_authority=str(
                        ((relation.get("evidence") or {}).get("authority"))
                        or "verified_relationships.json"
                    ),
                )
            )

            if target is None:
                candidates.append(
                    _candidate(
                        "verified_brand_target_missing",
                        severity="critical",
                        blocking=True,
                        subject=subject,
                        reason=(
                            "A verified brand/risk-carrier relationship points to an entity "
                            "that is absent from the current lifecycle inventory."
                        ),
                        signals=relationship,
                    )
                )
                continue

            target_state = str(
                ((target.get("query_context") or {}).get("entity_state")) or ""
            )
            if status == "current" and target_state in HISTORICAL_TARGET_STATES:
                candidates.append(
                    _candidate(
                        "verified_brand_current_target_is_historical",
                        severity="critical",
                        blocking=True,
                        subject=subject,
                        reason=(
                            "A relationship asserted as current points to a historical/closed "
                            "entity. The verified registry must be reviewed before downstream "
                            "Conduct or public profiles are rebuilt."
                        ),
                        signals={
                            **relationship,
                            "target_entity_state": target_state,
                        },
                    )
                )
    return candidates, observations


def _scan_alias_collisions(
    lifecycle_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    aliases: dict[str, list[dict[str, Any]]] = {}
    for brand in lifecycle_payload.get("brands") or []:
        brand_id = str(brand.get("brand_id") or "").strip()
        target_ids = sorted(
            {
                str(relation.get("target_entity_id"))
                for relation in (brand.get("relationships") or [])
                if relation.get("target_entity_id")
            }
        )
        for label in [brand.get("name"), *(brand.get("aliases") or [])]:
            key = normalize_name_key(label)
            if not key:
                continue
            aliases.setdefault(key, []).append(
                {
                    "brand_id": brand_id,
                    "label": label,
                    "target_entity_ids": target_ids,
                }
            )

    candidates: list[dict[str, Any]] = []
    for key, rows in aliases.items():
        brand_ids = {str(row.get("brand_id") or "") for row in rows}
        target_sets = {
            tuple(row.get("target_entity_ids") or [])
            for row in rows
        }
        if len(brand_ids) <= 1 or len(target_sets) <= 1:
            continue
        candidates.append(
            _candidate(
                "verified_brand_alias_collision",
                severity="critical",
                blocking=True,
                subject={"normalized_alias": key},
                reason=(
                    "The same normalized verified alias points to multiple brands with "
                    "different entity targets. Automatic provider resolution would be unsafe."
                ),
                signals={"collisions": rows},
            )
        )
    return candidates


def _scan_corporate_relationships(
    lifecycle_payload: dict[str, Any],
    entity_by_id: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []

    for entity in lifecycle_payload.get("entities") or []:
        source_context = entity.get("query_context") or {}
        for relation in entity.get("relationships") or []:
            if relation.get("relationship_type") != "incorporated_into":
                continue
            target_id = str(relation.get("target_entity_id") or "").strip()
            target = entity_by_id.get(target_id)
            relationship = {
                "relationship_type": "incorporated_into",
                "target_entity_id": target_id or None,
                "target_cnpj": normalize_cnpj_v2(
                    relation.get("target_cnpj") or (target or {}).get("cnpj")
                ),
                "effective_date": relation.get("effective_date"),
            }
            subject = {
                "entity_id": entity.get("entity_id"),
                "cnpj": normalize_cnpj_v2(entity.get("cnpj")),
                "legal_name": entity.get("legal_name"),
            }
            observations.append(
                _observation(
                    "verified_corporate_relationship",
                    subject=subject,
                    relationship=relationship,
                    source_authority=str(
                        ((relation.get("evidence") or {}).get("authority"))
                        or "verified_relationships.json"
                    ),
                )
            )
            if target is None:
                candidates.append(
                    _candidate(
                        "verified_corporate_target_missing",
                        severity="critical",
                        blocking=True,
                        subject=subject,
                        reason=(
                            "A verified incorporation points to an entity absent from the "
                            "current lifecycle inventory."
                        ),
                        signals=relationship,
                    )
                )
            if source_context.get("entity_state") == "current_ordinary_insurer":
                candidates.append(
                    _candidate(
                        "verified_incorporation_source_still_current",
                        severity="critical",
                        blocking=True,
                        subject=subject,
                        reason=(
                            "An entity asserted as incorporated_into is simultaneously classified "
                            "as a current ordinary insurer. This is a registry/lifecycle conflict."
                        ),
                        signals={
                            **relationship,
                            "source_entity_state": source_context.get("entity_state"),
                        },
                    )
                )
    return candidates, observations


def _group_observations(
    lifecycle_payload: dict[str, Any],
    entity_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for group in lifecycle_payload.get("groups") or []:
        member_ids = [
            str(item)
            for item in (group.get("member_entity_ids") or [])
            if str(item).strip()
        ]
        if not member_ids:
            continue
        members = []
        for entity_id in member_ids:
            entity = entity_by_id.get(entity_id) or {}
            members.append(
                {
                    "entity_id": entity_id,
                    "cnpj": normalize_cnpj_v2(entity.get("cnpj")),
                    "legal_name": entity.get("legal_name"),
                    "entity_state": (entity.get("query_context") or {}).get(
                        "entity_state"
                    ),
                }
            )
        observations.append(
            _observation(
                "official_economic_group_membership",
                subject={
                    "group_id": group.get("group_id"),
                    "group_code": group.get("group_code"),
                    "group_name": group.get("group_name"),
                    "observed_period": group.get("observed_period"),
                },
                relationship={
                    "relationship_type": "member_of_group",
                    "members": members,
                },
                source_authority="SUSEP SES / Ses_grupos_economicos.csv",
            )
        )
    return observations


def _canonical_brand_targets(
    lifecycle_payload: dict[str, Any],
) -> dict[str, set[str]]:
    entity_by_id, _ = _entity_indexes(lifecycle_payload)
    targets: dict[str, set[str]] = {}
    for brand in lifecycle_payload.get("brands") or []:
        brand_id = str(brand.get("brand_id") or "").strip()
        if not brand_id:
            continue
        current_targets: set[str] = set()
        for relation in brand.get("relationships") or []:
            if relation.get("relationship_type") != "risk_carrier":
                continue
            if str(relation.get("status") or "current") != "current":
                continue
            target_id = str(relation.get("target_entity_id") or "").strip()
            target = entity_by_id.get(target_id)
            cnpj = normalize_cnpj_v2(
                relation.get("target_cnpj") or (target or {}).get("cnpj")
            )
            if cnpj:
                current_targets.add(cnpj)
        targets[brand_id] = current_targets
    return targets


def _scan_sandbox_registry(
    lifecycle_payload: dict[str, Any],
    sandbox_registry: dict[str, Any],
    entity_by_cnpj: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []
    canonical_targets = _canonical_brand_targets(lifecycle_payload)

    for brand in sandbox_registry.get("brands") or []:
        brand_id = str(brand.get("brand_id") or "").strip()
        carrier_cnpj = normalize_cnpj_v2(brand.get("risk_carrier_cnpj"))
        subject = {
            "brand_id": brand_id or None,
            "name": brand.get("name"),
            "representative_cnpj": normalize_cnpj_v2(
                brand.get("representative_cnpj")
            ),
        }
        relationship = {
            "relationship_type": "risk_carrier",
            "target_cnpj": carrier_cnpj,
            "target_legal_name": brand.get("risk_carrier_name"),
            "regulatory_scope": brand.get("regulatory_scope"),
            "conduct_context_policy": brand.get("conduct_context_policy"),
        }
        observations.append(
            _observation(
                "verified_sandbox_brand_relationship",
                subject=subject,
                relationship=relationship,
                source_authority="sandbox_brand_relationships.json",
            )
        )

        if brand_id not in canonical_targets:
            candidates.append(
                _candidate(
                    "sandbox_brand_missing_from_canonical_registry",
                    severity="critical",
                    blocking=True,
                    subject=subject,
                    reason=(
                        "A Sandbox brand wrapper exists without a matching canonical brand "
                        "relationship in verified_relationships.json."
                    ),
                    signals=relationship,
                )
            )
            continue

        if carrier_cnpj not in canonical_targets[brand_id]:
            candidates.append(
                _candidate(
                    "sandbox_brand_carrier_registry_drift",
                    severity="critical",
                    blocking=True,
                    subject=subject,
                    reason=(
                        "The Sandbox wrapper risk carrier diverges from the canonical "
                        "verified brand relationship."
                    ),
                    signals={
                        **relationship,
                        "canonical_current_risk_carrier_cnpjs": sorted(
                            canonical_targets[brand_id]
                        ),
                    },
                )
            )

        if carrier_cnpj and carrier_cnpj not in entity_by_cnpj:
            candidates.append(
                _candidate(
                    "sandbox_brand_carrier_missing_from_lifecycle",
                    severity="critical",
                    blocking=True,
                    subject=subject,
                    reason=(
                        "The verified Sandbox risk carrier is absent from the current "
                        "lifecycle inventory."
                    ),
                    signals=relationship,
                )
            )

    return candidates, observations


def _scan_conduct_registry(
    conduct_registry: dict[str, Any],
    entity_by_cnpj: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for relation in conduct_registry.get("relationships") or []:
        relationship_id = str(relation.get("relationship_id") or "").strip()
        subject_cnpj = normalize_cnpj_v2(relation.get("subject_cnpj"))
        target_cnpjs = [
            cnpj
            for cnpj in (
                normalize_cnpj_v2(item)
                for item in (relation.get("target_cnpjs") or [])
            )
            if cnpj
        ]
        subject = {
            "relationship_id": relationship_id or None,
            "subject_cnpj": subject_cnpj,
        }
        relationship = {
            "relationship_type": relation.get("relationship_type"),
            "target_cnpjs": target_cnpjs,
            "effective_from": relation.get("effective_from"),
            "pressure_policy": relation.get("pressure_policy"),
            "reconciliation_state": relation.get("reconciliation_state"),
        }
        observations.append(
            _observation(
                "verified_conduct_subject_relationship",
                subject=subject,
                relationship=relationship,
                source_authority="conduct_subject_relationships.json",
            )
        )

        if not relationship_id or relationship_id in seen_ids:
            candidates.append(
                _candidate(
                    "conduct_relationship_id_invalid_or_duplicate",
                    severity="critical",
                    blocking=True,
                    subject=subject,
                    reason=(
                        "Conduct relationship IDs must be present and unique so candidate "
                        "and lineage handling remains deterministic."
                    ),
                    signals=relationship,
                )
            )
        seen_ids.add(relationship_id)

        for target_cnpj in target_cnpjs:
            target = entity_by_cnpj.get(target_cnpj)
            if target is None:
                candidates.append(
                    _candidate(
                        "conduct_relationship_target_missing",
                        severity="critical",
                        blocking=True,
                        subject=subject,
                        reason=(
                            "A source-backed Conduct relationship points to a target CNPJ "
                            "absent from the current lifecycle inventory."
                        ),
                        signals={
                            **relationship,
                            "missing_target_cnpj": target_cnpj,
                        },
                    )
                )
                continue
            target_state = str(
                ((target.get("query_context") or {}).get("entity_state")) or ""
            )
            if target_state in HISTORICAL_TARGET_STATES:
                candidates.append(
                    _candidate(
                        "conduct_relationship_target_is_historical",
                        severity="critical",
                        blocking=True,
                        subject=subject,
                        reason=(
                            "A source-backed Conduct relationship currently points to a "
                            "historical/closed target and requires documentary reconciliation."
                        ),
                        signals={
                            **relationship,
                            "target_cnpj": target_cnpj,
                            "target_entity_state": target_state,
                        },
                    )
                )

    return candidates, observations


def _consumer_provider_candidates(
    consumer_identity: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not consumer_identity:
        return []

    by_provider: dict[str, dict[str, Any]] = {}
    priority = {
        "consumer_provider_unresolved": 10,
        "consumer_provider_ambiguous": 20,
        "temporal_brand_unresolved": 30,
        "temporal_brand_ambiguous": 40,
    }

    def add(kind: str, row: dict[str, Any], *, reason: str) -> None:
        provider = str(row.get("provider") or "").strip()
        if not provider:
            return
        key = normalize_name_key(provider)
        candidate = _candidate(
            kind,
            severity="high" if "temporal_brand" in kind else "medium",
            blocking=False,
            subject={
                "provider": provider,
                "provider_key": key,
            },
            reason=reason,
            signals={
                "complaints": row.get("complaints"),
                "candidate_suggestions_non_authoritative": row.get(
                    "candidate_suggestions_non_authoritative"
                )
                or [],
            },
        )
        existing = by_provider.get(key)
        if existing is None or priority[kind] > priority[existing["candidate_type"]]:
            by_provider[key] = candidate

    for row in consumer_identity.get("unresolved_providers") or []:
        add(
            "consumer_provider_unresolved",
            row,
            reason=(
                "A Consumer.gov provider remains unresolved after deterministic CNPJ, "
                "verified brand and complete regulatory-universe resolution. It may represent "
                "a new trade name, distributor, carrier relationship or genuinely out-of-scope "
                "provider. No complaint is transferred automatically."
            ),
        )
    for row in consumer_identity.get("ambiguous_providers") or []:
        add(
            "consumer_provider_ambiguous",
            row,
            reason=(
                "A Consumer.gov provider has more than one defensible identity path. "
                "The ambiguity is preserved and requires source-backed review."
            ),
        )
    for row in consumer_identity.get("temporal_brand_unresolved_providers") or []:
        add(
            "temporal_brand_unresolved",
            row,
            reason=(
                "A known brand relationship does not cover the full Consumer.gov source month. "
                "The monthly aggregate cannot be split safely and remains unassigned."
            ),
        )
    for row in consumer_identity.get("temporal_brand_ambiguous_providers") or []:
        add(
            "temporal_brand_ambiguous",
            row,
            reason=(
                "A known brand has an ambiguous temporal relationship for the source month. "
                "The watchdog forbids automatic attribution until documentary dates reconcile."
            ),
        )

    return sorted(
        by_provider.values(),
        key=lambda item: (
            -priority[item["candidate_type"]],
            -int((item.get("signals") or {}).get("complaints") or 0),
            str((item.get("subject") or {}).get("provider_key") or ""),
        ),
    )


def build_relationship_watchdog(
    lifecycle_payload: dict[str, Any],
    *,
    consumer_identity: dict[str, Any] | None = None,
    conduct_registry: dict[str, Any] | None = None,
    sandbox_registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if lifecycle_payload.get("artifact") != "v2_lifecycle_relationship_inventory":
        raise TypeError("unexpected lifecycle artifact")
    if consumer_identity and consumer_identity.get("artifact") != (
        "v2_consumer_gov_identity_experiment"
    ):
        raise TypeError("unexpected Consumer.gov identity artifact")

    entities = list(lifecycle_payload.get("entities") or [])
    entity_by_id, entity_by_cnpj = _entity_indexes(lifecycle_payload)

    candidates = _scan_closed_without_successor(entities)
    candidates.extend(_scan_alias_collisions(lifecycle_payload))
    brand_candidates, brand_observations = _scan_materialized_brand_relationships(
        lifecycle_payload,
        entity_by_id,
    )
    candidates.extend(brand_candidates)
    observations = brand_observations

    corporate_candidates, corporate_observations = _scan_corporate_relationships(
        lifecycle_payload,
        entity_by_id,
    )
    candidates.extend(corporate_candidates)
    observations.extend(corporate_observations)
    observations.extend(_group_observations(lifecycle_payload, entity_by_id))

    if sandbox_registry is not None:
        sandbox_candidates, sandbox_observations = _scan_sandbox_registry(
            lifecycle_payload,
            sandbox_registry,
            entity_by_cnpj,
        )
        candidates.extend(sandbox_candidates)
        observations.extend(sandbox_observations)

    if conduct_registry is not None:
        conduct_candidates, conduct_observations = _scan_conduct_registry(
            conduct_registry,
            entity_by_cnpj,
        )
        candidates.extend(conduct_candidates)
        observations.extend(conduct_observations)

    candidates.extend(_consumer_provider_candidates(consumer_identity))
    candidates = sorted(
        candidates,
        key=lambda item: (
            not bool(item.get("blocking")),
            str(item.get("severity") or ""),
            str(item.get("candidate_type") or ""),
            str(item.get("candidate_id") or ""),
        ),
    )
    observations = sorted(
        observations,
        key=lambda item: (
            str(item.get("observation_type") or ""),
            str(item.get("observation_id") or ""),
        ),
    )

    blocking_count = sum(bool(item.get("blocking")) for item in candidates)
    review_count = len(candidates) - blocking_count
    provider_review_count = sum(
        str(item.get("candidate_type") or "").startswith("consumer_provider")
        or str(item.get("candidate_type") or "").startswith("temporal_brand")
        for item in candidates
    )

    return {
        "artifact": "v2_relationship_watchdog",
        "schema_version": 1,
        "generated_at": _utc_now(),
        "status": (
            "blocking_registry_drift"
            if blocking_count
            else "review_candidates"
            if review_count
            else "clear"
        ),
        "policy": {
            "discovery_mode": "automatic_every_generation",
            "assertion_mode": "source_backed_only",
            "candidate_assertion_effect": "none",
            "candidate_score_effect": "none",
            "candidate_complaint_transfer_effect": "none",
            "automatic_registry_mutation": "forbidden",
            "verified_registry_drift_behavior": "fail_closed",
            "official_group_behavior": (
                "materialize exact SUSEP group membership as context; never infer succession"
            ),
        },
        "summary": {
            "candidate_count": len(candidates),
            "blocking_registry_drift_count": blocking_count,
            "review_candidate_count": review_count,
            "consumer_provider_review_count": provider_review_count,
            "verified_or_official_observation_count": len(observations),
            "official_economic_group_observation_count": sum(
                item.get("observation_type") == "official_economic_group_membership"
                for item in observations
            ),
        },
        "inputs": {
            "lifecycle_artifact": lifecycle_payload.get("artifact"),
            "consumer_identity_artifact": (
                consumer_identity.get("artifact") if consumer_identity else None
            ),
            "verified_relationships": str(DEFAULT_VERIFIED_RELATIONSHIPS),
            "conduct_relationships": (
                str(DEFAULT_CONDUCT_RELATIONSHIPS)
                if conduct_registry is not None
                else None
            ),
            "sandbox_relationships": (
                str(DEFAULT_SANDBOX_RELATIONSHIPS)
                if sandbox_registry is not None
                else None
            ),
        },
        "candidates": candidates,
        "observations": observations,
    }


def validate_relationship_watchdog(payload: dict[str, Any]) -> None:
    if payload.get("artifact") != "v2_relationship_watchdog":
        raise RelationshipWatchdogError("unexpected relationship watchdog artifact")
    blocking = [
        item
        for item in (payload.get("candidates") or [])
        if bool(item.get("blocking"))
    ]
    if blocking:
        ids = [str(item.get("candidate_id")) for item in blocking[:10]]
        raise RelationshipWatchdogError(
            "verified relationship registry drift detected: " + ", ".join(ids)
        )


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_relationship_watchdog(
    payload: dict[str, Any],
    output: Path = DEFAULT_OUTPUT,
) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temp.replace(output)
    return output


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Discover and signal new v2 relationship cases without asserting "
            "unverified relationships."
        )
    )
    parser.add_argument(
        "--lifecycle-input",
        type=Path,
        default=DEFAULT_LIFECYCLE_INPUT,
    )
    parser.add_argument(
        "--consumer-identity-input",
        type=Path,
        default=DEFAULT_CONSUMER_IDENTITY_INPUT,
    )
    parser.add_argument(
        "--conduct-relationships",
        type=Path,
        default=DEFAULT_CONDUCT_RELATIONSHIPS,
    )
    parser.add_argument(
        "--sandbox-relationships",
        type=Path,
        default=DEFAULT_SANDBOX_RELATIONSHIPS,
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    payload = build_relationship_watchdog(
        _load_json(args.lifecycle_input),
        consumer_identity=_load_json(args.consumer_identity_input),
        conduct_registry=_load_json(args.conduct_relationships),
        sandbox_registry=_load_json(args.sandbox_relationships),
    )
    path = write_relationship_watchdog(payload, args.output)
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "status": payload["status"],
                "summary": payload["summary"],
                "written_to": str(path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    validate_relationship_watchdog(payload)


if __name__ == "__main__":
    main()
