from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj import load_lifecycle_records
from api.sources.ses import extract_ses_master_and_financials
from api.sources.susep_groups import load_susep_economic_groups
from api.sources.susep_licensed import fetch_licensed_entities
from api.sources.susep_sandbox import fetch_sandbox_participants
from api.sources.susep_special_regimes import fetch_special_regime_records
from api.v2.build_classification_inventory import build_classification_inventory
from api.v2.lifecycle import apply_legal_lifecycle, lifecycle_summary
from api.v2.regulatory_scope import is_special_purpose_insurer
from api.v2.relationships import (
    RelationshipConflictError,
    apply_corporate_relationships,
    apply_economic_groups,
    load_verified_relationship_registry,
    materialize_brands,
    relationship_summary,
)

DEFAULT_OUTPUT = Path("data/derived/v2/entity_lifecycle_relationship_inventory.json")
DEFAULT_CLASSIFICATION_INPUT = Path(
    "data/derived/v2/source/classification_inventory.json"
)
DEFAULT_RECEITA_LIFECYCLE_INPUT = Path(
    "data/derived/v2/source/receita_lifecycle_records.json"
)
DEFAULT_SES_ZIP = Path("data/raw/ses/BaseCompleta.zip")

REINSURANCE_ENTITY_TYPES = {
    "local_reinsurer",
    "admitted_reinsurer",
    "occasional_reinsurer",
    "reinsurance_broker",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _successor_map(entities: list[dict[str, Any]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for entity in entities:
        successors = [
            relation.get("target_entity_id")
            for relation in (entity.get("relationships") or [])
            if relation.get("relationship_type") == "incorporated_into"
            and relation.get("target_entity_id")
        ]
        if len(successors) > 1:
            raise RelationshipConflictError(
                f"Entity {entity['entity_id']} has multiple incorporated_into successors"
            )
        if successors:
            mapping[entity["entity_id"]] = successors[0]
    return mapping


def _resolve_successor_chain(
    entity_id: str,
    mapping: dict[str, str],
) -> list[str]:
    chain: list[str] = []
    seen = {entity_id}
    current = entity_id
    while current in mapping:
        target = mapping[current]
        if target in seen:
            raise RelationshipConflictError(
                f"Corporate succession cycle detected at {entity_id}: {target}"
            )
        chain.append(target)
        seen.add(target)
        current = target
    return chain


def _derive_query_context(entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = [deepcopy(item) for item in entities]
    successor_mapping = _successor_map(output)

    for entity in output:
        lifecycle = entity.get("legal_lifecycle") or {}
        entity_type = entity.get("entity_type") or "unknown"
        successor_chain = _resolve_successor_chain(entity["entity_id"], successor_mapping)

        if successor_chain:
            entity["query_context"] = {
                "entity_state": "historical_incorporated_entity",
                "filter_bucket": "historical",
                "immediate_successor_entity_id": successor_chain[0],
                "successor_entity_id": successor_chain[-1],
                "successor_chain": successor_chain,
                "guidance_code": "show_successor_current_entity",
                "score_behavior": "do_not_score_historical_entity",
                "lifecycle_evidence": (
                    "receita_and_corporate_relationship"
                    if lifecycle.get("cadastral_status") == "closed"
                    else "corporate_relationship"
                ),
            }
        elif lifecycle.get("cadastral_status") == "closed":
            entity["query_context"] = {
                "entity_state": "historical_closed_entity",
                "filter_bucket": "historical",
                "immediate_successor_entity_id": None,
                "successor_entity_id": None,
                "successor_chain": [],
                "guidance_code": "explain_closed_legal_entity",
                "score_behavior": "do_not_score_historical_entity",
                "lifecycle_evidence": "receita_cnpj",
            }
        elif entity_type == "sandbox_participant":
            entity["query_context"] = {
                "entity_state": "sandbox_experimental_participant",
                "filter_bucket": "sandbox",
                "guidance_code": "explain_sandbox_scope_and_limits",
                "score_behavior": "never_compare_with_ordinary_insurers",
            }
        elif is_special_purpose_insurer(entity):
            entity["query_context"] = {
                "entity_state": "special_purpose_insurer",
                "filter_bucket": "other",
                "guidance_code": "explain_sspe_scope_and_lrs_role",
                "score_behavior": "outside_consumer_insurer_comparator",
            }
        elif (
            entity_type == "insurer"
            and entity.get("regulatory_status") == "active_licensed"
            and entity.get("regulatory_regime") == "ordinary"
        ):
            entity["query_context"] = {
                "entity_state": "current_ordinary_insurer",
                "filter_bucket": "insurers",
                "guidance_code": "eligible_for_future_assessment_gate",
                "score_behavior": "assessment_not_yet_implemented",
            }
        elif entity.get("regulatory_regime") == "special":
            entity["query_context"] = {
                "entity_state": "special_regime_entity",
                "filter_bucket": "special_regime",
                "guidance_code": "show_special_regime_alert_and_guidance",
                "score_behavior": "do_not_rank",
            }
        elif entity_type == "open_pension_entity":
            entity["query_context"] = {
                "entity_state": "open_pension_entity",
                "filter_bucket": "pension",
                "guidance_code": "explain_open_pension_entity",
                "score_behavior": "do_not_compare_with_insurer_ranking",
            }
        elif entity_type == "capitalization_company":
            entity["query_context"] = {
                "entity_state": "capitalization_company",
                "filter_bucket": "capitalization",
                "guidance_code": "explain_capitalization_company",
                "score_behavior": "do_not_compare_with_insurer_ranking",
            }
        elif entity_type in REINSURANCE_ENTITY_TYPES:
            entity["query_context"] = {
                "entity_state": "reinsurance_market_entity",
                "filter_bucket": "other",
                "guidance_code": "briefly_explain_reinsurance_market_role",
                "score_behavior": "outside_consumer_insurer_ranking",
            }
        elif entity_type == "self_regulator":
            entity["query_context"] = {
                "entity_state": "self_regulatory_entity",
                "filter_bucket": "other",
                "guidance_code": "explain_non_insurer_entity",
                "score_behavior": "outside_consumer_insurer_ranking",
            }

    return sorted(output, key=lambda item: item["entity_id"])


def build_lifecycle_relationship_inventory(
    classification_payload: dict[str, Any],
    lifecycle_records: list[dict[str, Any]],
    relationship_registry: dict[str, Any],
    group_records: list[dict[str, Any]],
) -> dict[str, Any]:
    entities = list(classification_payload.get("entities") or [])
    entities, unresolved_lifecycle = apply_legal_lifecycle(entities, lifecycle_records)
    lifecycle_meta = lifecycle_summary(entities, lifecycle_records, unresolved_lifecycle)

    entities, corporate_resolved = apply_corporate_relationships(
        entities,
        relationship_registry,
    )
    entities, groups = apply_economic_groups(entities, group_records)
    brands = materialize_brands(entities, relationship_registry)
    entities = _derive_query_context(entities)
    relationship_meta = relationship_summary(
        entities,
        corporate_resolved,
        groups,
        brands,
    )

    classification_meta = dict(classification_payload.get("meta") or {})
    unresolved = {
        "classification": deepcopy(classification_payload.get("unresolved") or {}),
        "receita_lifecycle": unresolved_lifecycle,
    }
    receita_source_modes = sorted(
        {
            str(record.get("source_mode") or "unknown")
            for record in lifecycle_records
        }
    )
    bulk_active = "official_open_data_bulk" in receita_source_modes

    return {
        "artifact": "v2_lifecycle_relationship_inventory",
        "generated_at": _utc_now(),
        "status": "draft",
        "meta": {
            **classification_meta,
            **lifecycle_meta,
            **relationship_meta,
            "relationship_model_note": (
                "Receita cadastral lifecycle is kept separate from SUSEP regulatory status. "
                "Corporate succession is only materialized from explicit source-backed "
                "relationships; common names or economic groups never imply succession. "
                "Documented successor chains are resolved in the backend so historical "
                "queries can reach the terminal known successor without frontend logic. "
                "Query filter buckets separate ordinary consumer insurers, SSPEs, Sandbox, "
                "pension, capitalization, special-regime and other entities without creating "
                "additional rankings. Brands are resolver objects and never inherit an entity score."
            ),
            "receita_ingestion_status": (
                "official_open_data_bulk_filtered"
                if bulk_active
                else "verified_snapshot_bridge"
            ),
            "receita_source_modes": receita_source_modes,
            "receita_ingestion_note": (
                "The lifecycle builder prefers a filtered snapshot generated from the "
                "official Receita CNPJ open-data bulk files. When that snapshot is absent, "
                "the small verified official bridge remains an explicit fallback. When the "
                "bulk snapshot is present, the verified cases become regression checks and "
                "must agree on cadastral status, date and reason."
            ),
        },
        "unresolved": unresolved,
        "groups": groups,
        "brands": brands,
        "corporate_relationships": corporate_resolved,
        "entities": entities,
    }


def write_lifecycle_relationship_inventory(
    payload: dict[str, Any],
    output: Path = DEFAULT_OUTPUT,
) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(output)
    return output


def _build_legacy_inputs_from_sources() -> tuple[
    dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]
]:
    group_records = load_susep_economic_groups()
    ses_out = extract_ses_master_and_financials()
    if not isinstance(ses_out, tuple) or len(ses_out) < 2:
        raise RuntimeError(f"Unexpected SES return: {type(ses_out)}")

    classification = build_classification_inventory(
        ses_out[1],
        fetch_licensed_entities(),
        fetch_special_regime_records(),
        fetch_sandbox_participants(),
    )
    return classification, load_lifecycle_records(), group_records


def _load_classification_input(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("artifact") != "v2_classification_inventory":
        raise RuntimeError("unexpected classification input artifact")
    return payload


def _load_receita_lifecycle_input(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict) and isinstance(payload.get("records"), list):
        records = payload["records"]
    else:
        raise RuntimeError("unexpected Receita lifecycle input artifact")
    return [dict(record) for record in records]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the v2 lifecycle and relationship inventory."
    )
    parser.add_argument(
        "--classification-input",
        type=Path,
        help=(
            "Use a materialized classification inventory. Gate 4 uses this mode so "
            "Lifecycle does not refetch SUSEP regulatory sources."
        ),
    )
    parser.add_argument(
        "--receita-lifecycle-input",
        type=Path,
        help="Materialized Receita lifecycle records for the same Gate 4 generation.",
    )
    parser.add_argument(
        "--ses-zip",
        type=Path,
        default=DEFAULT_SES_ZIP,
        help="Validated BaseCompleta.zip snapshot used for economic-group derivation.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    gate4_mode = args.classification_input is not None or args.receita_lifecycle_input is not None
    if gate4_mode:
        if args.classification_input is None or args.receita_lifecycle_input is None:
            raise RuntimeError(
                "Gate 4 lifecycle mode requires both --classification-input and "
                "--receita-lifecycle-input"
            )
        classification = _load_classification_input(args.classification_input)
        lifecycle_records = _load_receita_lifecycle_input(args.receita_lifecycle_input)
        group_records = load_susep_economic_groups(args.ses_zip)
    else:
        classification, lifecycle_records, group_records = _build_legacy_inputs_from_sources()

    payload = build_lifecycle_relationship_inventory(
        classification,
        lifecycle_records,
        load_verified_relationship_registry(),
        group_records,
    )
    path = write_lifecycle_relationship_inventory(payload, args.output)
    meta = payload["meta"]
    print(
        "V2 lifecycle + relationships: "
        f"{len(payload['entities'])} entities; "
        f"{meta['receita_lifecycle_attached_count']} Receita lifecycle records attached; "
        f"{meta['corporate_relationships_resolved']} corporate relationships; "
        f"{meta['economic_groups_count']} economic groups; "
        f"{meta['brands_count']} brands; written to {path}"
    )


if __name__ == "__main__":
    main()
