from __future__ import annotations

import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2
from api.v2.identity import canonical_fip_code

DEFAULT_RELATIONSHIPS_PATH = Path("data/reference/v2/verified_relationships.json")


class RelationshipConflictError(ValueError):
    """Raised when a verified relationship cannot be resolved unambiguously."""


def load_verified_relationship_registry(
    path: Path = DEFAULT_RELATIONSHIPS_PATH,
) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload.get("corporate_relationships") or [], list):
        raise RelationshipConflictError("corporate_relationships must be a list")
    if not isinstance(payload.get("brands") or [], list):
        raise RelationshipConflictError("brands must be a list")
    return payload


def _append_relation(entity: dict[str, Any], relation: dict[str, Any]) -> None:
    relationships = list(entity.get("relationships") or [])
    key = (
        relation.get("relationship_type"),
        relation.get("target_entity_id"),
        relation.get("target_group_id"),
        relation.get("effective_date"),
    )
    existing = {
        (
            item.get("relationship_type"),
            item.get("target_entity_id"),
            item.get("target_group_id"),
            item.get("effective_date"),
        )
        for item in relationships
    }
    if key not in existing:
        relationships.append(relation)
    entity["relationships"] = relationships


def _cnpj_index(entities: list[dict[str, Any]]) -> dict[str, str]:
    index: dict[str, str] = {}
    duplicates: set[str] = set()
    for entity in entities:
        cnpj = normalize_cnpj_v2(entity.get("cnpj"))
        if not cnpj:
            continue
        if cnpj in index and index[cnpj] != entity.get("entity_id"):
            duplicates.add(cnpj)
        index[cnpj] = entity["entity_id"]
    if duplicates:
        raise RelationshipConflictError(
            f"Current entity CNPJ is not unique: {sorted(duplicates)[:5]}"
        )
    return index


def _entity_positions(entities: list[dict[str, Any]]) -> dict[str, int]:
    return {entity["entity_id"]: index for index, entity in enumerate(entities)}


def apply_corporate_relationships(
    entities: list[dict[str, Any]],
    registry: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Resolve explicit succession relationships by exact CNPJ.

    ``incorporated_into`` is never inferred from a similar name or common group.
    The inverse ``successor_of`` relationship is generated deterministically.
    """
    output = [deepcopy(item) for item in entities]
    cnpj_to_id = _cnpj_index(output)
    positions = _entity_positions(output)
    resolved: list[dict[str, Any]] = []

    for raw in registry.get("corporate_relationships") or []:
        relation = deepcopy(raw)
        relationship_type = relation.get("relationship_type")
        if relationship_type != "incorporated_into":
            raise RelationshipConflictError(
                f"Unsupported corporate relationship type: {relationship_type}"
            )
        source_cnpj = normalize_cnpj_v2(relation.get("source_cnpj"))
        target_cnpj = normalize_cnpj_v2(relation.get("target_cnpj"))
        source_id = cnpj_to_id.get(source_cnpj or "")
        target_id = cnpj_to_id.get(target_cnpj or "")
        if not source_id or not target_id:
            raise RelationshipConflictError(
                "Verified corporate relationship could not be resolved: "
                f"source={source_cnpj}:{source_id} target={target_cnpj}:{target_id}"
            )
        if source_id == target_id:
            raise RelationshipConflictError(
                f"Corporate relationship self-loop is invalid: {source_id}"
            )

        forward = {
            "relationship_type": "incorporated_into",
            "target_entity_id": target_id,
            "effective_date": relation.get("effective_date"),
            "evidence": deepcopy(relation.get("evidence") or {}),
        }
        inverse = {
            "relationship_type": "successor_of",
            "target_entity_id": source_id,
            "effective_date": relation.get("effective_date"),
            "evidence": deepcopy(relation.get("evidence") or {}),
        }
        _append_relation(output[positions[source_id]], forward)
        _append_relation(output[positions[target_id]], inverse)
        resolved.append(
            {
                "relationship_type": "incorporated_into",
                "source_entity_id": source_id,
                "target_entity_id": target_id,
                "effective_date": relation.get("effective_date"),
            }
        )

    return sorted(output, key=lambda item: item["entity_id"]), resolved


def _group_id(code: str | None, name: str | None) -> str:
    clean_code = str(code or "").strip()
    if clean_code:
        return f"susep-group:{clean_code}"
    slug = re.sub(r"[^a-z0-9]+", "-", str(name or "").casefold()).strip("-")
    if not slug:
        raise RelationshipConflictError("Economic group requires code or name")
    return f"susep-group-name:{slug}"


def apply_economic_groups(
    entities: list[dict[str, Any]],
    group_records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Attach official SES economic-group membership by exact FIP."""
    output = [deepcopy(item) for item in entities]
    positions = _entity_positions(output)
    by_fip = {
        canonical_fip_code(entity.get("fip_code")): entity["entity_id"]
        for entity in output
        if canonical_fip_code(entity.get("fip_code"))
    }
    groups: dict[str, dict[str, Any]] = {}

    for record in group_records:
        fip = canonical_fip_code(record.get("fip_code"))
        entity_id = by_fip.get(fip)
        if not entity_id:
            continue
        group_code = str(record.get("group_code") or "").strip() or None
        group_name = str(record.get("group_name") or "").strip() or None
        if not group_code and not group_name:
            continue
        group_id = _group_id(group_code, group_name)
        group = groups.setdefault(
            group_id,
            {
                "group_id": group_id,
                "group_code": group_code,
                "group_name": group_name,
                "source": record.get("source"),
                "member_entity_ids": [],
            },
        )
        if group.get("group_name") and group_name and group["group_name"] != group_name:
            raise RelationshipConflictError(
                f"Conflicting name for economic group {group_id}: "
                f"{group['group_name']} != {group_name}"
            )
        if entity_id not in group["member_entity_ids"]:
            group["member_entity_ids"].append(entity_id)

        entity = output[positions[entity_id]]
        entity["economic_group"] = {
            "group_id": group_id,
            "group_code": group_code,
            "group_name": group_name,
            "source": record.get("source"),
        }
        _append_relation(
            entity,
            {
                "relationship_type": "member_of_group",
                "target_group_id": group_id,
                "evidence": {"source": record.get("source")},
            },
        )

    normalized_groups = []
    for group_id in sorted(groups):
        group = groups[group_id]
        group["member_entity_ids"] = sorted(group["member_entity_ids"])
        normalized_groups.append(group)
    return sorted(output, key=lambda item: item["entity_id"]), normalized_groups


def materialize_brands(
    entities: list[dict[str, Any]],
    registry: dict[str, Any],
) -> list[dict[str, Any]]:
    """Resolve curated brand relationships to legal/regulatory entities.

    A brand has no score. ``risk_carrier`` points to the entity that may be
    assessed later; it never transfers that entity's assessment to the brand.
    """
    cnpj_to_id = _cnpj_index(entities)
    brands: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for raw_brand in registry.get("brands") or []:
        brand = deepcopy(raw_brand)
        brand_id = str(brand.get("brand_id") or "").strip()
        if not brand_id or brand_id in seen_ids:
            raise RelationshipConflictError(f"Invalid or duplicate brand_id: {brand_id}")
        seen_ids.add(brand_id)

        resolved_relationships: list[dict[str, Any]] = []
        for raw_relation in brand.get("relationships") or []:
            relation = deepcopy(raw_relation)
            target_cnpj = normalize_cnpj_v2(relation.pop("target_cnpj", None))
            target_id = cnpj_to_id.get(target_cnpj or "")
            if not target_id:
                raise RelationshipConflictError(
                    f"Brand {brand_id} target CNPJ cannot be resolved: {target_cnpj}"
                )
            relation["target_entity_id"] = target_id
            resolved_relationships.append(relation)

        brands.append(
            {
                "brand_id": brand_id,
                "name": str(brand.get("name") or "").strip(),
                "aliases": sorted(
                    {
                        str(alias).strip()
                        for alias in (brand.get("aliases") or [])
                        if str(alias).strip()
                    }
                ),
                "relationships": resolved_relationships,
            }
        )

    return sorted(brands, key=lambda item: item["brand_id"])


def relationship_summary(
    entities: list[dict[str, Any]],
    corporate_resolved: list[dict[str, Any]],
    groups: list[dict[str, Any]],
    brands: list[dict[str, Any]],
) -> dict[str, Any]:
    group_members = sum(bool(item.get("economic_group")) for item in entities)
    brand_relations = sum(len(item.get("relationships") or []) for item in brands)
    risk_carriers = sum(
        relation.get("relationship_type") == "risk_carrier"
        for brand in brands
        for relation in brand.get("relationships") or []
    )
    return {
        "corporate_relationships_resolved": len(corporate_resolved),
        "economic_groups_count": len(groups),
        "entities_with_economic_group": group_members,
        "brands_count": len(brands),
        "brand_relationships_count": brand_relations,
        "brand_risk_carrier_relationships_count": risk_carriers,
    }
