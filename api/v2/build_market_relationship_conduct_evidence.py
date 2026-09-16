from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import normalize_name_key
from api.v2.build_consumer_gov_conduct_evidence import _entity_totals, _provider_name
from api.v2.build_consumer_gov_identity_experiment import OUTPUT_PATH as IDENTITY_PATH
from api.v2.consumer_gov_conduct import build_conduct_film
from api.v2.consumer_gov_conduct_core import (
    add_entry_statistics,
    aggregate_entry_to_month,
    load_monthly_entries,
)
from api.v2.consumer_gov_universe_resolution import (
    build_full_universe_provider_index,
    resolve_cnpj_against_full_universe,
    resolve_provider_against_full_universe,
)

LIFECYCLE_PATH = Path("data/derived/v2/entity_lifecycle_relationship_inventory.json")
OUTPUT_PATH = Path("data/derived/v2/market_relationship_conduct_evidence.json")
LEGACY_SANDBOX_OUTPUT_PATH = Path(
    "data/derived/v2/sandbox_brand_conduct_evidence.json"
)


class MarketRelationshipConductError(RuntimeError):
    """Raised when verified market relationships cannot be projected safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _regulatory_label(entity: dict[str, Any]) -> str:
    regime = str(entity.get("regulatory_regime") or "unknown")
    entity_type = str(entity.get("entity_type") or "unknown")
    state = str((entity.get("query_context") or {}).get("entity_state") or "")
    if state == "historical_incorporated_entity":
        return "Entidade histórica incorporada"
    if state == "historical_closed_entity":
        return "Entidade jurídica histórica"
    if regime == "sandbox" or entity_type == "sandbox_participant":
        return "Participante do Sandbox regulatório"
    if regime == "special":
        return "Entidade em regime especial"
    if entity_type == "insurer":
        return "Seguradora"
    if entity_type == "open_pension_entity":
        return "Entidade aberta de previdência"
    if entity_type == "capitalization_company":
        return "Sociedade de capitalização"
    return "Entidade identificada no mercado supervisionado"


def _summary_from_month_stats(
    months: list[str],
    monthly_stats: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    monthly = [
        aggregate_entry_to_month(
            month,
            {"statistics": monthly_stats[month]},
            matched_current_insurer_market_complaints=0,
        )
        for month in months
    ]
    totals = _entity_totals(monthly)
    return {
        "complaints": totals["complaints"],
        "responded": totals["responded"],
        "response_rate": totals["response_rate"],
        "finalized": totals["finalized"],
        "finalized_rate": totals["finalized_rate"],
        "satisfaction_count": totals["satisfaction_count"],
        "average_satisfaction": totals["average_satisfaction"],
        "film": build_conduct_film(monthly),
        "monthly": monthly,
    }


def _verified_market_identity(brand: dict[str, Any]) -> dict[str, Any] | None:
    market = brand.get("market_identity")
    if not isinstance(market, dict) or not market:
        return None
    out = dict(market)
    cnpj = normalize_cnpj_v2(out.get("cnpj"))
    if cnpj:
        out["cnpj"] = cnpj
    identity_verified = bool(
        cnpj
        and str(out.get("legal_name") or "").strip()
        and (out.get("evidence") or {})
    )
    out["identity_state"] = str(out.get("identity_state") or "").strip() or (
        "verified" if identity_verified else "unverified"
    )
    out["distribution_role_state"] = str(
        out.get("distribution_role_state") or "unverified"
    ).strip()
    if out["distribution_role_state"] == "verified":
        if not str(out.get("distribution_role") or "").strip():
            raise MarketRelationshipConductError(
                f"verified distribution role missing for {brand.get('brand_id')}"
            )
        if not (out.get("evidence") or {}):
            raise MarketRelationshipConductError(
                f"verified distribution role lacks evidence for {brand.get('brand_id')}"
            )
    return out


def _current_risk_relationships(brand: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(relation)
        for relation in brand.get("relationships") or []
        if relation.get("relationship_type") == "risk_carrier"
        and str(relation.get("status") or "current") == "current"
        and relation.get("target_entity_id")
    ]


def _market_identity_key(market: dict[str, Any], brand_id: str) -> str:
    cnpj = normalize_cnpj_v2(market.get("cnpj"))
    if cnpj:
        return f"cnpj:{cnpj}"
    legal = normalize_name_key(str(market.get("legal_name") or ""))
    if legal:
        return f"legal:{legal}"
    return f"brand:{brand_id}"


def _build_relationship_model(
    lifecycle: dict[str, Any],
) -> tuple[
    dict[str, dict[str, Any]],
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, set[str]],
]:
    entities = {
        str(entity["entity_id"]): entity
        for entity in lifecycle.get("entities") or []
        if isinstance(entity, dict) and entity.get("entity_id")
    }
    brands: list[dict[str, Any]] = []
    identities: dict[str, dict[str, Any]] = {}
    aliases_by_identity: dict[str, set[str]] = {}

    for raw_brand in lifecycle.get("brands") or []:
        if not isinstance(raw_brand, dict):
            continue
        brand_id = str(raw_brand.get("brand_id") or "").strip()
        if not brand_id:
            continue
        market = _verified_market_identity(raw_brand)
        risk_relations = _current_risk_relationships(raw_brand)
        if market is None or not risk_relations:
            continue
        identity_key = _market_identity_key(market, brand_id)
        existing = identities.get(identity_key)
        if existing is not None:
            existing_cnpj = normalize_cnpj_v2(existing.get("cnpj"))
            market_cnpj = normalize_cnpj_v2(market.get("cnpj"))
            if existing_cnpj != market_cnpj:
                raise MarketRelationshipConductError(
                    f"market identity key collision: {identity_key}"
                )
        else:
            identities[identity_key] = market
            aliases_by_identity[identity_key] = set()

        labels = [
            raw_brand.get("name"),
            *(raw_brand.get("aliases") or []),
            market.get("trade_name"),
            market.get("legal_name"),
        ]
        for label in labels:
            key = normalize_name_key(str(label or ""))
            if key:
                aliases_by_identity[identity_key].add(key)

        risk_bearers: list[dict[str, Any]] = []
        for relation in risk_relations:
            entity_id = str(relation["target_entity_id"])
            entity = entities.get(entity_id)
            if entity is None:
                raise MarketRelationshipConductError(
                    f"risk carrier target not found: {brand_id} -> {entity_id}"
                )
            carrier_cnpj = normalize_cnpj_v2(entity.get("cnpj"))
            market_cnpj = normalize_cnpj_v2(market.get("cnpj"))
            if carrier_cnpj and market_cnpj and carrier_cnpj == market_cnpj:
                raise MarketRelationshipConductError(
                    f"market identity cannot equal risk bearer: {brand_id}"
                )
            risk_bearers.append(
                {
                    "entity_id": entity_id,
                    "profile_id": f"entity:{entity_id}",
                    "name": entity.get("display_name")
                    or entity.get("legal_name")
                    or entity_id,
                    "cnpj": carrier_cnpj,
                    "entity_type": entity.get("entity_type"),
                    "regulatory_regime": entity.get("regulatory_regime"),
                    "regulatory_status": entity.get("regulatory_status"),
                    "regulatory_class": _regulatory_label(entity),
                    "relationship_status": relation.get("status"),
                    "product_scope": relation.get("scope"),
                    "effective_from": relation.get("effective_from")
                    or relation.get("effective_date"),
                    "effective_to": relation.get("effective_to"),
                    "evidence": relation.get("evidence") or {},
                }
            )

        brands.append(
            {
                "brand_id": brand_id,
                "name": raw_brand.get("name"),
                "aliases": list(raw_brand.get("aliases") or []),
                "market_identity_key": identity_key,
                "market_identity": market,
                "risk_bearers": risk_bearers,
            }
        )

    return entities, brands, identities, aliases_by_identity


def _subject_indexes(
    identities: dict[str, dict[str, Any]],
    aliases_by_identity: dict[str, set[str]],
) -> tuple[dict[str, str], dict[str, str]]:
    by_cnpj: dict[str, str] = {}
    alias_owners: dict[str, set[str]] = {}
    for identity_key, market in identities.items():
        cnpj = normalize_cnpj_v2(market.get("cnpj"))
        if cnpj:
            previous = by_cnpj.get(cnpj)
            if previous and previous != identity_key:
                raise MarketRelationshipConductError(
                    f"market identity CNPJ collision: {cnpj}"
                )
            by_cnpj[cnpj] = identity_key
        if market.get("identity_state") != "verified":
            continue
        for alias in aliases_by_identity.get(identity_key, set()):
            alias_owners.setdefault(alias, set()).add(identity_key)

    by_name = {
        alias: next(iter(owners))
        for alias, owners in alias_owners.items()
        if len(owners) == 1
    }
    return by_name, by_cnpj


def _resolve_market_subject(
    provider: str,
    entry: dict[str, Any],
    *,
    by_name: dict[str, str],
    by_cnpj: dict[str, str],
) -> str | None:
    """Resolve the complaint subject as the consumer-facing legal identity.

    Source CNPJ has precedence. If a CNPJ is present but is not a verified market
    identity, a brand label is never allowed to override it. Name-only attribution
    requires an exact normalized, unique, verified alias and is never fuzzy.
    """
    cnpj = normalize_cnpj_v2(entry.get("cnpj"))
    if cnpj:
        return by_cnpj.get(cnpj)
    return by_name.get(normalize_name_key(provider))


def _resolve_entity_subject(
    provider: str,
    entry: dict[str, Any],
    full_universe_index: dict[str, Any],
) -> str | None:
    cnpj_resolution = resolve_cnpj_against_full_universe(
        normalize_cnpj_v2(entry.get("cnpj")), full_universe_index
    )
    if cnpj_resolution is not None:
        return str(cnpj_resolution.get("matched_canonical_entity_id") or "") or None
    canonical = resolve_provider_against_full_universe(provider, full_universe_index)
    if canonical is None:
        return None
    return str(canonical.get("matched_canonical_entity_id") or "") or None


def _load_entries_from_source(month: str) -> dict[str, Any]:
    _, entries, _ = load_monthly_entries(month)
    return entries


def build_market_relationship_conduct_evidence(
    lifecycle: dict[str, Any],
    identity: dict[str, Any],
    *,
    entry_loader: Callable[[str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    months = sorted(
        str(month) for month in (identity.get("source") or {}).get("months") or []
    )
    if len(months) != 12:
        raise MarketRelationshipConductError(
            f"expected 12 Consumer.gov months, got {len(months)}"
        )

    entities, brands, identities, aliases_by_identity = _build_relationship_model(
        lifecycle
    )
    by_name, by_cnpj = _subject_indexes(identities, aliases_by_identity)
    full_index = build_full_universe_provider_index(list(entities.values()))
    loader = entry_loader or _load_entries_from_source

    referenced_carrier_ids = {
        bearer["entity_id"]
        for brand in brands
        for bearer in brand["risk_bearers"]
    }
    # Preserve carrier-level Sandbox context for entity profiles during the legacy
    # migration. Sandbox is a regulatory attribute here, not the relationship model.
    sandbox_entity_ids = {
        entity_id
        for entity_id, entity in entities.items()
        if entity.get("regulatory_regime") == "sandbox"
        or entity.get("entity_type") == "sandbox_participant"
    }
    tracked_entity_ids = referenced_carrier_ids | sandbox_entity_ids

    identity_stats = {
        identity_key: {month: {} for month in months} for identity_key in identities
    }
    identity_labels = {identity_key: Counter() for identity_key in identities}
    entity_stats = {
        entity_id: {month: {} for month in months} for entity_id in tracked_entity_ids
    }
    entity_labels = {entity_id: Counter() for entity_id in tracked_entity_ids}

    direct_market_subject_complaints = 0
    resolved_risk_bearer_complaints = 0

    for month in months:
        entries = loader(month)
        for raw_key, raw_entry in entries.items():
            entry = raw_entry if isinstance(raw_entry, dict) else {}
            provider = _provider_name(entry, str(raw_key))
            if not provider:
                continue
            statistics = (
                entry.get("statistics")
                if isinstance(entry.get("statistics"), dict)
                else {}
            )
            complaints = int(
                statistics.get("complaintsCount")
                or statistics.get("total_claims")
                or 0
            )
            if complaints <= 0:
                continue

            identity_key = _resolve_market_subject(
                provider, entry, by_name=by_name, by_cnpj=by_cnpj
            )
            if identity_key is not None:
                added = add_entry_statistics(identity_stats[identity_key][month], entry)
                if added != complaints:
                    raise MarketRelationshipConductError(
                        f"market subject complaint count changed: {provider} {month}"
                    )
                identity_labels[identity_key][provider] += complaints
                direct_market_subject_complaints += complaints
                # One source row has one complaint subject. Once the distributor/legal
                # identity is resolved, the same row is never also counted for a carrier.
                continue

            entity_id = _resolve_entity_subject(provider, entry, full_index)
            if entity_id is None or entity_id not in tracked_entity_ids:
                continue
            added = add_entry_statistics(entity_stats[entity_id][month], entry)
            if added != complaints:
                raise MarketRelationshipConductError(
                    f"risk bearer complaint count changed: {provider} {month}"
                )
            entity_labels[entity_id][provider] += complaints
            resolved_risk_bearer_complaints += complaints

    market_identities: list[dict[str, Any]] = []
    identity_summary_by_key: dict[str, dict[str, Any]] = {}
    for identity_key, market in sorted(identities.items()):
        summary = _summary_from_month_stats(months, identity_stats[identity_key])
        summary["provider_labels"] = [
            {"provider": name, "complaints": int(count)}
            for name, count in identity_labels[identity_key].most_common()
        ]
        identity_summary_by_key[identity_key] = summary
        market_identities.append(
            {
                "market_identity_key": identity_key,
                "identity": market,
                "conduct_summary": summary,
                "attribution_note": (
                    "Consumer.gov statistics are attributed to this legal market identity "
                    "only by its verified CNPJ or, when source CNPJ is absent, by an exact "
                    "unique verified alias. They are never transferred to a risk bearer."
                ),
            }
        )

    bearer_summary_by_id: dict[str, dict[str, Any]] = {}
    risk_bearers: list[dict[str, Any]] = []
    for entity_id in sorted(tracked_entity_ids):
        entity = entities[entity_id]
        summary = _summary_from_month_stats(months, entity_stats[entity_id])
        summary["provider_labels"] = [
            {"provider": name, "complaints": int(count)}
            for name, count in entity_labels[entity_id].most_common()
        ]
        bearer_summary_by_id[entity_id] = summary
        risk_bearers.append(
            {
                "entity_id": entity_id,
                "profile_id": f"entity:{entity_id}",
                "cnpj": normalize_cnpj_v2(entity.get("cnpj")),
                "name": entity.get("display_name")
                or entity.get("legal_name")
                or entity_id,
                "entity_type": entity.get("entity_type"),
                "regulatory_regime": entity.get("regulatory_regime"),
                "regulatory_status": entity.get("regulatory_status"),
                "regulatory_class": _regulatory_label(entity),
                "conduct_summary": summary,
            }
        )

    brands_out: list[dict[str, Any]] = []
    for brand in brands:
        relationships = []
        for bearer in brand["risk_bearers"]:
            relationships.append(
                {
                    **bearer,
                    "conduct_summary": bearer_summary_by_id[bearer["entity_id"]],
                    "attribution_note": (
                        "These Consumer.gov statistics belong to the risk-bearing legal "
                        "entity itself. They are relationship context and are not complaints "
                        "of the distributor or exclusive complaints of this brand/product."
                    ),
                }
            )
        brands_out.append(
            {
                "brand_id": brand["brand_id"],
                "name": brand.get("name"),
                "aliases": brand.get("aliases") or [],
                "market_identity_key": brand["market_identity_key"],
                "market_identity": brand["market_identity"],
                "distribution_role": brand["market_identity"].get("distribution_role"),
                "distribution_role_state": brand["market_identity"].get(
                    "distribution_role_state"
                )
                or "unverified",
                "market_identity_conduct_summary": identity_summary_by_key[
                    brand["market_identity_key"]
                ],
                "risk_bearers": relationships,
                "conduct_scope": "separate_market_identity_and_risk_bearer_subjects",
                "ordinary_ranking_effect": "none",
                "ordinary_market_baseline_effect": "none",
            }
        )

    return {
        "artifact": "v2_market_relationship_conduct_evidence",
        "version": "2.0-draft-market-relationship-conduct-1",
        "generated_at": _utc_now(),
        "status": "experimental",
        "assessment_role": "verified_market_identity_and_risk_bearer_conduct_context",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "ordinary_ranking_effect": "none",
        "source": {
            "dataset": "reclamacoes-do-consumidor-gov-br",
            "months": months,
            "complaint_semantics": (
                "Observed consumer friction/allegation in Consumer.gov; not an adjudicated finding."
            ),
        },
        "policy": {
            "regulatory_regime_is_attribute_not_relationship_model": True,
            "relationship_requires_verified_canonical_registry": True,
            "market_identity_is_distinct_from_risk_bearer": True,
            "distributor_complaints_remain_with_distributor": True,
            "risk_bearer_complaints_remain_with_risk_bearer": True,
            "carrier_context_is_not_brand_exclusive_by_default": True,
            "generic_sales_channel_complaints_are_not_transferred": True,
            "cnpj_precedes_brand_label_for_subject_attribution": True,
            "exact_unique_alias_only_when_source_cnpj_absent": True,
            "fuzzy_complaint_attribution": False,
            "multi_carrier_relationships_must_remain_separate": True,
        },
        "summary": {
            "verified_market_identities": len(market_identities),
            "brands_with_market_relationship_context": len(brands_out),
            "referenced_risk_bearers": len(referenced_carrier_ids),
            "direct_market_subject_complaints": direct_market_subject_complaints,
            "resolved_risk_bearer_complaints": resolved_risk_bearer_complaints,
        },
        "market_identities": market_identities,
        "risk_bearers": risk_bearers,
        "brands": sorted(brands_out, key=lambda item: str(item["brand_id"])),
    }


def build_legacy_sandbox_projection(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Temporary compatibility output for the pre-generalization public contract.

    Only Sandbox entities/brands are projected here. New code must consume
    ``market_relationship_conduct_evidence.json`` instead.
    """
    source = payload.get("source") or {}
    sandbox_bearers = [
        row
        for row in payload.get("risk_bearers") or []
        if row.get("regulatory_regime") == "sandbox"
    ]
    bearer_by_id = {str(row["entity_id"]): row for row in sandbox_bearers}
    carriers = []
    for row in sandbox_bearers:
        summary = row.get("conduct_summary") or {}
        carriers.append(
            {
                "entity_id": row.get("entity_id"),
                "entity_type": "sandbox_participant",
                "cnpj": row.get("cnpj"),
                "legal_name": row.get("name"),
                "display_name": row.get("name"),
                "regulatory_regime": "sandbox",
                "regulatory_public_label": row.get("regulatory_class"),
                "ordinary_ranking_effect": "none",
                "ordinary_market_baseline_effect": "none",
                "consumer_gov_provider_labels_observed": summary.get("provider_labels")
                or [],
                "totals": {
                    key: summary.get(key)
                    for key in (
                        "complaints",
                        "responded",
                        "response_rate",
                        "finalized",
                        "finalized_rate",
                        "satisfaction_count",
                        "average_satisfaction",
                    )
                },
                "monthly": summary.get("monthly") or [],
                "film": summary.get("film") or {},
            }
        )

    brands = []
    for row in payload.get("brands") or []:
        sandbox_relations = [
            relation
            for relation in row.get("risk_bearers") or []
            if str(relation.get("entity_id") or "") in bearer_by_id
        ]
        if len(sandbox_relations) != 1 or len(row.get("risk_bearers") or []) != 1:
            continue
        relation = sandbox_relations[0]
        carrier = bearer_by_id[str(relation["entity_id"])]
        carrier_summary = carrier.get("conduct_summary") or {}
        brand_summary = row.get("market_identity_conduct_summary") or {}
        market = row.get("market_identity") or {}
        brands.append(
            {
                "brand_id": row.get("brand_id"),
                "name": row.get("name"),
                "aliases": row.get("aliases") or [],
                "representative_cnpj": market.get("cnpj"),
                "market_identity": market,
                "distribution_role": row.get("distribution_role"),
                "distribution_role_state": row.get("distribution_role_state"),
                "risk_carrier_entity_id": carrier.get("entity_id"),
                "risk_carrier_cnpj": carrier.get("cnpj"),
                "risk_carrier_name": carrier.get("name"),
                "risk_carrier_regulatory_class": carrier.get("regulatory_class"),
                "regulatory_scope": "sandbox",
                "product_scope": relation.get("product_scope"),
                "conduct_scope": "separate_distributor_subject_and_carrier_context",
                "conduct_context_policy": "separate_distributor_subject_from_carrier_context",
                "ordinary_ranking_effect": "none",
                "ordinary_market_baseline_effect": "none",
                "brand_subject_attribution_note": (
                    "Compatibility projection: complaints remain attached to the verified "
                    "market identity and are not transferred to the Sandbox risk bearer."
                ),
                "carrier_attribution_note": relation.get("attribution_note"),
                "brand_subject_provider_labels_observed": brand_summary.get(
                    "provider_labels"
                )
                or [],
                "brand_subject_conduct_summary": brand_summary,
                "carrier_conduct_summary": carrier_summary,
                "evidence": [relation.get("evidence") or {}],
            }
        )

    return {
        "artifact": "v2_sandbox_brand_conduct_evidence",
        "version": "2.0-compat-from-market-relationship-conduct-1",
        "generated_at": payload.get("generated_at") or _utc_now(),
        "status": "compatibility_projection",
        "assessment_role": "legacy_sandbox_projection_only",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "ordinary_ranking_effect": "none",
        "source": source,
        "policy": {
            "authoritative_relationship_model": "v2_market_relationship_conduct_evidence",
            "sandbox_is_regulatory_attribute_only": True,
            "sandbox_never_enters_ordinary_ranking": True,
            "distributor_complaints_remain_with_distributor": True,
            "cnpj_precedes_brand_label_for_subject_attribution": True,
            "fuzzy_brand_complaint_attribution": False,
        },
        "summary": {
            "sandbox_entities": len(carriers),
            "verified_brand_contexts": len(brands),
        },
        "carriers": carriers,
        "brands": brands,
    }


def build_from_files() -> tuple[dict[str, Any], dict[str, Any]]:
    lifecycle = json.loads(LIFECYCLE_PATH.read_text(encoding="utf-8"))
    identity = json.loads(IDENTITY_PATH.read_text(encoding="utf-8"))
    payload = build_market_relationship_conduct_evidence(lifecycle, identity)
    legacy = build_legacy_sandbox_projection(payload)
    return payload, legacy


def main() -> None:
    payload, legacy = build_from_files()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    LEGACY_SANDBOX_OUTPUT_PATH.write_text(
        json.dumps(legacy, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "summary": payload["summary"],
                "legacy_sandbox_projection": legacy["summary"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")
    print(f"compatibility projection written to {LEGACY_SANDBOX_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
