from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import normalize_name_key
from api.v2.build_consumer_gov_conduct_evidence import _entity_totals, _provider_name
from api.v2.consumer_gov_conduct import build_conduct_film
from api.v2.consumer_gov_conduct_core import (
    add_entry_statistics,
    aggregate_entry_to_month,
)
from api.v2.consumer_gov_universe_resolution import (
    build_full_universe_provider_index,
    resolve_cnpj_against_full_universe,
    resolve_provider_against_full_universe,
)


class MarketRelationshipConductError(RuntimeError):
    """Raised when verified market relationships cannot be projected safely."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def regulatory_label(entity: dict[str, Any]) -> str:
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


def summarize_months(
    months: list[str], monthly_stats: dict[str, dict[str, Any]]
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


def verified_market_identity(brand: dict[str, Any]) -> dict[str, Any] | None:
    market = brand.get("market_identity")
    if not isinstance(market, dict) or not market:
        return None
    out = dict(market)
    cnpj = normalize_cnpj_v2(out.get("cnpj"))
    if cnpj:
        out["cnpj"] = cnpj
    verified = bool(
        cnpj
        and str(out.get("legal_name") or "").strip()
        and (out.get("evidence") or {})
    )
    out["identity_state"] = str(out.get("identity_state") or "").strip() or (
        "verified" if verified else "unverified"
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


def current_risk_relationships(brand: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(relation)
        for relation in brand.get("relationships") or []
        if relation.get("relationship_type") == "risk_carrier"
        and str(relation.get("status") or "current") == "current"
        and relation.get("target_entity_id")
    ]


def market_identity_key(market: dict[str, Any], brand_id: str) -> str:
    cnpj = normalize_cnpj_v2(market.get("cnpj"))
    if cnpj:
        return f"cnpj:{cnpj}"
    legal = normalize_name_key(str(market.get("legal_name") or ""))
    if legal:
        return f"legal:{legal}"
    return f"brand:{brand_id}"


def build_relationship_model(lifecycle: dict[str, Any]) -> tuple:
    entities = {
        str(entity["entity_id"]): entity
        for entity in lifecycle.get("entities") or []
        if isinstance(entity, dict) and entity.get("entity_id")
    }
    brands: list[dict[str, Any]] = []
    identities: dict[str, dict[str, Any]] = {}
    aliases: dict[str, set[str]] = {}

    for raw_brand in lifecycle.get("brands") or []:
        if not isinstance(raw_brand, dict):
            continue
        brand_id = str(raw_brand.get("brand_id") or "").strip()
        market = verified_market_identity(raw_brand)
        relations = current_risk_relationships(raw_brand)
        if not brand_id or market is None or not relations:
            continue
        identity_key = market_identity_key(market, brand_id)
        identities.setdefault(identity_key, market)
        aliases.setdefault(identity_key, set())
        for label in [
            raw_brand.get("name"),
            *(raw_brand.get("aliases") or []),
            market.get("trade_name"),
            market.get("legal_name"),
        ]:
            key = normalize_name_key(str(label or ""))
            if key:
                aliases[identity_key].add(key)

        bearers = []
        for relation in relations:
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
            bearers.append(
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
                    "regulatory_class": regulatory_label(entity),
                    "sandbox_history": (
                        dict((entity.get("evidence") or {}).get("sandbox_history") or {})
                        or None
                    ),
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
                "risk_bearers": bearers,
            }
        )
    return entities, brands, identities, aliases


def subject_indexes(identities: dict, aliases: dict) -> tuple[dict[str, str], dict[str, str]]:
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
        for alias in aliases.get(identity_key, set()):
            alias_owners.setdefault(alias, set()).add(identity_key)
    by_name = {
        alias: next(iter(owners))
        for alias, owners in alias_owners.items()
        if len(owners) == 1
    }
    return by_name, by_cnpj


def resolve_market_subject(provider: str, entry: dict, by_name: dict, by_cnpj: dict):
    cnpj = normalize_cnpj_v2(entry.get("cnpj"))
    if cnpj:
        return by_cnpj.get(cnpj)
    return by_name.get(normalize_name_key(provider))


def resolve_entity_subject(provider: str, entry: dict, index: dict) -> str | None:
    resolved = resolve_cnpj_against_full_universe(
        normalize_cnpj_v2(entry.get("cnpj")), index
    )
    if resolved is None:
        resolved = resolve_provider_against_full_universe(provider, index)
    if resolved is None:
        return None
    return str(resolved.get("matched_canonical_entity_id") or "") or None


def build_market_relationship_conduct_evidence(
    lifecycle: dict[str, Any],
    identity: dict[str, Any],
    *,
    entry_loader: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    months = sorted(
        str(month) for month in (identity.get("source") or {}).get("months") or []
    )
    if len(months) != 12:
        raise MarketRelationshipConductError(
            f"expected 12 Consumer.gov months, got {len(months)}"
        )

    entities, brands, identities, aliases = build_relationship_model(lifecycle)
    by_name, by_cnpj = subject_indexes(identities, aliases)
    full_index = build_full_universe_provider_index(list(entities.values()))
    carrier_ids = {
        bearer["entity_id"] for brand in brands for bearer in brand["risk_bearers"]
    }
    sandbox_ids = {
        entity_id
        for entity_id, entity in entities.items()
        if entity.get("regulatory_regime") == "sandbox"
        or entity.get("entity_type") == "sandbox_participant"
    }
    tracked_ids = carrier_ids | sandbox_ids
    identity_stats = {
        key: {month: {} for month in months} for key in identities
    }
    identity_labels = {key: Counter() for key in identities}
    entity_stats = {
        key: {month: {} for month in months} for key in tracked_ids
    }
    entity_labels = {key: Counter() for key in tracked_ids}
    direct_count = 0
    carrier_count = 0

    for month in months:
        for raw_key, raw_entry in entry_loader(month).items():
            entry = raw_entry if isinstance(raw_entry, dict) else {}
            provider = _provider_name(entry, str(raw_key))
            stats = entry.get("statistics") if isinstance(entry.get("statistics"), dict) else {}
            complaints = int(stats.get("complaintsCount") or stats.get("total_claims") or 0)
            if not provider or complaints <= 0:
                continue
            identity_key = resolve_market_subject(provider, entry, by_name, by_cnpj)
            if identity_key is not None:
                if add_entry_statistics(identity_stats[identity_key][month], entry) != complaints:
                    raise MarketRelationshipConductError("market subject count changed")
                identity_labels[identity_key][provider] += complaints
                direct_count += complaints
                continue
            entity_id = resolve_entity_subject(provider, entry, full_index)
            if entity_id is None or entity_id not in tracked_ids:
                continue
            if add_entry_statistics(entity_stats[entity_id][month], entry) != complaints:
                raise MarketRelationshipConductError("risk bearer count changed")
            entity_labels[entity_id][provider] += complaints
            carrier_count += complaints

    identity_summaries = {}
    market_identities = []
    for key, market in sorted(identities.items()):
        summary = summarize_months(months, identity_stats[key])
        summary["provider_labels"] = [
            {"provider": name, "complaints": int(count)}
            for name, count in identity_labels[key].most_common()
        ]
        identity_summaries[key] = summary
        market_identities.append(
            {"market_identity_key": key, "identity": market, "conduct_summary": summary}
        )

    bearer_summaries = {}
    risk_bearers = []
    for entity_id in sorted(tracked_ids):
        entity = entities[entity_id]
        summary = summarize_months(months, entity_stats[entity_id])
        summary["provider_labels"] = [
            {"provider": name, "complaints": int(count)}
            for name, count in entity_labels[entity_id].most_common()
        ]
        bearer_summaries[entity_id] = summary
        risk_bearers.append(
            {
                "entity_id": entity_id,
                "profile_id": f"entity:{entity_id}",
                "cnpj": normalize_cnpj_v2(entity.get("cnpj")),
                "name": entity.get("display_name") or entity.get("legal_name") or entity_id,
                "entity_type": entity.get("entity_type"),
                "regulatory_regime": entity.get("regulatory_regime"),
                "regulatory_status": entity.get("regulatory_status"),
                "regulatory_class": regulatory_label(entity),
                "sandbox_history": (
                    dict((entity.get("evidence") or {}).get("sandbox_history") or {})
                    or None
                ),
                "conduct_summary": summary,
            }
        )

    brands_out = []
    for brand in brands:
        relationships = []
        for bearer in brand["risk_bearers"]:
            relationships.append(
                {
                    **bearer,
                    "conduct_summary": bearer_summaries[bearer["entity_id"]],
                }
            )
        brands_out.append(
            {
                **brand,
                "distribution_role": brand["market_identity"].get("distribution_role"),
                "distribution_role_state": brand["market_identity"].get(
                    "distribution_role_state"
                )
                or "unverified",
                "market_identity_conduct_summary": identity_summaries[
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
        "generated_at": utc_now(),
        "status": "experimental",
        "assessment_role": "verified_market_identity_and_risk_bearer_conduct_context",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "ordinary_ranking_effect": "none",
        "source": {
            "dataset": "reclamacoes-do-consumidor-gov-br",
            "months": months,
            "complaint_semantics": "Observed consumer friction/allegation; not an adjudicated finding.",
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
            "referenced_risk_bearers": len(carrier_ids),
            "direct_market_subject_complaints": direct_count,
            "resolved_risk_bearer_complaints": carrier_count,
        },
        "market_identities": market_identities,
        "risk_bearers": risk_bearers,
        "brands": sorted(brands_out, key=lambda row: str(row["brand_id"])),
    }
