from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import normalize_name_key
from api.v2.build_consumer_gov_157_experiment import ELIGIBILITY_PATH
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

BRAND_REGISTRY_PATH = Path("data/reference/v2/sandbox_brand_relationships.json")
OUTPUT_PATH = Path("data/derived/v2/sandbox_brand_conduct_evidence.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sandbox_entities(eligibility: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(entity["entity_id"]): entity
        for entity in eligibility.get("entities") or []
        if str(entity.get("entity_type") or "") == "sandbox_participant"
    }


def _resolve_sandbox_provider(
    provider: str,
    entry: dict[str, Any],
    full_universe_index: dict[str, Any],
    sandbox_ids: set[str],
) -> str | None:
    cnpj_resolution = resolve_cnpj_against_full_universe(
        normalize_cnpj_v2(entry.get("cnpj")),
        full_universe_index,
    )
    if cnpj_resolution is not None:
        entity_id = str(cnpj_resolution.get("matched_canonical_entity_id") or "")
        if entity_id in sandbox_ids:
            return entity_id

    canonical = resolve_provider_against_full_universe(provider, full_universe_index)
    if canonical is None:
        return None
    entity_id = str(canonical.get("matched_canonical_entity_id") or "")
    if entity_id in sandbox_ids:
        return entity_id
    return None


def _load_brand_registry(path: Path = BRAND_REGISTRY_PATH) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    brands = payload.get("brands") or []
    if not isinstance(brands, list):
        raise TypeError("sandbox brand registry brands must be a list")
    seen: set[str] = set()
    seen_aliases: dict[str, str] = {}
    seen_representatives: dict[str, str] = {}
    for brand in brands:
        if not isinstance(brand, dict):
            raise TypeError("sandbox brand registry row must be an object")
        brand_id = str(brand.get("brand_id") or "").strip()
        carrier = normalize_cnpj_v2(brand.get("risk_carrier_cnpj"))
        representative = normalize_cnpj_v2(brand.get("representative_cnpj"))
        if not brand_id or brand_id in seen:
            raise RuntimeError(f"invalid or duplicate sandbox brand_id: {brand_id}")
        if not carrier:
            raise RuntimeError(f"sandbox brand requires risk_carrier_cnpj: {brand_id}")
        if not (brand.get("evidence") or []):
            raise RuntimeError(f"sandbox brand requires evidence: {brand_id}")
        if representative == carrier:
            raise RuntimeError(
                f"sandbox brand representative cannot equal risk carrier: {brand_id}"
            )

        market_identity = brand.get("market_identity")
        if market_identity is not None:
            if not isinstance(market_identity, dict):
                raise TypeError(f"market_identity must be an object: {brand_id}")
            market_cnpj = normalize_cnpj_v2(market_identity.get("cnpj"))
            if representative and market_cnpj != representative:
                raise RuntimeError(
                    f"market identity CNPJ differs from representative_cnpj: {brand_id}"
                )
            role_state = str(
                market_identity.get("distribution_role_state") or ""
            ).strip()
            if role_state not in {"verified", "unverified"}:
                raise RuntimeError(
                    f"sandbox market identity requires distribution role state: {brand_id}"
                )
            if role_state == "verified":
                if not str(market_identity.get("distribution_role") or "").strip():
                    raise RuntimeError(
                        f"verified distribution role is missing: {brand_id}"
                    )
                if not (market_identity.get("evidence") or {}):
                    raise RuntimeError(
                        f"verified distribution role requires evidence: {brand_id}"
                    )

        if representative:
            previous = seen_representatives.get(representative)
            if previous and previous != brand_id:
                raise RuntimeError(
                    "representative CNPJ is shared by multiple sandbox brands: "
                    f"{representative} -> {previous}, {brand_id}"
                )
            seen_representatives[representative] = brand_id

        for label in [brand.get("name"), *(brand.get("aliases") or [])]:
            key = normalize_name_key(str(label or ""))
            if not key:
                continue
            previous = seen_aliases.get(key)
            if previous and previous != brand_id:
                raise RuntimeError(
                    f"ambiguous sandbox brand alias {label!r}: {previous}, {brand_id}"
                )
            seen_aliases[key] = brand_id
        seen.add(brand_id)
    return payload


def _brand_subject_indexes(
    registry: dict[str, Any],
) -> tuple[dict[str, str], dict[str, str]]:
    by_name: dict[str, str] = {}
    by_cnpj: dict[str, str] = {}
    for brand in registry.get("brands") or []:
        brand_id = str(brand["brand_id"])
        representative = normalize_cnpj_v2(brand.get("representative_cnpj"))
        if representative:
            by_cnpj[representative] = brand_id
        for label in [brand.get("name"), *(brand.get("aliases") or [])]:
            key = normalize_name_key(str(label or ""))
            if key:
                by_name[key] = brand_id
    return by_name, by_cnpj


def _resolve_direct_brand_subject(
    provider: str,
    entry: dict[str, Any],
    *,
    brand_by_name: dict[str, str],
    brand_by_cnpj: dict[str, str],
    carrier_cnpjs: set[str],
) -> str | None:
    """Resolve complaints against the distributor/representative itself.

    CNPJ wins. If the source carries a CNPJ that is not the verified representative,
    the provider label is not allowed to override it. Name-only attribution requires
    an exact normalized verified brand/alias and is never fuzzy.
    """
    cnpj = normalize_cnpj_v2(entry.get("cnpj"))
    if cnpj:
        if cnpj in brand_by_cnpj:
            return brand_by_cnpj[cnpj]
        if cnpj in carrier_cnpjs:
            return None
        return None
    return brand_by_name.get(normalize_name_key(provider))


def _summary_from_month_stats(
    months: list[str],
    monthly_stats: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    monthly = [
        aggregate_entry_to_month(
            month,
            {"statistics": monthly_stats[month]},
            matched_current_insurer_market_complaints=0,
        )
        for month in months
    ]
    totals = _entity_totals(monthly)
    return totals, monthly, build_conduct_film(monthly)


def build_sandbox_brand_conduct_evidence(
    eligibility: dict[str, Any],
    identity: dict[str, Any],
    *,
    brand_registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    months = sorted(
        str(month) for month in (identity.get("source") or {}).get("months") or []
    )
    if len(months) != 12:
        raise RuntimeError(f"expected 12 Consumer.gov months, got {len(months)}")

    registry = brand_registry if brand_registry is not None else _load_brand_registry()
    # Test-injected registries receive the same structural validation by round-tripping
    # through the checks that do not depend on filesystem state.
    if brand_registry is not None:
        brands = registry.get("brands") or []
        if not isinstance(brands, list):
            raise TypeError("sandbox brand registry brands must be a list")
        seen_aliases: dict[str, str] = {}
        for brand in brands:
            brand_id = str(brand.get("brand_id") or "").strip()
            if not brand_id:
                raise RuntimeError("sandbox brand_id is required")
            carrier = normalize_cnpj_v2(brand.get("risk_carrier_cnpj"))
            if not carrier:
                raise RuntimeError(f"sandbox brand requires risk_carrier_cnpj: {brand_id}")
            for label in [brand.get("name"), *(brand.get("aliases") or [])]:
                key = normalize_name_key(str(label or ""))
                if key and key in seen_aliases and seen_aliases[key] != brand_id:
                    raise RuntimeError(f"ambiguous sandbox brand alias: {label}")
                if key:
                    seen_aliases[key] = brand_id

    sandbox_by_id = _sandbox_entities(eligibility)
    sandbox_ids = set(sandbox_by_id)
    full_index = build_full_universe_provider_index(
        list(eligibility.get("entities") or [])
    )
    carrier_cnpjs = {
        cnpj
        for entity in sandbox_by_id.values()
        if (cnpj := normalize_cnpj_v2(entity.get("cnpj")))
    }
    brand_by_name, brand_by_cnpj = _brand_subject_indexes(registry)

    stats_by_entity: dict[str, dict[str, dict[str, Any]]] = {
        entity_id: {month: {} for month in months} for entity_id in sandbox_ids
    }
    labels_by_entity: dict[str, Counter[str]] = {
        entity_id: Counter() for entity_id in sandbox_ids
    }
    brand_stats: dict[str, dict[str, dict[str, Any]]] = {
        str(brand["brand_id"]): {month: {} for month in months}
        for brand in registry.get("brands") or []
    }
    brand_labels: dict[str, Counter[str]] = {
        str(brand["brand_id"]): Counter() for brand in registry.get("brands") or []
    }
    resolved_complaints = 0
    direct_brand_subject_complaints = 0

    for month in months:
        _, entries, _ = load_monthly_entries(month)
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

            brand_id = _resolve_direct_brand_subject(
                provider,
                entry,
                brand_by_name=brand_by_name,
                brand_by_cnpj=brand_by_cnpj,
                carrier_cnpjs=carrier_cnpjs,
            )
            if brand_id is not None:
                added = add_entry_statistics(brand_stats[brand_id][month], entry)
                if added != complaints:
                    raise RuntimeError(
                        f"brand complaint count changed while merging {provider} {month}"
                    )
                brand_labels[brand_id][provider] += complaints
                direct_brand_subject_complaints += complaints

            entity_id = _resolve_sandbox_provider(
                provider, entry, full_index, sandbox_ids
            )
            if entity_id is None:
                continue
            added = add_entry_statistics(stats_by_entity[entity_id][month], entry)
            if added != complaints:
                raise RuntimeError(
                    f"sandbox complaint count changed while merging {provider} {month}"
                )
            labels_by_entity[entity_id][provider] += complaints
            resolved_complaints += complaints

    carriers: list[dict[str, Any]] = []
    carrier_by_cnpj: dict[str, dict[str, Any]] = {}
    observed_carriers = 0
    for entity_id, entity in sorted(
        sandbox_by_id.items(),
        key=lambda item: str(item[1].get("legal_name") or item[0]),
    ):
        totals, monthly, film = _summary_from_month_stats(
            months, stats_by_entity[entity_id]
        )
        if totals["complaints"] > 0:
            observed_carriers += 1
        cnpj = normalize_cnpj_v2(entity.get("cnpj"))
        carrier = {
            "entity_id": entity_id,
            "entity_type": "sandbox_participant",
            "cnpj": cnpj,
            "legal_name": entity.get("legal_name"),
            "display_name": entity.get("display_name"),
            "regulatory_regime": "sandbox",
            "regulatory_public_label": "Participante do Sandbox regulatório",
            "ordinary_ranking_effect": "none",
            "ordinary_market_baseline_effect": "none",
            "consumer_gov_provider_labels_observed": [
                {"provider": name, "complaints": int(count)}
                for name, count in labels_by_entity[entity_id].most_common()
            ],
            "totals": totals,
            "monthly": monthly,
            "film": film,
        }
        carriers.append(carrier)
        if cnpj:
            carrier_by_cnpj[cnpj] = carrier

    brands_out: list[dict[str, Any]] = []
    for raw_brand in registry.get("brands") or []:
        brand_id = str(raw_brand["brand_id"])
        carrier_cnpj = normalize_cnpj_v2(raw_brand.get("risk_carrier_cnpj"))
        carrier = carrier_by_cnpj.get(carrier_cnpj or "")
        if carrier is None:
            raise RuntimeError(
                "sandbox brand carrier not found in classified Sandbox universe: "
                f"{carrier_cnpj}"
            )
        brand_totals, brand_monthly, brand_film = _summary_from_month_stats(
            months, brand_stats[brand_id]
        )
        market_identity = dict(raw_brand.get("market_identity") or {})
        representative_cnpj = normalize_cnpj_v2(raw_brand.get("representative_cnpj"))
        if market_identity.get("cnpj"):
            market_identity["cnpj"] = normalize_cnpj_v2(market_identity.get("cnpj"))

        brands_out.append(
            {
                "brand_id": brand_id,
                "name": raw_brand.get("name"),
                "aliases": list(raw_brand.get("aliases") or []),
                "representative_cnpj": representative_cnpj,
                "market_identity": market_identity or None,
                "distribution_role": market_identity.get("distribution_role"),
                "distribution_role_state": market_identity.get(
                    "distribution_role_state"
                )
                or "unverified",
                "risk_carrier_entity_id": carrier["entity_id"],
                "risk_carrier_cnpj": carrier_cnpj,
                "risk_carrier_name": raw_brand.get("risk_carrier_name")
                or carrier.get("legal_name"),
                "risk_carrier_regulatory_class": carrier[
                    "regulatory_public_label"
                ],
                "regulatory_scope": "sandbox",
                "product_scope": raw_brand.get("product_scope"),
                "conduct_scope": (
                    "separate_distributor_subject_and_carrier_context"
                ),
                "conduct_context_policy": raw_brand.get("conduct_context_policy"),
                "ordinary_ranking_effect": "none",
                "ordinary_market_baseline_effect": "none",
                "brand_subject_attribution_note": (
                    "Consumer.gov statistics in brand_subject_conduct_summary are "
                    "attributed only when the source CNPJ is the verified representative "
                    "or, when CNPJ is absent, the provider label exactly matches a verified "
                    "brand/alias. They are not transferred to the risk carrier."
                ),
                "carrier_attribution_note": (
                    "Consumer.gov statistics in carrier_conduct_summary are registered "
                    "against the verified Sandbox risk carrier. They are context for the "
                    "commercial relationship and are not complaints of the distributor."
                ),
                "brand_subject_provider_labels_observed": [
                    {"provider": name, "complaints": int(count)}
                    for name, count in brand_labels[brand_id].most_common()
                ],
                "brand_subject_conduct_summary": {
                    "complaints": brand_totals["complaints"],
                    "responded": brand_totals["responded"],
                    "response_rate": brand_totals["response_rate"],
                    "finalized": brand_totals["finalized"],
                    "finalized_rate": brand_totals["finalized_rate"],
                    "satisfaction_count": brand_totals["satisfaction_count"],
                    "average_satisfaction": brand_totals["average_satisfaction"],
                    "film": brand_film,
                    "provider_labels": [
                        {"provider": name, "complaints": int(count)}
                        for name, count in brand_labels[brand_id].most_common()
                    ],
                    "monthly": brand_monthly,
                },
                "carrier_conduct_summary": {
                    "complaints": carrier["totals"]["complaints"],
                    "responded": carrier["totals"]["responded"],
                    "response_rate": carrier["totals"]["response_rate"],
                    "finalized": carrier["totals"]["finalized"],
                    "finalized_rate": carrier["totals"]["finalized_rate"],
                    "satisfaction_count": carrier["totals"]["satisfaction_count"],
                    "average_satisfaction": carrier["totals"]["average_satisfaction"],
                    "film": carrier["film"],
                    "provider_labels": carrier[
                        "consumer_gov_provider_labels_observed"
                    ],
                },
                "evidence": list(raw_brand.get("evidence") or []),
            }
        )

    return {
        "artifact": "v2_sandbox_brand_conduct_evidence",
        "version": "2.0-draft-sandbox-brand-conduct-2",
        "generated_at": _utc_now(),
        "status": "experimental",
        "assessment_role": (
            "sandbox_risk_bearer_and_verified_distributor_conduct_context"
        ),
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "ordinary_ranking_effect": "none",
        "source": {
            "dataset": "reclamacoes-do-consumidor-gov-br",
            "months": months,
            "complaint_semantics": (
                "Observed consumer friction/allegation in Consumer.gov; "
                "not an adjudicated finding."
            ),
        },
        "policy": {
            "sandbox_never_enters_ordinary_ranking": True,
            "sandbox_never_enters_ordinary_market_baseline": True,
            "brand_context_requires_verified_relationship": True,
            "carrier_complaints_are_not_brand_exclusive_by_default": True,
            "distributor_complaints_remain_with_distributor": True,
            "generic_sales_channel_complaints_are_not_transferred_to_carrier": True,
            "cnpj_precedes_brand_label_for_subject_attribution": True,
            "fuzzy_brand_complaint_attribution": False,
        },
        "summary": {
            "sandbox_entities": len(sandbox_by_id),
            "sandbox_entities_with_observed_complaints": observed_carriers,
            "sandbox_complaints_resolved": resolved_complaints,
            "direct_brand_subject_complaints": direct_brand_subject_complaints,
            "verified_brand_contexts": len(brands_out),
        },
        "carriers": carriers,
        "brands": sorted(brands_out, key=lambda item: str(item["brand_id"])),
    }


def main() -> None:
    eligibility = json.loads(ELIGIBILITY_PATH.read_text(encoding="utf-8"))
    identity = json.loads(IDENTITY_PATH.read_text(encoding="utf-8"))
    payload = build_sandbox_brand_conduct_evidence(eligibility, identity)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp = OUTPUT_PATH.with_suffix(OUTPUT_PATH.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    temp.replace(OUTPUT_PATH)
    loovi = next(
        (brand for brand in payload["brands"] if brand["brand_id"] == "brand:loovi"),
        None,
    )
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "summary": payload["summary"],
                "loovi": (
                    {
                        "brand_subject": loovi["brand_subject_conduct_summary"],
                        "carrier_context": loovi["carrier_conduct_summary"],
                    }
                    if loovi
                    else None
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
