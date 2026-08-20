from __future__ import annotations

from calendar import monthrange
from collections import defaultdict
from datetime import date
from typing import Any

from api.utils.name_cleaner import normalize_name_key

ALLOWED_RELATIONSHIP_TYPES = {"brand_of", "risk_carrier"}


class TemporalBrandResolutionError(ValueError):
    """Raised when a brand relationship carries invalid temporal metadata."""


def _parse_date(value: Any, *, field: str, brand_id: str) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise TemporalBrandResolutionError(
            f"invalid {field} for {brand_id}: {text}"
        ) from exc


def _month_bounds(month: str) -> tuple[date, date]:
    try:
        year_text, month_text = month.split("-", 1)
        year = int(year_text)
        month_number = int(month_text)
        start = date(year, month_number, 1)
    except (TypeError, ValueError) as exc:
        raise TemporalBrandResolutionError(f"invalid month: {month}") from exc
    end = date(year, month_number, monthrange(year, month_number)[1])
    return start, end


def build_temporal_brand_index(
    brands: list[dict[str, Any]],
    eligible_entity_ids: set[str],
) -> dict[str, Any]:
    """Build an exact brand-name index while preserving relationship validity windows.

    Brand aliases are never collapsed into a timeless entity mapping. A relationship
    may be used for a monthly aggregate only when it covers the entire month.
    """
    by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    display_names: set[str] = set()

    for brand in brands:
        brand_id = str(brand.get("brand_id") or "").strip()
        names = [brand.get("name"), *(brand.get("aliases") or [])]
        clean_names = sorted(
            {
                str(name).strip()
                for name in names
                if str(name or "").strip()
            }
        )
        if not brand_id or not clean_names:
            continue

        for relation in brand.get("relationships") or []:
            if not isinstance(relation, dict):
                continue
            relationship_type = str(relation.get("relationship_type") or "").strip()
            target_entity_id = str(relation.get("target_entity_id") or "").strip()
            if relationship_type not in ALLOWED_RELATIONSHIP_TYPES:
                continue
            if target_entity_id not in eligible_entity_ids:
                continue

            effective_from = _parse_date(
                relation.get("effective_from") or relation.get("effective_date"),
                field="effective_from",
                brand_id=brand_id,
            )
            effective_until = _parse_date(
                relation.get("effective_until"),
                field="effective_until",
                brand_id=brand_id,
            )
            if (
                effective_from is not None
                and effective_until is not None
                and effective_until < effective_from
            ):
                raise TemporalBrandResolutionError(
                    f"effective_until precedes effective_from for {brand_id}"
                )

            record = {
                "brand_id": brand_id,
                "relationship_type": relationship_type,
                "target_entity_id": target_entity_id,
                "status": relation.get("status"),
                "effective_from": effective_from.isoformat() if effective_from else None,
                "effective_until": effective_until.isoformat() if effective_until else None,
            }
            for name in clean_names:
                display_names.add(name)
                by_name[normalize_name_key(name)].append(dict(record))

    return {
        "by_name": {
            key: sorted(
                rows,
                key=lambda item: (
                    str(item.get("effective_from") or ""),
                    str(item.get("effective_until") or ""),
                    str(item.get("target_entity_id") or ""),
                ),
            )
            for key, rows in sorted(by_name.items())
        },
        "display_names": sorted(display_names),
    }


def resolve_temporal_brand(
    provider_name: str,
    month: str,
    index: dict[str, Any],
) -> dict[str, Any] | None:
    """Resolve an exact brand alias for one monthly Consumer.gov aggregate.

    A relationship is accepted only if its validity window covers every day of the
    month. Mid-month starts/ends are deliberately left unresolved because the source
    snapshot is monthly and cannot safely split the complaints across carriers.
    """
    key = normalize_name_key(provider_name)
    relations = list((index.get("by_name") or {}).get(key) or [])
    if not relations:
        return None

    month_start, month_end = _month_bounds(month)
    full_month: list[dict[str, Any]] = []
    overlapping: list[dict[str, Any]] = []

    for relation in relations:
        effective_from = _parse_date(
            relation.get("effective_from"),
            field="effective_from",
            brand_id=str(relation.get("brand_id") or key),
        )
        effective_until = _parse_date(
            relation.get("effective_until"),
            field="effective_until",
            brand_id=str(relation.get("brand_id") or key),
        )
        starts_before_month_end = effective_from is None or effective_from <= month_end
        ends_after_month_start = effective_until is None or effective_until >= month_start
        if starts_before_month_end and ends_after_month_start:
            overlapping.append(relation)

        covers_start = effective_from is None or effective_from <= month_start
        covers_end = effective_until is None or effective_until >= month_end
        if covers_start and covers_end:
            full_month.append(relation)

    targets = {
        str(relation.get("target_entity_id") or "")
        for relation in full_month
        if relation.get("target_entity_id")
    }
    if len(targets) == 1:
        target = next(iter(targets))
        return {
            "resolution_state": "matched_current_insurer",
            "entity_id": target,
            "match_method": "verified_brand_exact_temporal",
            "provider_name": provider_name,
            "month": month,
            "relationships": full_month,
        }
    if len(targets) > 1:
        return {
            "resolution_state": "ambiguous",
            "entity_id": None,
            "match_method": "verified_brand_temporal_ambiguous",
            "provider_name": provider_name,
            "month": month,
            "relationships": full_month,
        }
    if overlapping:
        return {
            "resolution_state": "unresolved",
            "entity_id": None,
            "match_method": "verified_brand_partial_month_unresolved",
            "provider_name": provider_name,
            "month": month,
            "relationships": overlapping,
        }
    return {
        "resolution_state": "unresolved",
        "entity_id": None,
        "match_method": "verified_brand_out_of_window_unresolved",
        "provider_name": provider_name,
        "month": month,
        "relationships": relations,
    }
