from __future__ import annotations

from datetime import datetime
from typing import Any


class RegulatoryTemporalScopeError(ValueError):
    """Raised when a regulatory lifecycle boundary is malformed."""


def _period_from_iso(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        raise RegulatoryTemporalScopeError("regulatory transition timestamp is required")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RegulatoryTemporalScopeError(
            f"invalid regulatory transition timestamp: {value!r}"
        ) from exc
    return parsed.year * 100 + parsed.month


def next_period(period: int) -> int:
    year, month = divmod(int(period), 100)
    if year < 2000 or not 1 <= month <= 12:
        raise RegulatoryTemporalScopeError(f"invalid AAAAMM period: {period!r}")
    return (year + 1) * 100 + 1 if month == 12 else year * 100 + month + 1


def sandbox_graduation_boundary(entity: dict[str, Any]) -> dict[str, Any] | None:
    """Return a conservative monthly boundary for Sandbox -> ordinary graduation.

    Source evidence is monthly downstream, while the lifecycle observation is timestamped.
    The transition month is therefore never treated as a full ordinary month. The first
    comparison-eligible ordinary month is the following calendar month.

    If a future source provides an explicit effective_at timestamp, it takes precedence over
    observed_at. Until then, observed_at is the conservative fail-closed boundary.
    """

    history = (entity.get("evidence") or {}).get("sandbox_history")
    if not isinstance(history, dict):
        return None
    if history.get("current_outcome") != "graduated_to_ordinary_license":
        return None

    events = [
        dict(event)
        for event in history.get("events") or []
        if isinstance(event, dict)
        and event.get("event_type") == "sandbox_graduated_to_ordinary_license"
    ]
    if not events:
        raise RegulatoryTemporalScopeError(
            f"graduated Sandbox entity lacks graduation event: {entity.get('entity_id')}"
        )

    events.sort(key=lambda event: str(event.get("effective_at") or event.get("observed_at") or ""))
    event = events[0]
    basis_timestamp = event.get("effective_at") or event.get("observed_at")
    transition_period = _period_from_iso(basis_timestamp)
    return {
        "policy": "first_full_calendar_month_after_observed_or_effective_graduation",
        "basis": "effective_at" if event.get("effective_at") else "observed_at",
        "basis_timestamp": basis_timestamp,
        "transition_period": transition_period,
        "first_full_ordinary_period": next_period(transition_period),
        "event_id": event.get("event_id"),
        "fip_code": event.get("fip_code") or entity.get("fip_code"),
    }


def period_scope(entity: dict[str, Any], period: int) -> dict[str, Any]:
    boundary = sandbox_graduation_boundary(entity)
    if boundary is None:
        return {
            "ordinary_comparison_eligible": True,
            "scope": "ordinary_current_scope",
            "boundary": None,
        }

    period = int(period)
    transition = int(boundary["transition_period"])
    first_full = int(boundary["first_full_ordinary_period"])
    if period < transition:
        scope = "historical_sandbox_period"
        eligible = False
    elif period == transition:
        scope = "transition_month_excluded"
        eligible = False
    elif period < first_full:
        scope = "transition_month_excluded"
        eligible = False
    else:
        scope = "ordinary_post_graduation_full_month"
        eligible = True
    return {
        "ordinary_comparison_eligible": eligible,
        "scope": scope,
        "boundary": boundary,
    }


def filter_period_mapping_for_ordinary_scope(
    entity: dict[str, Any],
    mapping: dict[Any, Any],
) -> dict[Any, Any]:
    boundary = sandbox_graduation_boundary(entity)
    if boundary is None:
        return dict(mapping)
    first_full = int(boundary["first_full_ordinary_period"])
    return {
        key: value
        for key, value in mapping.items()
        if int(key) >= first_full
    }


def filter_period_set_for_ordinary_scope(
    entity: dict[str, Any],
    periods: set[int] | list[int] | tuple[int, ...],
) -> set[int]:
    boundary = sandbox_graduation_boundary(entity)
    values = {int(period) for period in periods}
    if boundary is None:
        return values
    first_full = int(boundary["first_full_ordinary_period"])
    return {period for period in values if period >= first_full}
