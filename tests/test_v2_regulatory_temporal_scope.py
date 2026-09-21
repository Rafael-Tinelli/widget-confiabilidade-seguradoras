from __future__ import annotations

import pytest

from api.v2.regulatory_temporal_scope import (
    RegulatoryTemporalScopeError,
    filter_period_mapping_for_ordinary_scope,
    filter_period_set_for_ordinary_scope,
    period_scope,
    sandbox_graduation_boundary,
)


def _graduated_entity(observed_at: str = "2026-09-21T16:20:00Z") -> dict:
    return {
        "entity_id": "fip:001988",
        "fip_code": "001988",
        "evidence": {
            "sandbox_history": {
                "current_outcome": "graduated_to_ordinary_license",
                "events": [
                    {
                        "event_id": "sandbox-event:lti",
                        "event_type": "sandbox_graduated_to_ordinary_license",
                        "observed_at": observed_at,
                        "fip_code": "001988",
                    }
                ],
            }
        },
    }


def test_graduation_boundary_starts_on_next_full_calendar_month() -> None:
    boundary = sandbox_graduation_boundary(_graduated_entity())

    assert boundary is not None
    assert boundary["basis"] == "observed_at"
    assert boundary["transition_period"] == 202609
    assert boundary["first_full_ordinary_period"] == 202610


def test_effective_at_precedes_observed_at_when_available() -> None:
    entity = _graduated_entity()
    event = entity["evidence"]["sandbox_history"]["events"][0]
    event["effective_at"] = "2026-08-31T00:00:00-03:00"

    boundary = sandbox_graduation_boundary(entity)

    assert boundary is not None
    assert boundary["basis"] == "effective_at"
    assert boundary["transition_period"] == 202608
    assert boundary["first_full_ordinary_period"] == 202609


def test_pre_graduation_and_transition_month_are_not_ordinary_comparable() -> None:
    entity = _graduated_entity()

    assert period_scope(entity, 202606)["scope"] == "historical_sandbox_period"
    assert period_scope(entity, 202606)["ordinary_comparison_eligible"] is False
    assert period_scope(entity, 202609)["scope"] == "transition_month_excluded"
    assert period_scope(entity, 202609)["ordinary_comparison_eligible"] is False
    assert period_scope(entity, 202610)["scope"] == "ordinary_post_graduation_full_month"
    assert period_scope(entity, 202610)["ordinary_comparison_eligible"] is True


def test_period_filters_keep_only_full_post_graduation_months() -> None:
    entity = _graduated_entity()
    assert filter_period_set_for_ordinary_scope(
        entity, {202608, 202609, 202610, 202611}
    ) == {202610, 202611}
    assert filter_period_mapping_for_ordinary_scope(
        entity,
        {
            202608: {"value": 1},
            202609: {"value": 2},
            202610: {"value": 3},
        },
    ) == {202610: {"value": 3}}


def test_graduated_entity_without_event_fails_closed() -> None:
    entity = _graduated_entity()
    entity["evidence"]["sandbox_history"]["events"] = []

    with pytest.raises(RegulatoryTemporalScopeError):
        sandbox_graduation_boundary(entity)
