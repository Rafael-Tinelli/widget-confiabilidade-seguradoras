from __future__ import annotations

import pytest

from api.intelligence import apply_intelligence_batch


def reputation_payload() -> dict[str, object]:
    return {
        "name": "Seguradora Exemplo",
        "statistics": {
            "complaintsCount": 30,
            "respondedCount": 28,
            "resolvedCount": 18,
            "finalizedCount": 30,
            "scoreSum": 80.0,
            "satisfactionCount": 20,
            "averageScore": 4.0,
        },
    }


def insurer_payload(
    *,
    premiums: float,
    reputation: dict[str, object] | None,
    opin: bool = False,
) -> dict[str, object]:
    return {
        "id": "000001",
        "name": "SEGURADORA EXEMPLO S.A.",
        "products": [],
        "flags": {"openInsuranceParticipant": opin},
        "reputation": reputation,
        "components": {
            "reputation": reputation,
            "financials": {
                "premiums": premiums,
                "claims": 20_000_000.0,
                "net_worth": 200_000_000.0,
            },
        },
        "data": {
            "premiums": premiums,
            "claims": 20_000_000.0,
            "net_worth": 200_000_000.0,
        },
    }


def test_reputation_match_is_applied_when_premiums_are_available() -> None:
    insurer = insurer_payload(
        premiums=100_000_000.0,
        reputation=reputation_payload(),
    )

    result = apply_intelligence_batch([insurer])[0]
    data = result["data"]

    assert data["availability"]["reputationMatched"] is True
    assert data["availability"]["reputationApplied"] is True
    assert data["availability"]["reputationReason"] == "applied"
    assert data["contributions"]["reputation"] > 0
    assert data["score"] == pytest.approx(data["contributions"]["total"])
    assert data["score"] == pytest.approx(
        data["contributions"]["solvency"]
        + data["contributions"]["reputation"]
        + data["contributions"]["innovation"]
    )


def test_reputation_match_is_not_applied_without_premiums() -> None:
    insurer = insurer_payload(
        premiums=0.0,
        reputation=reputation_payload(),
    )

    result = apply_intelligence_batch([insurer])[0]
    data = result["data"]

    assert data["availability"]["reputationMatched"] is True
    assert data["availability"]["reputationApplied"] is False
    assert data["availability"]["reputationReason"] == "insufficient_premiums"
    assert data["contributions"]["reputation"] == 0
    assert data["score"] == pytest.approx(
        data["contributions"]["solvency"]
        + data["contributions"]["innovation"]
    )


def test_missing_reputation_is_explicit() -> None:
    insurer = insurer_payload(premiums=100_000_000.0, reputation=None)

    result = apply_intelligence_batch([insurer])[0]
    availability = result["data"]["availability"]

    assert availability["reputationMatched"] is False
    assert availability["reputationApplied"] is False
    assert availability["reputationReason"] == "missing_reputation"
    assert result["data"]["contributions"]["reputation"] == 0


def test_open_insurance_flag_controls_innovation_contribution() -> None:
    non_participant = insurer_payload(
        premiums=100_000_000.0,
        reputation=None,
        opin=False,
    )
    participant = insurer_payload(
        premiums=100_000_000.0,
        reputation=None,
        opin=True,
    )
    participant["id"] = "000002"

    first, second = apply_intelligence_batch([non_participant, participant])

    assert first["data"]["innovationScore"] == 60.0
    assert first["data"]["contributions"]["innovation"] == pytest.approx(9.0)
    assert second["data"]["innovationScore"] == 80.0
    assert second["data"]["contributions"]["innovation"] == pytest.approx(12.0)
    assert second["data"]["availability"]["openInsuranceParticipant"] is True


def test_empty_reputation_dataset_disables_the_pillar_for_the_batch() -> None:
    empty_reputation = {
        "statistics": {
            "complaintsCount": 0,
            "respondedCount": 0,
            "resolvedCount": 0,
            "satisfactionCount": 0,
        }
    }
    insurer = insurer_payload(
        premiums=100_000_000.0,
        reputation=empty_reputation,
    )

    result = apply_intelligence_batch([insurer])[0]
    data = result["data"]

    assert data["availability"]["reputationMatched"] is True
    assert data["availability"]["reputationApplied"] is False
    assert data["availability"]["reputationReason"] == "dataset_disabled"
    assert data["weights"] == {
        "solvency": 0.6,
        "reputation": 0.0,
        "innovation": 0.4,
    }
