from __future__ import annotations

from api.v2.conduct_source_cascade import ConductSourceProbe, select_conduct_source


def _probe(source: str, **overrides: bool) -> ConductSourceProbe:
    values = {
        "current": True,
        "public": True,
        "structured": True,
        "consumable": True,
        "coverage_sufficient": True,
    }
    values.update(overrides)
    return ConductSourceProbe(source=source, **values)


def test_bdr_wins_when_all_sources_are_eligible() -> None:
    decision = select_conduct_source({
        "bdr_susepcon": _probe("bdr_susepcon"),
        "consumer_gov_basecompleta": _probe("consumer_gov_basecompleta"),
        "consumer_gov_core_plus_ses": _probe("consumer_gov_core_plus_ses"),
    })
    assert decision["selected_tier"] == 1
    assert decision["selected_source"] == "bdr_susepcon"


def test_stale_bdr_falls_to_current_basecompleta() -> None:
    decision = select_conduct_source({
        "bdr_susepcon": _probe("bdr_susepcon", current=False),
        "consumer_gov_basecompleta": _probe("consumer_gov_basecompleta"),
        "consumer_gov_core_plus_ses": _probe("consumer_gov_core_plus_ses"),
    })
    assert decision["selected_tier"] == 2
    assert decision["selected_source"] == "consumer_gov_basecompleta"
    assert decision["higher_priority_sources"]["bdr_susepcon"]["eligible"] is False


def test_missing_taxonomy_falls_to_core_plus_ses_without_stitching() -> None:
    decision = select_conduct_source({
        "bdr_susepcon": _probe("bdr_susepcon", current=False),
        "consumer_gov_basecompleta": _probe("consumer_gov_basecompleta", consumable=False),
        "consumer_gov_core_plus_ses": _probe("consumer_gov_core_plus_ses"),
    })
    assert decision["selected_tier"] == 3
    assert decision["selected_source"] == "consumer_gov_core_plus_ses"
    assert decision["methodology"] == "consumer_gov_rppa_proxy"
    assert decision["series_policy"] == "no_cross_source_stitching"


def test_no_valid_source_fails_closed() -> None:
    decision = select_conduct_source({
        "bdr_susepcon": _probe("bdr_susepcon", current=False),
        "consumer_gov_basecompleta": _probe("consumer_gov_basecompleta", consumable=False),
        "consumer_gov_core_plus_ses": _probe("consumer_gov_core_plus_ses", coverage_sufficient=False),
    })
    assert decision["state"] == "conduct_evidence_unavailable"
    assert decision["selected_source"] is None
    assert decision["scoring_state"] == "unavailable"
