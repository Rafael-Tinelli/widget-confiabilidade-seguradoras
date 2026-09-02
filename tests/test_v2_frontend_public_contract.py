from __future__ import annotations

from pathlib import Path


APP = Path("widget-ui/src/App.jsx")
CARD = Path("widget-ui/src/components/InsurerCard.jsx")
DATA_CLIENT = Path("widget-ui/src/v2Data.js")
PROFILE_MODAL = Path("widget-ui/src/InsurerProfileModal.jsx")


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_active_app_consumes_public_v2_contracts_instead_of_v1_score_endpoint():
    app = _text(APP)
    client = _text(DATA_CLIENT)

    assert "/api/v1/insurers.json" not in app
    assert "InsurerScoreModal" not in app
    assert "score_desc" not in app
    assert "Melhor Nota" not in app
    assert "loadPrimaryV2Catalog" in app
    assert "loadProfile" in app

    assert "/ranking-seguradoras/data/v2/public" in client
    for output in (
        "search_index.json",
        "insurer_explorer.json",
        "explore_index.json",
        "profiles/",
        "leaderboards/",
        "collections/",
    ):
        assert output in client


def test_active_card_does_not_render_legacy_score_or_open_insurance_pillar():
    card = _text(CARD)
    for forbidden in (
        "solvencyScore",
        "reputationScore",
        "innovationScore",
        "Open Insurance",
        "Participante OPIN",
        "Nota",
    ):
        assert forbidden not in card

    assert "assessment?.public_class" in card
    assert "financial?.public_interpretation?.headline" in card
    assert "conduct?.summary" in card


def test_profile_modal_renders_precomputed_public_semantics_only():
    modal = _text(PROFILE_MODAL)
    for forbidden in (
        "weights",
        "contributions",
        "final_score",
        "financial_score",
        "innovationScore",
        "OPIN_SCORE_THRESHOLD",
    ):
        assert forbidden not in modal

    for required in (
        "profile.public_summary",
        "profile.lifecycle",
        "relationship_context",
        "assessment.financial",
        "assessment.conduct",
        "sandbox_conduct",
        "profile.limits",
    ):
        assert required in modal
