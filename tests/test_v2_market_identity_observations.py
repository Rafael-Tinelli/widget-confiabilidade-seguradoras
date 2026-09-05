import pytest

from api.v2.market_identity_observations import (
    DemandReviewThresholds,
    candidate_registry_from_observations,
    gsc_query_observations,
    regulated_entity_delta_observations,
    sandbox_delta_observations,
    widget_unknown_search_observations,
)


def _search_index():
    return [
        {
            "name": "Porto Seguro Companhia de Seguros Gerais",
            "aliases": ["Porto Seguro", "Porto"],
            "cnpj": "61198164000160",
            "fip_code": "000588",
        },
        {
            "name": "AZOS TECNOLOGIA E SERVIÇOS LTDA",
            "aliases": ["Azos", "Azos Seguros"],
            "cnpj": "39520039000175",
            "fip_code": None,
        },
    ]


def test_known_search_result_is_not_recorded_as_unknown():
    rows = [
        {
            "normalized_query": "PORTO",
            "first_seen": "2026-09-01",
            "last_seen": "2026-09-02",
            "count": 8,
            "distinct_day_count": 2,
        }
    ]
    assert widget_unknown_search_observations(rows, _search_index()) == []


def test_empty_short_and_generic_noise_queries_are_ignored():
    rows = [
        {"normalized_query": "", "count": 3, "distinct_day_count": 3},
        {"normalized_query": "x", "count": 3, "distinct_day_count": 3},
        {"normalized_query": "seguros", "count": 3, "distinct_day_count": 3},
    ]
    assert widget_unknown_search_observations(rows, _search_index()) == []


def test_unknown_search_aggregates_without_pii_and_threshold_promotes_review():
    thresholds = DemandReviewThresholds(widget_min_count=2, widget_min_distinct_days=2)
    rows = [
        {
            "normalized_query": "Nova Plataforma",
            "first_seen": "2026-09-01",
            "last_seen": "2026-09-02",
            "count": 2,
            "distinct_day_count": 2,
        }
    ]
    observations = widget_unknown_search_observations(
        rows, _search_index(), thresholds=thresholds
    )
    assert len(observations) == 1
    assert observations[0]["normalized_query"] == "nova plataforma"
    assert observations[0]["lifecycle_state"] == "review_required"
    assert "ip" not in observations[0]
    assert "session_id" not in observations[0]

    registry = candidate_registry_from_observations(observations)
    candidate = registry["candidates"][0]
    assert candidate["assertion_effect"] == "none"
    assert candidate["score_effect"] == "none"
    assert candidate["complaint_transfer_effect"] == "none"
    assert candidate["automatic_registry_mutation"] == "forbidden"
    assert candidate["blocking"] is False


def test_unknown_search_rejects_personal_or_session_fields():
    rows = [
        {
            "normalized_query": "Nova Plataforma",
            "count": 2,
            "distinct_day_count": 2,
            "ip_address": "192.0.2.10",
        }
    ]
    with pytest.raises(ValueError, match="forbidden personal/session fields"):
        widget_unknown_search_observations(rows, _search_index())


def test_azos_sensor_regression_is_candidate_only_when_azos_is_not_yet_in_registry():
    index_before_azos = [_search_index()[0]]
    rows = [
        {
            "normalized_query": "Azos",
            "count": 2,
            "distinct_day_count": 2,
        }
    ]
    observations = widget_unknown_search_observations(rows, index_before_azos)
    registry = candidate_registry_from_observations(observations)
    candidate = registry["candidates"][0]
    assert candidate["review_state"] == "review_required"
    assert candidate["assertion_effect"] == "none"
    assert candidate["complaint_transfer_effect"] == "none"

    # Once Azos is canonical/searchable, the same signal is resolved exactly and ignored.
    assert widget_unknown_search_observations(rows, _search_index()) == []


def test_gsc_resolved_query_is_ignored_and_unknown_query_is_observational():
    rows = [
        {"query": "Porto Seguro", "impressions": 100, "clicks": 10},
        {"query": "Emergente Seguros", "impressions": 7, "clicks": 0},
    ]
    observations = gsc_query_observations(rows, _search_index())
    assert len(observations) == 1
    assert observations[0]["normalized_query"] == "emergente seguros"
    assert observations[0]["lifecycle_state"] == "review_required"
    assert observations[0]["confidence_semantics"] == "relevance_signal_only"


def test_gsc_known_identity_context_query_does_not_become_new_company_candidate():
    rows = [
        {"query": "porto seguro e confiavel", "impressions": 80, "clicks": 8},
        {"query": "reclamacoes azos", "impressions": 12, "clicks": 2},
        {"query": "emergente seguros e confiavel", "impressions": 9, "clicks": 1},
    ]
    observations = gsc_query_observations(rows, _search_index())
    assert len(observations) == 1
    assert observations[0]["normalized_query"] == "emergente seguros e confiavel"


def test_widget_unknown_search_keeps_recall_for_new_compound_name_containing_known_alias():
    rows = [
        {
            "normalized_query": "Porto Digital",
            "count": 2,
            "distinct_day_count": 2,
        }
    ]
    observations = widget_unknown_search_observations(rows, _search_index())
    assert len(observations) == 1
    assert observations[0]["normalized_query"] == "porto digital"
    assert observations[0]["lifecycle_state"] == "review_required"


def test_demand_sensor_unavailable_is_explicit_and_does_not_change_candidate_policy():
    registry = candidate_registry_from_observations(
        [], sensor_status={"gsc_query": "unavailable", "widget_unknown_search": "stale"}
    )
    assert registry["sensor_status"]["gsc_query"] == "unavailable"
    assert registry["policy"]["sensor_unavailable_invalidates_gate4"] is False
    assert registry["summary"]["candidate_count"] == 0


def test_new_regulated_entity_and_name_change_generate_candidates_only():
    previous = [
        {
            "fip_code": "000001",
            "cnpj": "11111111000111",
            "legal_name": "SEGURADORA ANTIGA S.A.",
            "entity_type": "insurer",
            "regulatory_status": "active_licensed",
        }
    ]
    current = [
        {
            "fip_code": "000001",
            "cnpj": "11111111000111",
            "legal_name": "SEGURADORA NOVO NOME S.A.",
            "entity_type": "insurer",
            "regulatory_status": "active_licensed",
        },
        {
            "fip_code": "000002",
            "cnpj": "22222222000122",
            "legal_name": "NOVA SEGURADORA S.A.",
            "entity_type": "insurer",
            "regulatory_status": "active_licensed",
        },
    ]
    observations = regulated_entity_delta_observations(previous, current)
    assert {row["candidate_type"] for row in observations} == {
        "new_regulated_entity",
        "regulated_name_change",
    }
    registry = candidate_registry_from_observations(observations)
    assert all(row["assertion_effect"] == "none" for row in registry["candidates"])
    assert all(row["automatic_registry_mutation"] == "forbidden" for row in registry["candidates"])


def test_unchanged_regulated_entity_does_not_generate_candidate():
    row = {
        "fip_code": "000001",
        "cnpj": "11111111000111",
        "legal_name": "SEGURADORA ESTÁVEL S.A.",
        "entity_type": "insurer",
        "regulatory_status": "active_licensed",
    }
    assert regulated_entity_delta_observations([row], [row]) == []


def test_new_sandbox_participant_is_detected_by_exact_cnpj_and_not_ordinary_inferred():
    observations = sandbox_delta_observations(
        [],
        [
            {
                "cnpj": "33333333000133",
                "legal_name": "SANDBOX TESTE S.A.",
                "regulatory_status": "temporary_authorized",
            }
        ],
    )
    assert len(observations) == 1
    observation = observations[0]
    assert observation["candidate_type"] == "new_sandbox_participant"
    assert observation["candidate_anchor"] == "cnpj:33333333000133"
    assert "ordinary_insurer" not in observation.values()


def test_exact_same_unknown_query_fuses_sources_but_similarity_never_resolves_identity():
    widget = widget_unknown_search_observations(
        [{"normalized_query": "Nova Marca", "count": 2, "distinct_day_count": 2}],
        [],
    )
    gsc = gsc_query_observations(
        [{"query": "nova marca", "impressions": 8, "clicks": 1}],
        [],
    )
    registry = candidate_registry_from_observations([*widget, *gsc])
    assert registry["summary"]["candidate_count"] == 1
    candidate = registry["candidates"][0]
    assert {row["source"] for row in candidate["observations"]} == {
        "widget_unknown_search",
        "gsc_query",
    }

    similar = gsc_query_observations(
        [{"query": "nova marca seguros", "impressions": 8, "clicks": 1}],
        [],
    )
    registry = candidate_registry_from_observations([*widget, *similar])
    assert registry["summary"]["candidate_count"] == 2
