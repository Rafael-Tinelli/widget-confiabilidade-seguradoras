from api.v2.build_market_identity_candidates import _rows
from api.v2.market_identity_observations import regulated_entity_delta_observations


def test_published_search_index_entries_shape_is_consumed():
    payload = {"artifact": "v2_public_search_index", "entries": [{"name": "Azos"}]}
    assert _rows(payload, "entries", "search_index", "records") == [{"name": "Azos"}]


def test_same_fip_with_changed_cnpj_is_explicit_review_observation():
    previous = [
        {
            "fip_code": "000123",
            "cnpj": "11111111000111",
            "legal_name": "SEGURADORA TESTE S.A.",
            "entity_type": "insurer",
        }
    ]
    current = [
        {
            "fip_code": "000123",
            "cnpj": "22222222000122",
            "legal_name": "SEGURADORA TESTE S.A.",
            "entity_type": "insurer",
        }
    ]
    observations = regulated_entity_delta_observations(previous, current)
    assert len(observations) == 1
    observation = observations[0]
    assert observation["candidate_type"] == "regulated_cnpj_change"
    assert observation["candidate_anchor"] == "fip:000123"
    assert observation["previous_value"] == "11111111000111"
    assert observation["observed_value"] == "22222222000122"
    assert observation["lifecycle_state"] == "review_required"
