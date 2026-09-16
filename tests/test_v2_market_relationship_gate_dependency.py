from api.v2.gate4_pipeline import stage_map, topological_order


def test_market_relationship_conduct_waits_for_canonical_consumer_conduct():
    stage = stage_map()["sandbox_brand_conduct"]

    assert set(stage.dependencies) == {
        "lifecycle",
        "conduct_source_snapshot",
        "consumer_conduct",
    }
    assert "data/derived/v2/market_relationship_conduct_evidence.json" in stage.outputs
    assert "data/derived/v2/sandbox_brand_conduct_evidence.json" in stage.outputs

    order = topological_order()
    assert order.index("consumer_conduct") < order.index("sandbox_brand_conduct")
    assert order.index("sandbox_brand_conduct") < order.index("public_profiles")
