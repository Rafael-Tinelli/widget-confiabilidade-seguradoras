from __future__ import annotations

import sys

import pytest

from api.v2.gate4_pipeline import (
    STAGES,
    PipelineDefinitionError,
    PipelineStage,
    ancestors,
    pipeline_contract,
    publication_blockers,
    stage_map,
    topological_order,
    validate_pipeline,
)


def _positions() -> dict[str, int]:
    return {stage_id: index for index, stage_id in enumerate(topological_order())}


def test_gate4_pipeline_is_acyclic_and_outputs_are_unique():
    validate_pipeline()
    mapping = stage_map()
    outputs = [output for stage in mapping.values() for output in stage.outputs]

    assert len(outputs) == len(set(outputs))
    assert topological_order()[0] == "source_snapshot"
    assert topological_order()[-1] == "distribution_manifest"


def test_financial_chain_is_topologically_ordered():
    position = _positions()

    assert position["source_snapshot"] < position["financial_evidence"]
    assert position["source_snapshot"] < position["lifecycle"]
    assert position["lifecycle"] < position["eligibility"]
    assert position["eligibility"] < position["liquidity"]
    assert position["eligibility"] < position["operating"]
    assert position["financial_evidence"] < position["financial_closure"]
    assert position["liquidity"] < position["financial_closure"]
    assert position["operating"] < position["financial_closure"]


def test_conduct_chain_is_topologically_ordered():
    position = _positions()

    assert position["eligibility"] < position["consumer_conduct"]
    assert position["consumer_conduct"] < position["conduct_coverage"]
    assert position["conduct_coverage"] < position["conduct_calibration"]
    assert position["conduct_calibration"] < position["conduct_credibility"]
    assert position["conduct_credibility"] < position["conduct_portfolio"]
    assert position["conduct_portfolio"] < position["conduct_closure"]


def test_public_package_depends_on_closed_cross_pillar_chain():
    required = ancestors("distribution_manifest")

    assert {
        "financial_closure",
        "conduct_closure",
        "cross_stage1",
        "cross_coverage",
        "cross_stage2",
        "semantic_contract",
        "assessment_eligibility",
        "ranking_preflight",
        "leaderboards",
        "lifecycle",
        "sandbox_brand_conduct",
        "public_profiles",
    } <= required


def test_current_blockers_are_explicit_instead_of_silently_published():
    blockers = set(publication_blockers())

    assert blockers == {
        "source_snapshot",
        "financial_evidence",
        "consumer_conduct",
        "lifecycle",
    }
    contract = pipeline_contract()
    assert contract["publication_ready"] is False
    assert contract["single_generation_workspace_required"] is True
    assert contract["cross_run_latest_successful_restore_forbidden"] is True


def test_unknown_dependency_is_rejected():
    broken = (
        PipelineStage(
            stage_id="one",
            kind="derive",
            dependencies=("missing",),
            commands=(),
            outputs=("one.json",),
        ),
    )

    with pytest.raises(PipelineDefinitionError, match="unknown dependency"):
        validate_pipeline(broken)


def test_cycle_is_rejected():
    broken = (
        PipelineStage(
            stage_id="one",
            kind="derive",
            dependencies=("two",),
            commands=(),
            outputs=("one.json",),
        ),
        PipelineStage(
            stage_id="two",
            kind="derive",
            dependencies=("one",),
            commands=(),
            outputs=("two.json",),
        ),
    )

    with pytest.raises(PipelineDefinitionError, match="contains a cycle"):
        validate_pipeline(broken)


def test_duplicate_output_is_rejected():
    broken = (
        PipelineStage(
            stage_id="one",
            kind="derive",
            dependencies=(),
            commands=(),
            outputs=("same.json",),
        ),
        PipelineStage(
            stage_id="two",
            kind="derive",
            dependencies=(),
            commands=(),
            outputs=("same.json",),
        ),
    )

    with pytest.raises(PipelineDefinitionError, match="produced by both"):
        validate_pipeline(broken)


def test_pipeline_keeps_ranking_blocked_methodologically_not_operationally():
    mapping = stage_map(STAGES)
    ranking = mapping["ranking_preflight"]

    assert ranking.commands == (
        (
            sys.executable,
            "-m",
            "api.v2.build_ranking_eligibility_preflight",
        ),
    )
    assert ranking.evergreen_ready is True
    assert "ranking_preflight" not in publication_blockers()
