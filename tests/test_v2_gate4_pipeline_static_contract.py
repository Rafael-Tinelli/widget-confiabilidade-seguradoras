from __future__ import annotations

import sys

import pytest

from api.v2.gate4_pipeline import (
    STAGES,
    PipelineDefinitionError,
    PipelineStage,
    pipeline_contract,
    stage_map,
    validate_pipeline,
)


def test_every_canonical_python_module_command_resolves_before_execution():
    validate_pipeline()
    contract = pipeline_contract()

    assert contract["module_commands_preflighted"] is True
    assert contract["publication_ready"] is True


def test_missing_python_module_is_rejected_by_static_preflight():
    broken = (
        PipelineStage(
            stage_id="broken",
            kind="derive",
            dependencies=(),
            commands=((sys.executable, "-m", "api.v2.module_that_does_not_exist"),),
            outputs=("broken.json",),
        ),
    )

    with pytest.raises(PipelineDefinitionError, match="references missing module"):
        validate_pipeline(broken)


def test_late_publication_stages_are_wired_to_real_contract_builders():
    mapping = stage_map(STAGES)

    leaderboards = mapping["leaderboards"]
    assert leaderboards.commands == (
        (sys.executable, "-m", "api.v2.build_exploratory_leaderboards_contract"),
    )
    assert {
        "assessment_eligibility",
        "semantic_contract",
        "financial_closure",
        "conduct_closure",
        "conduct_coverage",
        "ranking_preflight",
    } == set(leaderboards.dependencies)
    assert leaderboards.outputs == (
        "data/derived/v2/exploratory_leaderboards_contract.json",
        "data/derived/v2/public/insurer_explorer.json",
        "data/derived/v2/public/explore_index.json",
    )

    sandbox = mapping["sandbox_brand_conduct"]
    assert sandbox.commands == (
        (sys.executable, "-m", "api.v2.build_sandbox_brand_conduct_evidence"),
    )
    assert sandbox.dependencies == ("eligibility", "conduct_source_snapshot")
    assert sandbox.outputs == (
        "data/derived/v2/sandbox_brand_conduct_evidence.json",
    )

    profiles = mapping["public_profiles"]
    assert profiles.commands == (
        (sys.executable, "-m", "api.v2.build_public_search_profile_contract"),
    )
    assert profiles.outputs == (
        "data/derived/v2/public_search_profile_contract.json",
        "data/derived/v2/public/search_index.json",
        "data/derived/v2/public/profile_manifest.json",
    )

    distribution = mapping["distribution_manifest"]
    assert "conduct_source_snapshot" in distribution.dependencies
