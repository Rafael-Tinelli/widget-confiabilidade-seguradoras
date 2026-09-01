from __future__ import annotations

import sys

import pytest

from api.v2.build_eligibility_inventory import LIFECYCLE_ARTIFACT
from api.v2.gate4_pipeline import (
    STAGES,
    PipelineDefinitionError,
    PipelineExecutionError,
    PipelineStage,
    ancestors,
    pipeline_contract,
    publication_blockers,
    run_all,
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

    assert position["source_snapshot"] < position["lifecycle"]
    assert position["lifecycle"] < position["eligibility"]
    assert position["eligibility"] < position["financial_evidence"]
    assert position["eligibility"] < position["liquidity"]
    assert position["eligibility"] < position["operating"]
    assert position["financial_evidence"] < position["financial_closure"]
    assert position["liquidity"] < position["financial_closure"]
    assert position["operating"] < position["financial_closure"]


def test_conduct_chain_is_topologically_ordered():
    position = _positions()

    assert position["eligibility"] < position["conduct_source_snapshot"]
    assert position["lifecycle"] < position["relationship_watchdog"]
    assert position["conduct_source_snapshot"] < position["relationship_watchdog"]
    assert position["relationship_watchdog"] < position["consumer_conduct"]
    assert position["consumer_conduct"] < position["conduct_coverage"]
    assert position["conduct_coverage"] < position["conduct_calibration"]
    assert position["conduct_calibration"] < position["conduct_credibility"]
    assert position["conduct_credibility"] < position["conduct_portfolio"]
    assert position["conduct_portfolio"] < position["conduct_closure"]


def test_public_package_depends_on_closed_cross_pillar_chain():
    required = ancestors("distribution_manifest")

    assert {
        "financial_closure",
        "conduct_source_snapshot",
        "relationship_watchdog",
        "consumer_conduct",
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


def test_publication_contract_has_no_operational_blockers_after_conduct_proof():
    assert publication_blockers() == ()
    contract = pipeline_contract()
    assert contract["publication_ready"] is True
    assert contract["publication_blockers"] == []
    assert contract["single_generation_workspace_required"] is True
    assert contract["cross_run_latest_successful_restore_forbidden"] is True


def test_source_snapshot_is_formally_evergreen_and_executable():
    mapping = stage_map(STAGES)
    source = mapping["source_snapshot"]

    assert source.kind == "source"
    assert source.evergreen_ready is True
    assert source.commands == (
        (sys.executable, "-m", "api.v2.build_source_snapshot"),
    )
    assert "source_snapshot" not in publication_blockers()


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


def test_financial_evidence_uses_materialized_gate4_inputs():
    mapping = stage_map(STAGES)
    financial = mapping["financial_evidence"]

    assert financial.kind == "derive"
    assert financial.evergreen_ready is True
    assert financial.dependencies == ("source_snapshot", "eligibility")
    command = financial.commands[0]
    assert "--eligibility-input" in command
    assert "--ses-zip" in command
    assert "financial_evidence" not in publication_blockers()


def test_liquidity_and_operating_use_materialized_gate4_inputs():
    mapping = stage_map(STAGES)

    for stage_id in ("liquidity", "operating"):
        stage = mapping[stage_id]
        assert stage.kind == "derive"
        assert stage.dependencies == ("source_snapshot", "eligibility")
        command = stage.commands[0]
        assert "--eligibility-input" in command
        eligibility_index = command.index("--eligibility-input") + 1
        assert (
            command[eligibility_index]
            == "data/derived/v2/entity_eligibility_inventory.json"
        )
        assert "--ses-zip" in command
        ses_index = command.index("--ses-zip") + 1
        assert command[ses_index] == "data/raw/ses/BaseCompleta.zip"
        assert stage_id not in publication_blockers()


def test_lifecycle_uses_materialized_gate4_inputs():
    mapping = stage_map(STAGES)
    lifecycle = mapping["lifecycle"]

    assert lifecycle.kind == "derive"
    assert lifecycle.evergreen_ready is True
    assert lifecycle.dependencies == ("source_snapshot",)
    command = lifecycle.commands[0]
    assert "--classification-input" in command
    assert "--receita-lifecycle-input" in command
    assert "--ses-zip" in command
    assert "lifecycle" not in publication_blockers()


def test_gate4_eligibility_consumes_canonical_lifecycle_contract():
    mapping = stage_map(STAGES)
    lifecycle = mapping["lifecycle"]
    eligibility = mapping["eligibility"]

    assert LIFECYCLE_ARTIFACT == "v2_lifecycle_relationship_inventory"
    assert lifecycle.outputs == (
        "data/derived/v2/entity_lifecycle_relationship_inventory.json",
    )
    assert eligibility.dependencies == ("lifecycle",)
    command = eligibility.commands[0]
    input_index = command.index("--lifecycle-input") + 1
    assert command[input_index] == lifecycle.outputs[0]


def test_relationship_watchdog_is_evergreen_and_precedes_conduct():
    mapping = stage_map(STAGES)
    watchdog = mapping["relationship_watchdog"]

    assert watchdog.kind == "derive"
    assert watchdog.evergreen_ready is True
    assert watchdog.dependencies == ("lifecycle", "conduct_source_snapshot")
    assert watchdog.commands == (
        (
            sys.executable,
            "-m",
            "api.v2.relationship_watchdog",
            "--lifecycle-input",
            "data/derived/v2/entity_lifecycle_relationship_inventory.json",
            "--consumer-identity-input",
            "data/derived/v2/consumer_gov_identity_experiment.json",
        ),
    )
    assert "relationship_watchdog" not in publication_blockers()


def test_conduct_acquisition_is_isolated_from_conduct_derivation():
    mapping = stage_map(STAGES)
    source = mapping["conduct_source_snapshot"]
    conduct = mapping["consumer_conduct"]

    assert source.kind == "mixed_source_derive"
    assert source.dependencies == ("eligibility",)
    assert source.commands == (
        (sys.executable, "-m", "api.v2.build_conduct_source_snapshot"),
    )
    assert source.evergreen_ready is True
    assert "conduct_source_snapshot" not in publication_blockers()

    assert conduct.kind == "derive"
    assert conduct.dependencies == (
        "eligibility",
        "conduct_source_snapshot",
        "relationship_watchdog",
    )
    assert conduct.evergreen_ready is True
    flattened = [part for command in conduct.commands for part in command]
    assert "--force" not in flattened
    assert "api.build_consumidor_gov" not in flattened
    assert "api.v2.build_consumer_gov_receita_identity" not in flattened
    assert "api.v2.build_consumer_gov_receita_resolution_experiment" in flattened
    assert "api.v2.build_consumer_gov_conduct_evidence" in flattened


def test_consumer_cache_bootstrap_remains_outside_the_publication_dag():
    mapping = stage_map(STAGES)

    assert "conduct_consumer_bootstrap" not in mapping
    assert publication_blockers() == ()


def test_run_all_refuses_any_future_publication_blocker():
    blocked = (
        PipelineStage(
            stage_id="source_snapshot",
            kind="source",
            dependencies=(),
            commands=(),
            outputs=(),
            evergreen_ready=False,
        ),
        PipelineStage(
            stage_id="distribution_manifest",
            kind="package",
            dependencies=("source_snapshot",),
            commands=(),
            outputs=(),
        ),
    )

    with pytest.raises(PipelineExecutionError, match="publication blockers remain"):
        run_all(blocked)
