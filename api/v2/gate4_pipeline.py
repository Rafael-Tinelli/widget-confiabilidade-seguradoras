from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

StageKind = Literal["source", "derive", "mixed_source_derive", "package"]


@dataclass(frozen=True)
class PipelineStage:
    stage_id: str
    kind: StageKind
    dependencies: tuple[str, ...]
    commands: tuple[tuple[str, ...], ...]
    outputs: tuple[str, ...]
    evergreen_ready: bool = True

    def as_dict(self) -> dict:
        return {
            "stage_id": self.stage_id,
            "kind": self.kind,
            "dependencies": list(self.dependencies),
            "commands": [list(command) for command in self.commands],
            "outputs": list(self.outputs),
            "evergreen_ready": self.evergreen_ready,
        }


def _module(name: str, *args: str) -> tuple[str, ...]:
    return (sys.executable, "-m", name, *args)


STAGES: tuple[PipelineStage, ...] = (
    PipelineStage(
        stage_id="source_snapshot",
        kind="source",
        dependencies=(),
        commands=(),
        outputs=(
            "data/raw/ses/BaseCompleta.zip",
            "data/derived/v2/source/classification_inventory.json",
            "data/derived/v2/source/receita_lifecycle_records.json",
            "data/derived/v2/source_lineage.json",
        ),
        evergreen_ready=False,
    ),
    PipelineStage(
        stage_id="lifecycle",
        kind="derive",
        dependencies=("source_snapshot",),
        commands=(
            _module(
                "api.v2.build_lifecycle_relationship_inventory",
                "--classification-input",
                "data/derived/v2/source/classification_inventory.json",
                "--receita-lifecycle-input",
                "data/derived/v2/source/receita_lifecycle_records.json",
                "--ses-zip",
                "data/raw/ses/BaseCompleta.zip",
            ),
        ),
        outputs=("data/derived/v2/entity_lifecycle_relationship_inventory.json",),
    ),
    PipelineStage(
        stage_id="eligibility",
        kind="derive",
        dependencies=("lifecycle",),
        commands=(
            _module(
                "api.v2.build_eligibility_inventory",
                "--lifecycle-input",
                "data/derived/v2/entity_lifecycle_relationship_inventory.json",
            ),
        ),
        outputs=("data/derived/v2/entity_eligibility_inventory.json",),
    ),
    PipelineStage(
        stage_id="financial_evidence",
        kind="derive",
        dependencies=("source_snapshot", "eligibility"),
        commands=(
            _module(
                "api.v2.build_financial_evidence_inventory",
                "--eligibility-input",
                "data/derived/v2/entity_eligibility_inventory.json",
                "--ses-zip",
                "data/raw/ses/BaseCompleta.zip",
            ),
        ),
        outputs=("data/derived/v2/entity_financial_evidence_inventory.json",),
    ),
    PipelineStage(
        stage_id="liquidity",
        kind="derive",
        dependencies=("source_snapshot", "eligibility"),
        commands=(_module("api.v2.build_liquidity_experiment"),),
        outputs=("data/derived/v2/liquidity_experiment.json",),
    ),
    PipelineStage(
        stage_id="operating",
        kind="derive",
        dependencies=("source_snapshot", "eligibility"),
        commands=(_module("api.v2.build_operating_experiment"),),
        outputs=("data/derived/v2/operating_experiment.json",),
    ),
    PipelineStage(
        stage_id="financial_closure",
        kind="derive",
        dependencies=("financial_evidence", "liquidity", "operating"),
        commands=(_module("api.v2.build_financial_methodology_closure"),),
        outputs=("data/derived/v2/financial_methodology_closure.json",),
    ),
    PipelineStage(
        stage_id="conduct_source_snapshot",
        kind="mixed_source_derive",
        dependencies=("eligibility",),
        commands=(_module("api.v2.build_conduct_source_snapshot"),),
        outputs=(
            "data/cache/v2/conduct/consumer_gov_core_manifest.json",
            "data/derived/v2/consumer_gov_identity_experiment.json",
            "data/derived/v2/receita_consumer_gov_identity.json",
            "data/derived/v2/conduct_source_lineage.json",
        ),
        evergreen_ready=False,
    ),
    PipelineStage(
        stage_id="consumer_conduct",
        kind="derive",
        dependencies=("eligibility", "conduct_source_snapshot"),
        commands=(
            _module("api.v2.build_consumer_gov_receita_resolution_experiment"),
            _module("api.v2.build_consumer_gov_conduct_evidence"),
        ),
        outputs=(
            "data/derived/v2/consumer_gov_receita_resolution_experiment.json",
            "data/derived/v2/consumer_gov_conduct_evidence.json",
        ),
    ),
    PipelineStage(
        stage_id="conduct_coverage",
        kind="derive",
        dependencies=("source_snapshot", "eligibility", "consumer_conduct"),
        commands=(_module("api.v2.build_conduct_coverage_reconciliation"),),
        outputs=("data/derived/v2/conduct_coverage_reconciliation.json",),
    ),
    PipelineStage(
        stage_id="conduct_calibration",
        kind="derive",
        dependencies=("consumer_conduct", "conduct_coverage"),
        commands=(_module("api.v2.build_conduct_comparative_calibration_v2"),),
        outputs=("data/derived/v2/conduct_comparative_calibration_v2.json",),
    ),
    PipelineStage(
        stage_id="conduct_credibility",
        kind="derive",
        dependencies=("conduct_calibration",),
        commands=(_module("api.v2.build_conduct_credibility_diagnostic"),),
        outputs=("data/derived/v2/conduct_credibility_diagnostic.json",),
    ),
    PipelineStage(
        stage_id="conduct_portfolio",
        kind="derive",
        dependencies=("conduct_calibration", "conduct_credibility"),
        commands=(_module("api.v2.build_conduct_portfolio_mix_diagnostic"),),
        outputs=("data/derived/v2/conduct_portfolio_mix_diagnostic.json",),
    ),
    PipelineStage(
        stage_id="conduct_closure",
        kind="derive",
        dependencies=("conduct_calibration", "conduct_coverage", "conduct_portfolio"),
        commands=(_module("api.v2.build_conduct_methodology_closure"),),
        outputs=("data/derived/v2/conduct_methodology_closure.json",),
    ),
    PipelineStage(
        stage_id="cross_stage1",
        kind="derive",
        dependencies=("financial_closure", "conduct_closure"),
        commands=(_module("api.v2.build_cross_pillar_calibration_diagnostic"),),
        outputs=("data/derived/v2/cross_pillar_calibration_diagnostic.json",),
    ),
    PipelineStage(
        stage_id="cross_coverage",
        kind="derive",
        dependencies=("cross_stage1", "conduct_coverage"),
        commands=(_module("api.v2.build_cross_pillar_coverage_audit"),),
        outputs=("data/derived/v2/cross_pillar_coverage_audit.json",),
    ),
    PipelineStage(
        stage_id="cross_stage2",
        kind="derive",
        dependencies=("cross_stage1", "cross_coverage"),
        commands=(_module("api.v2.build_cross_pillar_architecture_experiment"),),
        outputs=("data/derived/v2/cross_pillar_architecture_experiment.json",),
    ),
    PipelineStage(
        stage_id="semantic_contract",
        kind="derive",
        dependencies=("cross_stage1", "cross_stage2"),
        commands=(_module("api.v2.build_cross_pillar_assessment_semantic_contract"),),
        outputs=("data/derived/v2/cross_pillar_assessment_semantic_contract.json",),
    ),
    PipelineStage(
        stage_id="assessment_eligibility",
        kind="derive",
        dependencies=("eligibility", "semantic_contract"),
        commands=(_module("api.v2.build_assessment_eligibility_contract"),),
        outputs=("data/derived/v2/assessment_eligibility_contract.json",),
    ),
    PipelineStage(
        stage_id="ranking_preflight",
        kind="derive",
        dependencies=(
            "assessment_eligibility",
            "cross_stage1",
            "cross_stage2",
            "cross_coverage",
        ),
        commands=(_module("api.v2.build_ranking_eligibility_preflight"),),
        outputs=("data/derived/v2/ranking_eligibility_preflight.json",),
    ),
    PipelineStage(
        stage_id="leaderboards",
        kind="derive",
        dependencies=(
            "assessment_eligibility",
            "semantic_contract",
            "financial_closure",
            "conduct_closure",
            "conduct_coverage",
            "ranking_preflight",
        ),
        commands=(_module("api.v2.build_exploratory_leaderboards_contract"),),
        outputs=(
            "data/derived/v2/exploratory_leaderboards_contract.json",
            "data/derived/v2/public/insurer_explorer.json",
            "data/derived/v2/public/explore_index.json",
        ),
    ),
    PipelineStage(
        stage_id="sandbox_brand_conduct",
        kind="derive",
        dependencies=("eligibility", "consumer_conduct"),
        commands=(_module("api.v2.build_sandbox_brand_conduct_evidence"),),
        outputs=("data/derived/v2/sandbox_brand_conduct_evidence.json",),
    ),
    PipelineStage(
        stage_id="public_profiles",
        kind="derive",
        dependencies=("lifecycle", "leaderboards", "sandbox_brand_conduct"),
        commands=(_module("api.v2.public_profile_regulatory_semantics"),),
        outputs=(
            "data/derived/v2/public_search_profile_contract.json",
            "data/derived/v2/public/search_index.json",
            "data/derived/v2/public/profile_manifest.json",
        ),
    ),
    PipelineStage(
        stage_id="distribution_manifest",
        kind="package",
        dependencies=("source_snapshot", "leaderboards", "public_profiles"),
        commands=(_module("api.v2.build_public_distribution_manifest"),),
        outputs=("data/derived/v2/public/distribution_manifest.json",),
    ),
)


class PipelineDefinitionError(RuntimeError):
    """Raised when the Gate 4 dependency graph is inconsistent."""


class PipelineExecutionError(RuntimeError):
    """Raised when a Gate 4 stage cannot be executed safely."""


def stage_map(stages: tuple[PipelineStage, ...] = STAGES) -> dict[str, PipelineStage]:
    mapping = {stage.stage_id: stage for stage in stages}
    if len(mapping) != len(stages):
        raise PipelineDefinitionError("duplicate Gate 4 stage_id")
    return mapping


def validate_pipeline(stages: tuple[PipelineStage, ...] = STAGES) -> None:
    mapping = stage_map(stages)
    produced_by: dict[str, str] = {}
    for stage in stages:
        for dependency in stage.dependencies:
            if dependency not in mapping:
                raise PipelineDefinitionError(
                    f"unknown dependency {dependency!r} for stage {stage.stage_id!r}"
                )
        for output in stage.outputs:
            previous = produced_by.get(output)
            if previous is not None:
                raise PipelineDefinitionError(
                    f"output {output!r} produced by both {previous!r} and {stage.stage_id!r}"
                )
            produced_by[output] = stage.stage_id
    topological_order(stages)


def topological_order(stages: tuple[PipelineStage, ...] = STAGES) -> tuple[str, ...]:
    mapping = stage_map(stages)
    indegree = {stage_id: 0 for stage_id in mapping}
    downstream: dict[str, list[str]] = defaultdict(list)
    for stage in stages:
        for dependency in stage.dependencies:
            if dependency not in mapping:
                raise PipelineDefinitionError(
                    f"unknown dependency {dependency!r} for stage {stage.stage_id!r}"
                )
            indegree[stage.stage_id] += 1
            downstream[dependency].append(stage.stage_id)

    queue = deque(stage.stage_id for stage in stages if indegree[stage.stage_id] == 0)
    ordered: list[str] = []
    while queue:
        stage_id = queue.popleft()
        ordered.append(stage_id)
        for child in downstream.get(stage_id, []):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if len(ordered) != len(stages):
        unresolved = sorted(stage_id for stage_id, degree in indegree.items() if degree)
        raise PipelineDefinitionError(f"Gate 4 pipeline contains a cycle: {unresolved}")
    return tuple(ordered)


def ancestors(stage_id: str, stages: tuple[PipelineStage, ...] = STAGES) -> set[str]:
    mapping = stage_map(stages)
    if stage_id not in mapping:
        raise PipelineDefinitionError(f"unknown stage: {stage_id}")
    result: set[str] = set()
    pending = list(mapping[stage_id].dependencies)
    while pending:
        current = pending.pop()
        if current in result:
            continue
        result.add(current)
        pending.extend(mapping[current].dependencies)
    return result


def publication_blockers(stages: tuple[PipelineStage, ...] = STAGES) -> tuple[str, ...]:
    mapping = stage_map(stages)
    required = ancestors("distribution_manifest", stages) | {"distribution_manifest"}
    return tuple(
        stage_id
        for stage_id in topological_order(stages)
        if stage_id in required and not mapping[stage_id].evergreen_ready
    )


def _verify_outputs(stage: PipelineStage) -> None:
    missing = [output for output in stage.outputs if not Path(output).exists()]
    if missing:
        raise PipelineExecutionError(
            f"stage {stage.stage_id!r} did not produce required outputs: {missing}"
        )


def _verify_dependencies(stage: PipelineStage, mapping: dict[str, PipelineStage]) -> None:
    missing: list[str] = []
    for dependency in stage.dependencies:
        for output in mapping[dependency].outputs:
            if not Path(output).exists():
                missing.append(output)
    if missing:
        raise PipelineExecutionError(
            f"stage {stage.stage_id!r} has missing dependency outputs: {missing}"
        )


def run_stage(stage_id: str, stages: tuple[PipelineStage, ...] = STAGES) -> None:
    mapping = stage_map(stages)
    try:
        stage = mapping[stage_id]
    except KeyError as exc:
        raise PipelineExecutionError(f"unknown stage: {stage_id}") from exc

    _verify_dependencies(stage, mapping)
    if not stage.commands:
        _verify_outputs(stage)
        return
    for command in stage.commands:
        subprocess.run(command, check=True)
    _verify_outputs(stage)


def pipeline_contract(stages: tuple[PipelineStage, ...] = STAGES) -> dict:
    validate_pipeline(stages)
    order = topological_order(stages)
    blockers = publication_blockers(stages)
    mapping = stage_map(stages)
    return {
        "artifact": "v2_gate4_pipeline_contract",
        "version": 1,
        "single_generation_workspace_required": True,
        "cross_run_latest_successful_restore_forbidden": True,
        "topological_order": list(order),
        "publication_ready": not blockers,
        "publication_blockers": list(blockers),
        "stages": [mapping[stage_id].as_dict() for stage_id in order],
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect or execute the Gate 4 v2 DAG.")
    parser.add_argument("--run-stage", choices=[stage.stage_id for stage in STAGES])
    parser.add_argument(
        "--write-contract",
        type=Path,
        help="Write the validated DAG contract to this JSON path.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    contract = pipeline_contract()
    if args.run_stage:
        run_stage(args.run_stage)
    if args.write_contract:
        args.write_contract.parent.mkdir(parents=True, exist_ok=True)
        args.write_contract.write_text(
            json.dumps(contract, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(contract, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
