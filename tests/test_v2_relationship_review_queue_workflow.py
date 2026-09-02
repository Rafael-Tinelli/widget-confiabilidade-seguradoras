from pathlib import Path

WORKFLOW = Path(".github/workflows/v2-relationship-review-queue.yml")


def test_relationship_review_queue_is_exact_run_main_only_and_non_authoritative():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    for required in (
        "V2 Gate 4 Full Generation Proof",
        "github.event.workflow_run.head_branch == 'main'",
        "relationship review queue accepts only a Full Generation from main",
        'gh run download "$SOURCE_RUN_ID"',
        "v2-gate4-full-generation-${source_run_id}-a${run_attempt}",
        "candidate_assertion_effect",
        "candidate_complaint_transfer_effect",
        "automatic_registry_mutation",
        "assertion_effect=none",
        "issues: write",
        "[v2] Relationship review queue",
    ):
        assert required in workflow

    assert "gh run list" not in workflow
    assert "latest successful" not in workflow.lower()
    assert "automatic_registry_mutation !=" not in workflow


def test_review_candidates_do_not_block_full_generation_or_publication():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "::warning title=Relationship review queue::" in workflow
    assert "review_required" in workflow
    assert "registry_drift" in workflow
    assert "gh issue create" in workflow
    assert "gh issue edit" in workflow
    assert "gh issue close" in workflow
