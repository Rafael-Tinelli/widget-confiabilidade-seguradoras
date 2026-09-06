from __future__ import annotations

import ast
import re
import textwrap
from pathlib import Path

WORKFLOW = Path(".github/workflows/v2-main-source-cache-seed.yml")
INLINE_PYTHON_PATTERN = re.compile(
    r"(?ms)^(?P<indent>[ \t]*)python - <<'PY'\n(?P<body>.*?)(?=^(?P=indent)PY\s*$)"
)
ENV_INLINE_PYTHON_PATTERN = re.compile(
    r"(?ms)^(?P<indent>[ \t]*)RUN_JSON=\"\$run_json\" python - <<'PY'\n"
    r"(?P<body>.*?)(?=^(?P=indent)PY\s*$)"
)


def test_seed_workflow_is_manual_main_only_and_confirmation_gated() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "schedule:" not in workflow
    assert "push:" not in workflow
    assert "github.ref == 'refs/heads/main'" in workflow
    assert "inputs.confirm_seed == 'SEED'" in workflow
    assert "bridge_run_id:" in workflow
    assert "confirm_seed:" in workflow


def test_seed_workflow_trusts_only_successful_export_from_legacy_v2_branch() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "actions: read" in workflow
    assert 'gh api "repos/$GITHUB_REPOSITORY/actions/runs/$BRIDGE_RUN_ID"' in workflow
    assert '"conclusion": "success"' in workflow
    assert '"head_branch": "refactor/v2-data-foundation"' in workflow
    assert (
        '"path": ".github/workflows/tmp-export-v2-validated-source-cache.yml"'
        in workflow
    )


def test_seed_workflow_validates_bootstrap_before_default_branch_cache_save() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "v2-receita-cache-bootstrap-$BRIDGE_RUN_ID" in workflow
    assert "Receita bootstrap artifact hash mismatch" in workflow
    assert "v2-receita-lifecycle-1" in workflow
    assert "target_universe_hash" in workflow
    assert "actions/cache/save@v4" in workflow
    assert "v2-gate4-source-${{ runner.os }}-main-bootstrap-${{ github.run_id }}" in workflow
    assert "V2_HOSTGATOR_DEPLOY_ENABLED" not in workflow
    assert "V2_PRODUCTION_AUTOMATION_ENABLED" not in workflow
    assert "V2_MARKET_SENSOR_AUTOMATION_ENABLED" not in workflow


def test_seed_workflow_inline_python_is_syntax_valid() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    blocks = [
        textwrap.dedent(match.group("body"))
        for pattern in (INLINE_PYTHON_PATTERN, ENV_INLINE_PYTHON_PATTERN)
        for match in pattern.finditer(workflow)
    ]

    assert len(blocks) >= 3
    for index, block in enumerate(blocks, start=1):
        try:
            ast.parse(block)
        except SyntaxError as exc:
            raise AssertionError(
                f"inline Python block {index} is not syntactically valid: {exc}"
            ) from exc
