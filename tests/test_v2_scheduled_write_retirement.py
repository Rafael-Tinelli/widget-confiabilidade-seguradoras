from __future__ import annotations

from pathlib import Path


WORKFLOW_DIR = Path(".github/workflows")
MANUAL_ONLY_WORKFLOWS = (
    "refresh-data.yml",
    "update-susep-sandbox.yml",
    "refresh-receita-lifecycle.yml",
)


def test_retired_main_writers_are_manual_only() -> None:
    for name in MANUAL_ONLY_WORKFLOWS:
        text = (WORKFLOW_DIR / name).read_text(encoding="utf-8")
        assert "workflow_dispatch:" in text, name
        assert "schedule:" not in text, name
        assert "cron:" not in text, name


def test_no_scheduled_workflow_has_contents_write_permission() -> None:
    offenders: list[str] = []
    for path in sorted(WORKFLOW_DIR.glob("*.yml")):
        text = path.read_text(encoding="utf-8")
        if "schedule:" in text and "contents: write" in text:
            offenders.append(path.name)
    assert offenders == []


def test_canonical_v2_scheduler_remains_explicitly_gated() -> None:
    text = (WORKFLOW_DIR / "v2-production-generation-schedule.yml").read_text(
        encoding="utf-8"
    )
    assert "schedule:" in text
    assert 'cron: "17 9 * * 1"' in text
    assert "vars.V2_PRODUCTION_AUTOMATION_ENABLED == 'true'" in text
    assert "contents: read" in text
