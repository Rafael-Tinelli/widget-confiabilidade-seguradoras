from __future__ import annotations

import inspect
import json

from api.v2 import build_conduct_source_snapshot, build_source_snapshot
from api.v2.generation import BuildContext
from api.v2.source_cache import (
    DEFAULT_SUSEP_FETCH_DEADLINE_SECONDS,
    _susep_fetch_deadline_seconds,
    finish_source_acquisition,
    start_source_acquisition,
)
from api.v2.source_snapshot import SourceObservation


def _context() -> BuildContext:
    return BuildContext(
        build_id="source-timing-test",
        source_head_sha="a" * 40,
        generated_at="2026-09-06T12:00:00Z",
        workflow_run_id="test",
        workflow_run_attempt=1,
    )


def test_gov_br_susep_namespace_receives_hard_deadline(monkeypatch) -> None:
    monkeypatch.delenv("V2_SUSEP_FETCH_DEADLINE_SECONDS", raising=False)

    assert (
        _susep_fetch_deadline_seconds(
            "https://www.gov.br/susep/pt-br/assuntos/sandbox-regulatorio/"
            "seguradoras-participantes-do-sandbox-1"
        )
        == DEFAULT_SUSEP_FETCH_DEADLINE_SECONDS
    )
    assert _susep_fetch_deadline_seconds("https://dados.gov.br/qualquer") is None


def test_source_timing_helpers_emit_matching_structured_records(
    tmp_path, capsys
) -> None:
    snapshot = tmp_path / "source.json"
    snapshot.write_text('{"ok": true}\n', encoding="utf-8")
    observation = SourceObservation(
        source_id="example_source",
        source_url="https://example.test/source",
        snapshot_path=snapshot,
        fetched_at="2026-09-06T12:00:00Z",
        current_validation_succeeded=True,
    )

    started = start_source_acquisition(
        source_id=observation.source_id,
        source_url=observation.source_url,
    )
    finish_source_acquisition(
        started=started,
        source_id=observation.source_id,
        context=_context(),
        observation=observation,
        used_cache=False,
    )

    records = []
    for line in capsys.readouterr().err.splitlines():
        prefix = "V2_SOURCE_ACQUISITION "
        if line.startswith(prefix):
            records.append(json.loads(line[len(prefix) :]))

    assert [record["event"] for record in records] == ["start", "end"]
    assert records[0]["source_id"] == "example_source"
    assert records[0]["deadline_seconds"] is None
    assert records[1]["source_id"] == "example_source"
    assert records[1]["state"] == "fresh"
    assert records[1]["freshness_method"] == "current_validation"
    assert records[1]["used_cache"] is False
    assert records[1]["duration_seconds"] >= 0


def test_regulatory_receita_and_conduct_sources_use_shared_timing_boundary() -> None:
    regulatory = inspect.getsource(build_source_snapshot.build_source_snapshot)
    conduct = inspect.getsource(build_conduct_source_snapshot.build_conduct_source_snapshot)

    assert 'source_id="receita_cnpj_lifecycle"' in regulatory
    assert "start_source_acquisition(" in regulatory
    assert "finish_source_acquisition(" in regulatory

    assert 'source_id="consumer_gov_core"' in conduct
    assert 'source_id="receita_consumer_identity"' in conduct
    assert conduct.count("start_source_acquisition(") >= 2
    assert conduct.count("finish_source_acquisition(") >= 2
