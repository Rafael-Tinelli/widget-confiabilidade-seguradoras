from __future__ import annotations

import json
from pathlib import Path

from api.sources import opin_products


def test_extract_participants_accepts_build_json_contract() -> None:
    payload = {
        "source": {"url": "https://example.invalid"},
        "participants": [
            {"RegistrationNumber": "12.345.678/0001-90"},
        ],
    }

    participants = opin_products._extract_participant_list(payload)

    assert participants == [{"RegistrationNumber": "12.345.678/0001-90"}]


def test_repo_snapshot_is_used_when_it_contains_participants(
    monkeypatch,
    tmp_path: Path,
) -> None:
    snapshot = tmp_path / "participants.json"
    snapshot.write_text(
        json.dumps(
            {
                "participants": [
                    {"RegistrationNumber": "12.345.678/0001-90"},
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(opin_products, "CACHE_PARTICIPANTS_FILE", tmp_path / "cache.json")
    monkeypatch.setattr(opin_products, "PARTICIPANTS_FILE", snapshot)
    monkeypatch.setattr(
        opin_products,
        "_build_session",
        lambda: (_ for _ in ()).throw(AssertionError("download indevido")),
    )

    participants = opin_products._load_participants()

    assert len(participants) == 1


def test_empty_local_snapshot_does_not_block_download(
    monkeypatch,
    tmp_path: Path,
) -> None:
    snapshot = tmp_path / "participants.json"
    snapshot.write_text(json.dumps({"participants": []}), encoding="utf-8")
    cache = tmp_path / "cache.json"

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return {
                "data": [
                    {"RegistrationNumber": "12.345.678/0001-90"},
                ]
            }

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == opin_products.REQUEST_TIMEOUT
            return FakeResponse()

    monkeypatch.setattr(opin_products, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(opin_products, "CACHE_PARTICIPANTS_FILE", cache)
    monkeypatch.setattr(opin_products, "PARTICIPANTS_FILE", snapshot)
    monkeypatch.setattr(opin_products, "_build_session", FakeSession)

    participants = opin_products._load_participants()

    assert participants == [{"RegistrationNumber": "12.345.678/0001-90"}]
    assert json.loads(cache.read_text(encoding="utf-8")) == participants
