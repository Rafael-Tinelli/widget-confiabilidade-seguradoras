"""A09.2 regressions for Receita metadata-bound cache freshness.

Every scenario is offline: no national CNPJ ZIP transfer, Full Generation or
HostGator request. Local content hashes protect the saved derived snapshot,
while official WebDAV metadata guards any assertion of *current* freshness.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from api.sources.receita_cnpj_cache_integrity import (
    FINGERPRINT_METHOD,
    checked_release_fingerprint,
    identity_cache_current,
    lifecycle_cache_current,
    lifecycle_release_fingerprint,
)
from api.sources.receita_cnpj_telemetry import release_fingerprint
from api.v2 import build_consumer_gov_receita_identity as identity_builder
from api.v2.source_cache import CachedSource, SourceCacheError


def _resources() -> list[dict]:
    return [
        {"name": "Motivos.zip", "size": 123, "etag": "motives", "last_modified": "t1"},
        {"name": "Estabelecimentos0.zip", "size": 1200, "etag": "est0", "last_modified": "t1"},
        {"name": "Estabelecimentos1.zip", "size": 1400, "etag": "est1", "last_modified": "t1"},
        {"name": "Empresas0.zip", "size": 800, "etag": "com0", "last_modified": "t1"},
        {"name": "Cnaes.zip", "size": 500, "etag": "cnaes", "last_modified": "t1"},
        {"name": "Naturezas.zip", "size": 600, "etag": "nat", "last_modified": "t1"},
    ]


def _release() -> SimpleNamespace:
    data = _resources()
    return SimpleNamespace(
        period="2026-10",
        release_url="https://arquivos.receitafederal.gov.br/2026-10/",
        reasons_file="Motivos.zip",
        establishment_files=("Estabelecimentos0.zip", "Estabelecimentos1.zip"),
        resource_metadata=tuple(item for item in data if
                                item["name"].startswith("Estabelecimentos")
                                or item["name"] == "Motivos.zip"),
    )


def _identity_names() -> list[str]:
    return ["Empresas0.zip", "Estabelecimentos0.zip", "Estabelecimentos1.zip",
            "Cnaes.zip", "Naturezas.zip"]


def test_a092_full_manifest_metadata_permutation_does_not_change_identity() -> None:
    data = _resources()
    required = _identity_names()
    ordered = [next(item for item in data if item["name"] == name)
               for name in required]
    a = checked_release_fingerprint("2026-10", ordered, required)
    b = checked_release_fingerprint("2026-10", list(reversed(ordered)), required)
    assert a == b == release_fingerprint("2026-10", ordered)


@pytest.mark.parametrize("field,new_value", [
    ("etag", "changed-even-with-same-size"),
    ("size", 1001),
    ("last_modified", "changed-date"),
])
def test_a092_any_official_metadata_change_in_same_month_changes_fingerprint(
    field, new_value,
) -> None:
    release = _release()
    source = [dict(item) for item in release.resource_metadata]
    old = lifecycle_release_fingerprint(release)
    source[1][field] = new_value
    updated = SimpleNamespace(**{**vars(release), "resource_metadata": tuple(source)})
    assert old and lifecycle_release_fingerprint(updated) != old


@pytest.mark.parametrize("invalid", [
    {"size": None},
    {"size": 0},
    {"size": -1},
    {"etag": None, "last_modified": None},
    {"etag": "", "last_modified": ""},
    {"size": True},
])
def test_a092_missing_or_untrustworthy_metadata_is_never_current(invalid) -> None:
    release = _release()
    data = [dict(item) for item in release.resource_metadata]
    data[0].update(invalid)
    updated = SimpleNamespace(**{**vars(release), "resource_metadata": tuple(data)})
    assert lifecycle_release_fingerprint(updated) is None


def test_a092_missing_duplicate_or_extra_partition_rejects_comparison() -> None:
    release = _release()
    required = ["Motivos.zip", *release.establishment_files]
    data = list(release.resource_metadata)
    assert checked_release_fingerprint("2026-10", data, required)
    assert checked_release_fingerprint("2026-10", data[:-1], required) is None
    assert checked_release_fingerprint("2026-10", [*data, data[-1]], required) is None
    assert checked_release_fingerprint("2026-10", data, [*required, "missing.zip"]) is None
    assert checked_release_fingerprint("2026-10", data, [*required, required[0]]) is None


def test_a092_lifecycle_cache_requires_period_url_fingerprint_and_method() -> None:
    release = _release()
    fingerprint = lifecycle_release_fingerprint(release)
    assert fingerprint
    payload = {"source": {
        "reference_period": release.period,
        "release_url": release.release_url,
        "release_fingerprint": fingerprint,
        "release_fingerprint_method": FINGERPRINT_METHOD,
    }}
    assert lifecycle_cache_current(payload, release)
    for key, altered in (
        ("reference_period", "2026-09"),
        ("release_url", "https://wrong.example/"),
        ("release_fingerprint", "0" * 64),
        ("release_fingerprint_method", "unverified_method"),
    ):
        changed = {"source": {**payload["source"], key: altered}}
        assert not lifecycle_cache_current(changed, release)
    assert not lifecycle_cache_current({"source": {"reference_period": "2026-10"}}, release)
    assert not lifecycle_cache_current({}, release)


def test_a092_identity_cache_requires_complete_release_metadata_fingerprint() -> None:
    release = _release()
    full = _resources()
    selected = [next(item for item in full if item["name"] == name)
                for name in _identity_names()]
    fingerprint = checked_release_fingerprint(release.period, selected, _identity_names())
    assert fingerprint
    payload = {"source": {
        "reference_period": release.period, "release_url": release.release_url,
        "release_fingerprint_method": FINGERPRINT_METHOD,
        "release_fingerprint": fingerprint,
    }}
    assert identity_cache_current(payload, release, fingerprint)
    assert not identity_cache_current(payload, release, None)
    assert not identity_cache_current(payload, release, "0" * 64)
    assert not identity_cache_current({"source": {}}, release, fingerprint)


def test_a092_same_size_tamper_is_rejected_by_existing_cachedsource_hash(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.json"
    source.write_text('{"a":1}', encoding="utf-8")
    cache = CachedSource(
        source_id="receita_consumer_identity", source_url="https://official.example/",
        content_path=tmp_path / "cache.json",
        metadata_path=tmp_path / "cache.meta.json",
    )
    cache.store(source, fetched_at="2026-10-31T00:00:00Z")
    assert cache.materialize(tmp_path / "accepted.json") == "2026-10-31T00:00:00Z"
    cache.content_path.write_text('{"a":2}', encoding="utf-8")
    with pytest.raises(SourceCacheError, match="hash mismatch"):
        cache.materialize(tmp_path / "tampered.json")
    assert not (tmp_path / "tampered.json").exists()


def test_a092_builder_reuses_only_when_same_official_manifest(
    tmp_path: Path, monkeypatch,
) -> None:
    release = _release()
    payload = {
        "source": {
            "reference_period": release.period,
            "release_url": release.release_url,
            "release_fingerprint_method": FINGERPRINT_METHOD,
            "release_fingerprint": "a" * 64,
        },
        "meta": {},
    }
    entities = [{"cnpj": "11111111000111", "entity_id": "e1", "legal_name": "Demo"}]
    providers = ["Demo Seguradora"]
    key = identity_builder._cache_key(
        reference_period=release.period,
        universe_hash=identity_builder.target_universe_hash(entities),
        provider_hash=identity_builder.unresolved_provider_hash(providers),
    )
    payload["meta"]["gate4_cache_key"] = key
    dest = tmp_path / "identity.json"
    dest.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(identity_builder, "_load_inputs", lambda: (entities, providers))
    monkeypatch.setattr(identity_builder, "discover_latest_release", lambda: release)
    monkeypatch.setattr(identity_builder, "_official_identity_release_fingerprint",
                        lambda _: "a" * 64)
    monkeypatch.setattr(identity_builder, "build_filtered_identity_snapshot",
                        lambda *_a, **_k: pytest.fail("should not download ZIPs"))
    assert identity_builder.build_snapshot(output=dest) is None
    # Next invocation observes different official metadata in the SAME month:
    monkeypatch.setattr(identity_builder, "_official_identity_release_fingerprint",
                        lambda _: "b" * 64)
    monkeypatch.setattr(identity_builder, "build_filtered_identity_snapshot",
                        lambda *_a, **_k: {
                            **payload,
                            "source": {**payload["source"], "release_fingerprint": "b" * 64},
                            "meta": {},
                        })
    updated = identity_builder.build_snapshot(output=dest)
    assert updated is not None
    assert updated["source"]["release_fingerprint"] == "b" * 64
    assert json.loads(dest.read_text())["meta"]["gate4_cache_key"] == key


def test_a092_legacy_unfingerprinted_snapshot_cannot_claim_current(
    tmp_path: Path, monkeypatch,
) -> None:
    release = _release()
    entities = [{"cnpj": "11111111000111", "entity_id": "e1", "legal_name": "Demo"}]
    providers = ["Demo Seguradora"]
    old = {
        "source": {"reference_period": release.period, "release_url": release.release_url},
        "meta": {"gate4_cache_key": identity_builder._cache_key(
            reference_period=release.period,
            universe_hash=identity_builder.target_universe_hash(entities),
            provider_hash=identity_builder.unresolved_provider_hash(providers),
        )},
    }
    dest = tmp_path / "legacy.json"
    dest.write_text(json.dumps(old), encoding="utf-8")
    monkeypatch.setattr(identity_builder, "_load_inputs", lambda: (entities, providers))
    monkeypatch.setattr(identity_builder, "discover_latest_release", lambda: release)
    monkeypatch.setattr(identity_builder, "_official_identity_release_fingerprint",
                        lambda _: "f" * 64)
    calls = []
    def rebuild(*_args, **_kwargs):
        calls.append(True)
        return {**old, "source": {
            **old["source"], "release_fingerprint": "f" * 64,
            "release_fingerprint_method": FINGERPRINT_METHOD,
        }}
    monkeypatch.setattr(identity_builder, "build_filtered_identity_snapshot", rebuild)
    assert identity_builder.build_snapshot(output=dest) is not None
    assert calls == [True]


def test_a092_lifecycle_standalone_skip_needs_metadata_fingerprint(
    tmp_path: Path,
) -> None:
    from api.v2.refresh_receita_lifecycle import _snapshot_release_metadata_current

    release = _release()
    fp = lifecycle_release_fingerprint(release)
    assert fp
    dest = tmp_path / "lifecycle.json"
    dest.write_text(json.dumps({"source": {
        "reference_period": release.period, "release_url": release.release_url,
        "release_fingerprint": fp, "release_fingerprint_method": FINGERPRINT_METHOD,
    }}), encoding="utf-8")
    assert _snapshot_release_metadata_current(dest, release)
    changed = _release()
    metadata = list(changed.resource_metadata)
    metadata[0] = {**metadata[0], "etag": "new"}
    revised = SimpleNamespace(**{**vars(changed), "resource_metadata": tuple(metadata)})
    assert not _snapshot_release_metadata_current(dest, revised)
    dest.write_text("{invalid JSON", encoding="utf-8")
    assert not _snapshot_release_metadata_current(dest, release)
