"""Offline adversarial tests for A09.5 Content-Length header-only diagnosis."""
from __future__ import annotations

import copy
from pathlib import Path

import pytest

from scripts import a095_content_length_diagnostic as probe
from scripts.a095_bounded_get_probe import GetProbeError


def _catalog(*, fingerprint: str = "a" * 64, size0: int = 2321438760) -> dict:
    return {
        "reference_period": "2026-11",
        "fingerprint": fingerprint,
        "lifecycle_fingerprint": "b" * 64,
        "required_files": 23,
        "company_partitions": 10,
        "establishment_partitions": 10,
        "resources": [
            {"url": "https://arquivos.receitafederal.gov.br/public.php/dav/files/secret/2026-11/Estabelecimentos0.zip",
             "file_index": 0, "size": size0, "etag": '"etag0"'},
            {"url": "https://arquivos.receitafederal.gov.br/public.php/dav/files/secret/2026-11/Estabelecimentos1.zip",
             "file_index": 1, "size": 342021474, "etag": '"etag1"'},
        ],
    }


def _samples(*, mismatch: bool = False, bad_head: bool = False) -> list[dict]:
    out = []
    for round_ in (1, 2):
        for idx, size in ((0, 2321438760), (1, 342021474)):
            for method in ("HEAD", "GET"):
                length = size + 123 if mismatch and round_ == 1 and idx == 1 and method == "GET" else size
                out.append({
                    "round": round_, "file_index": idx, "method": method,
                    "expected_bytes": size, "expected_etag": f'"etag{idx}"',
                    "http_status": 405 if bad_head and method == "HEAD" else 200,
                    "response_bytes": length,
                    "response_etag": f'"etag{idx}"',
                    "response_last_modified": "Mon, 16 Nov 2026 19:00:00 GMT",
                    "response_content_encoding": None, "response_content_range": None,
                })
    return out


def test_a095_header_evidence_identifies_index_and_exact_delta() -> None:
    outcome = probe._classify([_catalog(), _catalog(), _catalog()], _samples(mismatch=True))
    assert outcome["status"] == "content_length_mismatch_reproduced"
    assert outcome["catalog_stable"]
    assert outcome["content_length_mismatches"] == [{
        "round": 1,
        "file_index": 1,
        "expected_bytes": 342021474,
        "received_header_bytes": 342021597,
        "delta_bytes": 123,
        "response_etag": '"etag1"',
        "expected_etag": '"etag1"',
        "response_last_modified": "Mon, 16 Nov 2026 19:00:00 GMT",
    }]


def test_a095_headers_equal_does_not_exonerate_historical_mismatch() -> None:
    result = probe._classify([_catalog(), _catalog(), _catalog()], _samples(bad_head=True))
    assert result["status"] == "header_match_not_historical_exoneration"
    assert result["content_length_mismatches"] == []
    assert all(x["http_status"] == 405 for x in result["head_responses"])


def test_a095_detects_catalog_change_independent_of_get() -> None:
    result = probe._classify(
        [_catalog(), _catalog(fingerprint="c" * 64), _catalog()], _samples()
    )
    assert result["status"] == "official_catalog_changed"
    assert not result["catalog_stable"]


@pytest.mark.parametrize("field,value", [
    ("response_bytes", None),
    ("response_content_encoding", "gzip"),
    ("response_content_range", "bytes 0-99/100"),
    ("http_status", 500),
    ("response_etag", '"different"'),
])
def test_a095_inconclusive_headers_flagged(field: str, value) -> None:
    samples = _samples()
    target = next(x for x in samples if x["round"] == 1
                  and x["file_index"] == 0 and x["method"] == "GET")
    target[field] = value
    result = probe._classify([_catalog(), _catalog(), _catalog()], samples)
    assert result["status"] == "header_inconclusive_or_inconsistent"


class FakeResponse:
    status_code = 200
    url = "https://arquivos.receitafederal.gov.br/index.php/s/authentication-token"
    headers = {
        "Content-Length": "2321438760",
        "ETag": '"etag0"',
        "Last-Modified": "Mon, 16 Nov 2026 19:00:00 GMT",
        "Content-Type": "application/zip",
    }
    history = []

    def __init__(self) -> None:
        self.closed = False
        self.body_read = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.closed = True

    @property
    def content(self):
        self.body_read = True
        raise AssertionError("body must not be consumed")

    @property
    def text(self):
        self.body_read = True
        raise AssertionError("body must not be consumed")

    def iter_content(self, *_args, **_kwargs):
        self.body_read = True
        raise AssertionError("body must not be consumed")


def test_a095_only_gets_headers_and_never_logs_url_or_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = FakeResponse()
    requests_seen = []

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def request(self, method, url, **kwargs):
            requests_seen.append((method, url, kwargs))
            return response

    monkeypatch.setattr(probe.requests, "Session", Session)
    monkeypatch.setattr(probe, "_configure_official_session", lambda _: None)
    res = _catalog()["resources"][0]
    actual = probe._header_observation("GET", res, 1)
    assert requests_seen[0][0] == "GET"
    assert requests_seen[0][2]["stream"] is True
    assert requests_seen[0][2]["timeout"] == (10, 20)
    assert requests_seen[0][2]["allow_redirects"] is True
    assert requests_seen[0][2]["headers"]["Accept-Encoding"] == "identity"
    assert response.closed
    assert not response.body_read
    assert actual["response_bytes"] == 2321438760
    assert actual["application_body_bytes_read"] == 0
    assert actual["final_hostname"] == "arquivos.receitafederal.gov.br"
    assert "authentication-token" not in str(actual)
    assert "secret" not in str(actual)


@pytest.mark.parametrize("invalid", [
    "https://untrusted.example/file.zip",
    "http://arquivos.receitafederal.gov.br/file.zip",
])
def test_a095_rejects_untrusted_or_insecure_url_without_http(
    monkeypatch: pytest.MonkeyPatch, invalid: str,
) -> None:
    monkeypatch.setattr(probe.requests, "Session", lambda: pytest.fail("unexpected HTTP"))
    resource = {**_catalog()["resources"][0], "url": invalid}
    with pytest.raises(GetProbeError):
        probe._header_observation("GET", resource, 1)


def test_a095_two_rounds_four_get_four_head_no_zip_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _catalog()
    calls = []
    catalogs = [copy.deepcopy(first) for _ in range(3)]

    monkeypatch.setattr(probe, "_official_catalog", lambda: catalogs.pop(0))
    def fake_headers(method: str, resource: dict, round_number: int) -> dict:
        calls.append((method, resource["file_index"], round_number))
        return {
            "method": method,
            "file_index": resource["file_index"],
            "round": round_number,
            "expected_bytes": resource["size"],
            "expected_etag": resource["etag"],
            "response_bytes": resource["size"],
            "response_etag": resource["etag"],
            "response_last_modified": None,
            "response_content_encoding": None,
            "response_content_range": None,
            "http_status": 200,
        }
    monkeypatch.setattr(probe, "_header_observation", fake_headers)
    report = probe.run_diagnostic()
    assert report["status"] == "header_match_not_historical_exoneration"
    assert report["max_get_requests"] == 4
    assert report["max_head_requests"] == 4
    assert report["application_body_bytes_read"] == 0
    assert len(report["header_observations"]) == 8
    assert calls.count(("GET", 0, 1)) == 1
    assert calls.count(("HEAD", 1, 2)) == 1


def test_a095_stops_on_catalog_mutation_without_new_gets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalogs = [_catalog(), _catalog(fingerprint="c" * 64)]
    calls = []
    monkeypatch.setattr(probe, "_official_catalog", lambda: catalogs.pop(0))
    def fake_headers(method, resource, round_number):
        calls.append((round_number, method, resource["file_index"]))
        return {"method": method, "file_index": resource["file_index"]}
    monkeypatch.setattr(probe, "_header_observation", fake_headers)
    report = probe.run_diagnostic()
    assert report["status"] == "official_catalog_changed"
    assert len(calls) == 4
    assert not catalogs


def test_a095_diagnostic_is_manual_and_cannot_deploy() -> None:
    w = Path(".github/workflows/v2-a095-content-length-diagnostic.yml").read_text()
    pub = Path(".github/workflows/v2-hostgator-publication.yml").read_text()
    source = Path("scripts/a095_content_length_diagnostic.py").read_text()
    assert "name: V2 A09.5 Content-Length Diagnostic" in w
    assert "workflow_dispatch:" in w
    assert "if: inputs.confirm_probe == 'HEADERS'" in w
    assert "timeout-minutes: 8" in w
    assert "actions/cache/restore" not in w
    assert "actions/cache/save" not in w
    assert "gate4_pipeline" not in w
    assert "v2-hostgator" not in w
    assert "V2 A09.5 Content-Length Diagnostic" not in pub
    assert 'stream=True,' in source
    assert "for method in METHODS" in source
    assert "iter_content(" not in source
    assert "response.content" not in source
    assert "response.text" not in source
