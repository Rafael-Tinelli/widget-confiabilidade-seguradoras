"""A09.5: observe official WebDAV Content-Length without downloading ZIP bodies.

Two rounds of HEAD and streamed GET headers for two official ZIP partitions.
No body is read. Logs include safe sizes, ETag and redirect status/host only,
never authenticated URLs, credentials, raw content or personal data.
PROPFIND-only completeness checks are reused from the final crossover proof.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from urllib.parse import urlsplit

import requests

from api.sources.receita_cnpj_bulk import _configure_official_session
from scripts.a095_32m_crossover_probe import _official_catalog
from scripts.a095_bounded_get_probe import GetProbeError

SCHEMA = "a095-http-content-length-diagnostic-v1"
METHODS = ("HEAD", "GET")
ROUNDS = 2
MAX_FILES = 2
OFFICIAL_HOST = "arquivos.receitafederal.gov.br"


def _safe_header(value: str | None) -> str | None:
    """Keep evidence bounded without ever logging sensitive URL paths."""
    return str(value).strip()[:160] if value is not None else None


def _length(value: str | None) -> int | None:
    return int(value) if isinstance(value, str) and value.isdecimal() else None


def _header_observation(method: str, resource: dict, round_number: int) -> dict:
    """Only parse HTTP response headers; stream=True prevents body preload."""
    if method not in METHODS:
        raise GetProbeError("unsupported_method")
    if type(round_number) is not int or not 1 <= round_number <= ROUNDS:
        raise GetProbeError("unsupported_round")
    url = resource.get("url")
    if not isinstance(url, str) or urlsplit(url).hostname != OFFICIAL_HOST:
        raise GetProbeError("unexpected_official_resource_host")
    index, size = resource.get("file_index"), resource.get("size")
    if type(index) is not int or index not in (0, 1):
        raise GetProbeError("unexpected_file_index")
    if type(size) is not int or size < 1024:
        raise GetProbeError("invalid_official_resource_size")
    started = time.monotonic()
    with requests.Session() as sess:
        _configure_official_session(sess)
        with sess.request(
            method,
            url,
            stream=True,
            allow_redirects=True,
            timeout=(10, 20),
            headers={"Accept-Encoding": "identity"},
        ) as response:
            # Intentionally no response.content, response.text, read(),
            # iter_content() or download of a national ZIP.
            raw_length = _safe_header(response.headers.get("Content-Length"))
            header_etag = _safe_header(response.headers.get("ETag"))
            return {
                "round": round_number,
                "file_index": index,
                "method": method,
                "expected_bytes": size,
                "expected_etag": _safe_header(resource.get("etag")),
                "http_status": response.status_code,
                "response_content_length": raw_length,
                "response_bytes": _length(raw_length),
                "response_etag": header_etag,
                "response_last_modified": _safe_header(
                    response.headers.get("Last-Modified")
                ),
                "response_content_type": _safe_header(
                    response.headers.get("Content-Type")
                ),
                "response_content_encoding": _safe_header(
                    response.headers.get("Content-Encoding")
                ),
                "response_transfer_encoding": _safe_header(
                    response.headers.get("Transfer-Encoding")
                ),
                "response_content_range": _safe_header(
                    response.headers.get("Content-Range")
                ),
                "final_hostname": urlsplit(response.url).hostname,
                "redirect_statuses": [r.status_code for r in response.history],
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "application_body_bytes_read": 0,
            }


def _catalog_signature(catalog: dict) -> dict:
    resources = catalog["resources"]
    if len(resources) != MAX_FILES or [r["file_index"] for r in resources] != [0, 1]:
        raise GetProbeError("missing_expected_probe_files")
    return {
        "reference_period": catalog["reference_period"],
        "fingerprint": catalog["fingerprint"],
        "lifecycle_fingerprint": catalog["lifecycle_fingerprint"],
        "required_files": catalog["required_files"],
        "company_partitions": catalog["company_partitions"],
        "establishment_partitions": catalog["establishment_partitions"],
        "files": [
            {
                "file_index": r["file_index"],
                "expected_bytes": r["size"],
                "expected_etag": _safe_header(r.get("etag")),
            }
            for r in resources
        ],
    }


def _classify(catalogs: list[dict], samples: list[dict]) -> dict:
    if len(catalogs) != ROUNDS + 1:
        raise GetProbeError("missing_catalog_round")
    signatures = [_catalog_signature(c) for c in catalogs]
    stable = all(s == signatures[0] for s in signatures[1:])
    get_samples = [x for x in samples if x["method"] == "GET"]
    if len(get_samples) != ROUNDS * MAX_FILES:
        raise GetProbeError("missing_get_header_observation")

    unexpected_http = [
        {"round": x["round"], "file_index": x["file_index"],
         "method": x["method"], "http_status": x["http_status"]}
        for x in samples
        if (x["method"] == "GET" and x["http_status"] != 200)
        or (x["method"] == "GET" and x.get("response_content_range"))
    ]
    missing = [
        {"round": x["round"], "file_index": x["file_index"]}
        for x in get_samples if x["response_bytes"] is None
    ]
    mismatches = [
        {
            "round": x["round"],
            "file_index": x["file_index"],
            "expected_bytes": x["expected_bytes"],
            "received_header_bytes": x["response_bytes"],
            "delta_bytes": x["response_bytes"] - x["expected_bytes"],
            "response_etag": x["response_etag"],
            "expected_etag": x["expected_etag"],
            "response_last_modified": x["response_last_modified"],
        }
        for x in get_samples
        if x["response_bytes"] is not None
        and x["response_bytes"] != x["expected_bytes"]
    ]
    etag_mismatches = [
        {"round": x["round"], "file_index": x["file_index"]}
        for x in get_samples
        if x["expected_etag"] and x["response_etag"]
        and x["expected_etag"] != x["response_etag"]
    ]
    encoded = [
        {"round": x["round"], "file_index": x["file_index"]}
        for x in get_samples
        if x["response_content_encoding"] not in (None, "", "identity")
    ]

    if not stable:
        status = "official_catalog_changed"
    elif mismatches:
        status = "content_length_mismatch_reproduced"
    elif unexpected_http or missing or etag_mismatches or encoded:
        status = "header_inconclusive_or_inconsistent"
    else:
        status = "header_match_not_historical_exoneration"
    return {
        "status": status,
        "catalog_stable": stable,
        "content_length_mismatches": mismatches,
        "missing_get_content_length": missing,
        "etag_mismatch_get_vs_catalog": etag_mismatches,
        "unexpected_get_http": unexpected_http,
        "unexpected_get_encoding": encoded,
        "head_responses": [
            {"round": x["round"], "file_index": x["file_index"],
             "http_status": x["http_status"], "response_bytes": x["response_bytes"]}
            for x in samples if x["method"] == "HEAD"
        ],
    }


def run_diagnostic() -> dict:
    catalogs = [_official_catalog()]
    samples = []
    for round_number in range(1, ROUNDS + 1):
        current = catalogs[-1]
        for resource in current["resources"]:
            for method in METHODS:
                samples.append(_header_observation(method, resource, round_number))
        catalogs.append(_official_catalog())
        if _catalog_signature(catalogs[-1]) != _catalog_signature(catalogs[0]):
            break

    # Return partial evidence, not an ungrounded success, if the catalog changed.
    if len(catalogs) != ROUNDS + 1:
        outcome = {
            "status": "official_catalog_changed",
            "catalog_stable": False,
            "content_length_mismatches": [],
        }
    else:
        outcome = _classify(catalogs, samples)
    return {
        "schema": SCHEMA,
        **outcome,
        "catalogs": [_catalog_signature(c) for c in catalogs],
        "header_observations": samples,
        "max_get_requests": MAX_FILES * ROUNDS,
        "max_head_requests": MAX_FILES * ROUNDS,
        "application_body_bytes_read": 0,
        "limits": (
            "No body data is read by Python; HTTP/TCP buffers may receive bytes "
            "before socket close. Metadata checks cannot prove full ZIP integrity."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        report = run_diagnostic()
    except (GetProbeError, requests.RequestException, OSError, ValueError, KeyError) as exc:
        report = {
            "schema": SCHEMA,
            "status": "diagnostic_failed",
            # Never print exception text from HTTP clients: it may contain
            # the public-share authentication path/token.
            "reason": str(exc) if isinstance(exc, GetProbeError) else type(exc).__name__,
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2, ensure_ascii=False), flush=True)
    if report["status"] in ("diagnostic_failed", "official_catalog_changed"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
