"""Isolated official CSV proof; no production outputs, caches or credentials."""
import csv
import gzip
import hashlib
import json
import os
import resource
import shutil
import sys
import time
from collections import Counter
from pathlib import Path

import requests

from api.build_consumidor_gov import _aggregate_basecompleta
from api.sources.consumer_gov_csv import detect_csv_encoding
from api.sources.consumer_gov_direct import validate_month_csv
from api.v2.compare_consumer_gov_sources import _canonical_entry_payload

MONTH = "2026-03"
URL = (
    "https://dados.mj.gov.br/dataset/"
    "0182f1bf-e73d-42b1-ae8c-fa94d9ce9451/resource/"
    "0dff3649-252f-4d9e-90f6-6a89181d91fa/download/basecompleta2026-03.csv"
)
ROOT = Path(os.getenv("RUNNER_TEMP", "/tmp")) / "consumer-gov-official-csv-isolated"
HIST = Path("data/derived/consumidor_gov/monthly/consumidor_gov_2026-03.json.gz")
LIMIT = 450_000_000


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(2**20), b""):
            h.update(b)
    return h.hexdigest()


def resource_state():
    return {
        "peak_rss_mib": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2),
        "free_disk_mib": round(shutil.disk_usage(ROOT).free / 2**20, 1),
    }


def diagnose(path):
    """Count only temporal/enum distributions, never print consumer rows."""
    opened, answered, situations = Counter(), Counter(), Counter()
    encoding = detect_csv_encoding(path)
    with path.open("r", encoding=encoding, newline="") as f:
        first = f.readline()
        sep = ";" if first.count(";") >= first.count(",") else ","
        f.seek(0)
        reader = csv.DictReader(f, delimiter=sep)
        headers = reader.fieldnames
        for row in reader:
            opened[(row.get("Ano Abertura"), row.get("Mês Abertura"))] += 1
            answered[row.get("Respondida", "")] += 1
            situations[row.get("Situação", "")] += 1
    return {
        "encoding": encoding,
        "headers": headers,
        "opening_pairs_top": opened.most_common(8),
        "responded_values": answered.most_common(8),
        "situations_top": situations.most_common(8),
    }


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    path = ROOT / "official.csv"
    report = {
        "artifact": "consumer_gov_official_csv_isolated_probe",
        "source": URL, "source_family": "base_completa",
        "month": MONTH, "production_modified": False,
        "start_resources": resource_state(),
    }
    started = time.perf_counter()
    try:
        try:
            with requests.get(URL, stream=True, timeout=(12, 30), allow_redirects=True) as res:
                report["http_status"] = res.status_code
                report["response_host"] = requests.utils.urlparse(res.url).hostname
                res.raise_for_status()
                declared = res.headers.get("Content-Length")
                expected = int(declared) if declared is not None else None
                total = 0
                with path.open("xb") as f:
                    for chunk in res.iter_content(2**20):
                        if not chunk:
                            continue
                        total += len(chunk)
                        if total > LIMIT or time.perf_counter()-started > 105:
                            raise RuntimeError("download bound exceeded")
                        f.write(chunk)
                if expected is not None and total != expected:
                    raise RuntimeError(f"HTTP length mismatch: {total}/{expected}")
                report["csv_bytes"] = total
        except (requests.RequestException, ValueError) as e:
            raise RuntimeError(f"official download unavailable: {type(e).__name__}: {e}") from e
        report["download_seconds"] = round(time.perf_counter()-started, 2)
        with path.open("rb") as f:
            if f.read(64).lstrip().lower().startswith((b"<!doctype", b"<html")):
                raise RuntimeError("HTML returned instead of CSV")
        report["csv_sha256"] = digest(path)
        t = time.perf_counter()
        try:
            v = validate_month_csv(path, MONTH, min_bytes=20_000_000)
            report["validation"] = "pass"
            report["rows"] = v.rows
            report["encoding"] = v.encoding
        except Exception as e:
            report["validation"] = "fail"
            report["validation_error"] = f"{type(e).__name__}: {e}"
        report["validation_seconds"] = round(time.perf_counter()-t, 2)
        report["diagnostic"] = diagnose(path)
        if report["validation"] != "pass":
            raise RuntimeError("official temporal/schema contract not established")
        t = time.perf_counter()
        aggregated = _aggregate_basecompleta(path, MONTH, URL)
        report["aggregation_seconds"] = round(time.perf_counter()-t, 2)
        with gzip.open(HIST, "rt", encoding="utf-8") as f:
            baseline = json.load(f)
        earlier = _canonical_entry_payload(baseline)
        current = _canonical_entry_payload(aggregated)
        keys = set(earlier) | set(current)
        report["historical_sha256"] = digest(HIST)
        report["comparison"] = {
            "historical_providers": len(earlier),
            "official_providers": len(current),
            "historical_complaints": sum(int(v.get("complaintsCount") or 0) for v in earlier.values()),
            "official_complaints": sum(int(v.get("complaintsCount") or 0) for v in current.values()),
            "keys_changed": len(set(earlier) ^ set(current)),
            "providers_with_changed_metrics": sum(earlier.get(k) != current.get(k) for k in keys),
            "equal_by_provider_metrics": earlier == current,
            "observational_external_row_reference": 342527,
        }
        report["status"] = "PASS" if earlier == current else "MISMATCH"
    except Exception as e:
        report["status"] = "BLOCKED"
        report["failure_reason"] = f"{type(e).__name__}: {e}"
    finally:
        report["total_seconds"] = round(time.perf_counter()-started,2)
        report["final_resources"] = resource_state()
        path.unlink(missing_ok=True)
        print("ISOLATED_BASE_COMPLETA_RESULT")
        print(json.dumps(report,ensure_ascii=False,indent=2))
    return 0 if report["status"]=="PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
