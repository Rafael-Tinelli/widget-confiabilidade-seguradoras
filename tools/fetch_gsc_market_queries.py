#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote

import requests

SEARCH_ANALYTICS_ENDPOINT = (
    "https://www.googleapis.com/webmasters/v3/sites/{site}/searchAnalytics/query"
)
DEFAULT_OUTPUT = Path("data/derived/v2/sensors/gsc_market_queries.json")


class GscSensorError(RuntimeError):
    """Raised when an explicitly configured GSC sensor cannot return trusted rows."""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Search Console queries for the ranking page as an observational sensor."
    )
    parser.add_argument("--site-url", required=True, help="Search Console property URL or sc-domain value")
    parser.add_argument("--page-url", required=True, help="Exact canonical ranking/widget page URL")
    parser.add_argument("--days", type=int, default=28)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--row-limit", type=int, default=25000)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def fetch_queries(
    *,
    site_url: str,
    page_url: str,
    access_token: str,
    start_date: date,
    end_date: date,
    row_limit: int = 25000,
    session: requests.Session | None = None,
) -> list[dict[str, object]]:
    if not access_token:
        raise GscSensorError("GSC access token is required")
    if start_date > end_date:
        raise ValueError("GSC start_date must not be after end_date")
    if row_limit < 1 or row_limit > 25000:
        raise ValueError("GSC row_limit must be between 1 and 25000")

    client = session or requests.Session()
    endpoint = SEARCH_ANALYTICS_ENDPOINT.format(site=quote(site_url, safe=""))
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    output: list[dict[str, object]] = []
    start_row = 0
    while True:
        body = {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "dimensions": ["query"],
            "dimensionFilterGroups": [
                {
                    "groupType": "and",
                    "filters": [
                        {
                            "dimension": "page",
                            "operator": "equals",
                            "expression": page_url,
                        }
                    ],
                }
            ],
            "type": "web",
            "dataState": "final",
            "rowLimit": row_limit,
            "startRow": start_row,
        }
        response = client.post(endpoint, headers=headers, json=body, timeout=60)
        if response.status_code != 200:
            raise GscSensorError(
                f"Search Console API returned HTTP {response.status_code}: "
                f"{response.text[:400]}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise GscSensorError("Search Console API returned malformed JSON") from exc

        rows = payload.get("rows") or []
        if not isinstance(rows, list):
            raise GscSensorError("Search Console API rows are malformed")
        for row in rows:
            keys = row.get("keys") or []
            if len(keys) != 1 or not isinstance(keys[0], str):
                raise GscSensorError("Search Console query row has unexpected dimensions")
            output.append(
                {
                    "query": keys[0],
                    "clicks": int(round(float(row.get("clicks") or 0))),
                    "impressions": int(round(float(row.get("impressions") or 0))),
                }
            )

        if len(rows) < row_limit:
            break
        start_row += row_limit
    return output


def main() -> None:
    args = _parse_args()
    if args.days < 1:
        raise SystemExit("--days must be positive")
    # Finalized Search Console data can lag. Default to yesterday rather than today;
    # callers can pin an explicit end date for reproducible review runs.
    end_date = args.end_date or (date.today() - timedelta(days=1))
    start_date = end_date - timedelta(days=args.days - 1)
    token = os.getenv("GSC_ACCESS_TOKEN", "")
    rows = fetch_queries(
        site_url=args.site_url,
        page_url=args.page_url,
        access_token=token,
        start_date=start_date,
        end_date=end_date,
        row_limit=args.row_limit,
    )
    payload = {
        "artifact": "v2_gsc_market_query_snapshot",
        "authority": "Google Search Console demand signal; not identity authority",
        "site_url": args.site_url,
        "page_url": args.page_url,
        "window": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        "queries": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "query_count": len(rows),
                "window": payload["window"],
                "output": str(args.output),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
