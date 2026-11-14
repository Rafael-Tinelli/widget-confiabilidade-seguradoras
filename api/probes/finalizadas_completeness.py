"""A05: cautious population-completeness evidence for finalizadas.

A valid ZIP/CSV, a locally recorded SHA-256 and a matching published ZIP size
do NOT prove that every finalized complaint was included. No independent
official month-level row totals are available in the contracted publication
catalog. Never produce a 'complete' or 'certified' result from these checks.
"""
from __future__ import annotations

import calendar

# Plausibility signal, not an asserted publication requirement. The nationwide
# file is substantial; sparse small test corpora must not be treated as complete.
MIN_ROWS_FOR_CALENDAR_ALERT = 1_000
MISSING_DAYS_ALERT_STREAK = 3

COMPLETENESS_STATUS = "not_demonstrated"
NO_INDEPENDENT_TOTAL = "official_monthly_row_total_unavailable"


def _max_streak(days: list[int]) -> int:
    longest = current = 0
    previous: int | None = None
    for day in days:
        current = current + 1 if previous is not None and day == previous + 1 else 1
        longest = max(longest, current)
        previous = day
    return longest


def observed_population(month: str, dates: dict[int, int], total: int) -> dict:
    """Report all-segment calendar coverage without asserting completeness."""
    year, number = map(int, month.split("-"))
    last_day = calendar.monthrange(year, number)[1]
    observed_days = sorted(dates)
    missing_days = [d for d in range(1, last_day + 1) if d not in dates]
    if total != sum(dates.values()) or not observed_days:
        raise ValueError("Inconsistent observed month date distribution")
    if not all(1 <= d <= last_day and type(n) is int and n > 0
               for d, n in dates.items()):
        raise ValueError("Invalid day count in observed distribution")
    warnings: list[str] = []
    if (total >= MIN_ROWS_FOR_CALENDAR_ALERT
            and _max_streak(missing_days) >= MISSING_DAYS_ALERT_STREAK):
        warnings.append("calendar_coverage_gap_requires_review")
    return {
        "status": COMPLETENESS_STATUS,
        "reason": NO_INDEPENDENT_TOTAL,
        "official_monthly_rows": None,
        "observed_rows": total,
        "calendar": {
            "month": month, "calendar_days": last_day,
            "distinct_days": len(observed_days),
            "first_day": observed_days[0],
            "last_day": observed_days[-1],
            "missing_days": missing_days,
            "maximum_consecutive_missing_days": _max_streak(missing_days),
            "plausibility_min_rows": MIN_ROWS_FOR_CALENDAR_ALERT,
        },
        "checks": {
            "published_zip_bytes_match": None,
            "pinned_observation_sha256_match": None,
            "previous_observation_rows_match": None,
        },
        "warnings": warnings,
    }


def compare_publication(
    completeness: dict, *, zip_bytes: int, zip_sha256: str,
    publication: dict, previous: dict | None,
) -> dict:
    """Mark inconsistencies; never promote an unverified population."""
    evidence = {
        **completeness,
        "checks": dict(completeness["checks"]),
        "warnings": list(completeness["warnings"]),
    }
    checks = evidence["checks"]
    announced = publication.get("announced_bytes")
    if type(announced) is int:
        checks["published_zip_bytes_match"] = announced == zip_bytes
        if announced != zip_bytes:
            evidence["warnings"].append("published_zip_bytes_mismatch")
    pinned = publication.get("expected_sha256")
    if isinstance(pinned, str) and pinned:
        checks["pinned_observation_sha256_match"] = pinned == zip_sha256
        if pinned != zip_sha256:
            evidence["warnings"].append("pinned_observation_sha256_mismatch")

    # A historical observation is an anomaly detector, NOT an independently
    # certified number of complaints. Compare only the same source URL/code.
    prior = previous if isinstance(previous, dict) else {}
    previous_rows = prior.get("rows_total")
    same_publication = (
        prior.get("source_url") == publication.get("url")
        and prior.get("publication_code") == publication.get("publication_code")
        and prior.get("publication_code") not in (None, "")
    )
    if same_publication and type(previous_rows) is int:
        checks["previous_observation_rows_match"] = (
            previous_rows == completeness["observed_rows"]
        )
        if previous_rows != completeness["observed_rows"]:
            evidence["warnings"].append("previous_observation_rows_changed")
    return evidence
