from __future__ import annotations

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from api.v2.relationship_watchdog import validate_relationship_watchdog

DEFAULT_WATCHDOG = Path("data/derived/v2/relationship_watchdog.json")
DEFAULT_MARKET_REGISTRY = Path("data/derived/v2/market_identity_candidates.json")
DEFAULT_OUTPUT = Path("data/derived/v2/relationship_watchdog_with_market_sensors.json")


class MarketWatchdogMergeError(RuntimeError):
    """Raised when a market sensor could weaken the relationship watchdog contract."""


def _validate_market_registry(payload: dict[str, Any]) -> None:
    if payload.get("artifact") != "v2_market_identity_candidate_registry":
        raise MarketWatchdogMergeError("unexpected market identity candidate artifact")
    policy = payload.get("policy") or {}
    required = {
        "candidate_assertion_effect": "none",
        "candidate_score_effect": "none",
        "candidate_complaint_transfer_effect": "none",
        "automatic_registry_mutation": "forbidden",
    }
    for key, expected in required.items():
        if policy.get(key) != expected:
            raise MarketWatchdogMergeError(
                f"market candidate policy weakens {key}: {policy.get(key)!r}"
            )
    for candidate in payload.get("candidates") or []:
        if candidate.get("assertion_effect") != "none":
            raise MarketWatchdogMergeError("market candidate has assertion effect")
        if candidate.get("score_effect") != "none":
            raise MarketWatchdogMergeError("market candidate has score effect")
        if candidate.get("complaint_transfer_effect") != "none":
            raise MarketWatchdogMergeError("market candidate allows complaint transfer")
        if candidate.get("automatic_registry_mutation") != "forbidden":
            raise MarketWatchdogMergeError("market candidate allows registry mutation")
        if bool(candidate.get("blocking")):
            raise MarketWatchdogMergeError("observational market candidate cannot block Gate 4")


def merge_market_candidates_into_watchdog(
    watchdog: dict[str, Any],
    market_registry: dict[str, Any],
) -> dict[str, Any]:
    validate_relationship_watchdog(watchdog)
    _validate_market_registry(market_registry)

    output = deepcopy(watchdog)
    candidates = list(output.get("candidates") or [])
    existing_ids = {str(row.get("candidate_id") or "") for row in candidates}
    market_review_count = 0
    market_observed_count = 0
    for raw in market_registry.get("candidates") or []:
        candidate = deepcopy(raw)
        candidate_id = str(candidate.get("candidate_id") or "")
        if not candidate_id:
            raise MarketWatchdogMergeError("market candidate without candidate_id")
        if candidate_id in existing_ids:
            raise MarketWatchdogMergeError(f"candidate id collision: {candidate_id}")
        candidate["candidate_domain"] = "emerging_market_identity"
        candidate["severity"] = "review"
        candidate["signals"] = {
            "sources": sorted(
                {
                    str(row.get("source") or "unknown")
                    for row in candidate.get("observations") or []
                }
            ),
            "candidate_anchor": candidate.get("candidate_anchor"),
        }
        if candidate.get("review_state") == "review_required":
            market_review_count += 1
        else:
            market_observed_count += 1
        candidates.append(candidate)
        existing_ids.add(candidate_id)

    candidates.sort(
        key=lambda item: (
            not bool(item.get("blocking")),
            str(item.get("severity") or ""),
            str(item.get("candidate_type") or ""),
            str(item.get("candidate_id") or ""),
        )
    )
    output["candidates"] = candidates

    blocking_count = sum(bool(item.get("blocking")) for item in candidates)
    review_count = sum(
        item.get("review_state") in {"review_required", "registry_drift"}
        for item in candidates
    )
    summary = dict(output.get("summary") or {})
    summary["candidate_count"] = len(candidates)
    summary["blocking_registry_drift_count"] = blocking_count
    summary["review_candidate_count"] = review_count
    summary["market_identity_review_count"] = market_review_count
    summary["market_identity_observed_only_count"] = market_observed_count
    output["summary"] = summary
    output["status"] = (
        "blocking_registry_drift"
        if blocking_count
        else "review_candidates"
        if review_count
        else "clear"
    )
    inputs = dict(output.get("inputs") or {})
    inputs["market_identity_candidates_artifact"] = market_registry.get("artifact")
    output["inputs"] = inputs
    output["market_sensor_status"] = deepcopy(market_registry.get("sensor_status") or {})
    output["market_sensor_policy"] = deepcopy(market_registry.get("policy") or {})

    # The existing validator is intentionally kept as the final authority for blocking
    # verified-registry drift. Market candidates are non-blocking by construction.
    validate_relationship_watchdog(output)
    return output


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge observational market candidates into a relationship review artifact."
    )
    parser.add_argument("--watchdog", type=Path, default=DEFAULT_WATCHDOG)
    parser.add_argument("--market-registry", type=Path, default=DEFAULT_MARKET_REGISTRY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    watchdog = json.loads(args.watchdog.read_text(encoding="utf-8"))
    market_registry = json.loads(args.market_registry.read_text(encoding="utf-8"))
    payload = merge_market_candidates_into_watchdog(watchdog, market_registry)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "summary": payload["summary"],
                "market_sensor_status": payload.get("market_sensor_status"),
                "output": str(args.output),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
