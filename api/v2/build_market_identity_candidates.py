from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from api.v2.market_identity_observations import (
    DemandReviewThresholds,
    candidate_registry_from_observations,
    gsc_query_observations,
    regulated_entity_delta_observations,
    sandbox_delta_observations,
    widget_unknown_search_observations,
)

DEFAULT_SEARCH_INDEX = Path("data/derived/v2/public/search_index.json")
DEFAULT_OUTPUT = Path("data/derived/v2/market_identity_candidates.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        values = payload
    elif isinstance(payload, dict):
        values = None
        for key in keys:
            if isinstance(payload.get(key), list):
                values = payload[key]
                break
        if values is None:
            raise TypeError(f"snapshot does not contain any supported row list: {keys}")
    else:
        raise TypeError("snapshot must be a JSON list or object")
    if not all(isinstance(row, dict) for row in values):
        raise TypeError("snapshot rows must be JSON objects")
    return [dict(row) for row in values]


def _consume_optional_sensor(
    name: str,
    path: Path | None,
    loader: Callable[[Any], list[dict[str, Any]]],
    consumer: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    *,
    statuses: dict[str, str],
    errors: dict[str, str],
) -> list[dict[str, Any]]:
    if path is None:
        statuses[name] = "not_configured"
        return []
    if not path.exists():
        statuses[name] = "unavailable"
        errors[name] = f"snapshot not found: {path}"
        return []
    try:
        rows = loader(_load_json(path))
        result = consumer(rows)
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        statuses[name] = "malformed"
        errors[name] = str(exc)
        return []
    statuses[name] = "fresh"
    return result


def _consume_delta_sensor(
    name: str,
    previous_path: Path | None,
    current_path: Path | None,
    loader: Callable[[Any], list[dict[str, Any]]],
    consumer: Callable[[list[dict[str, Any]], list[dict[str, Any]]], list[dict[str, Any]]],
    *,
    statuses: dict[str, str],
    errors: dict[str, str],
) -> list[dict[str, Any]]:
    if previous_path is None and current_path is None:
        statuses[name] = "not_configured"
        return []
    if previous_path is None or current_path is None:
        statuses[name] = "unavailable"
        errors[name] = "delta sensor requires both previous and current snapshots"
        return []
    if not previous_path.exists() or not current_path.exists():
        statuses[name] = "unavailable"
        errors[name] = (
            f"delta snapshot missing: previous={previous_path.exists()} "
            f"current={current_path.exists()}"
        )
        return []
    try:
        previous = loader(_load_json(previous_path))
        current = loader(_load_json(current_path))
        result = consumer(previous, current)
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        statuses[name] = "malformed"
        errors[name] = str(exc)
        return []
    statuses[name] = "fresh"
    return result


def build_market_identity_candidates(
    search_index: list[dict[str, Any]],
    *,
    widget_rows: list[dict[str, Any]] | None = None,
    gsc_rows: list[dict[str, Any]] | None = None,
    previous_licensed: list[dict[str, Any]] | None = None,
    current_licensed: list[dict[str, Any]] | None = None,
    previous_sandbox: list[dict[str, Any]] | None = None,
    current_sandbox: list[dict[str, Any]] | None = None,
    thresholds: DemandReviewThresholds | None = None,
    sensor_status: dict[str, str] | None = None,
) -> dict[str, Any]:
    thresholds = thresholds or DemandReviewThresholds()
    observations: list[dict[str, Any]] = []
    if widget_rows is not None:
        observations.extend(
            widget_unknown_search_observations(
                widget_rows, search_index, thresholds=thresholds
            )
        )
    if gsc_rows is not None:
        observations.extend(gsc_query_observations(gsc_rows, search_index, thresholds=thresholds))
    if previous_licensed is not None and current_licensed is not None:
        observations.extend(
            regulated_entity_delta_observations(previous_licensed, current_licensed)
        )
    if previous_sandbox is not None and current_sandbox is not None:
        observations.extend(sandbox_delta_observations(previous_sandbox, current_sandbox))

    payload = candidate_registry_from_observations(
        observations, sensor_status=sensor_status
    )
    payload["generated_at"] = _utc_now()
    payload["thresholds"] = {
        "widget_min_count": thresholds.widget_min_count,
        "widget_min_distinct_days": thresholds.widget_min_distinct_days,
        "gsc_min_impressions": thresholds.gsc_min_impressions,
        "gsc_min_clicks": thresholds.gsc_min_clicks,
    }
    return payload


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the non-authoritative emerging market identity candidate registry."
    )
    parser.add_argument("--search-index", type=Path, default=DEFAULT_SEARCH_INDEX)
    parser.add_argument("--widget-snapshot", type=Path)
    parser.add_argument("--gsc-snapshot", type=Path)
    parser.add_argument("--previous-licensed", type=Path)
    parser.add_argument("--current-licensed", type=Path)
    parser.add_argument("--previous-sandbox", type=Path)
    parser.add_argument("--current-sandbox", type=Path)
    parser.add_argument("--widget-min-count", type=int, default=2)
    parser.add_argument("--widget-min-distinct-days", type=int, default=2)
    parser.add_argument("--gsc-min-impressions", type=int, default=5)
    parser.add_argument("--gsc-min-clicks", type=int, default=1)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    search_payload = _load_json(args.search_index)
    search_index = _rows(search_payload, "search_index", "records")
    thresholds = DemandReviewThresholds(
        widget_min_count=args.widget_min_count,
        widget_min_distinct_days=args.widget_min_distinct_days,
        gsc_min_impressions=args.gsc_min_impressions,
        gsc_min_clicks=args.gsc_min_clicks,
    )

    statuses: dict[str, str] = {}
    errors: dict[str, str] = {}
    widget_observations = _consume_optional_sensor(
        "widget_unknown_search",
        args.widget_snapshot,
        lambda payload: _rows(payload, "queries", "records"),
        lambda rows: widget_unknown_search_observations(
            rows, search_index, thresholds=thresholds
        ),
        statuses=statuses,
        errors=errors,
    )
    gsc_observations = _consume_optional_sensor(
        "gsc_query",
        args.gsc_snapshot,
        lambda payload: _rows(payload, "queries", "records"),
        lambda rows: gsc_query_observations(rows, search_index, thresholds=thresholds),
        statuses=statuses,
        errors=errors,
    )
    licensed_observations = _consume_delta_sensor(
        "susep_licensed_delta",
        args.previous_licensed,
        args.current_licensed,
        lambda payload: _rows(payload, "entities", "records"),
        regulated_entity_delta_observations,
        statuses=statuses,
        errors=errors,
    )
    sandbox_observations = _consume_delta_sensor(
        "susep_sandbox_delta",
        args.previous_sandbox,
        args.current_sandbox,
        lambda payload: _rows(payload, "participants", "records"),
        sandbox_delta_observations,
        statuses=statuses,
        errors=errors,
    )

    payload = candidate_registry_from_observations(
        [
            *widget_observations,
            *gsc_observations,
            *licensed_observations,
            *sandbox_observations,
        ],
        sensor_status=statuses,
    )
    payload["generated_at"] = _utc_now()
    payload["sensor_errors"] = errors
    payload["thresholds"] = {
        "widget_min_count": thresholds.widget_min_count,
        "widget_min_distinct_days": thresholds.widget_min_distinct_days,
        "gsc_min_impressions": thresholds.gsc_min_impressions,
        "gsc_min_clicks": thresholds.gsc_min_clicks,
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
                "sensor_status": statuses,
                "summary": payload["summary"],
                "output": str(args.output),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
