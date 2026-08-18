from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.v2.liquidity_transform_experiment import (
    build_liquidity_transform_experiment,
)

DEFAULT_INPUT = Path("data/derived/v2/liquidity_experiment.json")
DEFAULT_OUTPUT = Path("data/derived/v2/liquidity_transform_experiment.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_from_path(input_path: Path = DEFAULT_INPUT) -> dict[str, Any]:
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    result = build_liquidity_transform_experiment(payload)
    result["generated_at"] = _utc_now()
    result["source_artifact"] = str(input_path)
    return result


def write_transform_experiment(
    payload: dict[str, Any],
    output: Path = DEFAULT_OUTPUT,
) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(output)
    return output


def _fmt(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def main() -> None:
    payload = build_from_path()
    path = write_transform_experiment(payload)
    ilt = payload["metrics"]["ILT"]["transforms"]
    raw_stability = ilt["raw_ratio"]["rank_stability_vs_current"]["summary"]
    print(
        "V2 liquidity transform experiment: "
        f"period={payload['reference_period']} "
        f"raw_ILT_stability={_fmt(raw_stability.get('median_spearman'))}; "
        f"written to {path}"
    )
    for name in (
        "hard_log_cap_2_0",
        "hard_log_cap_3_0",
        "hard_log_cap_5_0",
        "tanh_log_tau_0_75",
        "tanh_log_tau_1_0",
        "tanh_log_tau_1_5",
        "history_geo_current_000",
        "history_geo_current_025",
        "history_geo_current_050",
        "history_geo_current_075",
    ):
        item = ilt[name]
        stability = item["rank_stability_vs_current"]["summary"]
        shift = item["current_rank_shift_vs_raw"]
        resolution = item["current_resolution"]
        print(
            f"  {name}: n={item['current_distribution'].get('count', 0)} "
            f"stability={_fmt(stability.get('median_spearman'))} "
            f"raw_rho={_fmt(item.get('current_rank_spearman_vs_raw'))} "
            f"mean_rank_shift={_fmt(shift.get('mean_absolute_rank_shift'))} "
            f"unique={resolution.get('unique_values')} "
            f"ceiling={resolution.get('exact_ceiling_count', 0)} "
            f"near_ceiling={resolution.get('near_ceiling_0_99_count', 0)}"
        )


if __name__ == "__main__":
    main()
