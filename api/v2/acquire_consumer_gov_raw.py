from __future__ import annotations

import argparse
import json
from pathlib import Path

from api.sources.consumer_gov_direct import ConsumerGovRawError, ensure_months

DEFAULT_IDENTITY = Path("data/derived/v2/consumer_gov_identity_experiment.json")


def _months_from_identity(path: Path) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    source = payload.get("source") if isinstance(payload, dict) else None
    if isinstance(source, dict):
        months = source.get("months")
        if isinstance(months, list) and months:
            return [str(month) for month in months]

    months = payload.get("months") if isinstance(payload, dict) else None
    if isinstance(months, list) and months:
        return [str(month) for month in months]

    raise RuntimeError(f"could not resolve source months from {path}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--identity", type=Path, default=DEFAULT_IDENTITY)
    parser.add_argument("--months", nargs="*")
    args = parser.parse_args()

    months = args.months or _months_from_identity(args.identity)
    try:
        acquired = ensure_months(months)
    except ConsumerGovRawError as exc:
        raise SystemExit(str(exc)) from exc

    summary = {
        month: {
            "acquisition": item.get("acquisition"),
            "discovery_method": item.get("discovery_method"),
            "bytes": item.get("bytes"),
            "resource_url": item.get("resource_url"),
        }
        for month, item in acquired.items()
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
