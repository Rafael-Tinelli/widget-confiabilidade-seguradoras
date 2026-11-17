"""Seal a transient, same-run 20-ZIP input manifest for OFFLINE replay.

No downloads and no ZIP artifacts. Call only inside the original GitHub runner,
after the source and conduct stages have finished, and before RUNNER_TEMP ends.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from api.sources.receita_cnpj_a096_retention import local_input_manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-cache", type=Path, required=True)
    parser.add_argument("--conduct-cache", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = local_input_manifest(
        source_root=args.source_cache,
        conduct_root=args.conduct_cache,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    # No URLs, content or CNPJ data in logs; only a receipt.
    print(json.dumps({
        "status": "validated",
        "build_id": result["build_id"],
        "reference_period": result["reference_period"],
        "zip_files_verified": result["zip_files_verified"],
        "retention": result["retention"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
