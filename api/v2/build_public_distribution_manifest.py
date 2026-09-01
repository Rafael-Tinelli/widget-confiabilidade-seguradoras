from __future__ import annotations

import argparse
import json
from pathlib import Path

from api.v2.audit_financial_publication_chain import run_audit
from api.v2.generation import (
    BuildContext,
    load_source_lineages,
    write_distribution_manifest,
)

DEFAULT_PUBLIC_DIR = Path("data/derived/v2/public")
DEFAULT_LINEAGE = Path("data/derived/v2/source_lineage.json")
DEFAULT_CONDUCT_LINEAGE = Path("data/derived/v2/conduct_source_lineage.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the Gate 4 v2 public distribution manifest."
    )
    parser.add_argument(
        "--public-dir",
        type=Path,
        default=DEFAULT_PUBLIC_DIR,
        help="Directory containing the complete public JSON package.",
    )
    parser.add_argument(
        "--source-lineage",
        type=Path,
        default=DEFAULT_LINEAGE,
        help="JSON file containing regulatory source lineage records for this build_id.",
    )
    parser.add_argument(
        "--conduct-source-lineage",
        type=Path,
        default=DEFAULT_CONDUCT_LINEAGE,
        help="JSON file containing Conduct source lineage records for this build_id.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    audit = run_audit()
    print(json.dumps(audit, ensure_ascii=False, indent=2, sort_keys=True))

    context = BuildContext.from_env()
    sources = load_source_lineages(
        (args.source_lineage, args.conduct_source_lineage),
        context,
    )
    output = write_distribution_manifest(
        public_dir=args.public_dir,
        context=context,
        sources=sources,
    )
    print(output)


if __name__ == "__main__":
    main()
