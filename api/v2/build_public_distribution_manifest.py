from __future__ import annotations

import argparse
from pathlib import Path

from api.v2.generation import BuildContext, load_source_lineage, write_distribution_manifest


DEFAULT_PUBLIC_DIR = Path("data/derived/v2/public")
DEFAULT_LINEAGE = Path("data/derived/v2/source_lineage.json")


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
        help="JSON file containing source lineage records for this build_id.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    context = BuildContext.from_env()
    sources = load_source_lineage(args.source_lineage, context)
    output = write_distribution_manifest(
        public_dir=args.public_dir,
        context=context,
        sources=sources,
    )
    print(output)


if __name__ == "__main__":
    main()
