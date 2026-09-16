from __future__ import annotations

"""Compatibility shim for the pre-generalization Sandbox conduct artifact.

The authoritative relationship/conduct model is now
``api.v2.build_market_relationship_conduct_evidence``. Sandbox is treated as a
regulatory attribute of a risk-bearing entity, not as the architectural reason
that distributor/representative and risk-bearer complaint subjects are kept
separate.
"""

import json

from api.v2.build_market_relationship_conduct_evidence import (
    LEGACY_SANDBOX_OUTPUT_PATH as OUTPUT_PATH,
    build_from_files as build_market_relationship_from_files,
    build_legacy_sandbox_projection,
)


def build_sandbox_brand_conduct_evidence(
    market_relationship_payload: dict,
) -> dict:
    """Return the legacy Sandbox projection from the generic conduct artifact."""
    return build_legacy_sandbox_projection(market_relationship_payload)


def main() -> None:
    _, legacy = build_market_relationship_from_files()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(legacy, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "artifact": legacy["artifact"],
                "status": legacy["status"],
                "summary": legacy["summary"],
                "authoritative_artifact": "v2_market_relationship_conduct_evidence",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"compatibility projection written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
