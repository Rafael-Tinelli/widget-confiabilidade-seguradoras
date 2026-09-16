from __future__ import annotations

"""Compatibility shim for the pre-generalization Sandbox conduct stage.

The authoritative relationship/conduct model is now
``api.v2.build_market_relationship_conduct_evidence``. Sandbox is treated as a
regulatory attribute of a risk-bearing entity, not as the architectural reason
that distributor/representative and risk-bearer complaint subjects are kept
separate.

The Gate 4 stage keeps its historical module name temporarily so existing DAG
contracts do not break in the same refactor. The stage now writes both the
generic artifact and the legacy Sandbox-only compatibility projection.
"""

import json

from api.v2.build_market_relationship_conduct_evidence import (
    LEGACY_SANDBOX_OUTPUT_PATH as OUTPUT_PATH,
    OUTPUT_PATH as MARKET_RELATIONSHIP_OUTPUT_PATH,
    build_from_files as build_market_relationship_from_files,
    build_legacy_sandbox_projection,
)


def build_sandbox_brand_conduct_evidence(
    market_relationship_payload: dict,
) -> dict:
    """Return the legacy Sandbox projection from the generic conduct artifact."""
    return build_legacy_sandbox_projection(market_relationship_payload)


def main() -> None:
    payload, legacy = build_market_relationship_from_files()
    MARKET_RELATIONSHIP_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    MARKET_RELATIONSHIP_OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    OUTPUT_PATH.write_text(
        json.dumps(legacy, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "summary": payload["summary"],
                "legacy_sandbox_projection": legacy["summary"],
                "compatibility_stage_name": "sandbox_brand_conduct",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {MARKET_RELATIONSHIP_OUTPUT_PATH}")
    print(f"compatibility projection written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
