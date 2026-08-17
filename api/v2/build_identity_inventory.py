from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from api.sources.ses import extract_ses_master_and_financials
from api.v2.identity import build_canonical_entities


DEFAULT_OUTPUT = Path("data/derived/v2/entity_identity_inventory.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_identity_inventory(ses_companies: Any) -> dict[str, Any]:
    """Create an internal inventory used to audit the v2 identity foundation.

    This is deliberately **not** the public ``api/v2/entities.json`` contract.
    The inventory exists so classification, source coverage and identity
    conflicts can be inspected before the public schema is frozen.
    """
    entities = build_canonical_entities(ses_companies)

    with_cnpj = sum(1 for item in entities if item.get("cnpj"))
    by_activity = {
        "insurance": sum(1 for item in entities if item["activities"].get("insurance")),
        "pension": sum(1 for item in entities if item["activities"].get("pension")),
        "capitalization": sum(1 for item in entities if item["activities"].get("capitalization")),
        "reinsurance": sum(1 for item in entities if item["activities"].get("reinsurance")),
    }

    return {
        "artifact": "v2_identity_inventory",
        "generated_at": _utc_now(),
        "status": "draft",
        "meta": {
            "count": len(entities),
            "with_cnpj": with_cnpj,
            "without_cnpj": len(entities) - with_cnpj,
            "by_activity_evidence": by_activity,
            "classification_note": (
                "Activity evidence is derived from SES data-flow presence and must not "
                "be interpreted as legal entity classification."
            ),
        },
        "entities": entities,
    }


def write_identity_inventory(payload: dict[str, Any], output: Path = DEFAULT_OUTPUT) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(output)
    return output


def main() -> None:
    ses_out = extract_ses_master_and_financials()
    if not isinstance(ses_out, tuple) or len(ses_out) < 2:
        raise RuntimeError(f"Unexpected SES return: {type(ses_out)}")

    ses_companies = ses_out[1]
    payload = build_identity_inventory(ses_companies)
    path = write_identity_inventory(payload)
    print(
        "V2 identity inventory: "
        f"{payload['meta']['count']} entities; "
        f"{payload['meta']['with_cnpj']} with CNPJ; "
        f"written to {path}"
    )


if __name__ == "__main__":
    main()
