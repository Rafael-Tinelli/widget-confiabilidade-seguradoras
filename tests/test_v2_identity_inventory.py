from __future__ import annotations

from api.v2.build_identity_inventory import build_identity_inventory


def test_identity_inventory_reports_activity_evidence_without_classifying_entities() -> None:
    payload = build_identity_inventory(
        {
            "000001": {
                "id": "000001",
                "name": "ENTIDADE DE SEGUROS",
                "cnpj": "12.345.678/0001-90",
                "sources_found": ["SEGUROS", "PATRIMONIO"],
            },
            "000002": {
                "id": "000002",
                "name": "ENTIDADE DE CAPITALIZACAO",
                "sources_found": ["CAPITALIZACAO"],
            },
        }
    )

    assert payload["artifact"] == "v2_identity_inventory"
    assert payload["status"] == "draft"
    assert payload["meta"]["count"] == 2
    assert payload["meta"]["with_cnpj"] == 1
    assert payload["meta"]["without_cnpj"] == 1
    assert payload["meta"]["by_activity_evidence"] == {
        "insurance": 1,
        "pension": 0,
        "capitalization": 1,
        "reinsurance": 0,
    }
    assert all(entity["entity_type"] == "unknown" for entity in payload["entities"])
