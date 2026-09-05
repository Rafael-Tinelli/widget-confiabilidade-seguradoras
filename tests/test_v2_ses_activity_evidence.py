from __future__ import annotations

import zipfile
from pathlib import Path

from api.v2.ses_activity_evidence import (
    derive_ses_activity_evidence,
    enrich_entities_with_ses_activity_evidence,
)


def _write_activity_zip(path: Path) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "Ses_seguros.csv",
            "coenti;damesano;coramo;premio_direto;premio_ganho\n"
            "5177;202606;0111;10;10\n"
            "5886;202401;0111;20;20\n",
        )
        archive.writestr(
            "Ses_contrib_benef.csv",
            "coenti;damesano;contribuicao;beneficio\n"
            "5177;202601;1;0\n"
            "5886;202301;1;0\n",
        )
        archive.writestr(
            "Ses_dados_cap.csv",
            "coenti;damesano;receita;resgate\n"
            "5886;202605;1;0\n",
        )


def test_recent_activity_rows_are_materialized_without_historical_carryover(
    tmp_path: Path,
) -> None:
    base = tmp_path / "BaseCompleta.zip"
    _write_activity_zip(base)

    payload = derive_ses_activity_evidence(base)

    assert payload["reference_period"] == 202606
    assert payload["window_start"] == 202507
    assert payload["activities_by_fip"]["005177"] == {
        "insurance": True,
        "pension": True,
        "capitalization": False,
        "reinsurance": False,
    }
    assert payload["labels_by_fip"]["005177"] == ["PREVIDENCIA", "SEGUROS"]
    assert payload["activities_by_fip"]["005886"] == {
        "insurance": False,
        "pension": False,
        "capitalization": True,
        "reinsurance": False,
    }
    assert payload["labels_by_fip"]["005886"] == ["CAPITALIZACAO"]


def test_enrichment_preserves_existing_activity_and_records_source_window(
    tmp_path: Path,
) -> None:
    base = tmp_path / "BaseCompleta.zip"
    _write_activity_zip(base)
    entities = [
        {
            "entity_id": "fip:005177",
            "fip_code": "005177",
            "activities": {
                "insurance": False,
                "pension": False,
                "capitalization": False,
                "reinsurance": True,
            },
            "evidence": {"activity_sources": ["RESSEGURO"]},
        }
    ]

    enriched = enrich_entities_with_ses_activity_evidence(entities, base)
    entity = enriched[0]

    assert entity["activities"] == {
        "insurance": True,
        "pension": True,
        "capitalization": False,
        "reinsurance": True,
    }
    assert entity["evidence"]["activity_sources"] == [
        "PREVIDENCIA",
        "RESSEGURO",
        "SEGUROS",
    ]
    assert entity["evidence"]["ses_recent_activity"] == {
        "source": "SUSEP SES / BaseCompleta.zip",
        "semantics": "recent_data_flow_row_presence_not_legal_classification",
        "reference_period": 202606,
        "window_start": 202507,
        "lookback_months": 12,
        "observed_labels": ["PREVIDENCIA", "SEGUROS"],
    }
