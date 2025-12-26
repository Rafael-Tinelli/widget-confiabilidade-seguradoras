from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sources.ses import DEFAULT_SES_ZIP_URL, extract_ses_master_and_financials


ROOT = Path(__file__).resolve().parents[1]
API_V1 = ROOT / "api" / "v1"
DATA_SNAPSHOTS = ROOT / "data" / "snapshots"

PARTICIPANTS_PATH = API_V1 / "participants.json"
INSURERS_PATH = API_V1 / "insurers.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _digits(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    import re
    d = re.sub(r"\D+", "", s)
    return d or None


def _load_opin_cnpjs() -> set[str]:
    if not PARTICIPANTS_PATH.exists():
        return set()
    data = json.loads(PARTICIPANTS_PATH.read_text(encoding="utf-8"))
    out = set()
    for p in (data.get("participants") or []):
        cnpj = _digits(p.get("registrationNumber"))
        if cnpj:
            out.add(cnpj)
    return out


def _assign_segments_by_percentile(items: List[Dict[str, Any]]) -> None:
    """
    Zero-manutenção: segmenta por percentis do próprio dataset (por prêmios rolling_12m).
      - Top 10%: S1
      - Próximos 20%: S2
      - Próximos 30%: S3
      - Restante: S4
    """
    items.sort(key=lambda x: float(x.get("data", {}).get("premiums") or 0.0), reverse=True)
    n = len(items)
    if n == 0:
        return

    s1 = max(1, round(n * 0.10))
    s2 = max(s1 + 1, s1 + round(n * 0.20)) if n >= 4 else min(n, s1 + 1)
    s3 = max(s2 + 1, s2 + round(n * 0.30)) if n >= 7 else min(n, s2 + 1)

    for i, it in enumerate(items):
        if i < s1:
            it["segment"] = "S1"
        elif i < s2:
            it["segment"] = "S2"
        elif i < s3:
            it["segment"] = "S3"
        else:
            it["segment"] = "S4"


def build_insurers() -> Dict[str, Any]:
    ses_zip_url = os.getenv("SES_ZIP_URL", DEFAULT_SES_ZIP_URL)

    ses_meta, ses_rows = extract_ses_master_and_financials(zip_url=ses_zip_url)
    opin_cnpjs = _load_opin_cnpjs()

    insurers: List[Dict[str, Any]] = []
    for ses_id, row in ses_rows.items():
        cnpj = row.get("cnpj")
        is_opin = bool(cnpj and cnpj in opin_cnpjs)

        insurers.append(
            {
                "id": cnpj or f"ses:{ses_id}",
                "name": row.get("name"),
                "segment": None,  # preenchido depois (percentis)
                "products": [],
                "data": {
                    "premiums": row.get("premiums"),
                    "claims": row.get("claims"),
                    "complaints": None,
                    "score": None,
                },
                "flags": {
                    "openInsuranceParticipant": is_opin,
                },
            }
        )

    _assign_segments_by_percentile(insurers)

    payload: Dict[str, Any] = {
        "schemaVersion": "1.0.0",
        "generatedAt": _now_iso(),
        "period": {
            "type": "rolling_12m",
            "description": "Acumulado dos últimos 12 meses disponíveis no SES.",
            "from": ses_meta.period_from,
            "to": ses_meta.period_to,
            "currency": "BRL",
        },
        "sources": {
            "ses": {
                "dataset": "SUSEP SES Base Completa",
                "asOf": ses_meta.period_to,
                "files": [ses_meta.cias_file, ses_meta.seguros_file],
                "url": ses_meta.zip_url,
            },
            "consumidorGov": {
                "dataset": "Dados Abertos (CSVs mensais)",
                "asOf": None,
                "note": "B1: ainda não integrado (será B2/B3).",
            },
            "opin": {
                "dataset": "Open Insurance Participants",
                "asOf": None,
                "note": "Flag openInsuranceParticipant via match por CNPJ quando disponível.",
            },
        },
        "taxonomy": {
            "segments": {
                "S1": "Seguradoras de Grande Porte",
                "S2": "Seguradoras de Médio Porte",
                "S3": "Seguradoras de Pequeno Porte",
                "S4": "Insurtechs / Long tail",
            },
            "products": {
                "auto": "Automóvel",
                "vida": "Pessoas e Vida",
                "patrimonial": "Residencial e Patrimonial",
                "rural": "Rural",
            },
        },
        "methodology": {
            "segment": {
                "rule": "Segmentação por percentil de prêmios (rolling_12m) para evitar thresholds fixos e manter evergreen.",
                "bands": {"S1": "top 10%", "S2": "next 20%", "S3": "next 30%", "S4": "rest"},
            },
            "score": {
                "range": [0, 100],
                "weights": {
                    "complaintsIndex": 0.40,
                    "resolutionRate": 0.25,
                    "satisfactionAvg": 0.20,
                    "avgResponseTime": 0.15,
                },
                "formula": "B1: score ainda não calculado (camada consumidor.gov.br entra em B2/B3).",
                "notes": [
                    "O índice de reclamações será normalizado por porte (reclamações/prêmios) para evitar vieses.",
                ],
            },
        },
        "insurers": insurers,
        "meta": {
            "count": len(insurers),
            "disclaimer": "B1: lista mestre + prêmios/sinistros (SES). Reputação/score entram nas próximas fases.",
        },
    }
    return payload


def write_outputs() -> None:
    API_V1.mkdir(parents=True, exist_ok=True)
    DATA_SNAPSHOTS.mkdir(parents=True, exist_ok=True)

    data = build_insurers()

    INSURERS_PATH.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")

    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap = DATA_SNAPSHOTS / f"insurers_full_{day}.json"
    snap.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    print(f"OK: generated {INSURERS_PATH} and snapshot {snap}")


if __name__ == "__main__":
    write_outputs()
