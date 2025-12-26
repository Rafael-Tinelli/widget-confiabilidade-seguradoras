from __future__ import annotations

import gzip
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from api.sources.ses import DEFAULT_SES_ZIP_URL, extract_ses_master_and_financials

ROOT = Path(__file__).resolve().parents[1]
API_V1 = ROOT / "api" / "v1"
DATA_RAW = ROOT / "data" / "raw"
DATA_SNAPSHOTS = ROOT / "data" / "snapshots"

API_INSURERS = API_V1 / "insurers.json"
FULL_RAW_GZ = DATA_RAW / "insurers_full.json.gz"

PARTICIPANTS_JSON = API_V1 / "participants.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _digits(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    d = re.sub(r"\D+", "", str(s))
    return d or None


def _taxonomy() -> Dict[str, Any]:
    return {
        "segments": {
            "S1": "Seguradoras de Grande Porte (comparáveis entre si)",
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
    }


def _methodology_stub() -> Dict[str, Any]:
    return {
        "segment": {
            "rule": "Percentil por prêmios (rolling_12m): top 10% = S1; próximos 20% = S2; próximos 30% = S3; restante = S4."
        },
        "score": {
            "range": [0, 100],
            "formula": "B1: score pendente (aguardando B2 Consumidor.gov e normalizações).",
        },
    }


def _load_opin_cnpjs() -> Set[str]:
    """
    Lê api/v1/participants.json (gerado pelo pipeline OPIN) e extrai registrationNumber (CNPJ) normalizado em dígitos.
    Retorna set de CNPJs (14 dígitos).
    """
    if not PARTICIPANTS_JSON.exists():
        return set()

    try:
        data = json.loads(PARTICIPANTS_JSON.read_text(encoding="utf-8"))
    except Exception:
        return set()

    out: Set[str] = set()
    for p in (data.get("participants") or []):
        reg = _digits(p.get("registrationNumber"))
        if reg and len(reg) == 14:
            out.add(reg)
    return out


def _assign_segments_by_percentile(items: List[Dict[str, Any]]) -> None:
    """
    Define S1..S4 por percentil de prêmios (desc).
    - S1: top 10%
    - S2: próximos 20%
    - S3: próximos 30%
    - S4: restante
    """
    items.sort(
        key=lambda x: float((x.get("metrics") or {}).get("ses", {}).get("premiums") or 0.0),
        reverse=True,
    )
    n = len(items)
    if n == 0:
        return

    s1_end = max(1, round(n * 0.10))
    s2_end = min(n, s1_end + max(1, round(n * 0.20)))
    s3_end = min(n, s2_end + max(1, round(n * 0.30)))

    for i, it in enumerate(items):
        if i < s1_end:
            it["segment"] = "S1"
        elif i < s2_end:
            it["segment"] = "S2"
        elif i < s3_end:
            it["segment"] = "S3"
        else:
            it["segment"] = "S4"


def build_insurers_payload(zip_url: str) -> Dict[str, Any]:
    ses_meta, ses_rows = extract_ses_master_and_financials(zip_url=zip_url)
    opin_cnpjs = _load_opin_cnpjs()

    insurers: List[Dict[str, Any]] = []

    for ses_id, row in ses_rows.items():
        premiums = float(row.get("premiums") or 0.0)
        claims = float(row.get("claims") or 0.0)

        # corte mínimo: remove lixo/linhas residuais
        if premiums <= 0:
            continue

        cnpj_digits = _digits(row.get("cnpj"))
        is_opin = bool(cnpj_digits and (cnpj_digits in opin_cnpjs))

        loss_ratio = (claims / premiums) if premiums > 0 else 0.0

        insurers.append(
            {
                "id": f"ses:{ses_id}",
                "name": row.get("name") or f"SES_ENTIDADE_{ses_id}",
                "cnpj": cnpj_digits,  # dígitos (14) ou None
                "segment": None,  # preenchido após ordenação/percentil
                "products": [],
                "metrics": {
                    "ses": {
                        "premiums": round(premiums, 2),
                        "claims": round(claims, 2),
                        "lossRatio": round(loss_ratio, 6),
                    },
                    "reputation": None,   # B1
                    "normalized": None,   # B1
                },
                "score": None,  # B1
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
            "from": ses_meta.period_from,
            "to": ses_meta.period_to,
            "currency": "BRL",
        },
        "sources": {
            "ses": {
                "dataset": "SUSEP SES Base Completa",
                "url": ses_meta.zip_url,
                "files": [ses_meta.cias_file, ses_meta.seguros_file],
            },
            "consumidorGov": {"note": "Aguardando B2"},
            "opin": {"note": "Flag openInsuranceParticipant via match de CNPJ (participants.json)"},
        },
        "taxonomy": _taxonomy(),
        "methodology": _methodology_stub(),
        "insurers": insurers,
        "meta": {
            "count": len(insurers),
            "disclaimer": "B1: Lista Mestre + prêmios/sinistros rolling_12m (SES). Score final entra após Consumidor.gov (B2).",
        },
    }
    return payload


def write_outputs(zip_url: str) -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    API_V1.mkdir(parents=True, exist_ok=True)

    payload = build_insurers_payload(zip_url)

    # FULL raw compactado
    with gzip.open(FULL_RAW_GZ, "wt", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))

    # snapshot FULL diário
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap_path = DATA_SNAPSHOTS / f"insurers_full_{day}.json.gz"
    with gzip.open(snap_path, "wt", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))

    # SLIM público (minificado)
    API_INSURERS.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


if __name__ == "__main__":
    zip_url = os.getenv("SES_ZIP_URL", DEFAULT_SES_ZIP_URL)
    write_outputs(zip_url)
    print("OK: generated api/v1/insurers.json (SLIM) and FULL archives (.json.gz)")

