from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Import robusto (permite rodar via `python api/build_insurers.py`)
try:
    from api.sources.ses import DEFAULT_SES_ZIP_URL, extract_ses_master_and_financials
except ModuleNotFoundError:
    from sources.ses import DEFAULT_SES_ZIP_URL, extract_ses_master_and_financials  # type: ignore


ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_SNAPSHOTS = ROOT / "data" / "snapshots"
API_V1 = ROOT / "api" / "v1"

API_INSURERS = API_V1 / "insurers.json"
FULL_RAW_GZ = DATA_RAW / "insurers_full.json.gz"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _taxonomy() -> Dict[str, Any]:
    # Taxonomia aprovada (pode ser expandida depois; frontend lê dinamicamente)
    return {
        "segments": {
            "S1": "Seguradoras de Grande Porte (Ex: Bancos / Conglomerados)",
            "S2": "Seguradoras de Médio Porte",
            "S3": "Seguradoras de Pequeno Porte",
            "S4": "Insurtechs / Supervisionadas Especiais",
        },
        "products": {
            "auto": "Automóvel",
            "vida": "Pessoas e Vida",
            "patrimonial": "Residencial e Patrimonial",
            "rural": "Rural",
        },
    }


def _methodology_b1_stub() -> Dict[str, Any]:
    # B1 ainda não tem Consumidor.gov nem Open Insurance join.
    # Mantemos autodocumentação para o frontend e para “zero manutenção”.
    return {
        "score": {
            "range": [0, 100],
            "weights": {
                "complaintsIndex": 0.40,
                "resolutionRate": 0.25,
                "satisfactionAvg": 0.20,
                "avgResponseTime": 0.15,
            },
            "formula": (
                "B1: lista mestre + SES rolling_12m (prêmios e sinistros). "
                "Score e reputação aguardam integração Consumidor.gov (B2) e normalização (B3)."
            ),
            "notes": [
                "Segmento (S1..S4) nesta fase pode ser inferido por prêmios (fallback).",
                "Os valores financeiros são acumulados nos últimos 12 meses disponíveis no SES.",
            ],
        }
    }


def _infer_segment_fallback(premiums: float) -> str:
    """
    Fallback por prêmio anual (rolling_12m).
    Limiares MVP (ajustáveis depois com base em critério oficial/negócio).
    """
    if premiums >= 2_000_000_000:  # 2 bi
        return "S1"
    if premiums >= 500_000_000:  # 500 mi
        return "S2"
    if premiums >= 50_000_000:  # 50 mi
        return "S3"
    return "S4"


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def build_insurers_payload(zip_url: str) -> Dict[str, Any]:
    meta, by_id = extract_ses_master_and_financials(zip_url=zip_url)

    insurers: List[Dict[str, Any]] = []
    for ses_id, item in by_id.items():
        premiums = _safe_float(item.get("premiums"))
        claims = _safe_float(item.get("claims"))
        if premiums <= 0:
            continue

        loss_ratio: Optional[float] = None
        if premiums > 0:
            loss_ratio = claims / premiums

        insurers.append(
            {
                "id": f"ses:{ses_id}",
                "name": item.get("name") or f"SES_ENTIDADE_{ses_id}",
                "segment": _infer_segment_fallback(premiums),
                "products": [],
                "data": {
                    "premiums": round(premiums, 2),
                    "claims": round(claims, 2),
                    "lossRatio": round(loss_ratio, 6) if loss_ratio is not None else None,
                    "complaints": None,  # B2
                    "score": None,       # B3
                },
                # Útil para join futuro (sem “inchar” demais)
                "refs": {
                    "sesId": ses_id,
                    "cnpj": item.get("cnpj"),
                },
            }
        )

    insurers.sort(key=lambda x: _safe_float(x.get("data", {}).get("premiums")), reverse=True)

    payload: Dict[str, Any] = {
        "schemaVersion": "1.0.0",
        "generatedAt": _now_iso(),
        "period": {
            "type": "rolling_12m",
            "description": "Acumulado dos últimos 12 meses disponíveis",
            "from": meta.period_from,
            "to": meta.period_to,
            "currency": "BRL",
        },
        "sources": {
            "ses": {
                "dataset": "SUSEP SES Base Completa",
                "zipUrl": meta.zip_url,
                "files": [meta.cias_file, meta.seguros_file],
            },
            "consumidorGov": {"note": "Aguardando B2 (Dados Abertos CSVs mensais)"},
            "opin": {"note": "Aguardando B3 (Open Insurance Participants)"},
        },
        "taxonomy": _taxonomy(),
        "methodology": _methodology_b1_stub(),
        "insurers": insurers,
        "meta": {
            "count": len(insurers),
            "disclaimer": "Dados públicos. Métricas de reputação e score serão adicionadas nas próximas fases.",
        },
    }
    return payload


def write_outputs(zip_url: str) -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    API_V1.mkdir(parents=True, exist_ok=True)

    payload = build_insurers_payload(zip_url=zip_url)

    # FULL raw compactado
    with gzip.open(FULL_RAW_GZ, "wt", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))

    # snapshot FULL (1 por dia)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap_path = DATA_SNAPSHOTS / f"insurers_full_{day}.json.gz"
    with gzip.open(snap_path, "wt", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))

    # SLIM público (minificado)
    API_INSURERS.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    print(f"OK: generated {API_INSURERS} (meta.count={payload['meta']['count']}) and FULL archives (.json.gz)")


if __name__ == "__main__":
    src = os.getenv("SES_ZIP_URL", DEFAULT_SES_ZIP_URL)
    write_outputs(zip_url=src)
