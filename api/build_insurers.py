from __future__ import annotations

import gzip
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Garantir que "api/" esteja no sys.path para permitir importar "sources.ses"
API_DIR = Path(__file__).resolve().parent
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from sources.ses import (  # type: ignore
    DEFAULT_SES_ZIP_URL,
    SesExtractionMeta,
    extract_ses_master_and_financials,
)

USER_AGENT = "widget-confiabilidade-seguradoras/0.1 (+https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras)"

ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_SNAPSHOTS = ROOT / "data" / "snapshots"
API_V1 = ROOT / "api" / "v1"

API_INSURERS = API_V1 / "insurers.json"
FULL_RAW_GZ = DATA_RAW / "insurers_full.json.gz"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _taxonomy() -> Dict[str, Any]:
    # Taxonomia “estável” para o front não hardcodar labels.
    return {
        "segments": {
            "S1": "Seguradoras de Grande Porte (Ex: conglomerados/bancassurance)",
            "S2": "Seguradoras de Médio Porte",
            "S3": "Seguradoras de Pequeno Porte",
            "S4": "Insurtechs e supervisionadas especiais",
        },
        "products": {
            "auto": "Automóvel",
            "vida": "Pessoas e Vida",
            "patrimonial": "Residencial e Patrimonial",
            "rural": "Rural",
        },
    }


def _methodology_stub_b1() -> Dict[str, Any]:
    # Em B1 ainda não há Consumidor.gov nem normalizações finais.
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
                "B1: lista mestre + prêmios/sinistros (rolling_12m) via SUSEP/SES. "
                "Índice de reputação/score será incluído na B2 (Consumidor.gov.br) "
                "e normalizações/flags na B3 (OPIN)."
            ),
            "notes": [
                "Segmento (S1..S4) em B1 é inferido por prêmios rolling_12m (fallback), até acoplar classificação oficial.",
            ],
        }
    }


def _infer_segment_fallback(premiums_12m: float) -> str:
    """
    Fallback robusto para S1..S4 com base em prêmios (rolling 12m).
    Thresholds: pragmáticos para MVP; podem ser refinados quando
    incorporarmos coluna/grupo prudencial oficial do SES.
    """
    if premiums_12m >= 2_000_000_000:  # >= 2 bi
        return "S1"
    if premiums_12m >= 500_000_000:  # >= 500 mi
        return "S2"
    if premiums_12m >= 50_000_000:  # >= 50 mi
        return "S3"
    return "S4"


def _safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def build_insurers_payload(
    ses_meta: SesExtractionMeta,
    ses_rows: Dict[str, Dict[str, Any]],
    generated_at: str,
) -> Dict[str, Any]:
    insurers: List[Dict[str, Any]] = []

    for ses_id, row in ses_rows.items():
        premiums = _safe_float(row.get("premiums"))
        claims = _safe_float(row.get("claims"))

        # corte mínimo para não poluir (ajuste se quiser listar tudo)
        if premiums <= 0:
            continue

        loss_ratio = (claims / premiums) if premiums > 0 else 0.0

        name = row.get("name") or f"SES_ENTIDADE_{ses_id}"
        cnpj = row.get("cnpj")

        insurer_obj: Dict[str, Any] = {
            "id": f"ses:{ses_id}",
            "name": str(name),
            "segment": _infer_segment_fallback(premiums),
            "products": [],  # B1 vazio
            "data": {
                "premiums": round(premiums, 2),
                "claimsPaid": round(claims, 2),
                "lossRatio": round(loss_ratio, 6),
                "complaints": None,  # B2
                "score": None,       # B2/B3
            },
        }

        # cnpj é útil para reconciliação; mantém como opcional
        if cnpj:
            insurer_obj["cnpj"] = str(cnpj)

        insurers.append(insurer_obj)

    insurers.sort(key=lambda x: float(x.get("data", {}).get("premiums") or 0.0), reverse=True)

    payload: Dict[str, Any] = {
        "schemaVersion": "1.0.0",
        "generatedAt": generated_at,
        "period": {
            "type": "rolling_12m",
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
                "note": "B2: integração via Dados Abertos (CSVs mensais).",
            },
            "opin": {
                "note": "B3: match com Open Insurance participants + flags.",
            },
        },
        "taxonomy": _taxonomy(),
        "methodology": _methodology_stub_b1(),
        "insurers": insurers,
        "meta": {
            "count": len(insurers),
            "disclaimer": "Dados públicos consolidados. MVP em evolução (B1). Não constitui recomendação.",
        },
    }

    return payload


def write_outputs(zip_url: str) -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    API_V1.mkdir(parents=True, exist_ok=True)

    generated_at = _now_iso()

    ses_meta, ses_rows = extract_ses_master_and_financials(zip_url=zip_url)
    payload = build_insurers_payload(ses_meta=ses_meta, ses_rows=ses_rows, generated_at=generated_at)

    # Guardrail mínimo: não escrever/commitar vazio
    if int(payload.get("meta", {}).get("count", 0)) <= 0:
        raise RuntimeError("ERRO: insurers meta.count <= 0. Abortando para não gerar lixo.")

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


if __name__ == "__main__":
    ses_url = os.getenv("SES_ZIP_URL", DEFAULT_SES_ZIP_URL)
    write_outputs(ses_url)
    print("OK: generated api/v1/insurers.json + FULL archives (data/raw + data/snapshots)")
