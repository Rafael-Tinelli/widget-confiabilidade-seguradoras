from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from api.sources.ses import extract_ses_master_and_financials


# --- Paths ---
ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_SNAPSHOTS = ROOT / "data" / "snapshots"
API_V1 = ROOT / "api" / "v1"

API_INSURERS = API_V1 / "insurers.json"
FULL_RAW_GZ = DATA_RAW / "insurers_full.json.gz"

# --- Hardening Configs ---
# Baseline atual é ~246. Se cair para menos de 200, algo está muito errado.
MIN_COUNT = int(os.getenv("MIN_INSURERS_COUNT", "200"))
# Se cair mais de 20% de um dia para o outro, aborte.
MAX_DROP_PCT = float(os.getenv("MAX_COUNT_DROP_PCT", "0.20"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _taxonomy() -> Dict[str, Any]:
    return {
        "segments": {
            "S1": "Seguradoras de Grande Porte (comparáveis entre si)",
            "S2": "Seguradoras de Médio Porte",
            "S3": "Seguradoras de Pequeno Porte",
            "S4": "Insurtechs / supervisionadas especiais",
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
        "score": {
            "range": [0, 100],
            "weights": {
                "complaintsIndex": 0.40,
                "resolutionRate": 0.25,
            },
            "formula": "B1: lista mestre + prêmios/sinistros rolling_12m (SES). Score e reputação entram na B2/B3.",
            "notes": [
                "O campo segment usa fallback por porte (prêmios) até termos um mapeamento oficial do SES.",
            ],
        }
    }


def _infer_segment_fallback(val: float) -> str:
    if val >= 2e9:
        return "S1"
    if val >= 5e8:
        return "S2"
    if val >= 5e7:
        return "S3"
    return "S4"


def _read_existing_count(path: Path) -> int | None:
    """Lê o arquivo atual (se existir) para comparar com o novo."""
    try:
        if not path.exists() or path.stat().st_size < 10:
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        return int(data.get("meta", {}).get("count", 0)) or None
    except Exception:
        return None


def _guard_count_regression(new_count: int, old_count: int | None) -> None:
    """Impede que uma falha silenciosa reduza drasticamente a base de dados."""
    if new_count <= 0:
        raise RuntimeError("CRITICAL: meta.count=0. Abortando para preservar dados existentes.")

    if new_count < MIN_COUNT:
        raise RuntimeError(f"CRITICAL: meta.count={new_count} < MIN_COUNT={MIN_COUNT}. Abortando.")

    if old_count and old_count > 0:
        drop_pct = (old_count - new_count) / float(old_count)
        if drop_pct > MAX_DROP_PCT:
            raise RuntimeError(
                f"CRITICAL: queda abrupta em meta.count ({old_count} -> {new_count}, queda de {drop_pct:.1%}) "
                f"acima do limite permitido ({MAX_DROP_PCT:.0%}). Abortando."
            )
    print(f"Stats Check: Count {old_count or 'N/A'} -> {new_count} (OK)")


def _atomic_write_text(path: Path, content: str) -> None:
    """Escreve em arquivo temporário e renomeia (atômico)."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def build_payload() -> Dict[str, Any]:
    meta, companies = extract_ses_master_and_financials()

    insurers = []
    for ses_id, it in companies.items():
        premiums = float(it.get("premiums") or 0.0)
        claims = float(it.get("claims") or 0.0)

        if premiums <= 0:
            continue

        loss_ratio = round((claims / premiums), 6) if premiums > 0 else 0.0
        segment = _infer_segment_fallback(premiums)

        insurers.append(
            {
                "id": f"ses:{ses_id}",
                "name": it.get("name") or f"SES_ENTIDADE_{ses_id}",
                "cnpj": it.get("cnpj"),
                "segment": segment,
                "products": [],
                # FIX: flags padronizadas para schema v1
                "flags": {
                    "openInsuranceParticipant": False
                },
                "data": {
                    "premiums": round(premiums, 2),
                    "claims": round(claims, 2),
                    "lossRatio": loss_ratio,
                    "complaints": None,
                    "score": None,
                },
            }
        )

    insurers.sort(key=lambda x: float(x.get("data", {}).get("premiums") or 0.0), reverse=True)

    return {
        "schemaVersion": "1.0.0",
        "generatedAt": _now_iso(),
        "period": {
            "type": "rolling_12m",
            "from": meta.period_from,
            "to": meta.period_to,
            "currency": "BRL",
        },
        "sources": {
            "ses": {
                "dataset": "SUSEP SES Base Completa",
                "url": meta.zip_url,
                "files": [meta.cias_file, meta.seguros_file],
            },
            "consumidorGov": {"note": "B2 (reputação) — pendente"},
            "opin": {"note": "B3 (Open Insurance participants match) — pendente"},
        },
        "taxonomy": _taxonomy(),
        "methodology": _methodology_stub(),
        "insurers": insurers,
        "meta": {
            "count": len(insurers),
            "disclaimer": "B1: métricas financeiras (SES).",
        },
    }


def write_outputs() -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    API_V1.mkdir(parents=True, exist_ok=True)

    # 1. Gera Payload em memória
    payload = build_payload()
    
    # 2. Verifica Guardrails (impede regressão)
    new_count = int(payload.get("meta", {}).get("count", 0))
    old_count = _read_existing_count(API_INSURERS)
    _guard_count_regression(new_count, old_count)

    # 3. Salva Full Raw (Gzip)
    with gzip.open(FULL_RAW_GZ, "wt", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))

    # 4. Salva Snapshot Histórico
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap_path = DATA_SNAPSHOTS / f"insurers_full_{day}.json.gz"
    with gzip.open(snap_path, "wt", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))

    # 5. Salva API Pública (Slim) de forma atômica
    slim_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    _atomic_write_text(API_INSURERS, slim_json)


if __name__ == "__main__":
    write_outputs()
    print("OK: generated api/v1/insurers.json (SLIM) and FULL archives")
