from __future__ import annotations
import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# MUDANÇA: Apenas a função necessária
from api.sources.ses import extract_ses_master_and_financials

ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_SNAPSHOTS = ROOT / "data" / "snapshots"
API_V1 = ROOT / "api" / "v1"
API_INSURERS = API_V1 / "insurers.json"
FULL_RAW_GZ = DATA_RAW / "insurers_full.json.gz"

def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _taxonomy() -> Dict[str, Any]:
    return { "segments": {"S1": "Grande", "S2": "Médio", "S3": "Pequeno", "S4": "Micro"}, "products": {} }

def _methodology_stub() -> Dict[str, Any]:
    return { "score": { "range": [0, 100], "formula": "B1: Pendente" } }

def _infer_segment_fallback(val: float) -> str:
    if val >= 2e9: return "S1"
    if val >= 5e8: return "S2"
    if val >= 5e7: return "S3"
    return "S4"

def build_payload() -> Dict[str, Any]:
    # Não passamos URL, o ses.py vai usar o browser
    meta, companies = extract_ses_master_and_financials()
    
    insurers = []
    for sid, data in companies.items():
        p = data["premiums"]
        c = data["claims"]
        if p <= 0: continue
        
        insurers.append({
            "id": f"ses:{sid}",
            "name": data["name"],
            "cnpj": data["cnpj"],
            "segment": _infer_segment_fallback(p),
            "products": [],
            "data": {
                "premiums": p,
                "claims": c,
                "lossRatio": round(c/p, 4),
                "complaints": None,
                "score": None
            }
        })
    
    insurers.sort(key=lambda x: x["data"]["premiums"], reverse=True)
    
    return {
        "schemaVersion": "1.0.0",
        "generatedAt": _now_iso(),
        "period": {"type": "rolling_12m", "to": meta.period_to, "currency": "BRL"},
        "sources": {"ses": {"dataset": "SUSEP SES", "files": [meta.cias_file, meta.seguros_file]}},
        "taxonomy": _taxonomy(),
        "methodology": _methodology_stub(),
        "insurers": insurers,
        "meta": {"count": len(insurers)}
    }

def write_outputs() -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    API_V1.mkdir(parents=True, exist_ok=True)
    
    payload = build_payload()
    
    with gzip.open(FULL_RAW_GZ, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
        
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with gzip.open(DATA_SNAPSHOTS / f"insurers_full_{day}.json.gz", "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
        
    API_INSURERS.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

if __name__ == "__main__":
    write_outputs()
