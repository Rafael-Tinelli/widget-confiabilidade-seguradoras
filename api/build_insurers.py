# api/build_insurers.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from api.matching.consumidor_gov_match import NameMatcher, format_cnpj, normalize_cnpj
from api.sources.opin_products import extract_open_insurance_products
from api.sources.ses import extract_ses_master_and_financials

OUTPUT_FILE = Path("api/v1/insurers.json")
CONSUMIDOR_GOV_FILE = Path("data/derived/consumidor_gov/aggregated.json")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_float(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        s = str(v).strip().replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


@dataclass(frozen=True)
class SegmentThresholds:
    s1_min: float
    s2_min: float
    s3_min: float


def compute_segment_thresholds(companies: Dict[str, Dict[str, Any]]) -> SegmentThresholds:
    nws = sorted([_safe_float(c.get("net_worth")) for c in companies.values() if _safe_float(c.get("net_worth")) > 0])
    if len(nws) < 8:
        return SegmentThresholds(15_000_000_000.0, 5_000_000_000.0, 1_000_000_000.0)

    def q(p):
        idx = (len(nws) - 1) * p
        idx_l, idx_h = int(idx), int(idx) + 1
        if idx_l == idx_h or idx_h >= len(nws):
            return nws[idx_l]
        return nws[idx_l] * (1 - (idx - idx_l)) + nws[idx_h] * (idx - idx_l)

    s1, s2, s3 = q(0.75), q(0.50), q(0.25)
    return SegmentThresholds(max(s1, s2), max(min(s2, s1), s3), min(s3, s2))


def calculate_segment(net_worth: Any, t: SegmentThresholds) -> str:
    nw = _safe_float(net_worth)
    if nw >= t.s1_min:
        return "S1"
    if nw >= t.s2_min:
        return "S2"
    if nw >= t.s3_min:
        return "S3"
    return "S4"


def main() -> None:
    print("\n--- INICIANDO COLETA SUSEP (FINANCEIRO) ---")
    _ses_meta, companies = extract_ses_master_and_financials()

    print("\n--- INICIANDO COLETA CONSUMIDOR.GOV ---")
    reputation_root = {}
    if CONSUMIDOR_GOV_FILE.exists():
        reputation_root = json.loads(CONSUMIDOR_GOV_FILE.read_text(encoding="utf-8"))
    matcher = NameMatcher(reputation_root)

    print("\n--- INICIANDO COLETA OPEN INSURANCE (PRODUTOS) ---")
    products_by_cnpj = extract_open_insurance_products()

    print("\n--- CONSOLIDANDO DADOS ---")
    thresholds = compute_segment_thresholds(companies)
    insurers = []
    matched_reputation = 0

    for raw_cnpj, comp in companies.items():
        cnpj_dig = normalize_cnpj(comp.get("cnpj") or raw_cnpj) or normalize_cnpj(raw_cnpj)
        cnpj_fmt = format_cnpj(cnpj_dig) if cnpj_dig else str(comp.get("cnpj") or raw_cnpj)

        name = comp.get("name") or comp.get("corporate_name") or comp.get("razao_social") or cnpj_fmt

        # Reputation Match
        rep_entry, _ = matcher.get_entry(str(name), cnpj=cnpj_dig or cnpj_fmt)
        if rep_entry:
            matched_reputation += 1

        # Products
        prods = products_by_cnpj.get(cnpj_dig, []) if cnpj_dig else []

        # Segment
        segment = calculate_segment(comp.get("net_worth"), thresholds)

        # Financial Score Calculation
        premiums = _safe_float(comp.get("premiums"))
        fin_score = 0.0
        if premiums > 0:
            import math
            log_val = math.log10(premiums)
            fin_score = min(100.0, max(0.0, (log_val - 6.0) * 22))

        insurers.append({
            "id": cnpj_dig or raw_cnpj,
            "name": str(name),
            "cnpj": cnpj_fmt,
            "segment": segment,
            "flags": {"openInsuranceParticipant": bool(cnpj_dig and cnpj_dig in products_by_cnpj)},
            "data": {
                "net_worth": _safe_float(comp.get("net_worth")),
                "premiums": premiums,
                "claims": _safe_float(comp.get("claims")),
                "financial_score": round(fin_score, 1),
                "components": {
                    "financial": {"status": "data_available" if premiums > 0 else "no_data", "value": round(fin_score, 1)},
                    "reputation": rep_entry
                },
                "products": prods
            },
            "products": prods  # Mantido na raiz para compatibilidade
        })

    insurers.sort(key=lambda x: x["data"]["financial_score"], reverse=True)

    out = {
        "schemaVersion": "1.0.0",
        "generatedAt": _utc_now_iso(),
        "period": "2024",
        "sources": ["SUSEP (SES)", "Open Insurance Brasil", "Consumidor.gov.br"],
        "meta": {
            "count": len(insurers),
            "stats": {
                "reputationMatched": matched_reputation,
                "openInsuranceParticipants": sum(1 for i in insurers if i["flags"]["openInsuranceParticipant"]),
            }
        },
        "insurers": insurers
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(out, ensure_ascii=False, indent=0, separators=(',', ':')), encoding="utf-8")
    print(f"OK: generated {OUTPUT_FILE} with {len(insurers)} insurers. Reputation matched: {matched_reputation}.")


if __name__ == "__main__":
    main()
