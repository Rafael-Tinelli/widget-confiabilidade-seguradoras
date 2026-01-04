# api/build_insurers.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

# Importações internas
from api.matching.consumidor_gov_match import NameMatcher, format_cnpj, normalize_cnpj
from api.sources.opin_products import extract_open_insurance_products
from api.sources.ses import extract_ses_master_and_financials
from api.intelligence import calculate_score  

OUTPUT_FILE = Path("api/v1/insurers.json")
CONSUMIDOR_GOV_FILE = Path("data/derived/consumidor_gov/aggregated.json")

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def main() -> None:
    # 1. COLETA FINANCEIRA (SUSEP)
    print("\n--- INICIANDO COLETA SUSEP (FINANCEIRO) ---")
    _ses_meta, companies = extract_ses_master_and_financials()

    # 2. PREPARAÇÃO DO MATCHER (CONSUMIDOR.GOV)
    print("\n--- INICIANDO COLETA CONSUMIDOR.GOV ---")
    reputation_root = {}
    if CONSUMIDOR_GOV_FILE.exists():
        try:
            reputation_root = json.loads(CONSUMIDOR_GOV_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Aviso: Erro ao ler {CONSUMIDOR_GOV_FILE}: {e}")
            
    matcher = NameMatcher(reputation_root)

    # 3. COLETA DE PRODUTOS (OPEN INSURANCE)
    print("\n--- INICIANDO COLETA OPEN INSURANCE (PRODUTOS) ---")
    products_by_cnpj = extract_open_insurance_products()

    # 4. CONSOLIDAÇÃO
    print("\n--- CONSOLIDANDO E CALCULANDO SCORES (VIA INTELLIGENCE) ---")
    insurers = []
    matched_reputation = 0

    for raw_cnpj, comp in companies.items():
        # Normalização de Identificadores
        cnpj_dig = normalize_cnpj(comp.get("cnpj") or raw_cnpj) or normalize_cnpj(raw_cnpj)
        cnpj_fmt = format_cnpj(cnpj_dig) if cnpj_dig else str(comp.get("cnpj") or raw_cnpj)
        name = comp.get("name") or comp.get("corporate_name") or comp.get("razao_social") or cnpj_fmt

        # A. Match com Consumidor.gov
        rep_entry, match_meta = matcher.get_entry(str(name), cnpj=cnpj_dig or cnpj_fmt)
        
        # B. Estrutura de Reputação (CORREÇÃO DE LÓGICA: 0 vs None)
        reputation_data = None
        if rep_entry:
            matched_reputation += 1
            stats = rep_entry.get("statistics") or {}
            
            # Helper para extrair float ou None (evita converter 0.0 falso)
            def get_float(key_primary, key_secondary=None):
                val = stats.get(key_primary)
                if val is None and key_secondary:
                    val = rep_entry.get(key_secondary)
                
                # Se for None ou string vazia, retorna None (dado ausente)
                if val in (None, ""):
                    return None
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return None

            sat_avg = get_float("overallSatisfaction", "satisfaction_avg")
            res_rate = get_float("solutionIndex", "resolution_rate")
            complaints = int(stats.get("complaintsCount") or rep_entry.get("complaints_total") or 0)

            # Se TODAS as métricas chave forem nulas/zero, consideramos sem dados
            # (evita reputação fantasma de 0.0)
            if (sat_avg is None or sat_avg == 0) and (res_rate is None or res_rate == 0):
                reputation_data = None
            else:
                reputation_data = {
                    "source": "consumidor.gov",
                    "match_score": match_meta.score if match_meta else 0,
                    "display_name": rep_entry.get("display_name") or rep_entry.get("name"),
                    "metrics": {
                        "satisfaction_avg": sat_avg if sat_avg is not None else 0.0,
                        "resolution_rate": res_rate if res_rate is not None else 0.0,
                        "complaints_total": complaints
                    }
                }

        # C. Produtos Open Insurance
        prods = products_by_cnpj.get(cnpj_dig, []) if cnpj_dig else []

        # D. Montagem do Objeto Mestre
        insurer_obj = {
            "id": cnpj_dig or raw_cnpj,
            "name": str(name),
            "cnpj": cnpj_fmt,
            "flags": {
                "openInsuranceParticipant": bool(cnpj_dig and cnpj_dig in products_by_cnpj)
            },
            "data": {
                "net_worth": comp.get("net_worth", 0.0),
                "premiums": comp.get("premiums", 0.0),
                "claims": comp.get("claims", 0.0),
            },
            "reputation": reputation_data,
            "products": prods
        }

        # E. Aplicação da Inteligência
        processed_insurer = calculate_score(insurer_obj)
        
        # F. Ajuste Fino para UI
        if "financial_score" not in processed_insurer["data"]:
             comp_fin = processed_insurer["data"].get("components", {}).get("solvency", 0.0)
             processed_insurer["data"]["financial_score"] = comp_fin

        insurers.append(processed_insurer)

    # 5. Ordenação Final
    insurers.sort(key=lambda x: x["data"].get("score", 0), reverse=True)

    # 6. Geração do JSON Final
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
