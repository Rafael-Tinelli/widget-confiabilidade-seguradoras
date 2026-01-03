# api/build_insurers.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

# Importações internas do projeto
from api.matching.consumidor_gov_match import NameMatcher, format_cnpj, normalize_cnpj
from api.sources.opin_products import extract_open_insurance_products
from api.sources.ses import extract_ses_master_and_financials
# AQUI ESTÁ A INTEGRAÇÃO QUE FALTAVA:
from api.intelligence import calculate_score  

# Caminhos de arquivos
OUTPUT_FILE = Path("api/v1/insurers.json")
CONSUMIDOR_GOV_FILE = Path("data/derived/consumidor_gov/aggregated.json")

def _utc_now_iso() -> str:
    """Retorna timestamp ISO 8601 atual."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def main() -> None:
    # 1. COLETA FINANCEIRA (SUSEP)
    # Agora usa o ses.py corrigido (sem o erro dos quatrilhões)
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

    # 4. CONSOLIDAÇÃO (ETAPA CRÍTICA)
    print("\n--- CONSOLIDANDO E CALCULANDO SCORES (VIA INTELLIGENCE) ---")
    insurers = []
    matched_reputation = 0

    for raw_cnpj, comp in companies.items():
        # Normalização de CNPJ e Nome
        cnpj_dig = normalize_cnpj(comp.get("cnpj") or raw_cnpj) or normalize_cnpj(raw_cnpj)
        cnpj_fmt = format_cnpj(cnpj_dig) if cnpj_dig else str(comp.get("cnpj") or raw_cnpj)
        name = comp.get("name") or comp.get("corporate_name") or comp.get("razao_social") or cnpj_fmt

        # A. Match com Consumidor.gov
        rep_entry, match_meta = matcher.get_entry(str(name), cnpj=cnpj_dig or cnpj_fmt)
        
        # B. Estrutura de Reputação para o Intelligence.py
        # O intelligence espera um dict com 'metrics' dentro de 'reputation'
        reputation_data = None
        if rep_entry:
            matched_reputation += 1
            # Extração segura das estatísticas vindas do Scraper HTML ou CSV
            stats = rep_entry.get("statistics") or {}
            
            reputation_data = {
                "source": "consumidor.gov",
                "match_score": match_meta.score if match_meta else 0,
                "display_name": rep_entry.get("display_name") or rep_entry.get("name"),
                "metrics": {
                    # Tenta pegar 'overallSatisfaction' (HTML scraper) ou 'satisfaction_avg' (CSV antigo)
                    "satisfaction_avg": stats.get("overallSatisfaction") or 
                                      rep_entry.get("satisfaction_avg") or 0.0,
                    
                    # Tenta pegar 'solutionIndex' (HTML) ou 'resolution_rate' (CSV)
                    "resolution_rate": stats.get("solutionIndex") or 
                                     rep_entry.get("resolution_rate") or 0.0,
                    
                    # Tenta pegar 'complaintsCount' (HTML) ou 'complaints_total' (CSV)
                    "complaints_total": stats.get("complaintsCount") or 
                                      rep_entry.get("complaints_total") or 0
                }
            }

        # C. Produtos Open Insurance vinculados a este CNPJ
        prods = products_by_cnpj.get(cnpj_dig, []) if cnpj_dig else []

        # D. Montagem do Objeto Mestre (Pré-Cálculo)
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
            "reputation": reputation_data, # Passa o objeto estruturado ou None
            "products": prods
        }

        # E. Aplicação da Inteligência (Cálculo de Scores)
        # Esta função modifica 'insurer_obj' in-place, adicionando 'score', 'components', etc.
        processed_insurer = calculate_score(insurer_obj)
        
        # F. Ajuste Fino para Compatibilidade com Frontend
        # Garante que 'financial_score' exista na raiz de 'data' (usado na ordenação e no Card)
        if "financial_score" not in processed_insurer["data"]:
             # Pega do componente calculado pelo intelligence
             comp_fin = processed_insurer["data"].get("components", {}).get("solvency", 0.0)
             processed_insurer["data"]["financial_score"] = comp_fin

        insurers.append(processed_insurer)

    # 5. Ordenação Final
    # Ordena pelo Score Geral (ponderado) calculado pelo intelligence
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
    # indent=0 economiza bytes, mas mantém quebras de linha para legibilidade mínima se necessário
    OUTPUT_FILE.write_text(json.dumps(out, ensure_ascii=False, indent=0, separators=(',', ':')), encoding="utf-8")
    
    print(f"OK: generated {OUTPUT_FILE} with {len(insurers)} insurers. Reputation matched: {matched_reputation}.")

if __name__ == "__main__":
    main()
