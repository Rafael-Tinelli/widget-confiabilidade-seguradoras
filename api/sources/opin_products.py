# api/build_insurers.py
from __future__ import annotations

import json
import math
from pathlib import Path
from api.sources.ses import extract_ses_master_and_financials
from api.sources.opin_products import extract_open_insurance_products
from api.matching.consumidor_gov_match import NameMatcher

# Configurações de Caminho
CONSUMIDOR_GOV_FILE = Path("data/derived/consumidor_gov/aggregated.json")
OUTPUT_FILE = Path("api/v1/insurers.json")

def main():
    # --- 1. SUSEP (Financeiro + Cadastro) ---
    print("\n--- INICIANDO COLETA SUSEP (FINANCEIRO) ---")
    # Retorna metadados e um dict: { 'id_susep': { 'name', 'cnpj', 'net_worth', ... } }
    ses_meta, companies = extract_ses_master_and_financials()

    # --- 2. OPIN (Produtos) ---
    print("\n--- INICIANDO COLETA OPEN INSURANCE (PRODUTOS) ---")
    # CORREÇÃO: A função retorna o dicionário {CNPJ: [Produtos]} diretamente.
    opin_products = extract_open_insurance_products()

    # --- 3. Consumidor.gov (Reputação) ---
    print("\n--- INICIANDO COLETA CONSUMIDOR.GOV ---")
    reputation_data = {}
    if CONSUMIDOR_GOV_FILE.exists():
        try:
            with open(CONSUMIDOR_GOV_FILE, "r", encoding="utf-8") as f:
                reputation_data = json.load(f)
        except Exception as e:
            print(f"AVISO: Erro ao ler base Consumidor.gov: {e}")
    else:
        print("AVISO: Base Consumidor.gov não encontrada (arquivo data/derived/... ausente).")

    # Prepara o Matcher de Nomes
    matcher = NameMatcher(reputation_data)

    # --- 4. Consolidação ---
    print("\n--- CONSOLIDANDO DADOS ---")
    
    insurers_list = []
    
    for susep_id, comp_data in companies.items():
        name = comp_data["name"]
        cnpj = comp_data["cnpj"]
        
        # Métricas Financeiras
        net_worth = comp_data.get("net_worth", 0.0)
        premiums = comp_data.get("premiums", 0.0)
        claims = comp_data.get("claims", 0.0)
        
        # Cálculo do Score Financeiro (Escala Logarítmica Suavizada)
        # Objetivo: Permitir que empresas médias tenham nota visível, sem que a Brasilprev (60Bi) quebre o gráfico.
        fin_score = 0.0
        if premiums > 0:
            # log10(10 Milhões) = 7.0
            # log10(60 Bilhões) = 10.7
            # Fórmula empírica para mapear 10M -> ~30pts e 60Bi -> 100pts
            log_val = math.log10(premiums)
            fin_score = min(100.0, max(0.0, (log_val - 6.0) * 22))
        
        # Vinculação de Produtos (Open Insurance)
        # Tenta CNPJ formatado e limpo
        cnpj_clean = "".join(filter(str.isdigit, cnpj))
        prods = opin_products.get(cnpj, [])
        if not prods:
            prods = opin_products.get(cnpj_clean, [])

        # Vinculação de Reputação (Matcher Inteligente)
        rep_match = matcher.best(name)
        rep_data = None
        if rep_match:
            # Se deu match, pegamos os dados reais do Consumidor.gov
            rep_data = reputation_data.get(rep_match.key)

        # Montagem do Objeto Final
        insurers_list.append({
            "id": susep_id,
            "cnpj": cnpj,
            "name": name,
            "data": {
                "net_worth": net_worth,
                "premiums": premiums,
                "claims": claims,
                "financial_score": round(fin_score, 1),
                "components": {
                    "financial": {
                        "status": "data_available" if (premiums > 0 or net_worth > 0) else "no_data",
                        "value": round(fin_score, 1)
                    },
                    "reputation": rep_data # Pode ser None (N/A)
                }
            },
            "products": prods
        })

    # Ordenação Padrão (Melhores financeiramente primeiro)
    insurers_list.sort(key=lambda x: x["data"]["financial_score"], reverse=True)

    # Output Final
    output = {
        "meta": {
            "count": len(insurers_list),
            "sources": ["SUSEP (SES)", "Open Insurance Brasil", "Consumidor.gov.br"]
        },
        "insurers": insurers_list
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        # Usa separators para compactar o JSON (remove espaços extras)
        json.dump(output, f, ensure_ascii=False, separators=(',', ':'))

    print(f"Stats Check: Count {len(companies)} -> {len(insurers_list)} (OK)")
    print(f"OK: generated {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
