# api/build_insurers.py
import json
from pathlib import Path
from datetime import datetime

# Importa extratores
from api.sources.ses import extract_ses_master_and_financials
from api.sources.opin_products import extract_open_insurance_products
from api.intelligence import calculate_score

CACHE_DIR = Path("data/raw")
OUTPUT_DIR = Path("api/v1")
SNAPSHOTS_DIR = Path("data/snapshots")

def _guard_count_regression(new_count, old_count):
    if old_count > 0 and new_count < (old_count * 0.8):
        raise RuntimeError(f"CRITICAL: Queda abrupta de seguradoras ({old_count} -> {new_count}).")

def main():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    print("\n--- INICIANDO COLETA SUSEP (FINANCEIRO) ---")
    meta_ses, companies = extract_ses_master_and_financials()
    
    print("\n--- INICIANDO COLETA OPEN INSURANCE (PRODUTOS) ---")
    opin_result = extract_open_insurance_products()
    
    # Extrai metadados e dados do resultado híbrido
    meta_opin_prod = opin_result
    opin_products = opin_result.data
    
    print("\n--- INICIANDO COLETA CONSUMIDOR.GOV ---")
    # Carrega dados já processados do consumidor.gov
    try:
        cons_gov_file = Path("data/derived/consumidor_gov/aggregated.json")
        if cons_gov_file.exists():
            with open(cons_gov_file, "r") as f:
                consumidor_gov_data = json.load(f)
        else:
            consumidor_gov_data = {}
    except Exception:
        consumidor_gov_data = {}

    print("\n--- CONSOLIDANDO DADOS ---")
    
    final_insurers = []
    
    for ses_id, comp_data in companies.items():
        cnpj = comp_data.get("cnpj")
        name = comp_data.get("name")
        
        # Dados Financeiros
        premiums = comp_data.get("premiums", 0.0)
        claims = comp_data.get("claims", 0.0)
        net_worth = comp_data.get("net_worth", 0.0) # AGORA LÊ O PATRIMÔNIO
        
        # Open Insurance
        products = opin_products.get(cnpj, [])
        
        # Consumidor.gov
        reputation = consumidor_gov_data.get(cnpj)
        
        # Monta objeto para cálculo
        insurer_obj = {
            "id": f"ses:{ses_id}",
            "name": name,
            "cnpj": cnpj,
            "data": {
                "premiums": premiums,
                "claims": claims,
                "net_worth": net_worth # Passa para o cálculo
            },
            "products": products,
            "reputation": reputation
        }
        
        # Calcula Score e Segmento
        # A função calculate_score precisa ser capaz de ler net_worth
        scored_insurer = calculate_score(insurer_obj)
        final_insurers.append(scored_insurer)

    # Ordena por Score
    final_insurers.sort(key=lambda x: x["data"].get("score", 0), reverse=True)
    
    # Salva JSON Final
    output = {
        "schemaVersion": "1.0.0",
        "generatedAt": datetime.now().isoformat(),
        "period": {"type": "rolling_12m", "currency": "BRL"},
        "sources": {
            "ses": {"dataset": meta_ses.source, "url": meta_ses.zip_url, "files": [meta_ses.cias_file, meta_ses.seguros_file]},
            "consumidorGov": {"dataset": "Consumidor.gov.br", "url": "https://dados.mj.gov.br/", "note": "B2 (reputação)"},
            "opin": {"dataset": "OPIN Participants", "url": "https://data.directory.opinbrasil.com.br/participants", "note": "B3 (flag)"},
            "open_insurance_products": {
                "source": meta_opin_prod.source,
                "stats": meta_opin_prod.stats,
                "files": [
                    meta_opin_prod.products_auto_file, 
                    meta_opin_prod.products_life_file, 
                    meta_opin_prod.products_home_file
                ]
            }
        },
        "taxonomy": {
            "segments": {
                "S1": "Seguradoras de Grande Porte",
                "S2": "Seguradoras de Médio Porte",
                "S3": "Seguradoras de Pequeno Porte",
                "S4": "Insurtechs / supervisionadas especiais"
            },
            "products": {
                "auto": "Automóvel",
                "vida": "Pessoas e Vida", 
                "patrimonial": "Residencial e Patrimonial",
                "rural": "Rural"
            }
        },
        "insurers": final_insurers,
        "meta": {
            "count": len(final_insurers),
            "disclaimer": "Dados consolidados automaticamente."
        }
    }

    # Validação de Segurança (Regressão)
    try:
        old_file = OUTPUT_DIR / "insurers.json"
        if old_file.exists():
            with open(old_file) as f:
                old_data = json.load(f)
                old_count = old_data.get("meta", {}).get("count", 0)
                _guard_count_regression(len(final_insurers), old_count)
                print(f"Stats Check: Count {old_count} -> {len(final_insurers)} (OK)")
    except Exception as e:
        print(f"Warning na validação: {e}")

    # Escreve arquivos
    with open(OUTPUT_DIR / "insurers.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(',', ':'))
    
    print("OK: generated api/v1/insurers.json")

if __name__ == "__main__":
    main()
