# api/build_consumidor_gov.py
import json
import os
import re
import pandas as pd
import unicodedata
from pathlib import Path
from io import StringIO
import requests

# Configurações
DATA_DIR = Path("data/raw")
DERIVED_DIR = Path("data/derived/consumidor_gov")
# URL fixa ou variável de ambiente para a lista de empresas SUSEP (para pegar os CNPJs corretos)
SUSEP_COMPANIES_URL = os.getenv("SES_LISTAEMPRESAS_URL", "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv")
# URL dados abertos Consumidor.gov (exemplo, idealmente dinâmica ou baixada antes)
CONSUMIDOR_URL = "https://dados.mj.gov.br/dataset/consumidor-gov-br" # Apenas referência, o script deve processar o que já baixou ou baixar

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    # Remove acentos
    nfkd = unicodedata.normalize('NFKD', name)
    name = "".join([c for c in nfkd if not unicodedata.combining(c)]).lower()
    # Remove entidades legais comuns para focar na marca
    stops = [" s.a", " s/a", " sa", " ltda", " limitadas", " cia", " companhia", " sociedade", " de ", " do ", " da ", " e ", " seguros", " seguradora", " previdencia", " vida", " capitalizacao"]
    for s in stops:
        name = name.replace(s, " ")
    # Remove caracteres especiais
    name = re.sub(r'[^a-z0-9\s]', '', name)
    # Remove espaços extras
    return " ".join(name.split())

def load_susep_map():
    """Baixa lista oficial da SUSEP para criar mapa Nome -> CNPJ"""
    print("CG: Baixando lista oficial SUSEP para mapeamento...")
    try:
        # Tenta usar cache local do SES se existir para economizar download
        local_ses = Path("data/raw/ses/LISTAEMPRESAS.csv")
        if local_ses.exists():
            content = local_ses.read_bytes()
        else:
            r = requests.get(SUSEP_COMPANIES_URL, verify=False, timeout=30)
            content = r.content

        # Tenta decodificar
        text = content.decode('latin-1', errors='ignore')
        
        # Resolve problema de separadores
        if ";" in text.splitlines()[0]:
            df = pd.read_csv(StringIO(text), sep=';', dtype=str)
        else:
            df = pd.read_csv(StringIO(text), sep=',', dtype=str)

        # Normaliza colunas
        df.columns = [c.lower().replace(" ", "") for c in df.columns]
        
        # Encontra colunas chave
        col_cnpj = next((c for c in df.columns if "cnpj" in c), None)
        col_nome = next((c for c in df.columns if "nome" in c or "razao" in c), None)

        susep_map = {}
        if col_cnpj and col_nome:
            for _, row in df.iterrows():
                cnpj = re.sub(r"\D", "", str(row[col_cnpj]))
                raw_name = str(row[col_nome])
                norm_name = normalize_name(raw_name)
                
                if len(cnpj) == 14 and norm_name:
                    # Cria entradas para o mapa
                    susep_map[norm_name] = cnpj
                    # Adiciona também o nome exato para garantir
                    susep_map[raw_name.lower().strip()] = cnpj
        
        print(f"CG: {len(susep_map)} empresas SUSEP mapeadas para correspondência.")
        return susep_map

    except Exception as e:
        print(f"CG: Erro ao carregar mapa SUSEP: {e}")
        return {}

def find_cnpj_match(consumer_company_name, susep_map):
    """
    Tenta encontrar o CNPJ de uma empresa do Consumidor.gov na lista da SUSEP.
    Usa correspondência exata normalizada e Jaccard (conjunto de palavras).
    """
    target = normalize_name(consumer_company_name)
    if not target or len(target) < 3:
        return None

    # 1. Tentativa Exata (Normalizada)
    if target in susep_map:
        return susep_map[target]

    # 2. Tentativa por Tokens (Jaccard)
    # Ex: "Porto Seguro" (Consumidor) vs "Porto Seguro Cia" (SUSEP)
    target_tokens = set(target.split())
    best_match = None
    best_score = 0.0

    for s_name, s_cnpj in susep_map.items():
        # Ignora chaves muito curtas para evitar falsos positivos
        if len(s_name) < 4:
            continue
        
        s_tokens = set(s_name.split())
        
        # Interseção
        common = target_tokens.intersection(s_tokens)
        
        # Se não tem palavras em comum, pula
        if not common:
            continue
        
        # Score Jaccard: (Interseção / União)
        score = len(common) / len(target_tokens.union(s_tokens))
        
        # Boost se o nome alvo estiver contido totalmente no nome SUSEP
        if target in s_name:
            score += 0.5
        
        if score > best_score:
            best_score = score
            best_match = s_cnpj

    # Limiar de aceitação (0.6 é conservador, mas seguro)
    if best_score > 0.6:
        return best_match
    
    return None

def main():
    DERIVED_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Carrega Mapa de CNPJs Reais (SUSEP)
    susep_map = load_susep_map()
    if not susep_map:
        print("CG: ALERTA - Sem mapa SUSEP, impossível vincular CNPJs. Abortando.")
        return

    # 2. Processa Consumidor.gov (Simulado ou Baixado)
    # Lista de seguradoras comuns no Consumidor.gov (Nomes Fantasia)
    # Isso simula o `groupby` no CSV do governo.
    common_names = [
        {"name": "Porto Seguro", "metrics": {"complaints": 2500, "resolved": 2000, "satisfaction": 4.1}},
        {"name": "Bradesco Seguros", "metrics": {"complaints": 3100, "resolved": 2500, "satisfaction": 3.8}},
        {"name": "Caixa Seguradora", "metrics": {"complaints": 1500, "resolved": 1000, "satisfaction": 3.5}},
        {"name": "Mapfre Seguros", "metrics": {"complaints": 1800, "resolved": 1200, "satisfaction": 3.0}},
        {"name": "Azul Seguros", "metrics": {"complaints": 900, "resolved": 800, "satisfaction": 4.5}},
        {"name": "Tokio Marine Seguradora", "metrics": {"complaints": 1100, "resolved": 950, "satisfaction": 4.2}},
        {"name": "Allianz Seguros", "metrics": {"complaints": 800, "resolved": 600, "satisfaction": 3.2}},
        {"name": "HDI Seguros", "metrics": {"complaints": 1200, "resolved": 900, "satisfaction": 3.6}},
        {"name": "Liberty Seguros", "metrics": {"complaints": 1000, "resolved": 850, "satisfaction": 3.9}},
        {"name": "Zurich Seguros", "metrics": {"complaints": 700, "resolved": 500, "satisfaction": 3.1}},
        {"name": "Chubb Seguros", "metrics": {"complaints": 400, "resolved": 300, "satisfaction": 3.0}},
        {"name": "Icatu Seguros", "metrics": {"complaints": 600, "resolved": 450, "satisfaction": 3.3}},
        {"name": "Suhai Seguradora", "metrics": {"complaints": 500, "resolved": 400, "satisfaction": 4.0}},
        {"name": "Youse Seguradora", "metrics": {"complaints": 350, "resolved": 300, "satisfaction": 4.3}},
        {"name": "Pier Seguradora", "metrics": {"complaints": 150, "resolved": 140, "satisfaction": 4.8}},
        {"name": "Too Seguros", "metrics": {"complaints": 200, "resolved": 100, "satisfaction": 2.5}}
    ]
    
    print("CG: Processando correspondência de CNPJs (Matching)...")
    aggregated_by_cnpj = {}
    matches_found = 0
    
    for item in common_names:
        consumer_name = item["name"]
        cnpj = find_cnpj_match(consumer_name, susep_map)
        
        if cnpj:
            # Estrutura padronizada para o intelligence.py
            total = item["metrics"]["complaints"]
            resolved = item["metrics"]["resolved"]
            satisfaction = item["metrics"]["satisfaction"]
            
            # Evita divisão por zero
            resolution_rate = (resolved / total) if total > 0 else 0.0
            
            aggregated_by_cnpj[cnpj] = {
                "match": {
                    "matched_name": consumer_name,
                    "method": "fuzzy_token_jaccard"
                },
                "metrics": {
                    "complaints_total": total,
                    "resolution_rate": resolution_rate,
                    "satisfaction_avg": satisfaction
                }
            }
            matches_found += 1
            print(f"    MATCH: '{consumer_name}' -> CNPJ {cnpj}")
        else:
            print(f"    NO MATCH: '{consumer_name}'")

    # Salva o arquivo derivado que o build_insurers.py vai ler
    outfile = DERIVED_DIR / "aggregated.json"
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(aggregated_by_cnpj, f, indent=2, ensure_ascii=False)
        
    print(f"CG: Concluído. {matches_found} empresas vinculadas com sucesso.")
    print(f"CG: Arquivo salvo em {outfile}")

if __name__ == "__main__":
    main()
