# api/build_consumidor_gov.py
import json
import re
import time
from pathlib import Path
from curl_cffi import requests

# Configurações
OUTPUT_FILE = Path("data/derived/consumidor_gov/aggregated.json")
CACHE_DIR = Path("data/raw/consumidor_gov")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# URL CORRETA (Retorna HTML da tabela)
API_URL = "https://www.consumidor.gov.br/pages/ranking/resultado-ranking"


def parse_html_table(html_content):
    """
    Extrai nomes e notas do HTML bruto retornado pelo servidor.
    """
    companies = {}
    
    # Encontrar linhas da tabela
    rows = re.findall(r'<tr.*?>(.*?)</tr>', html_content, re.DOTALL)
    
    for row in rows:
        try:
            # Extrair Nome (dentro do link <a>)
            name_match = re.search(r'<a[^>]*>(.*?)</a>', row)
            if not name_match:
                continue
            name = name_match.group(1).strip()
            
            # Extrair todas as células numéricas
            cols = re.findall(r'<td[^>]*>([\d,]+)%?</td>', row)
            
            # Se não achou colunas numéricas suficientes, pula
            if len(cols) < 3:
                continue
                
            # Col 3: Nota Consumidor (Ex: "8,5")
            nota_str = cols[2].replace(',', '.')
            try:
                nota = float(nota_str)
            except ValueError:
                nota = 0.0
                
            if name:
                companies[name] = {
                    "name": name,
                    "cnpj": None,
                    "statistics": {
                        "overallSatisfaction": nota,
                        "complaintsCount": 0,
                        "solutionIndex": 0,
                        "averageResponseTime": 0
                    },
                    "indexes": {
                        "b": {"nota": nota}
                    }
                }
        except Exception:
            continue
            
    return companies


def fetch_data_with_bypass():
    print("CG: Iniciando bypass com curl_cffi (Chrome impersonation)...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.consumidor.gov.br",
        "Referer": "https://www.consumidor.gov.br/pages/ranking/ranking-segmento"
    }

    # Segmentos: 2 (Bancos), 4 (Seguros/Prev)
    segmentos = [2, 4] 
    all_companies = {}

    for seg_id in segmentos:
        print(f"CG: Consultando segmento {seg_id}...")
        
        payload = {
            "segmento": str(seg_id),
            "area": "",
            "assunto": "",
            "grupoProblema": "",
            "periodo": "365",
            "dataTermino": time.strftime("%d/%m/%Y"),
            "regiao": "BR"
        }

        try:
            response = requests.post(
                API_URL, 
                data=payload, 
                headers=headers, 
                impersonate="chrome",
                timeout=30
            )
            
            if response.status_code == 200:
                companies = parse_html_table(response.text)
                count = len(companies)
                
                if count > 0:
                    print(f"CG: Sucesso! {count} empresas extraídas do HTML no segmento {seg_id}.")
                    all_companies.update(companies)
                else:
                    print(f"CG: Aviso - HTML baixado, mas regex não encontrou dados no segmento {seg_id}.")
            else:
                print(f"CG: Falha no segmento {seg_id}. Status: {response.status_code}")
                
        except Exception as e:
            print(f"CG: Erro crítico ao acessar Consumidor.gov: {e}")

    return all_companies


def normalize_key(text):
    import unicodedata
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    return text.lower().strip()


def main():
    print("\n--- BUILD CONSUMIDOR.GOV (HTML PARSER FIXED) ---")
    
    crawled_data = fetch_data_with_bypass()
    
    if not crawled_data:
        print("CG: ERRO - Nenhuma empresa coletada. Verifique logs.")
        crawled_data = {}

    aggregated = {
        "by_cnpj_key": {},
        "by_name": {}
    }

    for nome, data in crawled_data.items():
        norm_name = normalize_key(nome)
        aggregated["by_name"][norm_name] = data

    print(f"CG: Total de empresas indexadas: {len(aggregated['by_name'])}")
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(aggregated, f, ensure_ascii=False, separators=(',', ':'))
    
    print(f"CG: Arquivo salvo em {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
