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

# URL da tabela HTML (Confirmada)
API_URL = "https://www.consumidor.gov.br/pages/ranking/resultado-ranking"


def parse_html_table(html_content):
    """
    Extrai nomes e notas do HTML bruto da tabela.
    """
    companies = {}
    # Encontra as linhas da tabela
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
                
            # Coluna 3: Nota Consumidor (Ex: "8,5")
            nota_str = cols[2].replace(',', '.')
            try:
                nota = float(nota_str)
            except ValueError:
                nota = 0.0
                
            if name:
                companies[name] = {
                    "name": name,
                    "statistics": {
                        "overallSatisfaction": nota,
                        "complaintsCount": 0,
                        "solutionIndex": 0,
                        "averageResponseTime": 0
                    },
                    "indexes": {"b": {"nota": nota}}
                }
        except Exception:
            continue
            
    return companies


def fetch_data_with_bypass():
    print("CG: Iniciando bypass com curl_cffi...")
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.consumidor.gov.br",
        "Referer": "https://www.consumidor.gov.br/pages/ranking/ranking-segmento",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
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
            # impersonate="chrome" usa a assinatura TLS mais recente disponível na lib
            # Isso geralmente tem mais sucesso que fixar em versões antigas
            response = requests.post(
                API_URL, 
                data=payload, 
                headers=headers, 
                impersonate="chrome",
                timeout=60
            )
            
            if response.status_code == 200:
                companies = parse_html_table(response.text)
                if companies:
                    print(f"CG: Sucesso! {len(companies)} empresas extraídas.")
                    all_companies.update(companies)
                else:
                    print(f"CG: HTML baixado mas regex falhou no segmento {seg_id}.")
            else:
                print(f"CG: Falha HTTP {response.status_code}")
                
        except Exception as e:
            print(f"CG: Erro de conexão (possível bloqueio WAF): {e}")

    return all_companies


def normalize_key(text):
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    return text.lower().strip()


def main():
    import unicodedata  # Garantir import para normalize_key
    print("\n--- BUILD CONSUMIDOR.GOV (PURE SCRAPER) ---")
    
    crawled_data = fetch_data_with_bypass()
    
    # Se falhar, falha. Não inventa dados.
    if not crawled_data:
        print("CG: ERRO - Nenhuma empresa coletada. O WAF bloqueou ou o layout mudou.")
        # Gera JSON vazio para não quebrar o próximo passo do pipeline, 
        # mas deixa claro que não tem dados.
        crawled_data = {}

    aggregated = {
        "by_cnpj_key": {},
        "by_name": {}
    }

    # Normalização
    for name, data in crawled_data.items():
        # Função interna de normalização para evitar dependência externa
        norm_name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII').lower().strip()
        aggregated["by_name"][norm_name] = data

    print(f"CG: Total de empresas indexadas: {len(aggregated['by_name'])}")
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(aggregated, f, ensure_ascii=False, separators=(',', ':'))
    
    print(f"CG: Arquivo salvo em {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
