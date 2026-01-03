# api/build_consumidor_gov.py
import json
import re
import time
import unicodedata
from pathlib import Path
from curl_cffi import requests

# Configurações
OUTPUT_FILE = Path("data/derived/consumidor_gov/aggregated.json")
CACHE_DIR = Path("data/raw/consumidor_gov")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# URL da tabela HTML
API_URL = "https://www.consumidor.gov.br/pages/ranking/resultado-ranking"


def normalize_key(text):
    if not text:
        return ""
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    return text.lower().strip()


def parse_html_table(html_content):
    companies = {}
    rows = re.findall(r'<tr.*?>(.*?)</tr>', html_content, re.DOTALL)
    
    for row in rows:
        try:
            name_match = re.search(r'<a[^>]*>(.*?)</a>', row)
            if not name_match:
                continue
            name = name_match.group(1).strip()
            
            cols = re.findall(r'<td[^>]*>([\d,]+)%?</td>', row)
            if len(cols) < 3:
                continue
            
            # Nota Consumidor (3ª coluna numérica)
            nota_str = cols[2].replace(',', '.')
            try:
                nota = float(nota_str)
            except ValueError:
                nota = 0.0
                
            if name:
                norm_name = normalize_key(name)
                companies[norm_name] = {
                    "display_name": name,
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
    print("CG: Iniciando bypass com curl_cffi (Chrome 120)...")
    
    # Headers otimizados para parecer navegação real
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html, */*; q=0.01",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.consumidor.gov.br",
        "Referer": "https://www.consumidor.gov.br/pages/ranking/ranking-segmento",
        "X-Requested-With": "XMLHttpRequest",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin"
    }

    # Segmentos: 2 (Bancos), 4 (Seguros)
    segmentos = [2, 4] 
    all_data = {}

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
            # Impersonate atualizado para chrome120 (menos chance de block que o 110)
            # Timeout aumentado para lidar com lentidão do WAF
            response = requests.post(
                API_URL, 
                data=payload, 
                headers=headers, 
                impersonate="chrome120",
                timeout=60
            )
            
            if response.status_code == 200:
                extracted = parse_html_table(response.text)
                count = len(extracted)
                if count > 0:
                    print(f"CG: Sucesso! {count} empresas extraídas do HTML no segmento {seg_id}.")
                    all_data.update(extracted)
                else:
                    print(f"CG: Aviso - HTML baixado, mas regex não encontrou dados no segmento {seg_id}.")
            else:
                print(f"CG: Falha HTTP {response.status_code} no segmento {seg_id}")
                
        except Exception as e:
            print(f"CG: Erro de conexão (WAF/Rede) no segmento {seg_id}: {e}")

    return all_data


def main():
    print("\n--- BUILD CONSUMIDOR.GOV (ROBUST BYPASS) ---")
    
    crawled_data = fetch_data_with_bypass()
    
    if not crawled_data:
        print("CG: ALERTA - Nenhuma empresa coletada. O WAF pode estar bloqueando o IP do GitHub.")
        crawled_data = {}

    aggregated = {
        "by_cnpj_key": {},
        "by_name": crawled_data
    }

    print(f"CG: Total de empresas indexadas: {len(aggregated['by_name'])}")
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(aggregated, f, ensure_ascii=False, separators=(',', ':'))
    
    print(f"CG: Arquivo salvo em {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
