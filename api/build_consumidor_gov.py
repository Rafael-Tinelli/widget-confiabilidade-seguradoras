# api/build_consumidor_gov.py
import json
import time
from pathlib import Path
from curl_cffi import requests

# Configurações
OUTPUT_FILE = Path("data/derived/consumidor_gov/aggregated.json")
CACHE_DIR = Path("data/raw/consumidor_gov")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# URL da API interna que alimenta o ranking do site
API_URL = "https://www.consumidor.gov.br/pages/ranking/consultar-ranking-segmento.json"


def fetch_data_with_bypass():
    """
    Usa curl_cffi para emular um navegador Chrome real (TLS Fingerprint).
    Isso evita o bloqueio 403 que o requests normal sofre.
    """
    print("CG: Iniciando bypass com curl_cffi (Chrome impersonation)...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.consumidor.gov.br",
        "Referer": "https://www.consumidor.gov.br/pages/ranking/ranking-segmento"
    }

    # Segmentos ID: 
    # 2 = Bancos, Financeiras e Administradoras de Cartão
    # 4 = Seguros, Capitalização e Previdência
    segmentos = [2, 4] 
    
    all_companies = {}

    for seg_id in segmentos:
        print(f"CG: Consultando segmento {seg_id}...")
        
        payload = {
            "segmento": str(seg_id),
            "area": "",
            "assunto": "",
            "grupoProblema": "",
            "periodo": "365",  # Últimos 12 meses
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
                data = response.json()
                lista = data.get("listaRanking", [])
                print(f"CG: Sucesso! {len(lista)} empresas encontradas no segmento {seg_id}.")
                
                for item in lista:
                    # Normaliza a chave (Nome Fantasia)
                    nome = item.get("nomeFantasia", "").strip()
                    if not nome:
                        continue
                    
                    # Salva dados cruciais
                    all_companies[nome] = {
                        "name": nome,
                        "cnpj": None,
                        "statistics": {
                            "overallSatisfaction": item.get("notaConsumidor", 0),
                            "complaintsCount": item.get("totalReclamacoes", 0),
                            "solutionIndex": item.get("indiceSolucao", 0),
                            "averageResponseTime": item.get("tempoResposta", 0)
                        },
                        "indexes": {
                            "b": {"nota": item.get("notaConsumidor", 0)}
                        }
                    }
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
    print("\n--- BUILD CONSUMIDOR.GOV (REAL BYPASS) ---")
    
    # 1. Tenta baixar dados reais
    crawled_data = fetch_data_with_bypass()
    
    if not crawled_data:
        print("CG: CRÍTICO - Bypass falhou completamente. Verifique se o site mudou.")
        return

    # 2. Estrutura para o Matcher
    aggregated = {
        "by_cnpj_key": {},
        "by_name": {}
    }

    for nome, data in crawled_data.items():
        # Indexa por nome normalizado (para o Fuzzy Match funcionar)
        norm_name = normalize_key(nome)
        aggregated["by_name"][norm_name] = data

    print(f"CG: Total de empresas indexadas para match: {len(aggregated['by_name'])}")
    
    # Salva
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(aggregated, f, ensure_ascii=False, separators=(',', ':'))
    
    print(f"CG: Arquivo salvo em {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
