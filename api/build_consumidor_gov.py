# api/build_consumidor_gov.py
import json
import re
import time
import unicodedata
from pathlib import Path
from curl_cffi import requests

# Configurações de Saída
OUTPUT_FILE = Path("data/derived/consumidor_gov/aggregated.json")
CACHE_DIR = Path("data/raw/consumidor_gov")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# URL Real que alimenta a tabela do site (retorna HTML)
API_URL = "https://www.consumidor.gov.br/pages/ranking/resultado-ranking"


def normalize_key(text):
    """Normaliza o nome para ser usado como chave de busca (lowercase, sem acento)."""
    if not text:
        return ""
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    return text.lower().strip()


def parse_html_table(html_content):
    """
    Parser robusto via Regex para extrair dados da tabela HTML retornada pelo servidor.
    Extrai: Nome, Nota (Satisfação) e cria a estrutura esperada pelo Matcher.
    """
    companies = {}
    
    # Regex para encontrar as linhas da tabela <tr>...</tr>
    rows = re.findall(r'<tr.*?>(.*?)</tr>', html_content, re.DOTALL)
    
    for row in rows:
        try:
            # 1. Extrair Nome da Empresa (dentro da tag <a ...>)
            name_match = re.search(r'<a[^>]*>(.*?)</a>', row)
            if not name_match:
                continue
            name = name_match.group(1).strip()
            
            # 2. Extrair Células de Dados (td)
            # A tabela geralmente tem: [Posição, Nome, Reclamações, Respondidas, Nota Consumidor, ...]
            # A "Nota Consumidor" costuma ser a 3ª ou 4ª célula numérica relevante.
            cols = re.findall(r'<td[^>]*>([\d,]+)%?</td>', row)
            
            # Precisamos de pelo menos 3 números para chegar na nota
            if len(cols) < 3:
                continue
            
            # A nota está na coluna correspondente a "Nota do Consumidor"
            # No ranking padrão "Por Segmento", a ordem visual é:
            # Total Reclamações | Respondidas | Nota Consumidor (Ex: 8,5)
            
            # Vamos pegar a coluna de índice 2 (terceira coluna numérica encontrada)
            nota_str = cols[2].replace(',', '.')
            try:
                nota = float(nota_str)
            except ValueError:
                nota = 0.0
                
            if name:
                norm_name = normalize_key(name)
                # Estrutura compatível com o Agg.to_public() antigo
                companies[norm_name] = {
                    "display_name": name,
                    "complaints_total": 0,  # Dado secundário no bypass
                    "satisfaction_avg": nota,
                    "name": name,  # Para o matcher novo
                    "cnpj": None,  # Site não fornece CNPJ nesta view
                    "statistics": {
                        "overallSatisfaction": nota,
                    },
                    "indexes": {
                        "b": {"nota": nota}
                    }
                }
        except Exception:
            continue
            
    return companies


def fetch_data_with_bypass():
    """
    Executa o request simulando um Chrome real para evitar o erro 403/WAF.
    """
    print("CG: Iniciando bypass com curl_cffi (Chrome impersonation)...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.consumidor.gov.br",
        "Referer": "https://www.consumidor.gov.br/pages/ranking/ranking-segmento",
        "X-Requested-With": "XMLHttpRequest"
    }

    # IDs de Segmento no Consumidor.gov:
    # 2 = Bancos, Financeiras e Administradoras de Cartão
    # 4 = Seguros, Capitalização e Previdência
    segmentos = [2, 4] 
    all_data = {}

    for seg_id in segmentos:
        print(f"CG: Consultando segmento {seg_id}...")
        
        # Payload do formulário de busca
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
            # Impersonate 'chrome110' é geralmente mais estável contra WAFs do governo
            response = requests.post(
                API_URL, 
                data=payload, 
                headers=headers, 
                impersonate="chrome110",
                timeout=45
            )
            
            if response.status_code == 200:
                extracted = parse_html_table(response.text)
                count = len(extracted)
                if count > 0:
                    print(f"CG: Sucesso! {count} empresas extraídas do HTML no segmento {seg_id}.")
                    all_data.update(extracted)
                else:
                    print("CG: Aviso - HTML baixado, mas regex não encontrou dados. Layout mudou?")
            else:
                print(f"CG: Falha HTTP {response.status_code} no segmento {seg_id}")
                
        except Exception as e:
            print(f"CG: Erro de conexão (WAF/Rede) no segmento {seg_id}: {e}")

    return all_data


def main():
    print("\n--- BUILD CONSUMIDOR.GOV (BYPASS MODE RESTORED) ---")
    
    # 1. Scraping Direto (Sem CSVs quebrados)
    crawled_data = fetch_data_with_bypass()
    
    if not crawled_data:
        print("CG: ALERTA CRÍTICO - Nenhuma empresa coletada via Scraper.")
        crawled_data = {}

    # 2. Formata para o formato esperado pelo Matcher
    # O Matcher espera as chaves 'by_name' e 'by_cnpj_key' (mesmo que vazia)
    aggregated = {
        "by_cnpj_key": {},  # Scraper HTML não pega CNPJ, fica vazio
        "by_name": crawled_data  # Chave já está normalizada
    }

    print(f"CG: Total de empresas indexadas para match: {len(aggregated['by_name'])}")
    
    # 3. Salva JSON Final
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(aggregated, f, ensure_ascii=False, separators=(',', ':'))
    
    print(f"CG: Arquivo salvo em {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
