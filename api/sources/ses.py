# api/sources/ses.py
from __future__ import annotations

import io
import os
import zipfile
import requests
import pandas as pd
from dataclasses import dataclass
from pathlib import Path

# Headers para evitar bloqueio da SUSEP
SES_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

# URLs Oficiais
SES_LISTAEMPRESAS_URL = os.getenv("SES_LISTAEMPRESAS_URL", "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv")
SES_ZIP_URL = os.getenv("SES_ZIP_URL", "https://www2.susep.gov.br/redarq.asp?arq=BaseCompleta%2ezip")
CACHE_DIR = Path("data/raw/ses")

@dataclass
class SesMeta:
    source: str = "SUSEP (SES)"
    zip_url: str = SES_ZIP_URL
    cias_file: str = "LISTAEMPRESAS.csv"
    seguros_file: str = "BaseCompleta.zip"

def _normalize_id(val) -> str:
    """Normaliza o ID da SUSEP (FIP/CNPJ) removendo zeros à esquerda."""
    if pd.isna(val):
        return ""
    # Converte para string, remove decimais se houver (ex: 123.0 -> 123) e limpa
    return str(val).split('.')[0].strip().lstrip('0')

def _parse_br_float(series: pd.Series) -> pd.Series:
    """
    Converte strings numéricas brasileiras ('1.234,56') para float (1234.56).
    CRÍTICO: A entrada DEVE ser tratada como string antes da conversão para evitar
    que o Pandas interprete '1.000' como 1.0 ou 1000 incorretamente.
    """
    # 1. Garante que é string (mesmo que o Pandas tenha achado que era numero)
    s = series.astype(str)
    s = s.str.strip()
    
    # 2. Remove o ponto de milhar (1.000.000 -> 1000000)
    s = s.str.replace('.', '', regex=False)
    
    # 3. Troca a vírgula decimal por ponto (12,50 -> 12.50)
    s = s.str.replace(',', '.', regex=False)
    
    # 4. Converte para número, transformando erros em NaN e preenchendo com 0.0
    return pd.to_numeric(s, errors='coerce').fillna(0.0)

def _download_and_read_csv_list(url: str) -> pd.DataFrame:
    """Baixa e lê o CSV de cadastro de empresas."""
    print(f"SES: Baixando {url}...")
    try:
        response = requests.get(url, headers=SES_HEADERS, verify=False, timeout=60)
        response.raise_for_status()
        content = response.content
        
        # Tenta ler com dtype=str para preservar zeros à esquerda de CNPJs
        try:
            return pd.read_csv(io.BytesIO(content), sep=';', encoding='latin1', dtype=str, on_bad_lines='skip')
        except Exception:
            pass
        try:
            return pd.read_csv(io.BytesIO(content), sep=',', encoding='latin1', dtype=str, on_bad_lines='skip')
        except Exception:
            pass
        return pd.DataFrame()
    except Exception as e:
        print(f"SES CRITICAL: Falha lista empresas: {e}")
        return pd.DataFrame()

def _find_column_by_keyword(columns: list, keywords: list) -> str | None:
    """Encontra a primeira coluna que contenha uma das palavras-chave (case insensitive)."""
    for col in columns:
        for kw in keywords:
            if kw in col:
                return col
    return None

def extract_ses_master_and_financials():
    """
    Função Mestra:
    1. Baixa o cadastro de empresas (CNPJs e Nomes).
    2. Baixa o ZIP gigante com dados financeiros.
    3. Itera sobre os CSVs dentro do ZIP buscando Receitas, Despesas e Patrimônio.
    4. Consolida tudo em um dicionário.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    # --- 1. Processamento do Cadastro (Lista de Empresas) ---
    df_cias = _download_and_read_csv_list(SES_LISTAEMPRESAS_URL)
    companies = {}
    
    if not df_cias.empty:
        # Normaliza colunas para lower case
        df_cias.columns = [c.lower().strip() for c in df_cias.columns]
        
        # Heurística para achar as colunas certas
        col_id = next((c for c in df_cias.columns if 'cod' in c and ('fip' in c or 'ent' in c)), df_cias.columns[0])
        col_cnpj = next((c for c in df_cias.columns if 'cnpj' in c), df_cias.columns[1])
        col_nome = next((c for c in df_cias.columns if 'nome' in c or 'razao' in c), df_cias.columns[2])
        
        print("SES: Processando cadastro...")
        for _, row in df_cias.iterrows():
            try:
                # ID da SUSEP (Chave de ligação com os CSVs financeiros)
                sid = _normalize_id(row[col_id])
                if not sid:
                    continue
                
                # Limpeza do CNPJ
                raw_cnpj = str(row[col_cnpj])
                cnpj_nums = ''.join(filter(str.isdigit, raw_cnpj))
                
                if len(cnpj_nums) == 14:
                    cnpj = f"{cnpj_nums[:2]}.{cnpj_nums[2:5]}.{cnpj_nums[5:8]}/{cnpj_nums[8:12]}-{cnpj_nums[12:]}"
                else:
                    cnpj = cnpj_nums

                companies[sid] = {
                    "cnpj": cnpj,
                    "name": str(row[col_nome]).strip().title(),
                    "net_worth": 0.0,
                    "premiums": 0.0,
                    "claims": 0.0,
                    "sources_found": []
                }
            except Exception:
                continue
    
    print(f"SES: {len(companies)} empresas no cadastro base.")

    # --- 2. Processamento do ZIP Financeiro ---
    print("SES: Baixando e processando ZIP Completo (Financeiro)...")
    try:
        response = requests.get(SES_ZIP_URL, headers=SES_HEADERS, verify=False, timeout=180)
        response.raise_for_status()
        zip_bytes = io.BytesIO(response.content)
        
        with zipfile.ZipFile(zip_bytes) as z:
            all_files = [n for n in z.namelist()]
            print(f"SES INFO: Arquivos encontrados no ZIP: {all_files}")
            
            # Mapeamento de Arquivos -> Tipos de Dados
            # Mantivemos exatamente a sua lógica original de mapeamento
            targets = [
                ('pl_margem', 'PATRIMONIO'), 
                ('balanco', 'PATRIMONIO'),
                ('ses_seguros', 'SEGUROS'),
                ('contrib_benef', 'PREVIDENCIA'),     
                ('ses_dados_cap', 'CAPITALIZACAO'),   
                ('ses_cessoes_recebidas', 'RESSEGURO')
            ]

            processed_files = set() 

            for filename in all_files:
                filename_lower = filename.lower()
                
                # Identifica se o arquivo atual é um dos nossos alvos
                file_type = None
                for keyword, ftype in targets:
                    if keyword in filename_lower:
                        file_type = ftype
                        break
                
                if not file_type:
                    continue

                print(f"SES: Analisando {filename} como {file_type}...")
                
                with z.open(filename) as f:
                    # Lê apenas o cabeçalho primeiro para validar colunas
                    header_df = pd.read_csv(f, sep=';', encoding='latin1', nrows=0)
                    header = [c.lower().strip() for c in header_df.columns]
                    
                    # Identifica colunas obrigatórias: ID da Empresa e Data
                    c_id = next((c for c in header if 'coenti' in c), None)
                    c_data = next((c for c in header if 'damesano' in c), None)
                    
                    if not c_id:
                        print(f"SES SKIP: {filename} sem coluna de ID (coenti).")
                        continue

                    # Identifica colunas de Valores baseado no Tipo de Arquivo
                    c_receita = None
                    c_despesa = None
                    c_patrimonio = None

                    if file_type == 'PATRIMONIO':
                        c_patrimonio = _find_column_by_keyword(header, ['pla', 'patrimonio', 'liquido'])
                    
                    elif file_type == 'SEGUROS':
                        c_receita = _find_column_by_keyword(header, ['premio_ganho', 'premio_emitido', 'premios'])
                        c_despesa = _find_column_by_keyword(header, ['sinistro_corrido', 'sinistros'])

                    elif file_type == 'PREVIDENCIA':
                        c_receita = _find_column_by_keyword(header, ['contrib', 'arrecadacao'])
                        c_despesa = _find_column_by_keyword(header, ['benef', 'resgate'])

                    elif file_type == 'CAPITALIZACAO':
                        c_receita = _find_column_by_keyword(header, ['arrecadacao', 'receita', 'receitascap'])
                        c_despesa = _find_column_by_keyword(header, ['resgate', 'valorresg'])

                    elif file_type == 'RESSEGURO':
                        c_receita = _find_column_by_keyword(header, ['cessao', 'premio_aceito', 'receita'])
                        c_despesa = _find_column_by_keyword(header, ['recuperacao', 'sinistro_pago', 'despesa'])

                    # Se não achou as colunas de valor, pula o arquivo
                    if not (c_patrimonio or (c_receita and c_despesa)):
                        print(f"SES SKIP: {filename} colunas de valor não encontradas.")
                        continue

                    # --- LEITURA BLINDADA DO CSV ---
                    f.seek(0)
                    # dtype=str: ESSENCIAL para não corromper valores monetários brasileiros
                    df = pd.read_csv(f, sep=';', encoding='latin1', dtype=str, on_bad_lines='skip')
                    df.columns = [c.lower().strip() for c in df.columns]
                    
                    # Filtra pela data mais recente disponível no arquivo
                    if c_data:
                        # Converte data para numérico apenas para ordenação
                        dates_numeric = pd.to_numeric(df[c_data], errors='coerce').fillna(0)
                        max_date = dates_numeric.max()
                        
                        if max_date > 0:
                            if file_type == 'PATRIMONIO':
                                # Para patrimônio, queremos o retrato do último mês
                                df = df[dates_numeric == max_date]
                            else:
                                # Para fluxo (prêmios), queremos acumular o ano inteiro mais recente
                                target_year = str(int(max_date))[:4]
                                dates_str = dates_numeric.astype(str)
                                df = df[dates_str.str.startswith(target_year)]

                    # Normaliza o ID da empresa para bater com o cadastro
                    df['sid'] = df[c_id].apply(_normalize_id)

                    count_upd = 0
                    for _, row in df.iterrows():
                        sid = row['sid']
                        if sid in companies:
                            updated = False
                            
                            # Extração de Patrimônio
                            if file_type == 'PATRIMONIO' and c_patrimonio:
                                # Aqui usamos a função segura _parse_br_float
                                val = float(_parse_br_float(pd.Series([row[c_patrimonio]]))[0])
                                if val > 0:
                                    companies[sid]['net_worth'] = val
                                    updated = True
                            
                            # Extração de Receitas e Despesas (Prêmios e Sinistros)
                            elif c_receita and c_despesa:
                                val_rec = float(_parse_br_float(pd.Series([row[c_receita]]))[0])
                                val_desp = float(_parse_br_float(pd.Series([row[c_despesa]]))[0])
                                
                                if val_rec > 0 or val_desp > 0:
                                    companies[sid]['premiums'] += val_rec
                                    companies[sid]['claims'] += val_desp
                                    updated = True
                            
                            if updated:
                                if file_type not in companies[sid]['sources_found']:
                                    companies[sid]['sources_found'].append(file_type)
                                count_upd += 1
                                
                    print(f"SES: {count_upd} empresas atualizadas com dados de {filename}.")
                    processed_files.add(filename)

    except Exception as e:
        print(f"SES CRITICAL: Erro crítico ao processar o ZIP: {e}")

    return SesMeta(), companies
