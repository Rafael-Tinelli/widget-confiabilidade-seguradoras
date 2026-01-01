# api/sources/ses.py
from __future__ import annotations

import io
import os
import zipfile
import requests
import pandas as pd
from dataclasses import dataclass
from pathlib import Path

# Headers para simular navegador e evitar bloqueios simples
SES_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

# URLs oficiais
SES_LISTAEMPRESAS_URL = os.getenv("SES_LISTAEMPRESAS_URL", "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv")
SES_ZIP_URL = os.getenv("SES_ZIP_URL", "https://www2.susep.gov.br/redarq.asp?arq=BaseCompleta%2ezip")
CACHE_DIR = Path("data/raw/ses")

@dataclass
class SesMeta:
    source: str = "SUSEP (SES)"
    zip_url: str = SES_ZIP_URL
    cias_file: str = "LISTAEMPRESAS.csv"
    seguros_file: str = "SES_Seguros.csv"

def _download_and_read_csv(url: str, separator: str = ';') -> pd.DataFrame:
    """
    Baixa e lê CSV com tratamento robusto de erros.
    Usa 'on_bad_lines=skip' para ignorar linhas mal formatadas (erro da linha 43).
    """
    print(f"SES: Baixando {url}...")
    try:
        # verify=False é necessário para SUSEP pois certificado gov.br falha frequentemente
        response = requests.get(url, headers=SES_HEADERS, verify=False, timeout=60)
        response.raise_for_status()
        
        # Lê o conteúdo com Pandas
        df = pd.read_csv(
            io.BytesIO(response.content),
            sep=separator,
            encoding='latin1', # Padrão do governo
            thousands='.',
            decimal=',',
            on_bad_lines='skip',
            dtype=str  # Lê como string para evitar erros de conversão iniciais
        )
        return df
    except Exception as e:
        print(f"SES CRITICAL: Falha ao baixar/ler {url}: {e}")
        return pd.DataFrame()

def _extract_zip_financials() -> pd.DataFrame:
    """
    Baixa o ZIP gigante da SUSEP e extrai os dados financeiros.
    """
    print("SES: Baixando Base Completa (ZIP)...")
    try:
        response = requests.get(SES_ZIP_URL, headers=SES_HEADERS, verify=False, timeout=180)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            # Procura arquivos chave dentro do ZIP (nomes mudam case as vezes)
            csv_seguros = next((n for n in z.namelist() if 'ses_seguros' in n.lower()), None)
            
            if not csv_seguros:
                print("SES WARNING: Arquivo de seguros não encontrado no ZIP.")
                return pd.DataFrame()

            print(f"SES: Extraindo e processando {csv_seguros}...")
            with z.open(csv_seguros) as f:
                df = pd.read_csv(
                    f, 
                    sep=';', 
                    encoding='latin1',
                    thousands='.',
                    decimal=',',
                    on_bad_lines='skip',
                    usecols=['damesano', 'coenti', 'premio_ganho', 'sinistro_corrido'] # Otimização de memória
                )
                return df
    except Exception as e:
        print(f"SES CRITICAL: Erro no processamento do ZIP Financeiro: {e}")
        return pd.DataFrame()

def extract_ses_master_and_financials():
    """
    Função principal. Retorna (SesMeta, companies_dict).
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Carrega Lista de Empresas (Cadastro)
    df_cias = _download_and_read_csv(SES_LISTAEMPRESAS_URL)
    
    if df_cias.empty:
        print("SES: Lista de empresas vazia ou falha no download. Retornando vazio.")
        return SesMeta(), {}

    # Normaliza nomes de colunas
    df_cias.columns = [c.lower().strip() for c in df_cias.columns]
    
    if 'codigofip' not in df_cias.columns:
        # Tenta pegar por índice (0=id, 1=cnpj, 2=nome)
        df_cias.rename(columns={
            df_cias.columns[0]: 'codigofip', 
            df_cias.columns[1]: 'cnpj', 
            df_cias.columns[2]: 'nomeentidade'
        }, inplace=True)

    companies = {}
    
    print("SES: Processando dados cadastrais...")
    for _, row in df_cias.iterrows():
        try:
            # Limpeza de ID e CNPJ
            ses_id = str(row['codigofip']).strip()
            cnpj_nums = ''.join(filter(str.isdigit, str(row['cnpj'])))
            
            # Formata CNPJ: 12.345.678/0001-99
            if len(cnpj_nums) == 14:
                cnpj = f"{cnpj_nums[:2]}.{cnpj_nums[2:5]}.{cnpj_nums[5:8]}/{cnpj_nums[8:12]}-{cnpj_nums[12:]}"
            else:
                cnpj = cnpj_nums # Fallback se vier incompleto

            # Patrimônio Líquido (as vezes vem na lista principal)
            net_worth = 0.0
            if 'patrimonioliquido' in row:
                try:
                    val = str(row['patrimonioliquido']).replace('.', '').replace(',', '.')
                    net_worth = float(val)
                except (ValueError, TypeError):
                    pass

            companies[ses_id] = {
                "cnpj": cnpj,
                "name": str(row['nomeentidade']).strip().title(),
                "net_worth": net_worth,
                "premiums": 0.0,
                "claims": 0.0
            }
        except Exception:
            continue

    print(f"SES: {len(companies)} empresas cadastradas.")

    # 2. Carrega Dados Financeiros (Prêmios e Sinistros)
    df_fin = _extract_zip_financials()
    
    if not df_fin.empty:
        df_fin.columns = [c.lower().strip() for c in df_fin.columns]
        
        # Filtro de Data (Últimos 12 meses)
        if 'damesano' in df_fin.columns:
            df_fin['date'] = pd.to_datetime(df_fin['damesano'].astype(str), format='%Y%m', errors='coerce')
            latest_date = df_fin['date'].max()
            if pd.notnull(latest_date):
                cutoff_date = latest_date - pd.DateOffset(months=12)
                print(f"SES: Filtrando financeiro: {cutoff_date.date()} -> {latest_date.date()}")
                df_fin = df_fin[df_fin['date'] > cutoff_date]

        # Tratamento de Nulos e Conversão Numérica
        cols_to_sum = ['premio_ganho', 'sinistro_corrido']
        for col in cols_to_sum:
            if col not in df_fin.columns:
                continue
            if df_fin[col].dtype == object:
                 df_fin[col] = df_fin[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df_fin[col] = pd.to_numeric(df_fin[col], errors='coerce').fillna(0.0)

        # Agregação por Empresa
        # coenti = Código FIP (chave de ligação)
        grouped = df_fin.groupby('coenti')[cols_to_sum].sum()
        
        print("SES: Atualizando métricas financeiras...")
        matched_count = 0
        for ses_id, row in grouped.iterrows():
            sid = str(ses_id).strip()
            if sid in companies:
                companies[sid]['premiums'] = float(row['premio_ganho'])
                companies[sid]['claims'] = float(row['sinistro_corrido'])
                matched_count += 1
        
        print(f"SES: Dados financeiros vinculados a {matched_count} empresas.")
    
    return SesMeta(), companies
