# api/sources/ses.py
# api/sources/ses.py
from __future__ import annotations

import io
import os
import zipfile
import requests
import pandas as pd
from dataclasses import dataclass
from pathlib import Path

# Headers
SES_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

# URLs
SES_LISTAEMPRESAS_URL = os.getenv("SES_LISTAEMPRESAS_URL", "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv")
SES_ZIP_URL = os.getenv("SES_ZIP_URL", "https://www2.susep.gov.br/redarq.asp?arq=BaseCompleta%2ezip")
CACHE_DIR = Path("data/raw/ses")

@dataclass
class SesMeta:
    source: str = "SUSEP (SES)"
    zip_url: str = SES_ZIP_URL
    cias_file: str = "LISTAEMPRESAS.csv"
    seguros_file: str = "SES_Seguros.csv"

def _normalize_id(val) -> str:
    """Remove zeros à esquerda e espaços para garantir match de IDs (ex: '00512' -> '512')."""
    if pd.isna(val):
        return ""
    return str(val).strip().lstrip('0')

def _download_and_read_csv(url: str, separator: str = ';') -> pd.DataFrame:
    """Baixa e lê CSV com robustez."""
    print(f"SES: Baixando {url}...")
    try:
        response = requests.get(url, headers=SES_HEADERS, verify=False, timeout=60)
        response.raise_for_status()
        
        df = pd.read_csv(
            io.BytesIO(response.content),
            sep=separator,
            encoding='latin1',
            thousands='.',
            decimal=',',
            on_bad_lines='skip',
            dtype=str
        )
        return df
    except Exception as e:
        print(f"SES CRITICAL: Falha ao baixar/ler {url}: {e}")
        return pd.DataFrame()

def _extract_zip_financials() -> pd.DataFrame:
    """Extrai financeiro com busca dinâmica de colunas."""
    print("SES: Baixando Base Completa (ZIP)...")
    try:
        response = requests.get(SES_ZIP_URL, headers=SES_HEADERS, verify=False, timeout=180)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_name = next((n for n in z.namelist() if 'ses_seguros' in n.lower()), None)
            
            if not csv_name:
                print("SES WARNING: Arquivo de seguros não encontrado no ZIP.")
                return pd.DataFrame()

            print(f"SES: Extraindo {csv_name}...")
            with z.open(csv_name) as f:
                # Lê cabeçalho para identificar colunas
                header = pd.read_csv(f, sep=';', encoding='latin1', nrows=0).columns.tolist()
                header = [c.lower().strip() for c in header]
                
                col_premio = next((c for c in header if 'premio' in c and ('ganho' in c or 'emitido' in c)), None)
                col_sinistro = next((c for c in header if 'sinistro' in c and ('corrido' in c or 'retido' in c)), None)
                
                f.seek(0)
                
                if col_premio and col_sinistro:
                    # Lê tudo
                    df = pd.read_csv(
                        f, sep=';', encoding='latin1', thousands='.', decimal=',', on_bad_lines='skip'
                    )
                    df.columns = [c.lower().strip() for c in df.columns]
                    df.rename(columns={col_premio: 'premio_ganho', col_sinistro: 'sinistro_corrido'}, inplace=True)
                    return df
                else:
                    print(f"SES WARNING: Colunas financeiras não identificadas: {header}")
                    return pd.read_csv(f, sep=';', encoding='latin1', on_bad_lines='skip')

    except Exception as e:
        print(f"SES CRITICAL: Erro no processamento do ZIP Financeiro: {e}")
        return pd.DataFrame()

def extract_ses_master_and_financials():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Cadastro
    df_cias = _download_and_read_csv(SES_LISTAEMPRESAS_URL)
    if df_cias.empty:
        return SesMeta(), {}

    df_cias.columns = [c.lower().strip() for c in df_cias.columns]
    
    if 'codigofip' not in df_cias.columns:
        # Tenta fallback por posição
        df_cias.rename(columns={df_cias.columns[0]: 'codigofip', df_cias.columns[1]: 'cnpj', df_cias.columns[2]: 'nomeentidade'}, inplace=True)

    companies = {}
    print("SES: Processando dados cadastrais...")
    
    for _, row in df_cias.iterrows():
        try:
            # Normaliza ID para chave do dicionário (remove zeros à esquerda)
            sid = _normalize_id(row['codigofip'])
            if not sid:
                continue

            cnpj_nums = ''.join(filter(str.isdigit, str(row['cnpj'])))
            if len(cnpj_nums) == 14:
                cnpj = f"{cnpj_nums[:2]}.{cnpj_nums[2:5]}.{cnpj_nums[5:8]}/{cnpj_nums[8:12]}-{cnpj_nums[12:]}"
            else:
                cnpj = cnpj_nums

            nw = 0.0
            if 'patrimonioliquido' in row:
                try:
                    nw = float(str(row['patrimonioliquido']).replace('.', '').replace(',', '.'))
                except (ValueError, TypeError):
                    pass

            companies[sid] = {
                "cnpj": cnpj,
                "name": str(row['nomeentidade']).strip().title(),
                "net_worth": nw,
                "premiums": 0.0,
                "claims": 0.0
            }
        except Exception:
            continue

    print(f"SES: {len(companies)} empresas cadastradas.")

    # 2. Financeiro
    df_fin = _extract_zip_financials()
    
    if not df_fin.empty:
        df_fin.columns = [c.lower().strip() for c in df_fin.columns]
        
        # Garante que temos 'coenti' (Código FIP)
        if 'coenti' not in df_fin.columns:
             col_id = next((c for c in df_fin.columns if 'cod' in c or 'fip' in c or 'enti' in c), None)
             if col_id:
                 df_fin.rename(columns={col_id: 'coenti'}, inplace=True)

        # Filtro de Data
        if 'damesano' in df_fin.columns:
            df_fin['date'] = pd.to_datetime(df_fin['damesano'].astype(str), format='%Y%m', errors='coerce')
            latest = df_fin['date'].max()
            if pd.notnull(latest):
                start = latest - pd.DateOffset(months=12)
                print(f"SES: Filtrando financeiro de {start.date()} a {latest.date()}")
                df_fin = df_fin[df_fin['date'] > start]

        req = ['premio_ganho', 'sinistro_corrido']
        if 'coenti' in df_fin.columns and all(c in df_fin.columns for c in req):
            # Normaliza ID no DataFrame para bater com o dicionário
            df_fin['coenti_norm'] = df_fin['coenti'].apply(_normalize_id)
            
            for c in req:
                if df_fin[c].dtype == object:
                    df_fin[c] = df_fin[c].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df_fin[c] = pd.to_numeric(df_fin[c], errors='coerce').fillna(0.0)

            # Agrupa pelo ID normalizado
            grouped = df_fin.groupby('coenti_norm')[req].sum()
            
            count = 0
            for sid_norm, row in grouped.iterrows():
                # A chave do dicionário companies já está normalizada (sem zeros à esquerda)
                if sid_norm in companies:
                    companies[sid_norm]['premiums'] = float(row['premio_ganho'])
                    companies[sid_norm]['claims'] = float(row['sinistro_corrido'])
                    count += 1
            print(f"SES: Financeiro vinculado a {count} empresas.")
        else:
            print(f"SES WARNING: Colunas necessárias não encontradas. Cols: {df_fin.columns.tolist()}")

    return SesMeta(), companies
