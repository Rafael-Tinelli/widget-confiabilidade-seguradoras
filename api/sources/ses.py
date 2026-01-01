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
    """Remove zeros à esquerda e espaços para garantir match de IDs."""
    if pd.isna(val):
        return ""
    # Converte 512.0 -> 512 -> "512"
    s = str(val).split('.')[0].strip()
    return s.lstrip('0')

def _parse_br_float(series: pd.Series) -> pd.Series:
    """Converte formato brasileiro (1.000,00) para float."""
    if series.dtype == object:
        return pd.to_numeric(
            series.astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False),
            errors='coerce'
        ).fillna(0.0)
    return pd.to_numeric(series, errors='coerce').fillna(0.0)

def _download_and_read_csv_list(url: str) -> pd.DataFrame:
    """Lê a lista de empresas com múltiplas tentativas de encoding/separador."""
    print(f"SES: Baixando {url}...")
    try:
        response = requests.get(url, headers=SES_HEADERS, verify=False, timeout=60)
        response.raise_for_status()
        content = response.content
        
        # Tentativa 1: Padrão SUSEP (Ponto e virgula, Latin1)
        try:
            return pd.read_csv(io.BytesIO(content), sep=';', encoding='latin1', dtype=str, on_bad_lines='skip')
        except Exception:
            pass
            
        # Tentativa 2: Vírgula
        try:
            return pd.read_csv(io.BytesIO(content), sep=',', encoding='latin1', dtype=str, on_bad_lines='skip')
        except Exception:
            pass
            
        return pd.DataFrame()
    except Exception as e:
        print(f"SES CRITICAL: Falha lista empresas: {e}")
        return pd.DataFrame()

def extract_ses_master_and_financials():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    # --- 1. Cadastro ---
    df_cias = _download_and_read_csv_list(SES_LISTAEMPRESAS_URL)
    companies = {}
    
    if not df_cias.empty:
        df_cias.columns = [c.lower().strip() for c in df_cias.columns]
        
        # Mapeamento dinâmico de colunas
        col_id = next((c for c in df_cias.columns if 'cod' in c and ('fip' in c or 'ent' in c)), df_cias.columns[0])
        col_cnpj = next((c for c in df_cias.columns if 'cnpj' in c), df_cias.columns[1])
        col_nome = next((c for c in df_cias.columns if 'nome' in c or 'razao' in c), df_cias.columns[2])
        
        print("SES: Processando cadastro...")
        for _, row in df_cias.iterrows():
            try:
                sid = _normalize_id(row[col_id])
                if not sid:
                    continue
                
                # CNPJ Limpo
                raw_cnpj = str(row[col_cnpj])
                cnpj_nums = ''.join(filter(str.isdigit, raw_cnpj))
                
                if len(cnpj_nums) == 14:
                    cnpj = f"{cnpj_nums[:2]}.{cnpj_nums[2:5]}.{cnpj_nums[5:8]}/{cnpj_nums[8:12]}-{cnpj_nums[12:]}"
                else:
                    cnpj = cnpj_nums # Fallback

                # Tenta pegar PL se estiver na lista (backup)
                nw = 0.0
                if 'patrimonioliquido' in row:
                    try:
                        nw = float(str(row['patrimonioliquido']).replace('.', '').replace(',', '.'))
                    except (ValueError, TypeError):
                        pass

                companies[sid] = {
                    "cnpj": cnpj,
                    "name": str(row[col_nome]).strip().title(),
                    "net_worth": nw,
                    "premiums": 0.0,
                    "claims": 0.0
                }
            except Exception:
                continue
    
    print(f"SES: {len(companies)} empresas no cadastro base.")

    # --- 2. ZIP Financeiro (Agora lendo PL E Seguros) ---
    print("SES: Baixando e processando ZIP Completo...")
    try:
        response = requests.get(SES_ZIP_URL, headers=SES_HEADERS, verify=False, timeout=180)
        response.raise_for_status()
        zip_bytes = io.BytesIO(response.content)
        
        with zipfile.ZipFile(zip_bytes) as z:
            all_files = [n.lower() for n in z.namelist()]
            
            # A. Processa Patrimônio Líquido (CRUCIAL PARA O SCORE NÃO SER 20)
            file_pl = next((n for n in all_files if 'pl_margem' in n or 'balanco' in n), None)
            if file_pl:
                print(f"SES: Lendo Patrimônio Líquido de {file_pl}...")
                with z.open(z.namelist()[all_files.index(file_pl)]) as f:
                    # Lê cabeçalho
                    header = pd.read_csv(f, sep=';', encoding='latin1', nrows=0).columns.tolist()
                    header = [c.lower().strip() for c in header]
                    
                    # Colunas
                    c_id = next((c for c in header if 'coenti' in c), None)
                    c_data = next((c for c in header if 'damesano' in c), None)
                    c_pl = next((c for c in header if 'pla' in c or 'patrimonio' in c), None)
                    
                    if c_id and c_pl:
                        f.seek(0)
                        df_pl = pd.read_csv(f, sep=';', encoding='latin1', on_bad_lines='skip')
                        df_pl.columns = [c.lower().strip() for c in df_pl.columns]
                        
                        # Filtra data recente se possível
                        if c_data:
                            df_pl['dt'] = pd.to_numeric(df_pl[c_data], errors='coerce').fillna(0)
                            max_date = df_pl['dt'].max()
                            if max_date > 0:
                                df_pl = df_pl[df_pl['dt'] == max_date]
                        
                        # Atualiza companies
                        df_pl['sid'] = df_pl[c_id].apply(_normalize_id)
                        df_pl['val_pl'] = _parse_br_float(df_pl[c_pl])
                        
                        count_pl = 0
                        for _, row in df_pl.iterrows():
                            if row['sid'] in companies:
                                # Prioriza valor do arquivo de balanço sobre a lista
                                if row['val_pl'] > 0:
                                    companies[row['sid']]['net_worth'] = row['val_pl']
                                    count_pl += 1
                        print(f"SES: Patrimônio Líquido atualizado para {count_pl} empresas.")

            # B. Processa Seguros (Prêmios e Sinistros)
            file_seg = next((n for n in all_files if 'ses_seguros' in n), None)
            if file_seg:
                print(f"SES: Lendo Operações de {file_seg}...")
                with z.open(z.namelist()[all_files.index(file_seg)]) as f:
                    header = pd.read_csv(f, sep=';', encoding='latin1', nrows=0).columns.tolist()
                    header = [c.lower().strip() for c in header]
                    
                    c_id = next((c for c in header if 'coenti' in c), None)
                    c_prem = next((c for c in header if 'premio' in c and ('ganho' in c or 'emitido' in c)), None)
                    c_sin = next((c for c in header if 'sinistro' in c and ('corrido' in c or 'retido' in c)), None)
                    c_data = next((c for c in header if 'damesano' in c), None)
                    
                    if c_id and c_prem and c_sin:
                        f.seek(0)
                        df_seg = pd.read_csv(f, sep=';', encoding='latin1', on_bad_lines='skip')
                        df_seg.columns = [c.lower().strip() for c in df_seg.columns]
                        
                        # Filtro 12 meses
                        if c_data:
                            df_seg['dt'] = pd.to_datetime(df_seg[c_data].astype(str), format='%Y%m', errors='coerce')
                            latest = df_seg['dt'].max()
                            if pd.notnull(latest):
                                start = latest - pd.DateOffset(months=12)
                                df_seg = df_seg[df_seg['dt'] > start]
                        
                        # Agrega
                        df_seg['sid'] = df_seg[c_id].apply(_normalize_id)
                        df_seg['v_prem'] = _parse_br_float(df_seg[c_prem])
                        df_seg['v_sin'] = _parse_br_float(df_seg[c_sin])
                        
                        grouped = df_seg.groupby('sid')[['v_prem', 'v_sin']].sum()
                        
                        count_fin = 0
                        for sid, row in grouped.iterrows():
                            if sid in companies:
                                companies[sid]['premiums'] = float(row['v_prem'])
                                companies[sid]['claims'] = float(row['v_sin'])
                                count_fin += 1
                        print(f"SES: Financeiro (Prêmio/Sinistro) vinculado a {count_fin} empresas.")

    except Exception as e:
        print(f"SES CRITICAL: Erro no ZIP: {e}")

    return SesMeta(), companies
