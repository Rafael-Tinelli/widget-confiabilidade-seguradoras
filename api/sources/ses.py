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
    seguros_file: str = "BaseCompleta.zip"

def _normalize_id(val) -> str:
    if pd.isna(val): return ""
    return str(val).split('.')[0].strip().lstrip('0')

def _parse_br_float(series: pd.Series) -> pd.Series:
    if series.dtype == object:
        return pd.to_numeric(
            series.astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False),
            errors='coerce'
        ).fillna(0.0)
    return pd.to_numeric(series, errors='coerce').fillna(0.0)

def _download_and_read_csv_list(url: str) -> pd.DataFrame:
    print(f"SES: Baixando {url}...")
    try:
        response = requests.get(url, headers=SES_HEADERS, verify=False, timeout=60)
        response.raise_for_status()
        content = response.content
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
    """Encontra a primeira coluna que contenha uma das palavras-chave."""
    for col in columns:
        for kw in keywords:
            if kw in col:
                return col
    return None

def extract_ses_master_and_financials():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    # --- 1. Cadastro ---
    df_cias = _download_and_read_csv_list(SES_LISTAEMPRESAS_URL)
    companies = {}
    
    if not df_cias.empty:
        df_cias.columns = [c.lower().strip() for c in df_cias.columns]
        
        col_id = next((c for c in df_cias.columns if 'cod' in c and ('fip' in c or 'ent' in c)), df_cias.columns[0])
        col_cnpj = next((c for c in df_cias.columns if 'cnpj' in c), df_cias.columns[1])
        col_nome = next((c for c in df_cias.columns if 'nome' in c or 'razao' in c), df_cias.columns[2])
        
        print("SES: Processando cadastro...")
        for _, row in df_cias.iterrows():
            try:
                sid = _normalize_id(row[col_id])
                if not sid: continue
                
                raw_cnpj = str(row[col_cnpj])
                cnpj_nums = ''.join(filter(str.isdigit, raw_cnpj))
                
                if len(cnpj_nums) == 14:
                    cnpj = f"{cnpj_nums[:2]}.{cnpj_nums[2:5]}.{cnpj_nums[5:8]}/{cnpj_nums[8:12]}-{cnpj_nums[12:]}"
                else:
                    cnpj = cnpj_nums

                companies[sid] = {
                    "cnpj": cnpj,
                    "name": str(row[col_nome]).strip().title(),
                    "net_worth": 0.0, # Será preenchido pelo arquivo de Balanço
                    "premiums": 0.0,
                    "claims": 0.0,
                    "sources_found": [] # Debug: saber de onde veio o dado
                }
            except Exception:
                continue
    
    print(f"SES: {len(companies)} empresas no cadastro base.")

    # --- 2. ZIP Financeiro (Multi-arquivo) ---
    print("SES: Baixando e processando ZIP Completo...")
    try:
        response = requests.get(SES_ZIP_URL, headers=SES_HEADERS, verify=False, timeout=180)
        response.raise_for_status()
        zip_bytes = io.BytesIO(response.content)
        
        with zipfile.ZipFile(zip_bytes) as z:
            all_files = [n for n in z.namelist()]
            
            # Mapeamento de arquivos para processar
            # (Palavra chave no nome do arquivo, Tipo de Dado)
            targets = [
                ('pl_margem', 'PATRIMONIO'), 
                ('balanco', 'PATRIMONIO'),
                ('ses_seguros', 'SEGUROS'),
                ('ses_eapc', 'PREVIDENCIA'),          # <-- AQUI ESTÃO GBOEX, EVIDENCE, BTG
                ('ses_capitalizacao', 'CAPITALIZACAO'), # <-- BRASILCAP, ETC
                ('ses_ressegurador', 'RESSEGURO')     # <-- AXA, IRB
            ]

            processed_files = set()

            for filename in all_files:
                filename_lower = filename.lower()
                
                # Identifica o tipo do arquivo
                file_type = None
                for keyword, ftype in targets:
                    if keyword in filename_lower and keyword not in processed_files:
                        file_type = ftype
                        # processed_files.add(keyword) # Não marca processado para permitir múltiplos arquivos se houver
                        break
                
                if not file_type:
                    continue

                print(f"SES: Processando {filename} como {file_type}...")
                
                with z.open(filename) as f:
                    # Lê cabeçalho
                    header = pd.read_csv(f, sep=';', encoding='latin1', nrows=0).columns.tolist()
                    header = [c.lower().strip() for c in header]
                    
                    c_id = next((c for c in header if 'coenti' in c), None)
                    c_data = next((c for c in header if 'damesano' in c), None)
                    
                    if not c_id:
                        continue

                    # Identifica colunas de VALOR baseado no tipo
                    c_receita = None
                    c_despesa = None
                    c_patrimonio = None

                    if file_type == 'PATRIMONIO':
                        c_patrimonio = _find_column_by_keyword(header, ['pla', 'patrimonio', 'liquido'])
                    
                    elif file_type == 'SEGUROS':
                        c_receita = _find_column_by_keyword(header, ['premio_ganho', 'premio_emitido', 'premios'])
                        c_despesa = _find_column_by_keyword(header, ['sinistro_corrido', 'sinistros', 'sinistro_retido'])

                    elif file_type == 'PREVIDENCIA':
                        # EAPC usa "Contribuição" e "Benefício"
                        c_receita = _find_column_by_keyword(header, ['contribuicao', 'arrecadacao', 'receita'])
                        c_despesa = _find_column_by_keyword(header, ['beneficio', 'resgate', 'despesa'])

                    elif file_type == 'CAPITALIZACAO':
                        c_receita = _find_column_by_keyword(header, ['arrecadacao', 'receita'])
                        c_despesa = _find_column_by_keyword(header, ['resgate', 'sorteio'])

                    elif file_type == 'RESSEGURO':
                        c_receita = _find_column_by_keyword(header, ['premio', 'receita'])
                        c_despesa = _find_column_by_keyword(header, ['sinistro', 'despesa'])

                    # Se não achou colunas vitais, pula
                    if not (c_patrimonio or (c_receita and c_despesa)):
                        continue

                    # Lê o arquivo
                    f.seek(0)
                    df = pd.read_csv(f, sep=';', encoding='latin1', on_bad_lines='skip')
                    df.columns = [c.lower().strip() for c in df.columns]
                    
                    # Filtra Data (Últimos 12 meses)
                    if c_data:
                        df['dt'] = pd.to_numeric(df[c_data], errors='coerce').fillna(0)
                        max_date = df['dt'].max()
                        if max_date > 0:
                            # Para patrimônio pega só o último mês. Para fluxo, pega 12 meses.
                            if file_type == 'PATRIMONIO':
                                df = df[df['dt'] == max_date]
                            else:
                                # Aproximação simplificada: pega tudo do arquivo se for recente
                                # ou implementa lógica de data se necessário. 
                                # Como o ZIP geralmente é o "base completa", vamos filtrar o último ano do arquivo
                                target_year = str(max_date)[:4]
                                df['year'] = df['dt'].astype(str).str[:4]
                                df = df[df['year'] == target_year]

                    df['sid'] = df[c_id].apply(_normalize_id)

                    # Agrega
                    count_upd = 0
                    for _, row in df.iterrows():
                        sid = row['sid']
                        if sid in companies:
                            updated = False
                            
                            if file_type == 'PATRIMONIO' and c_patrimonio:
                                val = _parse_br_float(pd.Series([row[c_patrimonio]]))[0]
                                if val > 0:
                                    companies[sid]['net_worth'] = val
                                    updated = True
                            
                            elif c_receita and c_despesa:
                                val_rec = _parse_br_float(pd.Series([row[c_receita]]))[0]
                                val_desp = _parse_br_float(pd.Series([row[c_despesa]]))[0]
                                if val_rec > 0 or val_desp > 0:
                                    companies[sid]['premiums'] += val_rec
                                    companies[sid]['claims'] += val_desp
                                    updated = True
                            
                            if updated:
                                companies[sid]['sources_found'].append(file_type)
                                count_upd += 1
                                
                    print(f"SES: {count_upd} empresas atualizadas com {file_type}.")

    except Exception as e:
        print(f"SES CRITICAL: Erro no ZIP: {e}")

    return SesMeta(), companies
