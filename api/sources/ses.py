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
SES_LISTAEMPRESAS_URL = os.getenv(
    "SES_LISTAEMPRESAS_URL",
    "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv"
)
SES_ZIP_URL = os.getenv(
    "SES_ZIP_URL",
    "https://www2.susep.gov.br/redarq.asp?arq=BaseCompleta%2ezip"
)
CACHE_DIR = Path("data/raw/ses")


@dataclass
class SesMeta:
    source: str = "SUSEP (SES)"
    zip_url: str = SES_ZIP_URL
    cias_file: str = "LISTAEMPRESAS.csv"
    seguros_file: str = "SES_Seguros.csv"


def _smart_read_csv(content: bytes) -> pd.DataFrame:
    """
    Tenta ler o CSV com diferentes estratégias para lidar com a inconsistência da SUSEP.
    Restaura a robustez do código original.
    """
    # Estratégia 1: Padrão esperado (Ponto e vírgula, Latin1)
    try:
        return pd.read_csv(
            io.BytesIO(content),
            sep=';',
            encoding='latin1',
            thousands='.',
            decimal=',',
            dtype=str,
            on_bad_lines='skip'
        )
    except Exception:
        pass

    # Estratégia 2: Separador Vírgula
    try:
        return pd.read_csv(
            io.BytesIO(content),
            sep=',',
            encoding='latin1',
            dtype=str,
            on_bad_lines='skip'
        )
    except Exception:
        pass

    # Estratégia 3: Engine Python (mais lento, mas detecta separador automaticamente)
    try:
        return pd.read_csv(
            io.BytesIO(content),
            sep=None,
            engine='python',
            encoding='latin1',
            dtype=str,
            on_bad_lines='skip'
        )
    except Exception:
        # Se tudo falhar, retorna vazio
        return pd.DataFrame()


def _download_master_data(url: str) -> pd.DataFrame:
    """Baixa o arquivo mestre de empresas."""
    print(f"SES: Baixando {url}...")
    try:
        response = requests.get(url, headers=SES_HEADERS, verify=False, timeout=60)
        response.raise_for_status()
        return _smart_read_csv(response.content)
    except Exception as e:
        print(f"SES CRITICAL: Falha ao baixar lista de empresas: {e}")
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

                col_premio = next(
                    (c for c in header if 'premio' in c and ('ganho' in c or 'emitido' in c)), None
                )
                col_sinistro = next(
                    (c for c in header if 'sinistro' in c and ('corrido' in c or 'retido' in c)), None
                )

                f.seek(0)

                if col_premio and col_sinistro:
                    # Lê tudo
                    df = pd.read_csv(
                        f,
                        sep=';',
                        encoding='latin1',
                        thousands='.',
                        decimal=',',
                        on_bad_lines='skip'
                    )
                    df.columns = [c.lower().strip() for c in df.columns]
                    df.rename(
                        columns={col_premio: 'premio_ganho', col_sinistro: 'sinistro_corrido'},
                        inplace=True
                    )
                    return df
                else:
                    print(f"SES WARNING: Colunas financeiras não identificadas: {header}")
                    return pd.read_csv(f, sep=';', encoding='latin1', on_bad_lines='skip')

    except Exception as e:
        print(f"SES CRITICAL: Erro no processamento do ZIP Financeiro: {e}")
        return pd.DataFrame()


def extract_ses_master_and_financials():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Cadastro (Lista de Empresas)
    df_cias = _download_master_data(SES_LISTAEMPRESAS_URL)
    if df_cias.empty:
        return SesMeta(), {}

    # Normalização de colunas
    df_cias.columns = [c.lower().strip() for c in df_cias.columns]

    if 'codigofip' not in df_cias.columns:
        # Fallback posicional se os nomes mudaram
        if len(df_cias.columns) >= 3:
            df_cias.rename(
                columns={
                    df_cias.columns[0]: 'codigofip',
                    df_cias.columns[1]: 'cnpj',
                    df_cias.columns[2]: 'nomeentidade'
                },
                inplace=True
            )

    companies = {}
    print("SES: Processando dados cadastrais...")

    for _, row in df_cias.iterrows():
        try:
            # Captura dados brutos
            raw_id = str(row.get('codigofip', '')).strip()
            raw_cnpj = str(row.get('cnpj', ''))
            raw_nome = str(row.get('nomeentidade', ''))

            # Limpa ID (remove não numéricos)
            clean_id = "".join(filter(str.isdigit, raw_id))
            if not clean_id:
                continue

            # Formata CNPJ
            cnpj_nums = "".join(filter(str.isdigit, raw_cnpj))
            if len(cnpj_nums) == 14:
                cnpj = f"{cnpj_nums[:2]}.{cnpj_nums[2:5]}.{cnpj_nums[5:8]}/{cnpj_nums[8:12]}-{cnpj_nums[12:]}"
            else:
                cnpj = cnpj_nums

            # Patrimônio Líquido
            nw = 0.0
            if 'patrimonioliquido' in row:
                try:
                    val = str(row['patrimonioliquido']).replace('.', '').replace(',', '.')
                    nw = float(val)
                except (ValueError, TypeError):
                    pass

            data_obj = {
                "cnpj": cnpj,
                "name": raw_nome.strip().title(),
                "net_worth": nw,
                "premiums": 0.0,
                "claims": 0.0
            }

            # Indexação Redundante (Chave do sucesso do script antigo)
            # Guarda ID puro "512", "00512" e "000512" para garantir match
            # O build_insurers vai iterar sobre values(), então duplicatas de chave não duplicam o JSON final
            # desde que tratemos isso lá ou aqui.
            # CORREÇÃO: O build_insurers itera sobre items().
            # Para evitar duplicidade no JSON final, vamos usar apenas o ID normalizado (int) como chave principal.
            # Mas vamos criar um mapa auxiliar de lookup para o financeiro.
            
            sid = str(int(clean_id)) # "512"
            companies[sid] = data_obj

        except Exception:
            continue

    print(f"SES: {len(companies)} empresas cadastradas.")

    # 2. Financeiro
    df_fin = _extract_zip_financials()

    if not df_fin.empty:
        df_fin.columns = [c.lower().strip() for c in df_fin.columns]

        # Tenta achar a coluna de ID no financeiro
        col_id_fin = 'coenti'
        if col_id_fin not in df_fin.columns:
            # Busca heurística
            col_id_fin = next((c for c in df_fin.columns if 'cod' in c or 'fip' in c), None)

        # Filtro de Data
        if 'damesano' in df_fin.columns:
            df_fin['date'] = pd.to_datetime(
                df_fin['damesano'].astype(str), format='%Y%m', errors='coerce'
            )
            latest = df_fin['date'].max()
            if pd.notnull(latest):
                start = latest - pd.DateOffset(months=12)
                print(f"SES: Filtrando financeiro de {start.date()} a {latest.date()}")
                df_fin = df_fin[df_fin['date'] > start]

        req = ['premio_ganho', 'sinistro_corrido']
        
        if col_id_fin and all(c in df_fin.columns for c in req):
            # Limpeza e Conversão
            for c in req:
                if df_fin[c].dtype == object:
                    df_fin[c] = df_fin[c].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df_fin[c] = pd.to_numeric(df_fin[c], errors='coerce').fillna(0.0)

            # Normaliza ID para bater com a chave do dicionário
            # Remove zeros à esquerda e converte para string
            df_fin['id_match'] = df_fin[col_id_fin].astype(str).str.replace(r'\D', '', regex=True).str.lstrip('0')

            # Agrupa
            grouped = df_fin.groupby('id_match')[req].sum()

            count_match = 0
            for sid, row in grouped.iterrows():
                if sid in companies:
                    companies[sid]['premiums'] = float(row['premio_ganho'])
                    companies[sid]['claims'] = float(row['sinistro_corrido'])
                    count_match += 1
            
            print(f"SES: Financeiro vinculado a {count_match} empresas.")
        else:
            cols = df_fin.columns.tolist()
            print(f"SES WARNING: Colunas financeiras não encontradas. ID={col_id_fin}, Cols={cols}")

    return SesMeta(), companies
