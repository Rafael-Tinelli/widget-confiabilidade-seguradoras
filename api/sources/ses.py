# api/sources/ses.py
from __future__ import annotations

import io
import os
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import requests

# Headers para evitar bloqueio da SUSEP
SES_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

# URLs Oficiais
SES_LISTAEMPRESAS_URL = os.getenv(
    "SES_LISTAEMPRESAS_URL",
    "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv",
)

# Recomendo fortemente usar o ZIP estável direto (evergreen).
# Ainda é sobrescrevível por env var.
SES_ZIP_URL = os.getenv(
    "SES_ZIP_URL",
    "https://www2.susep.gov.br/download/estatisticas/BaseCompleta.zip",
)

CACHE_DIR = Path("data/raw/ses")

# Mantém compatibilidade com o seu comportamento atual (verify=False),
# mas permite ativar SSL verification quando quiser:
# export SES_VERIFY_SSL=1
VERIFY_SSL = os.getenv("SES_VERIFY_SSL", "0") == "1"

_DIGITS_RE = re.compile(r"\d+")


@dataclass(frozen=True)
class SesMeta:
    source: str = "SUSEP (SES)"
    zip_url: str = SES_ZIP_URL
    cias_file: str = "LISTAEMPRESAS.csv"
    seguros_file: str = "BaseCompleta.zip"


def _norm_col(name: str) -> str:
    return str(name).strip().lower()


def _canonical_ses_id(raw) -> str:
    """
    Canonicaliza ID SES para 6 dígitos.
    Ex.: 'ses:01007' -> '001007', '1007' -> '001007', 1007.0 -> '001007'
    """
    if raw is None:
        return ""
    try:
        if pd.isna(raw):
            return ""
    except Exception:
        pass

    s = str(raw).strip().lower()
    if not s:
        return ""

    s = s.replace("ses:", "").replace("susep:", "")
    s = re.sub(r"\.0+$", "", s)  # remove padrão "1007.0"

    digits = "".join(_DIGITS_RE.findall(s))
    if not digits:
        return ""

    return digits.zfill(6)


def _canonical_ses_id_series(series: pd.Series) -> pd.Series:
    """
    Canonicalização vetorizada (rápida). Mantém "" para vazios.
    """
    s = series.astype(str).str.strip().str.lower()
    s = s.str.replace("ses:", "", regex=False).str.replace("susep:", "", regex=False)
    s = s.str.replace(r"\.0+$", "", regex=True)
    digits = s.str.replace(r"\D+", "", regex=True)

    out = pd.Series([""] * len(digits), index=digits.index, dtype="object")
    m = digits.str.len().fillna(0).astype(int) > 0
    out.loc[m] = digits.loc[m].str.zfill(6)
    return out


def _parse_br_float(series: pd.Series) -> pd.Series:
    """
    Converte strings numéricas pt-BR ('1.234,56') para float (1234.56), vetorizado.
    """
    s = series.astype(str).str.strip()
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _download_bytes(url: str, timeout: int) -> bytes:
    resp = requests.get(url, headers=SES_HEADERS, verify=VERIFY_SSL, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def _read_csv_bytes(content: bytes) -> pd.DataFrame:
    """
    Tenta ler CSV em ; e depois em , mantendo dtype=str.
    """
    bio = io.BytesIO(content)
    try:
        return pd.read_csv(bio, sep=";", encoding="latin1", dtype=str, on_bad_lines="skip")
    except Exception:
        pass

    bio = io.BytesIO(content)
    try:
        return pd.read_csv(bio, sep=",", encoding="latin1", dtype=str, on_bad_lines="skip")
    except Exception:
        return pd.DataFrame()


def _pick_col(cols_norm: list, candidates: list) -> Optional[str]:
    """
    Retorna o nome NORMALIZADO (lower/strip) da coluna cujo texto contém um candidato.
    """
    for cand in candidates:
        cand = cand.lower()
        for c in cols_norm:
            if cand in c:
                return c
    return None


def _detect_sep_and_columns(
    z: zipfile.ZipFile, filename: str
) -> Tuple[str, list, list, Dict[str, str]]:
    """
    Lê header de forma segura, preservando nomes originais e criando map norm->orig.
    Retorna: sep, cols_orig, cols_norm, norm2orig
    """
    for sep in (";", ","):
        try:
            with z.open(filename) as f:
                hdr = pd.read_csv(f, sep=sep, encoding="latin1", nrows=0)
            cols_orig = list(hdr.columns)
            if not cols_orig:
                continue
            cols_norm = [_norm_col(c) for c in cols_orig]
            norm2orig = {n: o for n, o in zip(cols_norm, cols_orig)}
            return sep, cols_orig, cols_norm, norm2orig
        except Exception:
            continue
    return ";", [], [], {}


def _compute_max_date_in_zipfile(
    z: zipfile.ZipFile,
    filename: str,
    sep: str,
    c_data_orig: str,
) -> int:
    """
    Faz streaming somente da coluna de data (nome ORIGINAL) para obter o max.
    Retorna 0 se não encontrado.
    """
    max_date = 0
    with z.open(filename) as f:
        for chunk in pd.read_csv(
            f,
            sep=sep,
            encoding="latin1",
            dtype=str,
            usecols=[c_data_orig],
            on_bad_lines="skip",
            chunksize=300_000,
        ):
            # coluna vem com nome original; não renomeamos aqui
            d = pd.to_numeric(chunk[c_data_orig], errors="coerce").fillna(0).astype(int)
            if not d.empty:
                m = int(d.max())
                if m > max_date:
                    max_date = m
    return max_date


def extract_ses_master_and_financials():
    """
    Pipeline:
    1) baixa master (LISTAEMPRESAS.csv) e monta universo companies[SID]
    2) processa BaseCompleta.zip em streaming:
       - detecta colunas (com map norm->orig)
       - calcula max_date (quando aplicável)
       - aplica filtro temporal
       - canonicaliza sid
       - Universe Lockdown: sid precisa existir em companies
       - agrega por sid e atualiza companies
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # --- 1) MASTER LIST ---
    print(f"SES: Baixando master list: {SES_LISTAEMPRESAS_URL}")
    try:
        df_cias = _read_csv_bytes(_download_bytes(SES_LISTAEMPRESAS_URL, timeout=90))
    except Exception as e:
        print(f"SES CRITICAL: Falha ao baixar master list: {e}")
        df_cias = pd.DataFrame()

    companies: Dict[str, dict] = {}
    collisions = 0

    if not df_cias.empty:
        df_cias.columns = [_norm_col(c) for c in df_cias.columns]

        col_id = _pick_col(df_cias.columns.tolist(), ["codfip", "coenti", "cod_enti", "cod"])
        col_cnpj = _pick_col(df_cias.columns.tolist(), ["cnpj"])
        col_nome = _pick_col(df_cias.columns.tolist(), ["razao", "nome"])

        if not (col_id and col_cnpj and col_nome):
            print("SES CRITICAL: Master list sem colunas esperadas (id/cnpj/nome).")
        else:
            for _, row in df_cias.iterrows():
                sid = _canonical_ses_id(row.get(col_id))
                if not sid:
                    continue

                if sid in companies:
                    collisions += 1  # não altera universo; apenas indicador
                    # overwrite é aceitável; mantemos o último registro lido
                raw_cnpj = str(row.get(col_cnpj, "")).strip()
                cnpj_nums = "".join(ch for ch in raw_cnpj if ch.isdigit())
                if len(cnpj_nums) == 14:
                    cnpj = (
                        f"{cnpj_nums[:2]}.{cnpj_nums[2:5]}.{cnpj_nums[5:8]}/"
                        f"{cnpj_nums[8:12]}-{cnpj_nums[12:]}"
                    )
                else:
                    cnpj = cnpj_nums

                companies[sid] = {
                    "id": sid,
                    "cnpj": cnpj,
                    "name": str(row.get(col_nome, "")).strip().title(),
                    "net_worth": 0.0,
                    "premiums": 0.0,
                    "claims": 0.0,
                    "sources_found": [],
                }

    master_count = len(companies)
    print(f"SES: Universo Travado: {master_count} empresas (master list).")
    if collisions:
        print(f"SES WARN: {collisions} colisões de SID detectadas na master list (mesmo SID repetido).")

    # --- 2) ZIP ---
    print(f"SES: Baixando ZIP: {SES_ZIP_URL}")
    try:
        zip_content = _download_bytes(SES_ZIP_URL, timeout=240)
    except Exception as e:
        print(f"SES CRITICAL: Falha ao baixar ZIP: {e}")
        return SesMeta(), companies

    # Sanity check: evita BadZipFile quando a resposta é HTML/erro
    if not zip_content.startswith(b"PK"):
        print("SES CRITICAL: Conteúdo baixado não parece ZIP (assinatura 'PK' ausente).")
        return SesMeta(), companies

    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            all_files = z.namelist()

            targets = [
                ("pl_margem", "PATRIMONIO"),
                ("balanco", "PATRIMONIO"),
                ("ses_seguros", "SEGUROS"),
                ("contrib_benef", "PREVIDENCIA"),
                ("ses_dados_cap", "CAPITALIZACAO"),
                ("ses_cessoes_recebidas", "RESSEGURO"),
            ]

            for filename in all_files:
                filename_l = filename.lower()
                file_type = None
                for key, ftype in targets:
                    if key in filename_l:
                        file_type = ftype
                        break
                if not file_type:
                    continue

                sep, cols_orig, cols_norm, norm2orig = _detect_sep_and_columns(z, filename)
                if not cols_orig:
                    print(f"SES SKIP: {filename} sem header legível.")
                    continue

                # detecta colunas por nomes NORMALIZADOS
                c_id_norm = _pick_col(cols_norm, ["coenti"])
                c_data_norm = _pick_col(cols_norm, ["damesano"])

                if not c_id_norm:
                    print(f"SES SKIP: {filename} sem coluna de ID (coenti).")
                    continue

                # colunas de valor (NORM)
                c_receita_norm = None
                c_despesa_norm = None
                c_patrimonio_norm = None

                if file_type == "PATRIMONIO":
                    c_patrimonio_norm = _pick_col(cols_norm, ["patrimonio", "pla", "liquido"])
                elif file_type == "SEGUROS":
                    c_receita_norm = _pick_col(cols_norm, ["premio_ganho", "premio_emitido", "premios"])
                    c_despesa_norm = _pick_col(cols_norm, ["sinistro_corrido", "sinistros"])
                elif file_type == "PREVIDENCIA":
                    c_receita_norm = _pick_col(cols_norm, ["contrib", "arrecadacao"])
                    c_despesa_norm = _pick_col(cols_norm, ["benef", "resgate"])
                elif file_type == "CAPITALIZACAO":
                    c_receita_norm = _pick_col(cols_norm, ["arrecadacao", "receita"])
                    c_despesa_norm = _pick_col(cols_norm, ["resgate"])
                elif file_type == "RESSEGURO":
                    c_receita_norm = _pick_col(cols_norm, ["cessao", "premio_aceito", "receita"])
                    c_despesa_norm = _pick_col(cols_norm, ["recuperacao", "sinistro_pago", "despesa"])

                if not (c_patrimonio_norm or (c_receita_norm and c_despesa_norm)):
                    print(f"SES SKIP: {filename} sem colunas de valor reconhecíveis.")
                    continue

                # mapeia NORM -> ORIG para usecols (Pandas exige nomes originais)
                c_id_orig = norm2orig[c_id_norm]
                c_data_orig = norm2orig[c_data_norm] if c_data_norm else None
                c_patrimonio_orig = norm2orig[c_patrimonio_norm] if c_patrimonio_norm else None
                c_receita_orig = norm2orig[c_receita_norm] if c_receita_norm else None
                c_despesa_orig = norm2orig[c_despesa_norm] if c_despesa_norm else None

                print(f"SES: Processando {filename} ({file_type})...")

                # max_date via streaming (coluna ORIGINAL)
                max_date = 0
                if c_data_orig:
                    try:
                        max_date = _compute_max_date_in_zipfile(z, filename, sep, c_data_orig)
                    except Exception as e:
                        print(f"SES WARN: Falha ao calcular max_date em {filename}: {e}")
                        max_date = 0

                # usecols precisa ser ORIGINAL
                usecols = [c_id_orig]
                if c_data_orig:
                    usecols.append(c_data_orig)
                if c_patrimonio_orig:
                    usecols.append(c_patrimonio_orig)
                if c_receita_orig:
                    usecols.append(c_receita_orig)
                if c_despesa_orig:
                    usecols.append(c_despesa_orig)

                net_worth_upd: Dict[str, float] = {}
                prem_upd: Dict[str, float] = {}
                clm_upd: Dict[str, float] = {}

                with z.open(filename) as f:
                    for chunk in pd.read_csv(
                        f,
                        sep=sep,
                        encoding="latin1",
                        dtype=str,
                        usecols=usecols,
                        on_bad_lines="skip",
                        chunksize=300_000,
                    ):
                        # renomeia tudo para NORM para operar por chaves consistentes
                        chunk.columns = [_norm_col(c) for c in chunk.columns]

                        # filtro temporal (preserva sua intenção)
                        if c_data_norm and max_date > 0 and c_data_norm in chunk.columns:
                            dates_num = pd.to_numeric(chunk[c_data_norm], errors="coerce").fillna(0).astype(int)
                            if file_type == "PATRIMONIO":
                                chunk = chunk[dates_num == int(max_date)]
                            else:
                                target_year = str(int(max_date))[:4]
                                chunk = chunk[dates_num.astype(str).str.startswith(target_year)]
                            if chunk.empty:
                                continue

                        # canonicaliza sid e aplica LOCKDOWN real
                        sid = _canonical_ses_id_series(chunk[c_id_norm])
                        chunk = chunk.assign(sid=sid)
                        chunk = chunk[(chunk["sid"] != "") & (chunk["sid"].isin(companies))]
                        if chunk.empty:
                            continue

                        if file_type == "PATRIMONIO" and c_patrimonio_norm:
                            vals = _parse_br_float(chunk[c_patrimonio_norm])
                            g = vals.groupby(chunk["sid"]).max()
                            for k, v in g.items():
                                if v > 0:
                                    net_worth_upd[k] = max(net_worth_upd.get(k, 0.0), float(v))
                        else:
                            r = _parse_br_float(chunk[c_receita_norm]) if c_receita_norm else pd.Series(0.0, index=chunk.index)
                            d = _parse_br_float(chunk[c_despesa_norm]) if c_despesa_norm else pd.Series(0.0, index=chunk.index)
                            gr = r.groupby(chunk["sid"]).sum()
                            gd = d.groupby(chunk["sid"]).sum()

                            for k, v in gr.items():
                                if v != 0:
                                    prem_upd[k] = prem_upd.get(k, 0.0) + float(v)
                            for k, v in gd.items():
                                if v != 0:
                                    clm_upd[k] = clm_upd.get(k, 0.0) + float(v)

                # aplica updates no universo
                updated_any = 0

                for k, v in net_worth_upd.items():
                    companies[k]["net_worth"] = float(v)
                    if file_type not in companies[k]["sources_found"]:
                        companies[k]["sources_found"].append(file_type)
                    updated_any += 1

                for k, v in prem_upd.items():
                    companies[k]["premiums"] += float(v)
                    if file_type not in companies[k]["sources_found"]:
                        companies[k]["sources_found"].append(file_type)

                for k, v in clm_upd.items():
                    companies[k]["claims"] += float(v)
                    if file_type not in companies[k]["sources_found"]:
                        companies[k]["sources_found"].append(file_type)

                if prem_upd or clm_upd:
                    updated_any = len(set(prem_upd.keys()) | set(clm_upd.keys()))

                print(f"SES: {updated_any} empresas do universo atualizadas via {filename}.")

    except Exception as e:
        print(f"SES CRITICAL: Erro ao processar ZIP: {e}")

    # integridade do universo
    final_count = len(companies)
    if final_count != master_count:
        print(f"SES ERROR: Universo foi alterado! Inicial={master_count}, Final={final_count}")

    return SesMeta(), companies
