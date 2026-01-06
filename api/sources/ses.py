# api/sources/ses.py
from __future__ import annotations

import io
import os
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

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
SES_ZIP_URL = os.getenv(
    "SES_ZIP_URL",
    "https://www2.susep.gov.br/download/estatisticas/BaseCompleta.zip",
)
SES_ZIP_URL_FALLBACK = os.getenv(
    "SES_ZIP_URL_FALLBACK",
    "https://www2.susep.gov.br/redarq.asp?arq=BaseCompleta%2ezip",
)

# [MODIFICADO] Cache Dinâmico e Flag de Retenção
# Permite usar /tmp no CI/CD para evitar IO no diretório de trabalho do git
DEFAULT_CACHE_DIR = Path("data/raw/ses")
CACHE_DIR = Path(os.getenv("SES_CACHE_DIR", str(DEFAULT_CACHE_DIR)))
KEEP_ZIP = os.getenv("SES_KEEP_ZIP", "0") == "1"

ALLOW_INSECURE_SSL = os.getenv("SES_ALLOW_INSECURE_SSL", "0") == "1"

_DIGITS_RE = re.compile(r"\d+")


@dataclass(frozen=True)
class SesMeta:
    source: str = "SUSEP (SES)"
    zip_url: str = SES_ZIP_URL
    cias_file: str = "LISTAEMPRESAS.csv"
    seguros_file: str = "BaseCompleta.zip"


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
    # remove padrão típico "1007.0"
    s = re.sub(r"\.0+$", "", s)

    digits = "".join(_DIGITS_RE.findall(s))
    if not digits:
        return ""

    return digits.zfill(6)


def _canonical_ses_id_series(series: pd.Series) -> pd.Series:
    """
    Versão vetorizada da canonicalização.
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
    Converte strings numéricas pt-BR ('1.234,56') para float (1234.56).
    """
    s = series.astype(str).str.strip()
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _download_bytes(url: str, timeout: int) -> bytes:
    verify_ssl = not ALLOW_INSECURE_SSL
    resp = requests.get(url, headers=SES_HEADERS, verify=verify_ssl, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def _download_to_file(urls: Iterable[str], dest: Path, timeout: int) -> Path:
    """
    Faz download streaming para arquivo.
    """
    verify_ssl = not ALLOW_INSECURE_SSL
    dest.parent.mkdir(parents=True, exist_ok=True)

    last_err: Optional[Exception] = None
    for url in urls:
        try:
            part = dest.with_suffix(dest.suffix + ".part")
            with requests.get(url, headers=SES_HEADERS, verify=verify_ssl, timeout=timeout, stream=True) as r:
                r.raise_for_status()
                with open(part, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            part.replace(dest)
            return dest
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"Falha ao baixar arquivo. Último erro: {last_err}")  # noqa: TRY003


def _read_csv_bytes(content: bytes) -> pd.DataFrame:
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


def _pick_col(cols_lower: List[str], candidates: List[str]) -> Optional[str]:
    for cand in candidates:
        cand_l = cand.lower()
        for c in cols_lower:
            if cand_l in c:
                return c
    return None


def _detect_sep_and_header(z: zipfile.ZipFile, filename: str) -> Tuple[str, List[str], List[str], Dict[str, str]]:
    for sep in (";", ","):
        try:
            with z.open(filename) as f:
                hdr = pd.read_csv(f, sep=sep, encoding="latin1", nrows=0)
            header_orig = list(hdr.columns)
            header_lower = [c.lower().strip() for c in header_orig]
            if header_lower:
                lower_to_orig = {cl: co for co, cl in zip(header_orig, header_lower)}
                return sep, header_orig, header_lower, lower_to_orig
        except Exception:
            continue
    return ";", [], [], {}


def _compute_max_date_in_zipfile(
    z: zipfile.ZipFile,
    filename: str,
    sep: str,
    c_data_orig: str,
    c_data_lower: str,
) -> int:
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
            chunk.columns = [c.lower().strip() for c in chunk.columns]
            if c_data_lower not in chunk.columns:
                continue
            d = pd.to_numeric(chunk[c_data_lower], errors="coerce").fillna(0).astype(int)
            if not d.empty:
                m = int(d.max())
                if m > max_date:
                    max_date = m
    return max_date


def extract_ses_master_and_financials():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # --- 1) MASTER LIST ---
    print(f"SES: Baixando master list: {SES_LISTAEMPRESAS_URL}")
    try:
        df_cias = _read_csv_bytes(_download_bytes(SES_LISTAEMPRESAS_URL, timeout=90))
    except Exception as e:
        print(f"SES CRITICAL: Falha ao baixar master list: {e}")
        df_cias = pd.DataFrame()

    companies: Dict[str, dict] = {}

    if not df_cias.empty:
        df_cias.columns = [c.lower().strip() for c in df_cias.columns]

        col_id = _pick_col(list(df_cias.columns), ["codfip", "coenti", "cod_enti", "cod"])
        col_cnpj = _pick_col(list(df_cias.columns), ["cnpj"])
        col_nome = _pick_col(list(df_cias.columns), ["razao", "razão", "nome"])

        if not (col_id and col_cnpj and col_nome):
            print("SES CRITICAL: Master list sem colunas esperadas (id/cnpj/nome).")
        else:
            for _, row in df_cias.iterrows():
                sid = _canonical_ses_id(row.get(col_id))
                if not sid:
                    continue

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

    # --- 2) ZIP (download para disco) ---
    zip_path = CACHE_DIR / "BaseCompleta.zip"
    print(f"SES: Baixando ZIP: {SES_ZIP_URL} em {zip_path}")
    try:
        _download_to_file([SES_ZIP_URL, SES_ZIP_URL_FALLBACK], zip_path, timeout=600)
    except Exception as e:
        print(f"SES CRITICAL: Falha ao baixar ZIP: {e}")
        return SesMeta(), companies

    # --- 3) Processamento ZIP ---
    try:
        with zipfile.ZipFile(zip_path) as z:
            all_files = z.namelist()

            targets = [
                ("pl_margem", "PATRIMONIO"),
                ("balanco", "PATRIMONIO"),
                ("ses_seguros", "SEGUROS"),
                ("contrib_benef", "PREVIDENCIA"),
                ("ses_dados_cap", "CAPITALIZACAO"),
                ("ses_cessoes_recebidas", "RESSEGURO"),
            ]

            relevant = []
            for fn in all_files:
                fn_l = fn.lower()
                if any(k in fn_l for k, _ in targets):
                    relevant.append(fn)
            print(f"SES INFO: Arquivos-alvo no ZIP: {relevant}")

            for filename in relevant:
                filename_l = filename.lower()
                file_type = None
                for key, ftype in targets:
                    if key in filename_l:
                        file_type = ftype
                        break
                if not file_type:
                    continue

                sep, header_orig, header_lower, lower_to_orig = _detect_sep_and_header(z, filename)
                if not header_lower:
                    print(f"SES SKIP: {filename} sem header legível.")
                    continue

                c_id_l = _pick_col(header_lower, ["coenti"])
                c_data_l = _pick_col(header_lower, ["damesano"])

                if not c_id_l:
                    print(f"SES SKIP: {filename} sem coluna de ID (coenti). Header={header_lower[:20]}")
                    continue

                c_id = lower_to_orig.get(c_id_l, c_id_l)
                c_data = lower_to_orig.get(c_data_l, c_data_l) if c_data_l else None

                # Detecta colunas de valor
                c_patrimonio_l: Optional[str] = None
                c_receita_l: Optional[str] = None
                c_despesa_l: Optional[str] = None
                despesa_extra_l: Optional[str] = None
                balanco_layout = False

                if file_type == "PATRIMONIO":
                    if "valor" in header_lower and "quadro" in header_lower:
                        balanco_layout = True
                    else:
                        c_patrimonio_l = _pick_col(
                            header_lower,
                            ["plajustado", "pl_ajustado", "patrimonio", "patrim", "pla", "pl", "liquido", "líquido"],
                        )

                elif file_type == "SEGUROS":
                    c_receita_l = _pick_col(header_lower, ["premio_ganho", "premioganho", "premio_emitido", "premioemitido", "premios", "premio"])
                    c_despesa_l = _pick_col(header_lower, ["sinistro_ocorrido", "sinistroocorrido", "sinistro_corrido", "sinistrocorrido", "sinistros", "sinistro"])

                elif file_type == "PREVIDENCIA":
                    c_receita_l = _pick_col(header_lower, ["contrib", "arrec", "arrecadacao", "arrecada", "receita"])
                    c_despesa_l = _pick_col(header_lower, ["benef", "beneficio", "benefícios", "resgate", "pag"])

                elif file_type == "CAPITALIZACAO":
                    c_receita_l = _pick_col(header_lower, ["receitascap", "receitacap", "receita", "arrec", "arrecadacao"])
                    c_despesa_l = _pick_col(header_lower, ["valorresg", "resg", "resgate"])
                    despesa_extra_l = _pick_col(header_lower, ["sorteios", "sorteiopago", "sorteiospagos", "sorteio"])

                elif file_type == "RESSEGURO":
                    c_receita_l = _pick_col(header_lower, ["cessao", "cessoes", "premio_aceito", "premioaceito", "aceito", "receita"])
                    c_despesa_l = _pick_col(header_lower, ["recuperacao", "recuper", "sinistro_pago", "sinistropago", "despesa", "pago"])

                # Gate de validade
                if file_type == "PATRIMONIO":
                    if not (balanco_layout or c_patrimonio_l):
                        print(f"SES SKIP: {filename} sem colunas de valor reconhecíveis. Header={header_lower[:25]}")
                        continue
                else:
                    if not (c_receita_l and c_despesa_l):
                        print(f"SES SKIP: {filename} sem colunas de valor reconhecíveis. Header={header_lower[:25]}")
                        continue

                print(f"SES: Processando {filename} ({file_type})...")

                max_date = 0
                if c_data and c_data_l:
                    try:
                        max_date = _compute_max_date_in_zipfile(z, filename, sep, c_data, c_data_l)
                    except Exception as e:
                        print(f"SES WARN: Falha ao calcular max_date em {filename}: {e}")
                        max_date = 0

                usecols: List[str] = [c_id]
                if c_data:
                    usecols.append(c_data)

                if balanco_layout:
                    usecols.extend([lower_to_orig.get("valor", "valor"), lower_to_orig.get("quadro", "quadro")])
                else:
                    if c_patrimonio_l:
                        usecols.append(lower_to_orig.get(c_patrimonio_l, c_patrimonio_l))
                    if c_receita_l:
                        usecols.append(lower_to_orig.get(c_receita_l, c_receita_l))
                    if c_despesa_l:
                        usecols.append(lower_to_orig.get(c_despesa_l, c_despesa_l))
                    if despesa_extra_l:
                        usecols.append(lower_to_orig.get(despesa_extra_l, despesa_extra_l))

                net_worth_upd: Dict[str, float] = {}
                prem_upd: Dict[str, float] = {}
                clm_upd: Dict[str, float] = {}
                ativo_sum: Dict[str, float] = {}
                passivo_sum: Dict[str, float] = {}

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
                        chunk.columns = [c.lower().strip() for c in chunk.columns]

                        # filtro temporal
                        if c_data and c_data_l and max_date > 0 and c_data_l in chunk.columns:
                            dates_num = pd.to_numeric(chunk[c_data_l], errors="coerce").fillna(0).astype(int)
                            if file_type == "PATRIMONIO":
                                chunk = chunk[dates_num == int(max_date)]
                            else:
                                target_year = str(int(max_date))[:4]
                                chunk = chunk[dates_num.astype(str).str.startswith(target_year)]
                            if chunk.empty:
                                continue

                        # lockdown
                        sid_series = _canonical_ses_id_series(chunk[c_id_l])
                        chunk["sid"] = sid_series
                        chunk = chunk[(chunk["sid"] != "") & (chunk["sid"].isin(companies))]
                        if chunk.empty:
                            continue

                        # BALANCO
                        if file_type == "PATRIMONIO" and balanco_layout:
                            if "valor" not in chunk.columns or "quadro" not in chunk.columns:
                                continue
                            vals = _parse_br_float(chunk["valor"])
                            q = chunk["quadro"].astype(str).str.strip().str.upper()
                            mA = q == "22A"
                            if mA.any():
                                gA = vals[mA].groupby(chunk.loc[mA, "sid"]).sum()
                                for k, v in gA.items():
                                    ativo_sum[k] = ativo_sum.get(k, 0.0) + float(v)
                            mP = q == "22P"
                            if mP.any():
                                gP = vals[mP].groupby(chunk.loc[mP, "sid"]).sum()
                                for k, v in gP.items():
                                    passivo_sum[k] = passivo_sum.get(k, 0.0) + float(v)
                            continue

                        # PATRIMONIO DIRETO
                        if file_type == "PATRIMONIO" and c_patrimonio_l:
                            if c_patrimonio_l not in chunk.columns:
                                continue
                            vals = _parse_br_float(chunk[c_patrimonio_l])
                            g = vals.groupby(chunk["sid"]).max()
                            for k, v in g.items():
                                if float(v) > 0:
                                    net_worth_upd[k] = max(net_worth_upd.get(k, 0.0), float(v))
                        else:
                            # FLUXOS
                            if not (c_receita_l and c_despesa_l):
                                continue
                            if c_receita_l not in chunk.columns or c_despesa_l not in chunk.columns:
                                continue
                            r = _parse_br_float(chunk[c_receita_l])
                            d = _parse_br_float(chunk[c_despesa_l])
                            if file_type == "CAPITALIZACAO" and despesa_extra_l and despesa_extra_l in chunk.columns:
                                d = d + _parse_br_float(chunk[despesa_extra_l])
                            gr = r.groupby(chunk["sid"]).sum()
                            gd = d.groupby(chunk["sid"]).sum()
                            for k, v in gr.items():
                                if float(v) != 0.0:
                                    prem_upd[k] = prem_upd.get(k, 0.0) + float(v)
                            for k, v in gd.items():
                                if float(v) != 0.0:
                                    clm_upd[k] = clm_upd.get(k, 0.0) + float(v)

                # Updates
                if file_type == "PATRIMONIO" and balanco_layout:
                    updated_any = 0
                    for k in set(ativo_sum.keys()) | set(passivo_sum.keys()):
                        equity = float(ativo_sum.get(k, 0.0) - passivo_sum.get(k, 0.0))
                        if equity > 0 and equity > companies[k]["net_worth"]:
                            companies[k]["net_worth"] = equity
                            if file_type not in companies[k]["sources_found"]:
                                companies[k]["sources_found"].append(file_type)
                            updated_any += 1
                    print(f"SES: {updated_any} empresas do universo atualizadas via {filename}.")
                    continue

                updated_any = 0
                for k, v in net_worth_upd.items():
                    if float(v) > companies[k]["net_worth"]:
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
    finally:
        # [MODIFICADO] Cleanup do ZIP (evita commit acidental de arquivo gigante)
        if not KEEP_ZIP:
            try:
                if zip_path.exists():
                    zip_path.unlink()
                    print(f"SES: ZIP temporário removido ({zip_path})")
            except Exception as e:
                print(f"SES WARN: Falha ao remover ZIP temporário: {e}")

    final_count = len(companies)
    if final_count != master_count:
        print(f"SES ERROR: Universo foi alterado! Inicial={master_count}, Final={final_count}")

    return SesMeta(), companies
