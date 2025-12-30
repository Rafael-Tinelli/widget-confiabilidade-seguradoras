# api/sources/ses.py
from __future__ import annotations

import csv
import io
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from requests.exceptions import SSLError

SES_HOME_DEFAULT = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"

SES_UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

_SESSION = requests.Session()
_SESSION.trust_env = False  # Evita proxies do runner


@dataclass
class SesMeta:
    source: str = "SES/SUSEP"
    zip_url: str = ""
    zip_name: str = ""
    cias_file: str = ""
    seguros_file: str = ""
    as_of: str = ""
    period_from: str = ""
    period_to: str = ""
    window_months: int = 12
    files: list[str] | None = None
    warning: str = ""


def _env_bool(key: str, default: bool = False) -> bool:
    val = os.getenv(key, str(default)).lower()
    return val in ("1", "true", "yes", "on")


def _read_text_safe(path: Path) -> str:
    """Lê arquivo tentando encodings comuns."""
    if not path.exists():
        return ""
    raw = path.read_bytes()
    for enc in ["utf-8-sig", "latin-1", "cp1252"]:
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return ""


def _download_robust(url: str, dest: Path) -> bool:
    """
    Baixa arquivo. Retorna True se baixou, False se falhou.
    Não lança exceção para não interromper o fluxo de tentativa.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    allow_insecure = _env_bool("SES_ALLOW_INSECURE_SSL", True)

    print(f"SES: Baixando {url} ...")
    try:
        try:
            resp = _SESSION.get(url, headers=SES_UA, timeout=60, verify=True)
        except SSLError:
            if not allow_insecure:
                raise
            print("SES: SSL falhou, tentando insecure...")
            resp = _SESSION.get(url, headers=SES_UA, timeout=60, verify=False)

        resp.raise_for_status()
        
        # Verifica se é HTML de bloqueio antes de salvar
        snippet = resp.content[:500].lower()
        if b"<!doctype" in snippet or b"<html" in snippet:
            print("SES WARNING: Download retornou HTML (Bloqueio WAF). Ignorando.")
            return False

        with open(dest, "wb") as f:
            f.write(resp.content)
        return True

    except Exception as e:
        print(f"SES WARNING: Falha no download: {e}")
        return False


def _parse_csv_content(text: str) -> dict[str, dict[str, Any]]:
    """Transforma texto CSV em dicionário de empresas."""
    if not text.strip():
        return {}

    f = io.StringIO(text)
    # Detecta delimitador
    header_line = text.splitlines()[0]
    delim = ";" if header_line.count(";") > header_line.count(",") else ","

    reader = csv.DictReader(f, delimiter=delim)

    # Normaliza headers
    headers_map = {}
    if reader.fieldnames:
        for h in reader.fieldnames:
            norm = h.lower().replace(" ", "").replace("_", "").replace(".", "").strip()
            headers_map[norm] = h
    else:
        return {}

    # Mapeamento flexível
    col_cod = headers_map.get("codigofip") or headers_map.get("codfip") or headers_map.get("fip")
    col_cnpj = headers_map.get("cnpj") or headers_map.get("numcnpj")
    col_nome = (
        headers_map.get("nomeentidade")
        or headers_map.get("nome")
        or headers_map.get("razaosocial")
        or headers_map.get("nomerazaosocial")
    )

    if not col_cod or not col_cnpj:
        # Só loga se tiver conteúdo real mas não achou colunas
        if len(text) > 100:
            print(f"SES DEBUG: Colunas não encontradas. Headers: {list(headers_map.keys())}")
        return {}

    out = {}
    for row in reader:
        cod = re.sub(r"\D", "", row.get(col_cod, ""))
        cnpj = re.sub(r"\D", "", row.get(col_cnpj, ""))
        nome = (row.get(col_nome) or "").strip()

        if cod and cnpj:
            out[cod.zfill(5)] = {"cnpj": cnpj, "name": nome}
            out[cod.zfill(6)] = {"cnpj": cnpj, "name": nome}

    return out


def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, Any]]:
    """
    Estratégia Híbrida Blindada:
    1. Tenta Download -> Parse. Se > 50 registros, usa.
    2. Se falhar, Tenta Estático -> Parse. Se > 50 registros, usa.
    3. Se falhar, ERRO.
    """
    lista_url = os.getenv(
        "SES_LISTAEMPRESAS_URL",
        "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv",
    )

    # Caminhos
    # api/sources/ses.py -> api/sources -> api -> (static fica em api/static)
    root_api = Path(__file__).resolve().parent.parent
    static_path = root_api / "static" / "LISTAEMPRESAS.csv"
    
    # Cache path
    cache_dir = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")).resolve()
    # Se cache_dir for relativo, resolve a partir da raiz do repo (assumindo rodar na raiz)
    if not cache_dir.is_absolute():
        cache_dir = Path.cwd() / cache_dir
    cache_path = cache_dir / "LISTAEMPRESAS.csv"

    companies: dict[str, dict[str, Any]] = {}
    source_used = "none"

    # --- TENTATIVA 1: Download Fresco ---
    if _download_robust(lista_url, cache_path):
        content = _read_text_safe(cache_path)
        data = _parse_csv_content(content)
        if len(data) > 50:
            companies = data
            source_used = "download"
            print(f"SES: Sucesso via download ({len(companies)} empresas).")
        else:
            print(f"SES WARNING: Download obteve apenas {len(data)} registros. Descartando.")

    # --- TENTATIVA 2: Fallback Estático (Se T1 falhou) ---
    if not companies:
        if static_path.exists():
            print(f"SES: Tentando fallback estático em {static_path}...")
            content = _read_text_safe(static_path)
            data = _parse_csv_content(content)
            if len(data) > 50:
                companies = data
                source_used = "static_repo"
                print(f"SES: Sucesso via estático ({len(companies)} empresas).")
            else:
                print(f"SES ERROR: Arquivo estático existe mas tem apenas {len(data)} registros.")
        else:
            print(f"SES INFO: Arquivo estático não encontrado em {static_path}")

    # --- Validação Final ---
    if not companies:
        raise RuntimeError("SES: Falha crítica. Nem download nem arquivo estático produziram dados válidos.")

    # Formata saída
    final_companies = {}
    for cod, val in companies.items():
        final_companies[cod] = {
            "name": val["name"],
            "cnpj": val["cnpj"],
            "premiums": 0.0,
            "claims": 0.0,
        }

    meta = SesMeta(
        cias_file=f"LISTAEMPRESAS.csv ({source_used})",
        as_of=datetime.now().strftime("%Y-%m"),
        warning="Apenas dados cadastrais (Zero Maintenance Strategy)",
    )

    return meta, final_companies
