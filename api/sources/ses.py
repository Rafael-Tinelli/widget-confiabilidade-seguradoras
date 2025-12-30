# api/sources/ses.py
from __future__ import annotations

import csv
import io
import os
import re
import warnings
import zipfile
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from requests.exceptions import SSLError

SES_HOME_DEFAULT = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"

# User-Agent de navegador real para evitar bloqueio WAF da SUSEP
SES_UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

_SESSION = requests.Session()
_SESSION.trust_env = False # Evita proxies do runner

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

def _validate_downloaded_file(dest: Path) -> None:
    """
    Valida cache/download para evitar que HTML/erro de redirect seja tratado como CSV/ZIP válido.
    """
    if not dest.exists() or dest.stat().st_size <= 0:
        raise RuntimeError(f"SES: arquivo vazio ou inexistente: {dest}")

    # Lê o início do arquivo para inspeção
    try:
        head = dest.read_bytes()[:4096]
    except Exception as e:
        raise RuntimeError(f"SES: erro ao ler arquivo {dest}: {e}")

    # 1. Validação de HTML (Bloqueio WAF)
    head_lower = head.lower()
    if b"<!doctype" in head_lower or b"<html" in head_lower or b"request blocked" in head_lower:
        snippet = head.decode("latin-1", errors="replace")[:300].replace("\n", " ")
        raise RuntimeError(f"SES: download retornou HTML/Bloqueio em vez de dados. File={dest.name}. Head={snippet}")

    # 2. Validação específica para LISTAEMPRESAS.csv
    if dest.name.upper().startswith("LISTAEMPRESAS") and dest.suffix.lower() == ".csv":
        try:
            txt = head.decode("utf-8-sig")
        except UnicodeDecodeError:
            txt = head.decode("latin-1", errors="replace")
        
        if not txt.strip():
            raise RuntimeError("SES: LISTAEMPRESAS vazio (após decode)")
            
        # Verifica se tem cabeçalhos esperados (CodigoFIP ou similar)
        # SUSEP usa CodigoFIP, NomeEntidade, CNPJ (variando case e acento)
        if "fip" not in txt.lower() and "cnpj" not in txt.lower():
            raise RuntimeError(f"SES: LISTAEMPRESAS parece não ter cabeçalho CSV válido. Preview: {txt[:100]}")

def _download_robust(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    allow_insecure = _env_bool("SES_ALLOW_INSECURE_SSL", True)

    def _do(verify: bool) -> None:
        with _SESSION.get(url, headers=SES_UA, stream=True, timeout=(20, 300), verify=verify) as r:
            r.raise_for_status()
            tmp = dest.with_suffix(dest.suffix + ".part")
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk: f.write(chunk)
            tmp.replace(dest)

    try:
        _do(verify=True)
    except (requests.exceptions.SSLError, requests.exceptions.ConnectionError):
        if not allow_insecure: raise
        warnings.warn(f"SES: SSL falhou para {url}, tentando insecure...")
        _do(verify=False)

def _get_or_download_listaempresas(url: str, dest: Path) -> Path:
    """
    Usa cache se válido; caso contrário baixa e valida.
    Resolve o problema de caches corrompidos (HTML com status 200).
    """
    # 1. Tenta validar cache existente
    if dest.exists() and dest.stat().st_size > 0:
        try:
            _validate_downloaded_file(dest)
            return dest
        except RuntimeError as e:
            print(f"SES: Cache inválido ({e}), baixando novamente...")
            try:
                dest.unlink()
            except Exception:
                pass

    # 2. Baixa e valida
    _download_robust(url, dest)
    _validate_downloaded_file(dest)
    return dest

def _parse_lista_empresas(csv_path: Path) -> dict[str, dict[str, Any]]:
    # Garante leitura correta
    raw = csv_path.read_bytes()
    text = ""
    for enc in ["utf-8-sig", "latin-1", "cp1252"]:
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    
    if not text:
        raise RuntimeError("SES: Falha ao decodificar LISTAEMPRESAS (encoding desconhecido)")

    f = io.StringIO(text)
    # Detecção simples de delimitador
    sample = text[:4096]
    delim = ";" if sample.count(";") > sample.count(",") else ","
    
    reader = csv.DictReader(f, delimiter=delim)
    
    # Normaliza headers
    headers_map = {}
    if reader.fieldnames:
        for h in reader.fieldnames:
            norm = h.lower().replace(" ", "").replace("_", "").strip()
            headers_map[norm] = h
    
    # Identifica colunas
    col_cod = headers_map.get("codigofip") or headers_map.get("codfip") or headers_map.get("fip")
    col_cnpj = headers_map.get("cnpj") or headers_map.get("numcnpj")
    col_nome = headers_map.get("nomeentidade") or headers_map.get("nome") or headers_map.get("razaosocial")

    if not col_cod or not col_cnpj:
        print(f"SES: Headers encontrados: {list(headers_map.keys())}")
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
    """Função principal chamada pelo build_insurers."""
    
    lista_url = os.getenv("SES_LISTAEMPRESAS_URL", "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv")
    cache_dir = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")).resolve()
    lista_path = cache_dir / "LISTAEMPRESAS.csv"
    
    # Baixa/Valida (Garante que não é HTML de bloqueio)
    try:
        _get_or_download_listaempresas(lista_url, lista_path)
        master_data = _parse_lista_empresas(lista_path)
    except Exception as e:
        raise RuntimeError(f"SES: Falha crítica ao obter LISTAEMPRESAS: {e}")

    if len(master_data) < 50:
        raise RuntimeError(f"SES: master LISTAEMPRESAS pequeno demais ({len(master_data)}). Conteúdo provavelmente inválido.")

    print(f"SES: Mestre de empresas carregado ({len(master_data)} registros).")

    companies = {}
    for cod, data in master_data.items():
        companies[cod] = {
            "name": data["name"],
            "cnpj": data["cnpj"],
            "premiums": 0.0,
            "claims": 0.0
        }

    meta = SesMeta(
        cias_file="LISTAEMPRESAS.csv",
        as_of=datetime.now().strftime("%Y-%m"),
        warning="Apenas dados cadastrais (WAF bypass applied)"
    )
    
    return meta, companies
