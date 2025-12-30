# api/sources/ses.py
from __future__ import annotations

import csv
import io
import os
import re
import warnings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from requests.exceptions import SSLError

SES_HOME_DEFAULT = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"

# User-Agent "camuflado"
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

def _validate_and_read(path: Path) -> str:
    """Lê arquivo e valida se parece um CSV de empresas válido."""
    if not path.exists() or path.stat().st_size < 100:
        raise RuntimeError("Arquivo inexistente ou muito pequeno")
    
    raw = path.read_bytes()
    # Debug: mostrar o que baixou se for pequeno (HTML de erro)
    if b"<!doctype" in raw.lower()[:50] or b"<html" in raw.lower()[:50]:
        raise RuntimeError("Conteúdo é HTML (provável bloqueio WAF/Proxy)")
    
    # Tenta decodificar
    for enc in ["utf-8-sig", "latin-1", "cp1252"]:
        try:
            txt = raw.decode(enc)
            return txt
        except UnicodeDecodeError:
            continue
            
    raise RuntimeError("Falha de encoding (não é utf-8 nem latin-1)")

def _download_robust(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    allow_insecure = _env_bool("SES_ALLOW_INSECURE_SSL", True)

    print(f"SES: Baixando {url} ...")
    try:
        # Tenta com verify=True
        resp = _SESSION.get(url, headers=SES_UA, timeout=60, verify=True)
    except SSLError:
        if not allow_insecure: raise
        print("SES: SSL falhou, tentando insecure...")
        resp = _SESSION.get(url, headers=SES_UA, timeout=60, verify=False)
    
    resp.raise_for_status()
    
    # Salva
    with open(dest, "wb") as f:
        f.write(resp.content)

def _parse_csv_content(text: str) -> dict[str, dict[str, Any]]:
    f = io.StringIO(text)
    # Detecta delimitador na força bruta
    header_line = text.splitlines()[0]
    delim = ";" if header_line.count(";") > header_line.count(",") else ","
    
    reader = csv.DictReader(f, delimiter=delim)
    
    # Normaliza headers para lower e sem acento
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
    col_nome = headers_map.get("nomeentidade") or headers_map.get("nome") or headers_map.get("razaosocial")

    if not col_cod or not col_cnpj:
        print(f"SES ERROR: Colunas obrigatórias não encontradas. Map: {list(headers_map.keys())}")
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
    Tenta obter o mestre de empresas (Download > Cache > Static Fallback).
    """
    lista_url = os.getenv("SES_LISTAEMPRESAS_URL", "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv")
    
    # Caminhos
    root = Path(__file__).resolve().parents[2] # assumindo api/sources/ses.py -> root
    cache_dir = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")).resolve()
    if not cache_dir.is_absolute():
        cache_dir = root / cache_dir
        
    cache_path = cache_dir / "LISTAEMPRESAS.csv"
    static_path = root / "api" / "static" / "LISTAEMPRESAS.csv"

    text_content = None
    source_used = "none"

    # 1. Tenta baixar (se falhar, não quebra, só loga e tenta o próximo)
    try:
        _download_robust(lista_url, cache_path)
        text_content = _validate_and_read(cache_path)
        source_used = "download"
    except Exception as e:
        print(f"SES WARNING: Falha no download direto: {e}")
    
    # 2. Se download falhou (ou veio HTML/vazio), tenta fallback estático no repo
    if not text_content:
        if static_path.exists():
            print(f"SES: Usando fallback estático: {static_path}")
            try:
                text_content = _validate_and_read(static_path)
                source_used = "static_repo"
            except Exception as e:
                print(f"SES ERROR: Arquivo estático inválido: {e}")
        else:
            print(f"SES INFO: Arquivo estático não encontrado em {static_path}")

    if not text_content:
        # Última chance: talvez o cache antigo (de um run anterior de sucesso) ainda exista?
        if cache_path.exists() and cache_path.stat().st_size > 100:
             try:
                 text_content = _validate_and_read(cache_path)
                 source_used = "stale_cache"
             except Exception:
                 pass

    if not text_content:
        # Se chegou aqui, não tem jeito.
        raise RuntimeError("SES: Impossível obter LISTAEMPRESAS válido (Download falhou, Static não existe, Cache inválido).")

    # Parse
    master_data = _parse_csv_content(text_content)
    
    if len(master_data) < 10:
        raise RuntimeError(f"SES: LISTAEMPRESAS parseado tem apenas {len(master_data)} registros. Inválido.")

    print(f"SES: Sucesso carregando mestre ({len(master_data)} registros) via {source_used}.")

    companies = {}
    for cod, data in master_data.items():
        companies[cod] = {
            "name": data["name"],
            "cnpj": data["cnpj"],
            "premiums": 0.0,
            "claims": 0.0
        }

    meta = SesMeta(
        cias_file=f"LISTAEMPRESAS.csv ({source_used})",
        as_of=datetime.now().strftime("%Y-%m"),
        warning="Apenas dados cadastrais (Zero Maintenance Fallback)"
    )
    
    return meta, companies
