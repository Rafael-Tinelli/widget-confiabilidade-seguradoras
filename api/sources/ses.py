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
    
    # Se estiver vazio
    if not raw:
        return ""

    for enc in ["utf-8-sig", "latin-1", "cp1252"]:
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    
    # Último recurso: ignorar erros para tentar ler algo
    return raw.decode("latin-1", errors="ignore")


def _download_robust(url: str, dest: Path) -> bool:
    """Baixa arquivo. Retorna True se baixou, False se falhou."""
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
        
        # Verifica se é HTML de bloqueio
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


def _parse_csv_brute_force(text: str) -> dict[str, dict[str, Any]]:
    """
    Parser 'Marreta' para o formato específico:
    CodigoFIP;NomeEntidade;CNPJ
    """
    out = {}
    lines = text.splitlines()
    print(f"SES DEBUG: Tentando parse bruto em {len(lines)} linhas.")
    
    for line in lines:
        line = line.strip()
        if not line: 
            continue
        
        # Ignora linha de header se parecer header
        if "codigofip" in line.lower() and "nome" in line.lower():
            continue

        # Tenta splitar por ponto e vírgula (formato do seu arquivo)
        parts = line.split(";")
        if len(parts) < 3:
            # Tenta vírgula caso tenha sido convertido
            parts = line.split(",")
        
        if len(parts) >= 3:
            # Assume ordem: FIP, Nome, CNPJ (ou FIP, CNPJ, Nome - tenta inferir)
            p0 = parts[0].strip() # FIP?
            p1 = parts[1].strip() # Nome?
            p2 = parts[2].strip() # CNPJ?

            # Limpa caracteres não numéricos para testar
            d0 = re.sub(r"\D", "", p0)
            d2 = re.sub(r"\D", "", p2)

            fip = ""
            cnpj = ""
            name = ""

            # Lógica heurística:
            # FIP tem 4 a 6 dígitos. CNPJ tem 14.
            if len(d0) in [4, 5, 6] and len(d2) >= 8:
                fip = d0
                name = p1
                cnpj = d2
            elif len(d0) >= 8 and len(d2) in [4, 5, 6]:
                # Invertido?
                cnpj = d0
                name = p1
                fip = d2
            
            if fip and cnpj:
                out[fip.zfill(5)] = {"cnpj": cnpj, "name": name}
                out[fip.zfill(6)] = {"cnpj": cnpj, "name": name}

    return out


def _parse_csv_content(text: str) -> dict[str, dict[str, Any]]:
    """Tenta parse estruturado, se falhar, vai pro bruto."""
    if not text.strip():
        return {}

    f = io.StringIO(text)
    header_line = text.splitlines()[0]
    delim = ";" if header_line.count(";") > header_line.count(",") else ","

    reader = csv.DictReader(f, delimiter=delim)
    
    # Normaliza headers
    headers_map = {}
    if reader.fieldnames:
        for h in reader.fieldnames:
            norm = h.lower().replace(" ", "").replace("_", "").replace(".", "").strip()
            headers_map[norm] = h
    
    col_cod = headers_map.get("codigofip") or headers_map.get("codfip") or headers_map.get("fip")
    col_cnpj = headers_map.get("cnpj") or headers_map.get("numcnpj")
    col_nome = (
        headers_map.get("nomeentidade")
        or headers_map.get("nome")
        or headers_map.get("razaosocial")
        or headers_map.get("nomerazaosocial")
    )

    out = {}
    # Se achou as colunas bonitinho, usa o DictReader
    if col_cod and col_cnpj and col_nome:
        for row in reader:
            cod = re.sub(r"\D", "", row.get(col_cod, ""))
            cnpj = re.sub(r"\D", "", row.get(col_cnpj, ""))
            nome = (row.get(col_nome) or "").strip()
            if cod and cnpj:
                out[cod.zfill(5)] = {"cnpj": cnpj, "name": nome}
                out[cod.zfill(6)] = {"cnpj": cnpj, "name": nome}
    
    # Se o método "elegante" falhou ou retornou vazio, chama a marreta
    if len(out) < 10:
        print("SES DEBUG: Parse estruturado falhou ou vazio. Acionando modo Brute Force.")
        out_brute = _parse_csv_brute_force(text)
        if len(out_brute) > len(out):
            return out_brute
            
    return out


def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, Any]]:
    """
    Estratégia Híbrida: Tenta Download -> Tenta Estático -> Falha.
    """
    lista_url = os.getenv(
        "SES_LISTAEMPRESAS_URL",
        "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv",
    )

    # Caminho exato para o arquivo que você subiu
    # api/sources/ses.py -> api/sources -> api -> (static fica em api/static)
    root_api = Path(__file__).resolve().parent.parent
    static_path = root_api / "static" / "LISTAEMPRESAS.csv"
    
    # Cache path
    cache_dir = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")).resolve()
    if not cache_dir.is_absolute():
        cache_dir = Path.cwd() / cache_dir
    cache_path = cache_dir / "LISTAEMPRESAS.csv"

    companies: dict[str, dict[str, Any]] = {}
    source_used = "none"

    # 1. Tentativa via Download
    if _download_robust(lista_url, cache_path):
        content = _read_text_safe(cache_path)
        data = _parse_csv_content(content)
        if len(data) > 50:
            companies = data
            source_used = "download"
            print(f"SES: Sucesso via download ({len(companies)} registros).")
        else:
            print(f"SES WARNING: Download obteve apenas {len(data)} registros. Descartando.")

    # 2. Tentativa via Estático (Fallback)
    if not companies:
        if static_path.exists():
            print(f"SES: Tentando fallback estático em {static_path}...")
            content = _read_text_safe(static_path)
            # Log do início do arquivo para provar que está lendo o certo
            print(f"SES DEBUG: Inicio do arquivo estatico: {content[:100]!r}")
            
            data = _parse_csv_content(content)
            if len(data) > 10: # Aceita qualquer coisa > 10 como sucesso no fallback
                companies = data
                source_used = "static_repo"
                print(f"SES: Sucesso via estático ({len(companies)} registros).")
            else:
                print("SES ERROR: Arquivo estático existe mas parseou 0 registros.")
        else:
            print(f"SES INFO: Arquivo estático não encontrado em {static_path}")

    if not companies:
        raise RuntimeError("SES: Falha crítica. LISTAEMPRESAS.csv vazio ou inválido em todas as fontes.")

    # Formata para output
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
        warning="Apenas dados cadastrais (Fallback Strategy Applied)",
    )

    return meta, final_companies
