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

# Substituímos requests por curl_cffi para bypass de WAF/TLS Fingerprint
from curl_cffi import requests as cffi_requests

SES_HOME_DEFAULT = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"

# Headers reais de navegador
SES_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}

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


def _download_with_impersonation(url: str, dest: Path) -> None:
    """
    Baixa arquivo usando curl_cffi para simular um navegador Chrome real.
    Isso evita o bloqueio de WAF/Anti-Bot da SUSEP que barra o 'python-requests'.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"SES: Baixando {url} (impersonate='chrome110')...")
    
    try:
        # impersonate="chrome110" gera um TLS Fingerprint idêntico ao Chrome
        response = cffi_requests.get(
            url, 
            headers=SES_HEADERS, 
            impersonate="chrome110", 
            timeout=120,
            verify=False # Frequentemente governos têm cadeias de cert incompletas
        )
        response.raise_for_status()
        
        # Validação anti-bloqueio (se retornou HTML de erro 200 OK)
        content_start = response.content[:1000].lower()
        if b"<!doctype" in content_start or b"<html" in content_start:
            raise RuntimeError("Servidor retornou HTML (Bloqueio WAF) em vez de CSV.")
            
        with open(dest, "wb") as f:
            f.write(response.content)
            
    except Exception as e:
        print(f"SES: Erro no download com impersonation: {e}")
        raise


def _parse_csv_content(text: str) -> dict[str, dict[str, Any]]:
    """Parser resiliente para o CSV de empresas."""
    if not text.strip():
        return {}

    f = io.StringIO(text)
    # Detecta delimitador (Susep usa ; mas as vezes converte)
    header = text.splitlines()[0]
    delim = ";" if header.count(";") > header.count(",") else ","
    
    reader = csv.DictReader(f, delimiter=delim)
    
    # Normaliza headers
    headers_map = {}
    if reader.fieldnames:
        for h in reader.fieldnames:
            # remove espaços, acentos e deixa minusculo
            norm = h.lower().replace(" ", "").replace("_", "").replace(".", "").strip()
            headers_map[norm] = h
    
    # Busca colunas chave
    col_cod = headers_map.get("codigofip") or headers_map.get("codfip") or headers_map.get("fip")
    col_cnpj = headers_map.get("cnpj") or headers_map.get("numcnpj")
    col_nome = (
        headers_map.get("nomeentidade") 
        or headers_map.get("nome") 
        or headers_map.get("razaosocial")
    )

    if not col_cod or not col_cnpj:
        print(f"SES DEBUG: Colunas não encontradas. Headers disponíveis: {list(headers_map.keys())}")
        return {}

    out = {}
    for row in reader:
        cod = re.sub(r"\D", "", row.get(col_cod, ""))
        cnpj = re.sub(r"\D", "", row.get(col_cnpj, ""))
        nome = (row.get(col_nome) or "").strip()
        
        if cod and cnpj:
            # Normaliza e guarda
            out[cod.zfill(5)] = {"cnpj": cnpj, "name": nome}
            out[cod.zfill(6)] = {"cnpj": cnpj, "name": nome}
            
    return out


def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, Any]]:
    """
    Tenta baixar o LISTAEMPRESAS usando bypass de WAF.
    Se falhar, o pipeline falha (sem false positive).
    """
    lista_url = os.getenv(
        "SES_LISTAEMPRESAS_URL",
        "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv",
    )
    zip_url_oficial = "https://www2.susep.gov.br/download/estatisticas/BaseCompleta.zip"

    # Define caminho de cache
    cache_dir = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")).resolve()
    if not cache_dir.is_absolute():
        cache_dir = Path.cwd() / cache_dir
    
    cache_path = cache_dir / "LISTAEMPRESAS.csv"

    # Tenta baixar
    try:
        _download_with_impersonation(lista_url, cache_path)
    except Exception as e:
        raise RuntimeError(f"SES: Falha crítica no download (WAF/Rede). Erro: {e}")

    # Lê e parseia
    try:
        # Tenta decodificar (latin1 é o padrão susep, mas tentamos utf-8)
        raw_bytes = cache_path.read_bytes()
        text_content = ""
        for enc in ["utf-8-sig", "latin-1", "cp1252"]:
            try:
                text_content = raw_bytes.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        
        companies = _parse_csv_content(text_content)
    except Exception as e:
        raise RuntimeError(f"SES: Falha no parsing do arquivo baixado. Erro: {e}")

    if len(companies) < 50:
        # Dump para debug no log do Actions
        print(f"SES DUMP (inicio): {text_content[:200]}")
        raise RuntimeError(f"SES: Arquivo baixado parece inválido (apenas {len(companies)} registros).")

    print(f"SES: Sucesso. {len(companies)} empresas carregadas.")

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
        cias_file="LISTAEMPRESAS.csv",
        as_of=datetime.now().strftime("%Y-%m"),
        zip_url=zip_url_oficial,
        warning="Dados cadastrais via bypass WAF"
    )

    return meta, final_companies
