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

# Biblioteca que fura o bloqueio WAF (TLS Fingerprint)
from curl_cffi import requests as cffi_requests

SES_HOME_DEFAULT = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"

# Headers idênticos ao Chrome para passar pelo firewall
SES_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"SES: Baixando {url} (impersonate='chrome110')...")
    
    try:
        # verify=False pois governos as vezes tem cadeia de cert incompleta/antiga
        response = cffi_requests.get(
            url, 
            headers=SES_HEADERS, 
            impersonate="chrome110", 
            timeout=120,
            verify=False 
        )
        response.raise_for_status()
        
        # Validação simples anti-bloqueio (se retornou HTML de erro 200 OK)
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
    # 1. LIMPEZA CRÍTICA: Remove espaços/newlines do início que quebram o DictReader
    text = text.strip()
    
    if not text:
        return {}

    f = io.StringIO(text)
    
    # 2. Detecção de delimitador na primeira linha VÁLIDA
    header_line = text.splitlines()[0]
    delim = ";" if header_line.count(";") > header_line.count(",") else ","
    
    reader = csv.DictReader(f, delimiter=delim)
    
    # 3. Normaliza headers (remove espaços, acentos, lower)
    headers_map = {}
    if reader.fieldnames:
        for h in reader.fieldnames:
            norm = h.lower().replace(" ", "").replace("_", "").replace(".", "").strip()
            headers_map[norm] = h
    else:
        # Se fieldnames for None, o arquivo pode ter apenas 1 linha ou estar malformado
        print(f"SES DEBUG: Fieldnames vazio. Header line: {header_line[:50]}")
        return {}
    
    # Busca colunas chave com variações conhecidas da SUSEP
    col_cod = headers_map.get("codigofip") or headers_map.get("codfip") or headers_map.get("fip")
    col_cnpj = headers_map.get("cnpj") or headers_map.get("numcnpj")
    col_nome = (
        headers_map.get("nomeentidade") 
        or headers_map.get("nome") 
        or headers_map.get("razaosocial")
    )

    if not col_cod or not col_cnpj:
        print(f"SES DEBUG: Colunas não encontradas. Headers disponíveis: {list(headers_map.keys())}")
        # Retorna vazio mas não quebra aqui, deixa o check de len() no caller decidir
        return {}

    out = {}
    for row in reader:
        # Extrai valores usando o nome real da coluna encontrado
        cod_val = row.get(col_cod, "")
        cnpj_val = row.get(col_cnpj, "")
        nome_val = row.get(col_nome, "") if col_nome else ""

        # Limpa dígitos
        cod = re.sub(r"\D", "", cod_val)
        cnpj = re.sub(r"\D", "", cnpj_val)
        nome = nome_val.strip()
        
        if cod and cnpj:
            # Normaliza e guarda (zfill para garantir match com bases que usam 0 à esquerda)
            out[cod.zfill(5)] = {"cnpj": cnpj, "name": nome}
            out[cod.zfill(6)] = {"cnpj": cnpj, "name": nome}
            
    return out


def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, Any]]:
    """
    Tenta baixar o LISTAEMPRESAS usando bypass de WAF.
    """
    lista_url = os.getenv(
        "SES_LISTAEMPRESAS_URL",
        "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv",
    )
    zip_url_oficial = "https://www2.susep.gov.br/download/estatisticas/BaseCompleta.zip"

    # Define caminho de cache
    cache_dir = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")).resolve()
    # Resolve caminhos relativos baseado no diretório de execução atual
    if not cache_dir.is_absolute():
        cache_dir = Path.cwd() / cache_dir
    
    cache_path = cache_dir / "LISTAEMPRESAS.csv"

    # 1. Download (Bypass WAF)
    try:
        _download_with_impersonation(lista_url, cache_path)
    except Exception as e:
        # Se o download falhar, verifica se já existe um cache válido antigo
        if cache_path.exists() and cache_path.stat().st_size > 1000:
            print(f"SES WARNING: Download falhou ({e}), mas usando cache existente.")
        else:
            raise RuntimeError(f"SES: Falha crítica no download (WAF/Rede) e sem cache. Erro: {e}")

    # 2. Parse
    companies = {}
    text_content = "" # para debug em caso de erro
    
    try:
        raw_bytes = cache_path.read_bytes()
        # Tenta decodificar
        for enc in ["utf-8-sig", "latin-1", "cp1252"]:
            try:
                text_content = raw_bytes.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        
        if not text_content:
             # Fallback agressivo
             text_content = raw_bytes.decode("latin-1", errors="replace")

        companies = _parse_csv_content(text_content)
    except Exception as e:
        raise RuntimeError(f"SES: Falha no parsing do arquivo baixado. Erro: {e}")

    # 3. Validação
    if len(companies) < 50:
        # Dump para debug no log do Actions
        print(f"SES DUMP (inicio): \n{text_content[:300]}")
        raise RuntimeError(f"SES: Arquivo baixado parece inválido (apenas {len(companies)} registros).")

    print(f"SES: Sucesso. {len(companies)} empresas carregadas.")

    # Formata saída para o builder
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
