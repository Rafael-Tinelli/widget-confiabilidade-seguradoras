# api/sources/ses.py
from __future__ import annotations

import csv
import io
import os
import re
import shutil
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
            verify=False,
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
    text = text.strip()
    if not text:
        return {}

    f = io.StringIO(text)

    # Detecção de delimitador na primeira linha VÁLIDA
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
        print(f"SES DEBUG: Fieldnames vazio. Header line: {header_line[:50]}")
        return {}

    # Busca colunas chave
    col_cod = (
        headers_map.get("codigofip") or headers_map.get("codfip") or headers_map.get("fip")
    )
    col_cnpj = headers_map.get("cnpj") or headers_map.get("numcnpj")
    col_nome = (
        headers_map.get("nomeentidade")
        or headers_map.get("nome")
        or headers_map.get("razaosocial")
    )

    if not col_cod or not col_cnpj:
        print(
            f"SES DEBUG: Colunas não encontradas. Headers disponíveis: {list(headers_map.keys())}"
        )
        return {}

    out = {}
    for row in reader:
        cod_val = row.get(col_cod, "")
        cnpj_val = row.get(col_cnpj, "")
        nome_val = row.get(col_nome, "") if col_nome else ""

        cod = re.sub(r"\D", "", cod_val)
        cnpj = re.sub(r"\D", "", cnpj_val)
        nome = nome_val.strip()

        if cod and cnpj:
            out[cod.zfill(5)] = {"cnpj": cnpj, "name": nome}
            out[cod.zfill(6)] = {"cnpj": cnpj, "name": nome}

    return out


def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, Any]]:
    """
    Estratégia Evergreen: Tenta Download -> Falha? -> Usa Last-Known-Good do Cache.
    """
    lista_url = os.getenv(
        "SES_LISTAEMPRESAS_URL",
        "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv",
    )
    zip_url_oficial = "https://www2.susep.gov.br/download/estatisticas/BaseCompleta.zip"

    # Define caminho de cache (Last Known Good)
    cache_dir = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")).resolve()
    if not cache_dir.is_absolute():
        cache_dir = Path.cwd() / cache_dir

    cache_path = cache_dir / "LISTAEMPRESAS.csv"
    temp_path = cache_dir / "LISTAEMPRESAS_TEMP.csv"

    source_used = "none"

    # 1. Tenta Download para arquivo TEMPORÁRIO
    # Se falhar aqui, não estraga o arquivo bom que já existe no cache_path
    try:
        _download_with_impersonation(lista_url, temp_path)

        # Valida se o download novo é parseável antes de substituir o antigo
        raw_bytes = temp_path.read_bytes()
        text_temp = raw_bytes.decode("latin-1", errors="replace")
        data_temp = _parse_csv_content(text_temp)

        if len(data_temp) > 50:
            # Sucesso! Substitui o cache oficial pelo novo
            shutil.move(str(temp_path), str(cache_path))
            source_used = "download_fresh"
            print(
                f"SES: Download novo com sucesso ({len(data_temp)} registros). Cache atualizado."
            )
        else:
            print(
                "SES WARNING: Download novo veio vazio ou inválido. Mantendo cache antigo."
            )
            if temp_path.exists():
                temp_path.unlink()

    except Exception as e:
        print(
            f"SES WARNING: Falha no download ou validação ({e}). Tentando usar Last-Known-Good..."
        )
        if temp_path.exists():
            temp_path.unlink()

    # 2. Carrega do Cache (Seja ele o novo que acabamos de baixar, ou o antigo do repo)
    if not cache_path.exists():
        # Se não tem nem download novo nem arquivo antigo, é falha crítica
        raise RuntimeError(
            "SES: Falha crítica. Sem download e sem cache (Last-Known-Good) disponível."
        )

    try:
        raw_bytes = cache_path.read_bytes()
        text_content = ""
        for enc in ["utf-8-sig", "latin-1", "cp1252"]:
            try:
                text_content = raw_bytes.decode(enc)
                break
            except UnicodeDecodeError:
                continue

        if not text_content:
            text_content = raw_bytes.decode("latin-1", errors="replace")

        companies = _parse_csv_content(text_content)

        if source_used == "none":
            source_used = "last_known_good_cache"
            print(f"SES: Usando dados do cache/repo ({len(companies)} registros).")

    except Exception as e:
        raise RuntimeError(f"SES: Cache local corrompido. Erro: {e}")

    # 3. Validação Final
    if len(companies) < 50:
        raise RuntimeError(
            f"SES: Dados finais inválidos (apenas {len(companies)} registros)."
        )

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
        zip_url=zip_url_oficial,
        warning="Dados via estratégia Evergreen",
    )

    return meta, final_companies
