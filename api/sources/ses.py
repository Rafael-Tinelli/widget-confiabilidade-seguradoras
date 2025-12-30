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

from curl_cffi import requests as cffi_requests

SES_HOME_DEFAULT = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"

SES_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
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


def _env_bool(key: str, default: bool = False) -> bool:
    v = str(os.getenv(key, str(int(default)))).strip().lower()
    return v in {"1", "true", "yes", "on"}


def _looks_like_html_or_block(content: bytes) -> bool:
    head = content[:2000].lower()
    if b"<!doctype" in head or b"<html" in head:
        return True
    # padrões comuns de WAF/bloqueio (varia por vendor)
    needles = [
        b"access denied",
        b"forbidden",
        b"captcha",
        b"cloudflare",
        b"attention required",
        b"waf",
        b"incapsula",
        b"akamai",
    ]
    return any(n in head for n in needles)


def _download_with_impersonation(url: str, dest: Path) -> None:
    """
    Baixa arquivo usando curl_cffi para simular um navegador Chrome real.
    Estratégia: tenta verify=True; se falhar e SES_ALLOW_INSECURE_SSL=1, tenta verify=False.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    allow_insecure = _env_bool("SES_ALLOW_INSECURE_SSL", True)

    print("SES: Baixando %s (impersonate='chrome110')..." % url)

    def _do(verify: bool) -> bytes:
        r = cffi_requests.get(
            url,
            headers=SES_HEADERS,
            impersonate="chrome110",
            timeout=120,
            verify=verify,
        )
        r.raise_for_status()
        return r.content

    try:
        content = _do(verify=True)
    except Exception as e:
        if not allow_insecure:
            raise
        print(f"SES: SSL/handshake falhou com verify=True ({e}). Tentando verify=False...")
        content = _do(verify=False)

    if _looks_like_html_or_block(content):
        raise RuntimeError("Servidor retornou HTML/página de bloqueio em vez de CSV (WAF).")

    dest.write_bytes(content)


def _first_non_empty_line(text: str) -> str:
    for line in text.splitlines():
        if line.strip():
            return line
    return ""


def _parse_csv_content(text: str) -> dict[str, dict[str, Any]]:
    """
    Parser resiliente para o CSV de empresas.
    - detecta delimitador
    - normaliza headers
    - extrai cod_fip + cnpj + nome
    """
    text = text.strip()
    if not text:
        return {}

    header_line = _first_non_empty_line(text)
    if not header_line:
        return {}

    delim = ";" if header_line.count(";") > header_line.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)

    if not reader.fieldnames:
        print(f"SES DEBUG: fieldnames vazio. Header line: {header_line[:80]}")
        return {}

    headers_map: dict[str, str] = {}
    for h in reader.fieldnames:
        norm = h.lower().replace(" ", "").replace("_", "").replace(".", "").strip()
        headers_map[norm] = h

    col_cod = headers_map.get("codigofip") or headers_map.get("codfip") or headers_map.get("fip")
    col_cnpj = headers_map.get("cnpj") or headers_map.get("numcnpj")
    col_nome = headers_map.get("nomeentidade") or headers_map.get("nome") or headers_map.get("razaosocial")

    if not col_cod or not col_cnpj:
        print(f"SES DEBUG: colunas-chave ausentes. Headers: {list(headers_map.keys())}")
        return {}

    out: dict[str, dict[str, str]] = {}
    for row in reader:
        cod_raw = str(row.get(col_cod, "") or "")
        cnpj_raw = str(row.get(col_cnpj, "") or "")
        nome_raw = str(row.get(col_nome, "") or "") if col_nome else ""

        cod = re.sub(r"\D", "", cod_raw)
        cnpj = re.sub(r"\D", "", cnpj_raw)
        nome = nome_raw.strip()

        if cod and cnpj:
            out[cod.zfill(5)] = {"cnpj": cnpj, "name": nome}
            out[cod.zfill(6)] = {"cnpj": cnpj, "name": nome}

    return out


def _decode_best_effort(raw: bytes) -> str:
    for enc in ("utf-8-sig", "latin-1", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace")


def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, Any]]:
    """
    Estratégia Evergreen:
    - tenta baixar LISTAEMPRESAS.csv via curl_cffi (bypass WAF)
    - valida parsing antes de substituir o cache
    - se falhar, usa Last-Known-Good (arquivo versionado no repo / data/raw/ses)
    """
    lista_url = os.getenv(
        "SES_LISTAEMPRESAS_URL",
        "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv",
    )
    zip_url_oficial = os.getenv(
        "SES_ZIP_URL",
        "https://www2.susep.gov.br/download/estatisticas/BaseCompleta.zip",
    )

    # Cache dir: respeita env; relativo -> relativo ao root do repo (cwd no Actions)
    cache_dir = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses"))
    if not cache_dir.is_absolute():
        cache_dir = (Path.cwd() / cache_dir).resolve()
    else:
        cache_dir = cache_dir.resolve()

    cache_dir.mkdir(parents=True, exist_ok=True)

    cache_path = cache_dir / "LISTAEMPRESAS.csv"
    temp_path = cache_dir / "LISTAEMPRESAS_TEMP.csv"

    source_used = "none"

    # 1) tenta baixar para TEMP e só promove se passar na validação
    try:
        _download_with_impersonation(lista_url, temp_path)

        raw = temp_path.read_bytes()
        txt = _decode_best_effort(raw)
        parsed = _parse_csv_content(txt)

        if len(parsed) > 50:
            # replace é atômico (mesmo filesystem)
            temp_path.replace(cache_path)
            source_used = "download_fresh"
            print(f"SES: Download novo OK ({len(parsed)} registros). Cache atualizado.")
        else:
            print("SES WARNING: Download novo inválido/pequeno. Mantendo Last-Known-Good.")
            temp_path.unlink(missing_ok=True)

    except Exception as e:
        print(f"SES WARNING: Falha no download/validação ({e}). Usando Last-Known-Good...")
        temp_path.unlink(missing_ok=True)

    # 2) garante que existe algum LKG
    if not cache_path.exists():
        raise RuntimeError("SES: Sem download e sem Last-Known-Good disponível (cache ausente).")

    # 3) carrega do cache e valida
    raw = cache_path.read_bytes()
    txt = _decode_best_effort(raw)
    companies = _parse_csv_content(txt)

    if source_used == "none":
        source_used = "last_known_good_cache"
        print(f"SES: Usando cache/repo ({len(companies)} registros).")

    if len(companies) < 50:
        raise RuntimeError(f"SES: Dados finais inválidos (apenas {len(companies)} registros).")

    final_companies: dict[str, Any] = {}
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
        warning="Dados via estratégia Evergreen (download + last-known-good).",
    )
    return meta, final_companies
