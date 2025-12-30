# api/sources/ses.py
from __future__ import annotations

import csv
import gzip
import io
import os
import re
import shutil
import zipfile
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
    cias_file: str = ""
    seguros_file: str = ""
    balanco_file: str = ""
    as_of: str = ""
    # Campos restaurados para compatibilidade com build_insurers.py
    period_from: str = ""
    period_to: str = ""
    window_months: int = 12
    warning: str = ""


def _download_with_impersonation(url: str, dest: Path) -> None:
    """
    Baixa arquivo usando curl_cffi para simular um navegador Chrome real.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"SES: Baixando {url} (impersonate='chrome110')...")

    try:
        # verify=False pois governos as vezes tem cadeia de cert incompleta
        response = cffi_requests.get(
            url,
            headers=SES_HEADERS,
            impersonate="chrome110",
            timeout=300,  # ZIP é grande, aumenta timeout
            verify=False,
        )
        response.raise_for_status()

        # Validação simples anti-bloqueio
        content_start = response.content[:1000].lower()
        if b"<!doctype" in content_start or b"<html" in content_start:
            raise RuntimeError("Servidor retornou HTML (Bloqueio WAF) em vez de arquivo.")

        with open(dest, "wb") as f:
            f.write(response.content)

    except Exception as e:
        print(f"SES: Erro no download com impersonation: {e}")
        raise


def _extract_and_compress_files(zip_path: Path, output_dir: Path) -> list[str]:
    """
    Extrai CSVs críticos do ZIP e os salva COMPRIMIDOS (.csv.gz) para economizar espaço
    e permitir commit no GitHub (evita limite de 100MB).
    """
    target_map = {
        "ses_seguros": "Ses_seguros.csv",
        "ses_balanco": "Ses_balanco.csv",
        "ses_pl_margem": "Ses_pl_margem.csv",
        "ses_campos": "Ses_campos.csv",
    }

    extracted = []

    try:
        if not zipfile.is_zipfile(zip_path):
            raise RuntimeError("Arquivo baixado não é um ZIP válido.")

        with zipfile.ZipFile(zip_path, "r") as z:
            name_map = {n.lower(): n for n in z.namelist()}

            for _, target_name in target_map.items():
                # Encontra o arquivo no ZIP
                found_name = None
                for z_name_lower in name_map:
                    if target_name.lower() in z_name_lower:
                        found_name = name_map[z_name_lower]
                        break

                if found_name:
                    # Define saída com .gz
                    final_name = f"{target_name}.gz"
                    target_path = output_dir / final_name
                    
                    print(f"SES: Extraindo e comprimindo {found_name} -> {final_name} ...")
                    
                    # Lê do ZIP (stream) e escreve no GZIP (stream)
                    with z.open(found_name) as source, gzip.open(target_path, "wb") as dest:
                        shutil.copyfileobj(source, dest)
                    
                    extracted.append(final_name)
                else:
                    print(f"SES WARNING: {target_name} não encontrado no ZIP.")

    except Exception as e:
        print(f"SES: Erro na extração/compressão: {e}")
        raise

    return extracted


def _parse_lista_empresas(cache_path: Path) -> dict[str, dict[str, Any]]:
    """Lê o LISTAEMPRESAS.csv (pode estar gz ou não) e retorna dict."""
    text = ""
    try:
        # Suporta ler direto de GZ ou de texto puro
        if str(cache_path).endswith(".gz"):
             with gzip.open(cache_path, "rt", encoding="latin-1", errors="replace") as f:
                 text = f.read()
        else:
            raw = cache_path.read_bytes()
            for enc in ["utf-8-sig", "latin-1", "cp1252"]:
                try:
                    text = raw.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue
            if not text:
                text = raw.decode("latin-1", errors="replace")
    except Exception:
        return {}

    # Parser resiliente
    text = text.strip()
    if not text:
        return {}

    f = io.StringIO(text)
    header_line = text.splitlines()[0]
    delim = ";" if header_line.count(";") > header_line.count(",") else ","
    reader = csv.DictReader(f, delimiter=delim)

    headers_map = {}
    if reader.fieldnames:
        for h in reader.fieldnames:
            norm = h.lower().replace(" ", "").replace("_", "").replace(".", "").strip()
            headers_map[norm] = h

    col_cod = (
        headers_map.get("codigofip") or headers_map.get("codfip") or headers_map.get("fip")
    )
    col_cnpj = headers_map.get("cnpj") or headers_map.get("numcnpj")
    col_nome = (
        headers_map.get("nomeentidade")
        or headers_map.get("nome")
        or headers_map.get("razaosocial")
    )

    out = {}
    if col_cod and col_cnpj:
        for row in reader:
            cod = re.sub(r"\D", "", row.get(col_cod, ""))
            cnpj = re.sub(r"\D", "", row.get(col_cnpj, ""))
            nome = (row.get(col_nome, "") if col_nome else "").strip()
            if cod and cnpj:
                out[cod.zfill(5)] = {"cnpj": cnpj, "name": nome}
                out[cod.zfill(6)] = {"cnpj": cnpj, "name": nome}
    return out


def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, Any]]:
    """
    Fluxo: Baixa ZIP -> Extrai e Comprime (.csv.gz) -> Salva Cache.
    """
    url_lista = os.getenv(
        "SES_LISTAEMPRESAS_URL",
        "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv",
    )
    url_zip = os.getenv(
        "SES_ZIP_URL",
        "https://www2.susep.gov.br/download/estatisticas/BaseCompleta.zip",
    )

    # Diretórios
    cache_dir = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")).resolve()
    if not cache_dir.is_absolute():
        cache_dir = Path.cwd() / cache_dir
    cache_dir.mkdir(parents=True, exist_ok=True)

    # --- PASSO 1: LISTAEMPRESAS ---
    # Tenta manter o LISTAEMPRESAS também comprimido se possível, mas o código atual lê CSV
    path_lista = cache_dir / "LISTAEMPRESAS.csv"
    temp_lista = cache_dir / "LISTAEMPRESAS_TEMP.csv"

    try:
        _download_with_impersonation(url_lista, temp_lista)
        if len(_parse_lista_empresas(temp_lista)) > 50:
            shutil.move(str(temp_lista), str(path_lista))
            print("SES: LISTAEMPRESAS atualizado.")
        else:
            print("SES WARNING: LISTAEMPRESAS novo inválido.")
    except Exception as e:
        print(f"SES: Falha download LISTAEMPRESAS ({e}). Usando Last-Known-Good.")

    if not path_lista.exists():
        raise RuntimeError("SES: Falha crítica. LISTAEMPRESAS não disponível.")

    companies = _parse_lista_empresas(path_lista)
    if len(companies) < 50:
        raise RuntimeError(f"SES: Cadastro insuficiente ({len(companies)}).")

    # --- PASSO 2: ZIP FINANCEIRO (GZIP STRATEGY) ---
    path_zip = cache_dir / "BaseCompleta.zip"
    temp_zip = cache_dir / "BaseCompleta_TEMP.zip"
    
    # Verifica se precisamos baixar (se já temos os GZs recentes, talvez não precise,
    # mas o conceito Evergreen pede atualização. Vamos baixar.)
    try:
        _download_with_impersonation(url_zip, temp_zip)
        if zipfile.is_zipfile(temp_zip):
            # Não movemos o ZIP para o cache final para não comitar o arquivo gigante.
            # Usamos o temp para extrair e depois apagamos.
            print("SES: BaseCompleta baixado com sucesso. Extraindo...")
            extracted_files = _extract_and_compress_files(temp_zip, cache_dir)
            print(f"SES: Arquivos extraídos e comprimidos: {extracted_files}")
        else:
            print("SES WARNING: BaseCompleta novo corrompido.")
    except Exception as e:
        print(f"SES: Falha processamento ZIP ({e}). Verificando cache existente...")
    
    # Limpeza
    if temp_zip.exists(): temp_zip.unlink()
    if path_zip.exists(): path_zip.unlink() # Garante que não sobrou lixo antigo

    # Verifica o que temos no cache (GZ)
    files_in_cache = [f.name for f in cache_dir.glob("*.gz")]

    meta = SesMeta(
        source="SES/SUSEP",
        zip_url=url_zip,
        cias_file="LISTAEMPRESAS.csv",
        # Apontamos para os arquivos .gz agora
        seguros_file="Ses_seguros.csv.gz" if "Ses_seguros.csv.gz" in files_in_cache else "",
        balanco_file="Ses_balanco.csv.gz" if "Ses_balanco.csv.gz" in files_in_cache else "",
        as_of=datetime.now().strftime("%Y-%m"),
        period_from="",
        period_to="",
        window_months=12,
        warning="Arquivos financeiros comprimidos (GZIP)",
    )

    return meta, {
        k: {"name": v["name"], "cnpj": v["cnpj"], "premiums": 0.0, "claims": 0.0}
        for k, v in companies.items()
    }
