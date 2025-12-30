# api/sources/ses.py
from __future__ import annotations

import csv
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


def _extract_target_files_from_zip(zip_path: Path, output_dir: Path) -> list[str]:
    """
    Extrai apenas os CSVs críticos do ZIP da Susep.
    Retorna lista de arquivos extraídos com sucesso.
    """
    # Arquivos alvo (nomes podem variar case, então buscamos por 'contains')
    target_map = {
        "ses_seguros": "Ses_seguros.csv",  # Operacional (Sinistros/Prêmios)
        "ses_balanco": "Ses_balanco.csv",  # Financeiro (Despesas)
        "ses_pl_margem": "Ses_pl_margem.csv",  # Solvência
        "ses_campos": "Ses_campos.csv",  # Dicionário de Campos (CMPID)
    }

    extracted = []

    try:
        if not zipfile.is_zipfile(zip_path):
            raise RuntimeError("Arquivo baixado não é um ZIP válido.")

        with zipfile.ZipFile(zip_path, "r") as z:
            # Normaliza nomes dentro do ZIP para busca case-insensitive
            name_map = {n.lower(): n for n in z.namelist()}

            for _, target_name in target_map.items():
                # Tenta achar o arquivo no ZIP (ex: SES_SEGUROS.csv ou Ses_Seguros.csv)
                found_name = None
                for z_name_lower in name_map:
                    if target_name.lower() in z_name_lower:
                        found_name = name_map[z_name_lower]
                        break

                if found_name:
                    # Extrai para o diretório raw
                    source = z.open(found_name)
                    target_path = output_dir / target_name
                    with open(target_path, "wb") as f_out:
                        shutil.copyfileobj(source, f_out)
                    extracted.append(target_name)
                    print(f"SES: Extraído {found_name} -> {target_name}")
                else:
                    print(
                        f"SES WARNING: Arquivo {target_name} não encontrado dentro do ZIP."
                    )

    except Exception as e:
        print(f"SES: Erro na extração do ZIP: {e}")
        raise

    return extracted


def _parse_lista_empresas(cache_path: Path) -> dict[str, dict[str, Any]]:
    """Lê o LISTAEMPRESAS.csv já baixado e retorna dict de empresas."""
    text = ""
    try:
        raw = cache_path.read_bytes()
        # Tenta decodificar
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
    Fluxo Completo de Extração:
    1. Baixa/Atualiza LISTAEMPRESAS.csv (Cadastro)
    2. Baixa/Atualiza BaseCompleta.zip (Financeiro/Operacional)
    3. Extrai CSVs críticos do ZIP
    4. Retorna metadados e lista de empresas base
    """
    # URLs
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

    # --- PASSO 1: LISTAEMPRESAS (Cadastro) ---
    path_lista = cache_dir / "LISTAEMPRESAS.csv"
    temp_lista = cache_dir / "LISTAEMPRESAS_TEMP.csv"

    try:
        _download_with_impersonation(url_lista, temp_lista)
        # Valida
        if len(_parse_lista_empresas(temp_lista)) > 50:
            shutil.move(str(temp_lista), str(path_lista))
            print("SES: LISTAEMPRESAS atualizado.")
        else:
            print("SES WARNING: LISTAEMPRESAS novo inválido. Mantendo antigo.")
    except Exception as e:
        print(f"SES: Falha download LISTAEMPRESAS ({e}). Usando Last-Known-Good.")

    if not path_lista.exists():
        raise RuntimeError("SES: Falha crítica. LISTAEMPRESAS não disponível.")

    companies = _parse_lista_empresas(path_lista)
    if len(companies) < 50:
        raise RuntimeError(f"SES: Cadastro de empresas insuficiente ({len(companies)}).")

    # --- PASSO 2: ZIP FINANCEIRO (BaseCompleta) ---
    path_zip = cache_dir / "BaseCompleta.zip"
    temp_zip = cache_dir / "BaseCompleta_TEMP.zip"

    try:
        _download_with_impersonation(url_zip, temp_zip)
        # Valida se é ZIP
        if zipfile.is_zipfile(temp_zip):
            shutil.move(str(temp_zip), str(path_zip))
            print("SES: BaseCompleta.zip atualizado.")
        else:
            print("SES WARNING: BaseCompleta novo corrompido. Mantendo antigo.")
            if temp_zip.exists():
                temp_zip.unlink()
    except Exception as e:
        print(f"SES: Falha download ZIP ({e}). Usando Last-Known-Good.")
        if temp_zip.exists():
            temp_zip.unlink()

    # --- PASSO 3: EXTRAÇÃO DOS CSVs CRÍTICOS ---
    extracted_files = []
    if path_zip.exists():
        try:
            print("SES: Iniciando extração dos CSVs financeiros...")
            extracted_files = _extract_target_files_from_zip(path_zip, cache_dir)
        except Exception as e:
            print(f"SES ERROR: Falha ao extrair arquivos do ZIP: {e}")
    else:
        print(
            "SES WARNING: Nenhum BaseCompleta.zip disponível. Análise financeira será impossível."
        )

    # Retorno
    meta = SesMeta(
        source="SES/SUSEP",
        zip_url=url_zip,
        cias_file="LISTAEMPRESAS.csv",
        seguros_file="Ses_seguros.csv" if "Ses_seguros.csv" in extracted_files else "",
        balanco_file="Ses_balanco.csv" if "Ses_balanco.csv" in extracted_files else "",
        as_of=datetime.now().strftime("%Y-%m"),
        # Preenche os campos faltantes com defaults seguros por enquanto
        period_from="",
        period_to="",
        window_months=12,
        warning="Operando em modo Evergreen (Last-Known-Good)",
    )

    return meta, {
        k: {"name": v["name"], "cnpj": v["cnpj"], "premiums": 0.0, "claims": 0.0}
        for k, v in companies.items()
    }
