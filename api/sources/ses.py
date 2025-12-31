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

import pandas as pd
from curl_cffi import requests as cffi_requests

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
    period_from: str = ""
    period_to: str = ""
    window_months: int = 12
    warning: str = ""

def _download_with_impersonation(url: str, dest: Path) -> None:
    """Baixa arquivos tentando simular um browser real para evitar bloqueio WAF."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"SES: Baixando {url}...")
    try:
        response = cffi_requests.get(
            url,
            headers=SES_HEADERS,
            impersonate="chrome110",
            timeout=600,
            verify=False,
        )
        response.raise_for_status()

        content_start = response.content[:1000].lower()
        if b"<!doctype" in content_start or b"<html" in content_start:
            raise RuntimeError("Servidor retornou HTML (Bloqueio WAF) em vez de arquivo.")

        with open(dest, "wb") as f:
            f.write(response.content)
    except Exception as e:
        print(f"SES: Erro no download: {e}")
        raise

def _filter_and_compress_balanco(source_stream, dest_path: Path):
    """Lê o CSV linha a linha, descarta valores 0,00 e salva comprimido."""
    text_reader = io.TextIOWrapper(source_stream, encoding="latin-1", newline="")
    try:
        sample = text_reader.read(1024)
        text_reader.seek(0)
        delim = ";" if sample.count(";") > sample.count(",") else ","
    except Exception:
        delim = ";" # Fallback seguro para SUSEP

    reader = csv.DictReader(text_reader, delimiter=delim)
    if not reader.fieldnames:
        return

    col_valor = next((h for h in reader.fieldnames if "valor" in h.lower()), None)
    
    if not col_valor:
        print("SES: Coluna de valor não identificada no Balanço. Ignorando filtro.")
        return 

    print(f"SES: Otimizando Balanço... Coluna valor: {col_valor}")

    with gzip.open(dest_path, "wt", encoding="latin-1", newline="", compresslevel=9) as f_out:
        writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames, delimiter=delim)
        writer.writeheader()
        kept = 0
        for row in reader:
            try:
                val_str = row[col_valor].replace(".", "").replace(",", ".")
                if float(val_str) != 0:
                    writer.writerow(row)
                    kept += 1
            except ValueError:
                writer.writerow(row)
                kept += 1
    print(f"SES: Balanço otimizado. Linhas mantidas: {kept}")

def _extract_and_compress_files(zip_path: Path, output_dir: Path) -> list[str]:
    target_map = {
        "ses_seguros": "Ses_seguros.csv",
        "ses_balanco": "Ses_balanco.csv",
        "ses_pl_margem": "Ses_pl_margem.csv",
        "ses_campos": "Ses_campos.csv",
    }
    extracted = []
    try:
        if not zipfile.is_zipfile(zip_path):
            return []
        with zipfile.ZipFile(zip_path, "r") as z:
            name_map = {n.lower(): n for n in z.namelist()}
            for target_key, target_name in target_map.items():
                found_name = next((name_map[n] for n in name_map if target_name.lower() in n), None)
                if found_name:
                    final_name = f"{target_name}.gz"
                    target_path = output_dir / final_name
                    if target_path.exists() and target_path.stat().st_size > 1000:
                         extracted.append(final_name)
                         continue
                    print(f"SES: Extraindo {found_name} -> {final_name} ...")
                    with z.open(found_name) as source:
                        if target_key == "ses_balanco":
                            _filter_and_compress_balanco(source, target_path)
                        else:
                            with gzip.open(target_path, "wb", compresslevel=9) as dest:
                                shutil.copyfileobj(source, dest)
                    extracted.append(final_name)
    except Exception as e:
        print(f"SES: Erro na extração: {e}")
    return extracted

def _parse_lista_empresas(cache_path: Path) -> dict[str, dict[str, Any]]:
    """Lê o arquivo de cadastro de empresas."""
    companies = {}
    print(f"SES: Lendo Lista de Empresas em {cache_path}...")
    
    try:
        # Tenta ler forçando separador ; e encoding cp1252 (padrão Excel BR)
        try:
            df = pd.read_csv(cache_path, sep=';', encoding="cp1252", dtype=str, on_bad_lines='skip')
        except Exception:
            # Fallback para latin-1
            df = pd.read_csv(cache_path, sep=';', encoding="latin-1", dtype=str, on_bad_lines='skip')

        # Normaliza colunas
        df.columns = [c.lower().strip().replace("_", "").replace(" ", "").replace(".", "") for c in df.columns]
        
        # Debug: Mostra colunas encontradas se der erro
        possible_cods = ["codigofip", "codfip", "fip", "coenti"]
        possible_cnpjs = ["cnpj", "numcnpj", "nucnpj"]
        possible_names = ["nomeentidade", "nome", "razaosocial", "noenti"]

        col_cod = next((c for c in df.columns if c in possible_cods), None)
        col_cnpj = next((c for c in df.columns if c in possible_cnpjs), None)
        col_nome = next((c for c in df.columns if c in possible_names), None)

        if not (col_cod and col_cnpj):
            print(f"SES: ERRO - Colunas não identificadas na Lista de Empresas. Encontradas: {list(df.columns)}")
            return {}

        for _, row in df.iterrows():
            try:
                cod = re.sub(r"\D", "", str(row[col_cod]))
                cnpj = re.sub(r"\D", "", str(row[col_cnpj]))
                nome = str(row[col_nome]).strip() if col_nome else ""
                
                if cod and cnpj:
                    data = {"cnpj": cnpj, "name": nome, "premiums": 0.0, "claims": 0.0}
                    companies[cod.zfill(5)] = data # Ex: 01234
                    companies[cod.zfill(6)] = data # Ex: 001234
            except Exception:
                continue
                
    except Exception as e:
        print(f"SES: Erro crítico ao ler Lista de Empresas: {e}")
    
    return companies

def _enrich_with_financials(companies: dict, cache_dir: Path) -> dict:
    fin_file = cache_dir / "Ses_seguros.csv.gz"
    if not fin_file.exists():
        return companies

    print("SES: Calculando métricas financeiras...")
    try:
        # Lê o CSV financeiro
        df = pd.read_csv(
            fin_file, 
            compression='gzip', 
            encoding='latin-1', 
            sep=';', 
            decimal=',',
            low_memory=False,
            on_bad_lines='skip'
        )
        
        df.columns = [c.lower().strip() for c in df.columns]
        
        # Procura coluna de ID da entidade
        col_id = next((c for c in df.columns if c.startswith("co_enti") or c == "coenti"), None)
        
        # Procura colunas de valor
        col_prem = next((c for c in df.columns if "premio" in c and "ganho" in c), None)
        if not col_prem:
            col_prem = next((c for c in df.columns if "premio" in c and "emitido" in c), None)
        
        col_claim = next((c for c in df.columns if "sinistro" in c and "ocorrido" in c), None)
        if not col_claim:
            col_claim = next((c for c in df.columns if "sinistro" in c), None)

        print(f"SES: Colunas Financeiras -> ID: {col_id} | Prêmio: {col_prem} | Sinistro: {col_claim}")

        if col_id and col_prem:
            # Limpa ID
            df[col_id] = df[col_id].astype(str).str.replace(r'\D', '', regex=True)
            
            # Converte valores
            for c in [col_prem, col_claim]:
                if c:
                    df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

            cols = [col_prem]
            if col_claim: cols.append(col_claim)
            
            grouped = df.groupby(col_id)[cols].sum()

            updated = 0
            for code, row in grouped.iterrows():
                code_str = str(code).zfill(5)
                # Tenta chaves variadas
                comp = companies.get(code_str) or companies.get(str(code)) or companies.get(str(code).zfill(6))
                
                if comp:
                    comp["premiums"] = float(row[col_prem])
                    if col_claim:
                        comp["claims"] = float(row[col_claim])
                    updated += 1
            
            print(f"SES: Dados financeiros atribuídos para {updated} empresas.")
        else:
            print(f"SES: ERRO - Colunas não encontradas. Headers disponíveis: {list(df.columns)}")

    except Exception as e:
        print(f"SES: Erro ao processar financeiro: {e}")

    return companies

def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, Any]]:
    url_lista = os.getenv("SES_LISTAEMPRESAS_URL", "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv")
    url_zip = os.getenv("SES_ZIP_URL", "https://www2.susep.gov.br/redarq.asp?arq=BaseCompleta%2ezip")

    cache_dir = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")).resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)

    # 1. Lista Empresas
    path_lista = cache_dir / "LISTAEMPRESAS.csv"
    if not path_lista.exists() or path_lista.stat().st_size < 100:
        _download_with_impersonation(url_lista, path_lista)
    
    companies = _parse_lista_empresas(path_lista)
    print(f"SES: {len(companies)} empresas identificadas na lista mestre.")

    # 2. Dados Financeiros
    path_zip = cache_dir / "BaseCompleta.zip"
    if not (cache_dir / "Ses_seguros.csv.gz").exists():
        _download_with_impersonation(url_zip, path_zip)
        _extract_and_compress_files(path_zip, cache_dir)
        if path_zip.exists(): path_zip.unlink()
    
    companies = _enrich_with_financials(companies, cache_dir)

    files = [f.name for f in cache_dir.glob("*.gz")]
    meta = SesMeta(
        zip_url=url_zip,
        cias_file="LISTAEMPRESAS.csv",
        seguros_file="Ses_seguros.csv.gz" if "Ses_seguros.csv.gz" in files else "",
        as_of=datetime.now().strftime("%Y-%m"),
        warning="Pandas/ETL v2"
    )

    return meta, companies
