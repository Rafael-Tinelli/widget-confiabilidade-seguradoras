# api/sources/ses.py
from __future__ import annotations

import csv
import gzip
import io
import os
import re
import shutil
import zipfile
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from curl_cffi import requests as cffi_requests

SES_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
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

def _normalize_header(header: str) -> str:
    """Remove acentos, espaços e deixa minúsculo. Ex: 'Cód. FIP' -> 'codfip'"""
    if not isinstance(header, str): return str(header)
    nfkd = unicodedata.normalize('NFKD', header)
    ascii_text = "".join([c for c in nfkd if not unicodedata.combining(c)])
    return re.sub(r'[^a-z0-9]', '', ascii_text.lower())

def _download_with_impersonation(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"SES: Baixando {url}...")
    try:
        response = cffi_requests.get(
            url, headers=SES_HEADERS, impersonate="chrome110", timeout=600, verify=False
        )
        response.raise_for_status()
        content_start = response.content[:1000].lower()
        if b"<!doctype" in content_start or b"<html" in content_start:
            raise RuntimeError("WAF Blocked (HTML returned).")
        with open(dest, "wb") as f:
            f.write(response.content)
    except Exception as e:
        print(f"SES: Erro no download: {e}")
        raise

def _extract_and_compress_files(zip_path: Path, output_dir: Path) -> list[str]:
    target_map = {
        "ses_seguros": "Ses_seguros.csv",
        "ses_balanco": "Ses_balanco.csv",
        "ses_pl_margem": "Ses_pl_margem.csv",
        "ses_campos": "Ses_campos.csv",
    }
    extracted = []
    try:
        if not zipfile.is_zipfile(zip_path): return []
        with zipfile.ZipFile(zip_path, "r") as z:
            name_map = {n.lower(): n for n in z.namelist()}
            for target_key, target_name in target_map.items():
                found_name = next((name_map[n] for n in name_map if target_name.lower() in n), None)
                if found_name:
                    final_name = f"{target_name}.gz"
                    target_path = output_dir / final_name
                    # Removemos a checagem de cache para forçar extração correta desta vez
                    print(f"SES: Extraindo {found_name} -> {final_name} ...")
                    with z.open(found_name) as source:
                        # Cópia direta sem filtro por enquanto para garantir integridade
                        with gzip.open(target_path, "wb", compresslevel=9) as dest:
                            shutil.copyfileobj(source, dest)
                    extracted.append(final_name)
    except Exception as e:
        print(f"SES: Erro extração: {e}")
    return extracted

def _parse_lista_empresas(cache_path: Path) -> dict[str, dict[str, Any]]:
    companies = {}
    print(f"SES: Lendo Lista de Empresas...")
    
    try:
        # Tenta ler com encoding do Excel
        try:
            df = pd.read_csv(cache_path, sep=';', encoding="cp1252", dtype=str, on_bad_lines='skip')
        except:
            df = pd.read_csv(cache_path, sep=';', encoding="latin-1", dtype=str, on_bad_lines='skip')

        # Normaliza cabeçalhos
        raw_cols = list(df.columns)
        df.columns = [_normalize_header(c) for c in df.columns]
        
        # Debug Forense: O que estamos vendo?
        print(f"DEBUG: Colunas Originais encontradas: {raw_cols}")
        print(f"DEBUG: Colunas Normalizadas: {list(df.columns)}")

        # Busca Inteligente
        col_cod = next((c for c in df.columns if "cod" in c and ("fip" in c or "ent" in c)), None)
        if not col_cod: col_cod = next((c for c in df.columns if "coenti" in c), None)

        col_cnpj = next((c for c in df.columns if "cnpj" in c), None)
        
        col_nome = next((c for c in df.columns if "nome" in c or "razao" in c), None)

        print(f"SES: Mapeamento -> Cod: {col_cod}, CNPJ: {col_cnpj}, Nome: {col_nome}")

        if col_cod and col_cnpj:
            for _, row in df.iterrows():
                try:
                    cod = re.sub(r"\D", "", str(row[col_cod]))
                    cnpj = re.sub(r"\D", "", str(row[col_cnpj]))
                    nome = str(row[col_nome]).strip() if col_nome else ""
                    if cod and cnpj:
                        data = {"cnpj": cnpj, "name": nome, "premiums": 0.0, "claims": 0.0}
                        companies[cod.zfill(5)] = data
                        companies[cod.zfill(6)] = data
                except: continue
    except Exception as e:
        print(f"SES: Erro crítico Lista Empresas: {e}")
    
    return companies

def _enrich_with_financials(companies: dict, cache_dir: Path) -> dict:
    fin_file = cache_dir / "Ses_seguros.csv.gz"
    if not fin_file.exists(): return companies

    print("SES: Processando Financeiro...")
    try:
        # Lê apenas as primeiras linhas para pegar o header correto
        with gzip.open(fin_file, 'rt', encoding='latin-1') as f:
            header_line = f.readline().strip().split(';')
        
        raw_headers = header_line
        norm_headers = [_normalize_header(h) for h in raw_headers]
        
        print(f"DEBUG: Headers Financeiro Normalizados: {norm_headers}")

        # Mapeamento Manual
        idx_id = next((i for i, h in enumerate(norm_headers) if "coenti" in h), -1)
        idx_prem = next((i for i, h in enumerate(norm_headers) if "premioganho" in h), -1)
        if idx_prem == -1: idx_prem = next((i for i, h in enumerate(norm_headers) if "premioemitido" in h), -1)
        
        idx_claim = next((i for i, h in enumerate(norm_headers) if "sinistroocorrido" in h), -1)
        if idx_claim == -1: idx_claim = next((i for i, h in enumerate(norm_headers) if "sinistro" in h), -1)

        print(f"SES: Índices -> ID: {idx_id}, Prêmio: {idx_prem}, Sinistro: {idx_claim}")

        if idx_id == -1 or idx_prem == -1:
            print("SES: ERRO - Colunas financeiras críticas não encontradas.")
            return companies

        # Leitura manual linha a linha (mais robusto que pandas para arquivos quebrados)
        count = 0
        with gzip.open(fin_file, 'rt', encoding='latin-1') as f:
            reader = csv.reader(f, delimiter=';')
            next(reader) # Pula header
            
            for row in reader:
                if len(row) <= max(idx_id, idx_prem): continue
                
                try:
                    cod = re.sub(r"\D", "", row[idx_id]).zfill(5)
                    
                    # Trata valor brasileiro 1.000,00
                    def parse_br(val):
                        return float(val.replace('.', '').replace(',', '.')) if val else 0.0

                    prem = parse_br(row[idx_prem])
                    claim = parse_br(row[idx_claim]) if idx_claim != -1 else 0.0

                    # Tenta chaves variadas
                    comp = companies.get(cod) or companies.get(cod.zfill(6)) or companies.get(str(int(cod)))
                    
                    if comp:
                        comp["premiums"] += prem
                        comp["claims"] += claim
                        count += 1
                except:
                    continue
        
        print(f"SES: Financeiro somado para {count} registros.")

    except Exception as e:
        print(f"SES: Erro no processamento financeiro: {e}")

    return companies

def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, Any]]:
    url_lista = os.getenv("SES_LISTAEMPRESAS_URL", "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv")
    url_zip = os.getenv("SES_ZIP_URL", "https://www2.susep.gov.br/redarq.asp?arq=BaseCompleta%2ezip")
    cache_dir = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")).resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)

    path_lista = cache_dir / "LISTAEMPRESAS.csv"
    _download_with_impersonation(url_lista, path_lista)
    
    companies = _parse_lista_empresas(path_lista)
    print(f"SES: {len(companies)} empresas na lista.")

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
        warning="Pandas Forense v3"
    )
    return meta, companies
