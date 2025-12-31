# api/sources/ses.py
from __future__ import annotations

import csv
import gzip
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

def _normalize_col(col_name: str) -> str:
    if not isinstance(col_name, str):
        return str(col_name)
    nfkd = unicodedata.normalize('NFKD', col_name)
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
        
        if b"<!doctype" in response.content[:500].lower():
            raise RuntimeError("WAF Blocked (HTML returned)")
            
        with open(dest, "wb") as f:
            f.write(response.content)
    except Exception as e:
        print(f"SES: Erro download: {e}")

def _extract_and_compress_files(zip_path: Path, output_dir: Path) -> list[str]:
    # Lista explícita do que precisamos
    target_map = {
        "ses_seguros": "Ses_seguros.csv",
        "ses_balanco": "Ses_balanco.csv",
        "ses_pl_margem": "Ses_pl_margem.csv", # OBRIGATÓRIO PARA SOLVÊNCIA
    }
    extracted = []
    
    if not zip_path.exists():
        return []

    try:
        if not zipfile.is_zipfile(zip_path): 
            print("SES: Arquivo ZIP inválido.")
            return []
            
        with zipfile.ZipFile(zip_path, "r") as z:
            name_map = {n.lower(): n for n in z.namelist()}
            for target_key, target_name in target_map.items():
                found_name = next((name_map[n] for n in name_map if target_name.lower().replace(".csv", "") in n), None)
                
                if found_name:
                    final_name = f"{target_name}.gz"
                    target_path = output_dir / final_name
                    
                    # Extrai sempre para garantir (sobreescreve)
                    print(f"SES: Extraindo {found_name} -> {final_name} ...")
                    with z.open(found_name) as source:
                        with gzip.open(target_path, "wb", compresslevel=9) as dest:
                            shutil.copyfileobj(source, dest)
                    extracted.append(final_name)
                else:
                    print(f"SES: AVISO - Arquivo {target_name} não encontrado no ZIP.")
    except Exception as e:
        print(f"SES: Erro extração ZIP: {e}")
        
    return extracted

def _parse_lista_empresas(cache_path: Path) -> dict[str, dict[str, Any]]:
    companies = {}
    print(f"SES: Lendo Lista de Empresas ({cache_path.name})...")
    
    if not cache_path.exists():
        return {}

    try:
        try:
            df = pd.read_csv(cache_path, sep=';', encoding="latin-1", dtype=str, on_bad_lines='skip')
        except Exception:
            df = pd.read_csv(cache_path, sep=',', encoding="utf-8", dtype=str, on_bad_lines='skip')

        if len(df.columns) < 2:
             # Fallback agressivo se o separador falhar
             df = pd.read_csv(cache_path, sep=None, engine='python', encoding="latin-1", dtype=str, on_bad_lines='skip')

        df.columns = [_normalize_col(c) for c in df.columns]
        
        col_cod = next((c for c in df.columns if "cod" in c and ("fip" in c or "ent" in c)), None)
        if not col_cod:
            col_cod = next((c for c in df.columns if "coenti" in c), None)

        col_cnpj = next((c for c in df.columns if "cnpj" in c), None)
        col_nome = next((c for c in df.columns if "nome" in c or "razao" in c), None)
        if not col_nome:
            col_nome = next((c for c in df.columns if "noenti" in c), None)

        print(f"DEBUG: Mapeado Lista -> ID: {col_cod}, CNPJ: {col_cnpj}, Nome: {col_nome}")

        if col_cod and col_cnpj:
            count = 0
            for _, row in df.iterrows():
                try:
                    cod = re.sub(r"\D", "", str(row[col_cod]))
                    cnpj = re.sub(r"\D", "", str(row[col_cnpj]))
                    nome = str(row[col_nome]).strip() if col_nome else ""
                    
                    if cod and cnpj:
                        data = {
                            "cnpj": cnpj, 
                            "name": nome, 
                            "premiums": 0.0, 
                            "claims": 0.0,
                            "net_worth": 0.0, 
                            "solvency_margin": 0.0
                        }
                        companies[cod.zfill(5)] = data
                        companies[cod.zfill(6)] = data
                        count += 1
                except Exception:
                    continue
            print(f"SES: {count} empresas carregadas.")
    except Exception as e:
        print(f"SES: Erro crítico ao ler Lista: {e}")
    
    return companies

def _enrich_with_solvency(companies: dict, cache_dir: Path) -> dict:
    """Lê Ses_pl_margem.csv.gz e preenche Patrimônio Líquido."""
    solv_file = cache_dir / "Ses_pl_margem.csv.gz"
    
    # Se arquivo não existe, avisa mas não crasha
    if not solv_file.exists():
        print(f"SES: ERRO - Arquivo {solv_file} não encontrado. Solvência será 0.")
        return companies

    print("SES: Processando Solvência (PL/Margem)...")
    try:
        with gzip.open(solv_file, 'rt', encoding='latin-1') as f:
            header_line = f.readline().strip().split(';')
            if len(header_line) < 2:
                f.seek(0)
                header_line = f.readline().strip().split(',')
                delim = ','
            else:
                delim = ';'

        norm_headers = [_normalize_col(h) for h in header_line]
        
        idx_id = next((i for i, h in enumerate(norm_headers) if "coenti" in h), -1)
        # Patrimônio Líquido Ajustado (plajustado)
        idx_pl = next((i for i, h in enumerate(norm_headers) if "plajustado" in h or "patrimonioliquido" in h), -1)
        if idx_pl == -1:
            idx_pl = next((i for i, h in enumerate(norm_headers) if "pla" in h), -1)
        
        # Margem de Solvência
        idx_margem = next((i for i, h in enumerate(norm_headers) if "margem" in h), -1)

        print(f"DEBUG: Solvência Índices -> ID: {idx_id}, PL: {idx_pl}, Margem: {idx_margem}")

        if idx_id == -1 or idx_pl == -1:
            print("SES: ERRO - Colunas de Solvência não mapeadas.")
            return companies

        count = 0
        with gzip.open(solv_file, 'rt', encoding='latin-1') as f:
            next(f)
            reader = csv.reader(f, delimiter=delim)
            for row in reader:
                if len(row) <= max(idx_id, idx_pl):
                    continue
                try:
                    cod = re.sub(r"\D", "", row[idx_id]).zfill(5)
                    
                    def parse_br(val):
                        if not val:
                            return 0.0
                        return float(val.replace('.', '').replace(',', '.'))

                    pl = parse_br(row[idx_pl])
                    margem = parse_br(row[idx_margem]) if idx_margem != -1 else 0.0

                    # Tenta encontrar a empresa e atualizar
                    # (Sobrescreve o valor 0.0 inicial)
                    comp = companies.get(cod) or companies.get(cod.zfill(6))
                    if comp:
                        comp["net_worth"] = pl
                        comp["solvency_margin"] = margem
                        count += 1
                except Exception:
                    continue
        
        print(f"SES: Solvência atualizada para {count} registros.")

    except Exception as e:
        print(f"SES: Erro processando solvência: {e}")

    return companies

def _enrich_with_financials(companies: dict, cache_dir: Path) -> dict:
    fin_file = cache_dir / "Ses_seguros.csv.gz"
    if not fin_file.exists():
        return companies

    print("SES: Processando Financeiro (Prêmios/Sinistros)...")
    try:
        with gzip.open(fin_file, 'rt', encoding='latin-1') as f:
            header_line = f.readline().strip().split(';')
            if len(header_line) < 2:
                f.seek(0)
                header_line = f.readline().strip().split(',')
                delim = ','
            else:
                delim = ';'

        norm_headers = [_normalize_col(h) for h in header_line]
        
        idx_id = next((i for i, h in enumerate(norm_headers) if "coenti" in h), -1)
        idx_prem = next((i for i, h in enumerate(norm_headers) if "premioganho" in h), -1)
        if idx_prem == -1: 
            idx_prem = next((i for i, h in enumerate(norm_headers) if "premioemitido" in h), -1)
            
        idx_claim = next((i for i, h in enumerate(norm_headers) if "sinistroretido" in h), -1)
        if idx_claim == -1:
            idx_claim = next((i for i, h in enumerate(norm_headers) if "sinistrodireto" in h), -1)

        print(f"DEBUG: Fin Índices -> ID: {idx_id}, Prêmio: {idx_prem}, Sinistro: {idx_claim}")

        count = 0
        with gzip.open(fin_file, 'rt', encoding='latin-1') as f:
            next(f)
            reader = csv.reader(f, delimiter=delim)
            for row in reader:
                if len(row) <= max(idx_id, idx_prem, idx_claim):
                    continue
                try:
                    cod = re.sub(r"\D", "", row[idx_id]).zfill(5)
                    
                    def parse_br(val):
                        if not val:
                            return 0.0
                        return float(val.replace('.', '').replace(',', '.'))

                    prem = parse_br(row[idx_prem])
                    claim = parse_br(row[idx_claim]) if idx_claim != -1 else 0.0

                    comp = companies.get(cod) or companies.get(cod.zfill(6))
                    if comp:
                        comp["premiums"] += prem
                        comp["claims"] += claim
                        count += 1
                except Exception:
                    continue
        print(f"SES: Financeiro somado para {count} registros.")
    except Exception as e:
        print(f"SES: Erro financeiro: {e}")

    return companies

def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, Any]]:
    url_lista = os.getenv("SES_LISTAEMPRESAS_URL", "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv")
    url_zip = os.getenv("SES_ZIP_URL", "https://www2.susep.gov.br/redarq.asp?arq=BaseCompleta%2ezip")

    cache_dir = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")).resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)

    # 1. Lista
    path_lista = cache_dir / "LISTAEMPRESAS.csv"
    _download_with_impersonation(url_lista, path_lista)
    companies = _parse_lista_empresas(path_lista)

    # 2. Dados (ZIP)
    path_zip = cache_dir / "BaseCompleta.zip"
    
    # VERIFICAÇÃO RIGOROSA: Se faltar qualquer um dos 2 arquivos, baixa de novo
    # Isso garante que 'Ses_pl_margem.csv.gz' será criado se não existir
    missing_financial = not (cache_dir / "Ses_seguros.csv.gz").exists()
    missing_solvency = not (cache_dir / "Ses_pl_margem.csv.gz").exists()
    
    if missing_financial or missing_solvency:
        print("SES: Arquivos de cache incompletos. Baixando ZIP...")
        _download_with_impersonation(url_zip, path_zip)
        _extract_and_compress_files(path_zip, cache_dir)
        if path_zip.exists():
            path_zip.unlink()
    
    if companies:
        print("DEBUG: Iniciando enriquecimento de dados...")
        companies = _enrich_with_financials(companies, cache_dir)
        
        print("DEBUG: Chamando enriquecimento de solvência...")
        companies = _enrich_with_solvency(companies, cache_dir)

    files = [f.name for f in cache_dir.glob("*.gz")]
    meta = SesMeta(
        zip_url=url_zip,
        cias_file="LISTAEMPRESAS.csv",
        seguros_file="Ses_seguros.csv.gz" if "Ses_seguros.csv.gz" in files else "",
        balanco_file="Ses_pl_margem.csv.gz" if "Ses_pl_margem.csv.gz" in files else "",
        as_of=datetime.now().strftime("%Y-%m"),
        warning="Pandas/ETL v9 - Solvency Forced"
    )
    return meta, companies
