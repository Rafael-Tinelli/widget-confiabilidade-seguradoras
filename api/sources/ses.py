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

def _normalize_col(col_name: str) -> str:
    """Padroniza nomes de colunas: remove acentos, espaços e underscores."""
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
    target_map = {
        "ses_seguros": "Ses_seguros.csv",
        "ses_balanco": "Ses_balanco.csv",
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
                # Busca parcial para achar o arquivo certo dentro do ZIP
                found_name = next((name_map[n] for n in name_map if target_name.lower().replace(".csv", "") in n), None)
                
                if found_name:
                    final_name = f"{target_name}.gz"
                    target_path = output_dir / final_name
                    print(f"SES: Extraindo {found_name} -> {final_name} ...")
                    with z.open(found_name) as source:
                        with gzip.open(target_path, "wb", compresslevel=9) as dest:
                            shutil.copyfileobj(source, dest)
                    extracted.append(final_name)
    except Exception as e:
        print(f"SES: Erro extração ZIP: {e}")
        
    return extracted

def _parse_lista_empresas(cache_path: Path) -> dict[str, dict[str, Any]]:
    companies = {}
    print(f"SES: Lendo Lista de Empresas ({cache_path.name})...")
    
    if not cache_path.exists():
        return {}

    try:
        # Tenta ler com engine python que é mais tolerante
        try:
            df = pd.read_csv(cache_path, sep=None, engine='python', encoding="latin-1", dtype=str, on_bad_lines='skip')
        except Exception:
            df = pd.read_csv(cache_path, sep=None, engine='python', encoding="utf-8", dtype=str, on_bad_lines='skip')

        df.columns = [_normalize_col(c) for c in df.columns]
        
        # Mapeamento conforme PDF e Arquivos Manuais (Coenti, Noenti)
        col_cod = next((c for c in df.columns if c in ["coenti", "codigofip"]), None)
        col_cnpj = next((c for c in df.columns if "cnpj" in c), None)
        col_nome = next((c for c in df.columns if c in ["noenti", "nomeentidade", "razaosocial"]), None)

        print(f"DEBUG: Mapeado -> ID: {col_cod}, CNPJ: {col_cnpj}, Nome: {col_nome}")

        if col_cod and col_cnpj:
            count = 0
            for _, row in df.iterrows():
                try:
                    cod = re.sub(r"\D", "", str(row[col_cod]))
                    cnpj = re.sub(r"\D", "", str(row[col_cnpj]))
                    nome = str(row[col_nome]).strip() if col_nome else ""
                    
                    if cod and cnpj:
                        data = {"cnpj": cnpj, "name": nome, "premiums": 0.0, "claims": 0.0}
                        companies[cod.zfill(5)] = data
                        companies[cod.zfill(6)] = data
                        count += 1
                except Exception:
                    continue
            print(f"SES: {count} empresas carregadas.")
    except Exception as e:
        print(f"SES: Erro crítico ao ler Lista: {e}")
    
    return companies

def _enrich_with_financials(companies: dict, cache_dir: Path) -> dict:
    fin_file = cache_dir / "Ses_seguros.csv.gz"
    if not fin_file.exists():
        print("SES: Arquivo financeiro não encontrado.")
        return companies

    print("SES: Processando Financeiro (Modo Cirúrgico)...")
    try:
        # Lê apenas o cabeçalho para mapear índices
        with gzip.open(fin_file, 'rt', encoding='latin-1') as f:
            header_line = f.readline().strip().split(';')
            
            # Fallback se separador for vírgula
            if len(header_line) < 2:
                f.seek(0)
                header_line = f.readline().strip().split(',')
                delim = ','
            else:
                delim = ';'

        # Normaliza os nomes das colunas (remove acentos e espaços)
        norm_headers = [_normalize_col(h) for h in header_line]
        print(f"DEBUG: Headers Normalizados (primeiros 15): {norm_headers[:15]}...")

        # --- MAPEAMENTO CRÍTICO (BASEADO NO SEU DIAGNÓSTICO) ---
        
        # 1. ID da Empresa (coenti)
        idx_id = next((i for i, h in enumerate(norm_headers) if "coenti" in h), -1)
        
        # 2. Prêmios: Prioridade para "premio_ganho"
        idx_prem = next((i for i, h in enumerate(norm_headers) if "premioganho" in h), -1)
        if idx_prem == -1: 
            idx_prem = next((i for i, h in enumerate(norm_headers) if "premioemitido" in h), -1)
            
        # 3. Sinistros: AQUI ESTÁ A CORREÇÃO PRINCIPAL
        # Prioriza 'sinistroretido' porque 'sinistroocorrido' vem zerado
        idx_claim = next((i for i, h in enumerate(norm_headers) if "sinistroretido" in h), -1)
        
        # Fallback apenas se não achar o retido
        if idx_claim == -1:
            idx_claim = next((i for i, h in enumerate(norm_headers) if "sinistrodireto" in h), -1)
        if idx_claim == -1:
            idx_claim = next((i for i, h in enumerate(norm_headers) if "sinistro" in h), -1)

        print(f"SES: Índices encontrados -> ID: {idx_id}, Prêmio: {idx_prem}, Sinistro: {idx_claim}")

        if idx_id == -1 or idx_prem == -1:
            print("SES: ERRO - Colunas financeiras críticas não encontradas.")
            return companies

        # Leitura linha a linha (não usa Pandas para evitar memory overflow)
        count = 0
        with gzip.open(fin_file, 'rt', encoding='latin-1') as f:
            # Pula o header que já lemos
            next(f)
            reader = csv.reader(f, delimiter=delim)
            
            for row in reader:
                # Pula linhas quebradas
                if len(row) <= max(idx_id, idx_prem, idx_claim):
                    continue
                
                try:
                    # Normaliza código da empresa
                    cod_raw = row[idx_id].strip()
                    cod = re.sub(r"\D", "", cod_raw).zfill(5)
                    
                    # Parser Brasileiro Manual (1.000,00 -> 1000.00)
                    def parse_br(val):
                        if not val: return 0.0
                        clean = val.replace('.', '').replace(',', '.')
                        try:
                            return float(clean)
                        except:
                            return 0.0

                    prem = parse_br(row[idx_prem])
                    
                    # Se achou coluna de sinistro, pega o valor, senão 0
                    claim = parse_br(row[idx_claim]) if idx_claim != -1 else 0.0

                    # Tenta encontrar a empresa no dicionário
                    # Tenta com 5 digitos, 6 digitos ou numérico
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

    # 1. Lista Empresas
    path_lista = cache_dir / "LISTAEMPRESAS.csv"
    _download_with_impersonation(url_lista, path_lista)
    companies = _parse_lista_empresas(path_lista)

    # 2. Dados Financeiros
    path_zip = cache_dir / "BaseCompleta.zip"
    if not (cache_dir / "Ses_seguros.csv.gz").exists():
        _download_with_impersonation(url_zip, path_zip)
        _extract_and_compress_files(path_zip, cache_dir)
        if path_zip.exists():
            path_zip.unlink()
    
    companies = _enrich_with_financials(companies, cache_dir)

    files = [f.name for f in cache_dir.glob("*.gz")]
    meta = SesMeta(
        zip_url=url_zip,
        cias_file="LISTAEMPRESAS.csv",
        seguros_file="Ses_seguros.csv.gz" if "Ses_seguros.csv.gz" in files else "",
        as_of=datetime.now().strftime("%Y-%m"),
        warning="Pandas/ETL v5 - Manual Match"
    )
    return meta, companies
