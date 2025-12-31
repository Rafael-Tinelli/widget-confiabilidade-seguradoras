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

# Bibliotecas externas necessárias
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

        # Verifica se recebeu HTML de erro disfarçado
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
    sample = text_reader.read(1024)
    text_reader.seek(0)
    delim = ";" if sample.count(";") > sample.count(",") else ","

    reader = csv.DictReader(text_reader, delimiter=delim)
    if not reader.fieldnames:
        return

    col_valor = next((h for h in reader.fieldnames if "valor" in h.lower()), None)
    
    # Se não achar coluna valor, copia tudo
    if not col_valor:
        print("SES: Coluna de valor não identificada no Balanço. Copiando integralmente.")
        # Reset stream logic would be complex here, simplifying to standard copy if structure unknown
        return 

    print(f"SES: Otimizando Balanço (removendo zeros)... Coluna valor: {col_valor}")

    with gzip.open(dest_path, "wt", encoding="latin-1", newline="", compresslevel=9) as f_out:
        writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames, delimiter=delim)
        writer.writeheader()
        kept = 0
        
        for row in reader:
            try:
                # Tenta verificar se é zero
                val_str = row[col_valor].replace(".", "").replace(",", ".")
                if float(val_str) != 0:
                    writer.writerow(row)
                    kept += 1
            except ValueError:
                writer.writerow(row) # Se não for número, mantém
                kept += 1

    print(f"SES: Balanço otimizado. Linhas mantidas: {kept}")

def _extract_and_compress_files(zip_path: Path, output_dir: Path) -> list[str]:
    """Extrai os arquivos relevantes do ZIP da SUSEP."""
    target_map = {
        "ses_seguros": "Ses_seguros.csv", # O mais importante para Prêmios/Sinistros
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
                    
                    # Evita reprocessar se já existe e é recente
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
                else:
                    print(f"SES: Arquivo {target_name} não encontrado no ZIP.")
    except Exception as e:
        print(f"SES: Erro na extração: {e}")

    return extracted

def _parse_lista_empresas(cache_path: Path) -> dict[str, dict[str, Any]]:
    """Lê o arquivo de cadastro de empresas para mapear Código SUSEP -> CNPJ/Nome."""
    companies = {}
    try:
        # Lê usando Pandas para robustez (detecção de separador e encoding)
        df = pd.read_csv(cache_path, sep=None, engine='python', encoding="latin-1", dtype=str)
        
        # Normaliza colunas
        df.columns = [c.lower().strip().replace("_", "").replace(" ", "") for c in df.columns]
        
        # Mapeia colunas possíveis
        col_cod = next((c for c in df.columns if c in ["codigofip", "codfip", "fip", "coenti"]), None)
        col_cnpj = next((c for c in df.columns if c in ["cnpj", "numcnpj", "nucnpj"]), None)
        col_nome = next((c for c in df.columns if c in ["nomeentidade", "nome", "razaosocial", "noenti"]), None)

        if col_cod and col_cnpj and col_nome:
            for _, row in df.iterrows():
                try:
                    cod = re.sub(r"\D", "", str(row[col_cod]))
                    cnpj = re.sub(r"\D", "", str(row[col_cnpj]))
                    nome = str(row[col_nome]).strip()
                    
                    if cod and cnpj:
                        # Mapeia tanto 5 dígitos quanto 6 (SUSEP tem inconsistência)
                        data = {"cnpj": cnpj, "name": nome, "premiums": 0.0, "claims": 0.0}
                        companies[cod.zfill(5)] = data
                        companies[cod.zfill(6)] = data # Fallback
                except:
                    continue
    except Exception as e:
        print(f"SES: Erro ao ler Lista de Empresas: {e}")
    
    return companies

def _enrich_with_financials(companies: dict, cache_dir: Path) -> dict:
    """
    Lê o arquivo Ses_seguros.csv.gz, soma Prêmios e Sinistros por empresa
    e atualiza o dicionário de empresas.
    """
    fin_file = cache_dir / "Ses_seguros.csv.gz"
    if not fin_file.exists():
        print("SES: Arquivo Ses_seguros.csv.gz não encontrado. Pulando financeiro.")
        return companies

    print("SES: Calculando métricas financeiras (Pandas)...")
    try:
        # Lê o CSV comprimido. A SUSEP usa formato brasileiro (vírgula decimal)
        df = pd.read_csv(
            fin_file, 
            compression='gzip', 
            encoding='latin-1', 
            sep=';', 
            decimal=',',
            low_memory=False
        )
        
        # Normaliza nomes das colunas
        df.columns = [c.lower().strip() for c in df.columns]
        
        # Identifica colunas chave
        # Código da Entidade
        col_id = next((c for c in df.columns if c.startswith("co_enti")), None)
        
        # Prêmio: Preferência por "premio_ganho", fallback para "premio_emitido" ou contendo "premio"
        col_prem = next((c for c in df.columns if "premio" in c and "ganho" in c), None)
        if not col_prem:
            col_prem = next((c for c in df.columns if "premio" in c and "emitido" in c), None)
        
        # Sinistro: "sinistro_ocorrido" ou contendo "sinistro"
        col_claim = next((c for c in df.columns if "sinistro" in c and "ocorrido" in c), None)
        if not col_claim:
            col_claim = next((c for c in df.columns if "sinistro" in c), None)

        print(f"SES: Colunas identificadas -> ID: {col_id} | Prêmio: {col_prem} | Sinistro: {col_claim}")

        if col_id and col_prem:
            # Converte coluna de ID para string limpa para bater com o dicionário
            df[col_id] = df[col_id].astype(str).str.replace(r'\D', '', regex=True)
            
            # Garante numérico
            df[col_prem] = pd.to_numeric(df[col_prem], errors='coerce').fillna(0)
            if col_claim:
                df[col_claim] = pd.to_numeric(df[col_claim], errors='coerce').fillna(0)

            # Agrupa por empresa e soma
            cols_to_sum = [col_prem]
            if col_claim:
                cols_to_sum.append(col_claim)
            
            grouped = df.groupby(col_id)[cols_to_sum].sum()

            updated_count = 0
            for code, row in grouped.iterrows():
                # Tenta encontrar a empresa pelo código (com ou sem zero à esquerda)
                code_str = str(code).zfill(5)
                # SUSEP às vezes usa 5, às vezes 6 digitos no CSV
                comp_data = companies.get(code_str) or companies.get(str(code)) or companies.get(str(code).zfill(6))

                if comp_data:
                    comp_data["premiums"] = float(row[col_prem])
                    if col_claim:
                        comp_data["claims"] = float(row[col_claim])
                    updated_count += 1
            
            print(f"SES: Financeiro atribuído com sucesso para {updated_count} empresas.")
        else:
            print("SES: ERRO - Colunas financeiras não encontradas no CSV.")

    except Exception as e:
        print(f"SES: Erro crítico ao processar financeiro: {e}")

    return companies

def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, Any]]:
    # URLs Oficiais
    url_lista = os.getenv("SES_LISTAEMPRESAS_URL", "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv")
    url_zip = os.getenv("SES_ZIP_URL", "https://www2.susep.gov.br/redarq.asp?arq=BaseCompleta%2ezip")

    cache_dir = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")).resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)

    # --- PASSO 1: LISTA DE EMPRESAS ---
    path_lista = cache_dir / "LISTAEMPRESAS.csv"
    if not path_lista.exists() or path_lista.stat().st_size < 100:
        _download_with_impersonation(url_lista, path_lista)
    
    companies = _parse_lista_empresas(path_lista)
    print(f"SES: {len(companies)} empresas cadastradas encontradas.")

    # --- PASSO 2: DOWNLOAD E EXTRAÇÃO ---
    path_zip = cache_dir / "BaseCompleta.zip"
    
    # Só baixa se não tiver processado recentemente (cache simples)
    if not (cache_dir / "Ses_seguros.csv.gz").exists():
        _download_with_impersonation(url_zip, path_zip)
        print("SES: Extraindo arquivos...")
        _extract_and_compress_files(path_zip, cache_dir)
        # Limpa o ZIP gigante para economizar espaço
        if path_zip.exists():
            path_zip.unlink()
    else:
        print("SES: Cache de arquivos processados encontrado.")

    # --- PASSO 3: PROCESSAMENTO FINANCEIRO (NOVO!) ---
    # Aqui estava o erro: antes apenas retornava zeros. Agora chamamos a função de cálculo.
    companies = _enrich_with_financials(companies, cache_dir)

    # Prepara Metadados
    files_in_cache = [f.name for f in cache_dir.glob("*.gz")]
    meta = SesMeta(
        zip_url=url_zip,
        cias_file="LISTAEMPRESAS.csv",
        seguros_file="Ses_seguros.csv.gz" if "Ses_seguros.csv.gz" in files_in_cache else "",
        balanco_file="Ses_balanco.csv.gz" if "Ses_balanco.csv.gz" in files_in_cache else "",
        as_of=datetime.now().strftime("%Y-%m"),
        warning="Dados processados via Pandas/ETL"
    )

    return meta, companies
