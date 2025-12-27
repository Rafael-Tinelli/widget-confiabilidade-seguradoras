from __future__ import annotations

import csv
import gzip
import hashlib
import io
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, Optional

import requests

DATASET_URL = "https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br"
# Regex para capturar URLs dos CSVs mensais na página do CKAN
BASECOMPLETA_RE = re.compile(r"/download/(basecompleta(?P<ym>\d{4}-\d{2})\.csv)\b", re.IGNORECASE)

# --- Normalização robusta ---

def _norm_text(s: str) -> str:
    s = s or ""
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return re.sub(r"\s+", " ", s)

def _norm_header(h: str) -> str:
    # ex.: "Nome Fantasia" -> "nome fantasia" -> "nomefantasia"
    return _norm_text(h).replace(" ", "")

HEADER_ALIASES = {
    "nomefantasia": "nome_fantasia",
    "respondida": "respondida",
    "situacao": "situacao",
    "avaliacaoreclamacao": "avaliacao",
    "notadoconsumidor": "nota",
    "notaconsumidor": "nota",
    "temporesposta": "tempo_resposta",
}

def _get_field(row: dict, canonical: str) -> str:
    return (row.get(canonical) or "").strip()

def _safe_int(s: str) -> Optional[int]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None

def _safe_float(s: str) -> Optional[float]:
    s = (s or "").strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


@dataclass
class Agg:
    display_name: str = ""
    total: int = 0
    finalizadas: int = 0
    respondidas: int = 0
    resolvidas_indicador: int = 0
    nota_sum: float = 0.0
    nota_count: int = 0
    tempo_sum: int = 0
    tempo_count: int = 0

    def to_public(self) -> dict:
        sat = (self.nota_sum / self.nota_count) if self.nota_count else None
        avg_tempo = (self.tempo_sum / self.tempo_count) if self.tempo_count else None
        
        responded_rate = (self.respondidas / self.finalizadas) if self.finalizadas else 0.0
        resolution_rate = (self.resolvidas_indicador / self.finalizadas) if self.finalizadas else 0.0

        return {
            "display_name": self.display_name,
            "complaints_total": self.total,
            "complaints_finalizadas": self.finalizadas,
            "responded_rate": round(responded_rate, 4),
            "resolution_rate": round(resolution_rate, 4),
            "satisfaction_avg": round(sat, 2) if sat else None,
            "avg_response_days": round(avg_tempo, 1) if avg_tempo else None,
        }


def discover_basecompleta_urls(months: int = 12, dataset_url: str = DATASET_URL) -> Dict[str, str]:
    """
    Retorna dict { 'YYYY-MM': 'https://.../download/basecompletaYYYY-MM.csv' }
    """
    print(f"Consumidor.gov: Buscando URLs em {dataset_url}...")
    try:
        html = requests.get(dataset_url, timeout=60).text
    except Exception as e:
        print(f"ERRO ao acessar dataset: {e}")
        return {}

    matches = {}
    
    # 1. Tenta regex relativa simples
    for m in BASECOMPLETA_RE.finditer(html):
        ym = m.group("ym")
        # O link pode ser relativo ou absoluto, mas o CKAN costuma ter o link completo no href
        # Vamos confiar mais na busca por links absolutos abaixo, mas manter essa como backup
    
    # 2. Busca links absolutos (mais seguro no CKAN)
    # Procura href=".../download/basecompletaYYYY-MM.csv"
    abs_re = re.compile(r'href="(?P<url>https?://[^"]+/download/basecompleta(?P<ym>\d{4}-\d{2})\.csv)"', re.IGNORECASE)
    
    for m in abs_re.finditer(html):
        url = m.group("url")
        ym = m.group("ym")
        matches[ym] = url

    print(f"Consumidor.gov: Encontrados {len(matches)} arquivos mensais.")
    
    # Ordena decrescente (mais recente primeiro) e pega os N últimos
    yms = sorted(matches.keys(), reverse=True)[:months]
    selected = {ym: matches[ym] for ym in yms}
    print(f"Consumidor.gov: Selecionados {len(selected)} meses: {list(selected.keys())}")
    return selected


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def download_csv_to_gz(url: str, out_gz_path: str, timeout: int = 300) -> dict:
    os.makedirs(os.path.dirname(out_gz_path), exist_ok=True)
    
    print(f"Downloading {url} -> {out_gz_path} ...")
    tmp_path = out_gz_path + ".tmp"
    
    try:
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with gzip.open(tmp_path, "wb") as gz:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        gz.write(chunk)
        
        os.replace(tmp_path, out_gz_path)
        return {
            "url": url,
            "path": out_gz_path,
            "sha256": _sha256_file(out_gz_path),
            "downloaded_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "bytes": os.path.getsize(out_gz_path),
        }
    except Exception as e:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise RuntimeError(f"Falha no download de {url}: {e}")


def iter_rows_from_gz_csv(path: str, encoding: str = "latin-1") -> Iterable[dict]:
    # Primeira passada para detectar header
    with gzip.open(path, "rb") as f:
        # Pula BOM se houver e lê header
        text = io.TextIOWrapper(f, encoding=encoding, errors="replace", newline="")
        # Lê a primeira linha crua para detectar delimitador se precisar, ou confia no ;
        try:
            sample = text.read(1024)
        except Exception:
            return # Arquivo vazio ou corrompido
            
    # Reabre para ler de verdade
    with gzip.open(path, "rb") as f:
        text = io.TextIOWrapper(f, encoding=encoding, errors="replace", newline="")
        # O csv.reader lida melhor com o iterator do que ler na mão
        reader = csv.reader(text, delimiter=";")
        
        try:
            headers = next(reader)
        except StopIteration:
            return

        norm_headers = [_norm_header(h) for h in headers]
        # Mapeia headers para nomes canônicos
        fieldnames = [HEADER_ALIASES.get(h, h) for h in norm_headers]

        # Processa o restante
        for row in reader:
            if not row: continue
            # Cria dict na mão para evitar overhead do DictReader se headers mudarem no meio (improvável, mas seguro)
            # ou simplesmente zipa
            if len(row) != len(fieldnames):
                continue # Pula linhas quebradas
            yield dict(zip(fieldnames, row))


def aggregate_month(path_gz: str) -> Dict[str, Agg]:
    print(f"Aggregating {path_gz} ...")
    aggs: Dict[str, Agg] = {}
    count = 0

    for row in iter_rows_from_gz_csv(path_gz):
        count += 1
        nome = _get_field(row, "nome_fantasia")
        if not nome:
            continue

        key = _norm_text(nome)
        if key not in aggs:
            aggs[key] = Agg(display_name=nome)

        a = aggs[key]
        a.total += 1

        situacao = _get_field(row, "situacao").lower()
        # "Finalizada avaliada" ou "Finalizada não avaliada"
        is_finalizada = "finalizada" in situacao

        if is_finalizada:
            a.finalizadas += 1

            respondida = _get_field(row, "respondida").upper() # S ou N
            if respondida == "S":
                a.respondidas += 1

            avaliacao = _get_field(row, "avaliacao").lower()
            # Regra de resolução: "Resolvida" pelo consumidor OU "Não avaliada" (assumida resolvida)
            if "nao avaliada" in situacao or "não avaliada" in situacao:
                a.resolvidas_indicador += 1
            elif "resolvida" in avaliacao:
                a.resolvidas_indicador += 1

            nota = _safe_float(_get_field(row, "nota"))
            if nota is not None:
                a.nota_sum += nota
                a.nota_count += 1

            # Tempo só conta se foi respondida
            tempo = _safe_int(_get_field(row, "tempo_resposta"))
            if tempo is not None and respondida == "S":
                a.tempo_sum += tempo
                a.tempo_count += 1
    
    print(f"  -> {count} rows processed. {len(aggs)} unique companies.")
    return aggs
