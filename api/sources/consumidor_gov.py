# api/sources/consumidor_gov.py
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import os
import re
import unicodedata
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

CKAN_BASE = "https://dados.mj.gov.br"
DATASET_ID = "reclamacoes-do-consumidor-gov-br"

# UA que imita navegador para evitar bloqueios simples, mas identifica o bot
UA = {
    "User-Agent": "Mozilla/5.0 (compatible; SanidaBot/1.0; +https://github.com/exemplo)",
    "Accept": "text/csv,application/octet-stream;q=0.9,*/*;q=0.8",
}

_SESSION = requests.Session()
_SESSION.trust_env = False  # bypass de proxies do ambiente (essencial para GitHub Actions)


# ---------------------------------------------------------------------
# Modelo de agregação (compatível com builder)
# ---------------------------------------------------------------------

@dataclass
class Agg:
    display_name: str = ""
    total: int = 0
    finalizadas: int = 0
    respondidas: int = 0
    resolvidas_indicador: int = 0
    nota_sum: float = 0.0
    nota_count: int = 0
    tempo_sum: float = 0.0
    tempo_count: int = 0

    def merge(self, other: "Agg") -> None:
        if not self.display_name and other.display_name:
            self.display_name = other.display_name
        self.total += other.total
        self.finalizadas += other.finalizadas
        self.respondidas += other.respondidas
        self.resolvidas_indicador += other.resolvidas_indicador
        self.nota_sum += other.nota_sum
        self.nota_count += other.nota_count
        self.tempo_sum += other.tempo_sum
        self.tempo_count += other.tempo_count

    def merge_raw(self, raw: dict[str, Any]) -> None:
        """Compatibilidade com o builder: mescla um 'raw dict' na agregação."""
        if not isinstance(raw, dict):
            return

        def _as_int(v: Any) -> int:
            try:
                return int(float(str(v or 0).replace(",", ".")))
            except (ValueError, TypeError):
                return 0

        def _as_float(v: Any) -> float:
            try:
                return float(str(v or 0).replace(",", "."))
            except (ValueError, TypeError):
                return 0.0

        dn = raw.get("display_name") or raw.get("fornecedor") or raw.get("nome_fornecedor") or ""
        if not self.display_name and isinstance(dn, str) and dn.strip():
            self.display_name = dn.strip()

        # Suporta chaves do formato interno e do formato público (complaints_total)
        self.total += _as_int(raw.get("total") or raw.get("complaints_total"))
        self.finalizadas += _as_int(raw.get("finalizadas") or raw.get("complaints_finalizadas"))
        self.respondidas += _as_int(raw.get("respondidas"))
        self.resolvidas_indicador += _as_int(raw.get("resolvidas_indicador"))

        self.nota_sum += _as_float(raw.get("nota_sum"))
        self.nota_count += _as_int(raw.get("nota_count"))
        self.tempo_sum += _as_float(raw.get("tempo_sum"))
        self.tempo_count += _as_int(raw.get("tempo_count"))

    def to_public(self) -> dict[str, Any]:
        responded_rate = (self.respondidas / self.finalizadas) if self.finalizadas else None
        resolution_rate = (self.resolvidas_indicador / self.finalizadas) if self.finalizadas else None
        satisfaction_avg = (self.nota_sum / self.nota_count) if self.nota_count else None
        avg_response_days = (self.tempo_sum / self.tempo_count) if self.tempo_count else None
        return {
            "display_name": self.display_name,
            "complaints_total": self.total,
            "complaints_finalizadas": self.finalizadas,
            "responded_rate": responded_rate,
            "resolution_rate": resolution_rate,
            "satisfaction_avg": satisfaction_avg,
            "avg_response_days": avg_response_days,
        }


# ---------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------

def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _digits(s: Any) -> str:
    return re.sub(r"\D+", "", str(s or ""))

def _norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s.strip()

def _to_float(x: Any) -> float:
    try:
        return float(str(x).strip().replace(",", "."))
    except Exception:
        return 0.0

def _pick_col(row: dict[str, Any], candidates: list[str]) -> Any:
    # Busca case-insensitive
    row_lower = {k.lower(): k for k in row.keys()}
    for c in candidates:
        if c in row:
            return row[c]
        if c.lower() in row_lower:
            return row[row_lower[c.lower()]]
    return None

def _bool_from_pt(x: Any) -> bool:
    s = str(x or "").strip().lower()
    return s in {"1", "true", "sim", "s", "yes", "y"}

def _sniff_delimiter(sample: str) -> str:
    return ";" if sample.count(";") >= sample.count(",") else ","

def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    if os.path.exists(path):
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    return ""

def _extract_ym(text: str) -> str | None:
    t = (text or "").lower()
    m = re.search(r"(20\d{2})[-_]?([01]\d)", t)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return None

# ---------------------------------------------------------------------
# Download / Discovery
# ---------------------------------------------------------------------

def download_csv_to_gz(url: str, out_gz_path: str) -> dict[str, Any]:
    os.makedirs(os.path.dirname(out_gz_path), exist_ok=True)
    with _SESSION.get(url, headers=UA, timeout=600, stream=True) as r:
        r.raise_for_status()
        ct = (r.headers.get("content-type") or "").lower()
        is_gz = url.lower().endswith(".gz") or "gzip" in ct

        if is_gz:
            with open(out_gz_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk: f.write(chunk)
        else:
            with gzip.open(out_gz_path, "wb") as gz:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk: gz.write(chunk)

    return {
        "url": url,
        "bytes": os.path.getsize(out_gz_path),
        "sha256": _sha256_file(out_gz_path),
        "downloaded_at": _utc_now(),
    }

def download_month_csv_gz(a: str, b: str | None = None, *, out_dir: str | None = None) -> tuple[str, dict[str, Any]]:
    # Wrapper de compatibilidade para diferentes assinaturas de chamada
    default_out_dir = os.getenv("CONSUMIDOR_GOV_CACHE_DIR", "data/raw/consumidor_gov")
    out_dir = out_dir or default_out_dir
    
    url = a if b is None else b
    ym = a if b is not None else (_extract_ym(url) or "unknown")
    
    out_path = str(Path(out_dir) / f"basecompleta_{ym}.csv.gz")
    meta = download_csv_to_gz(url, out_path)
    meta["ym"] = ym
    return out_path, meta

def aggregate_month_dual_with_stats(gz_path: str) -> tuple[dict[str, Agg], dict[str, Agg], dict[str, Any]]:
    by_name: dict[str, Agg] = {}
    by_cnpj: dict[str, Agg] = {}
    
    rows = 0
    rows_cnpj = 0
    
    if os.path.exists(gz_path):
        with gzip.open(gz_path, "rt", encoding="latin-1", errors="replace") as f:
            sample = f.read(4096)
            f.seek(0)
            delim = _sniff_delimiter(sample)
            reader = csv.DictReader(f, delimiter=delim)
            
            for row in reader:
                rows += 1
                name = str(_pick_col(row, ["Nome Fantasia", "Nome do Fornecedor", "Empresa"]) or "").strip()
                if not name: continue
                
                # CNPJ Cleaning
                cnpj_raw = _pick_col(row, ["CNPJ", "CNPJ do Fornecedor", "Documento"])
                cnpj = _digits(cnpj_raw)
                cnpj_key = cnpj if len(cnpj) == 14 else ""
                
                # Metrics
                finalizada = _bool_from_pt(_pick_col(row, ["Finalizada?", "Status"]))
                respondida = _bool_from_pt(_pick_col(row, ["Respondida?", "Respondida"]))
                resolvida = _bool_from_pt(_pick_col(row, ["Resolvida?", "Resolvida"]))
                nota = _to_float(_pick_col(row, ["Nota do Consumidor", "Nota"]))
                dias = _to_float(_pick_col(row, ["Tempo de Resposta", "Dias"]))

                def _up(agg: Agg):
                    if not agg.display_name: agg.display_name = name
                    agg.total += 1
                    if finalizada: agg.finalizadas += 1
                    if respondida: agg.respondidas += 1
                    if resolvida: agg.resolvidas_indicador += 1
                    if nota > 0:
                        agg.nota_sum += nota
                        agg.nota_count += 1
                    if dias > 0:
                        agg.tempo_sum += dias
                        agg.tempo_count += 1

                nk = _norm_key(name)
                if nk:
                    if nk not in by_name: by_name[nk] = Agg(display_name=name)
                    _up(by_name[nk])
                
                if cnpj_key:
                    rows_cnpj += 1
                    if cnpj_key not in by_cnpj: by_cnpj[cnpj_key] = Agg(display_name=name)
                    _up(by_cnpj[cnpj_key])

    stats = {
        "rows_total": rows,
        "rows_with_cnpj": rows_cnpj,
        "parsed_at": _utc_now()
    }
    return by_name, by_cnpj, stats

# Alias para compatibilidade
aggregate_month_dual = lambda p: aggregate_month_dual_with_stats(p)[:2]

def discover_basecompleta_urls(months: int = 12) -> dict[str, str]:
    return {} # Placeholder se necessário, mas geralmente o builder usa URLs fixas ou scan prévio
