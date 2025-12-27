# api/sources/consumidor_gov.py
from __future__ import annotations

import csv
import gzip
import io
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests


@dataclass
class Agg:
    display_name: str
    complaints_total: int = 0
    complaints_finalizadas: int = 0
    responded_total: int = 0
    resolved_total: int = 0
    satisfaction_sum: float = 0.0
    satisfaction_n: int = 0
    response_days_sum: float = 0.0
    response_days_n: int = 0

    def merge(self, other: Agg) -> None:
        self.complaints_total += other.complaints_total
        self.complaints_finalizadas += other.complaints_finalizadas
        self.responded_total += other.responded_total
        self.resolved_total += other.resolved_total
        self.satisfaction_sum += other.satisfaction_sum
        self.satisfaction_n += other.satisfaction_n
        self.response_days_sum += other.response_days_sum
        self.response_days_n += other.response_days_n

    def to_public(self) -> dict[str, Any]:
        responded_rate = (self.responded_total / self.complaints_finalizadas) if self.complaints_finalizadas else None
        resolution_rate = (self.resolved_total / self.complaints_finalizadas) if self.complaints_finalizadas else None
        satisfaction_avg = (self.satisfaction_sum / self.satisfaction_n) if self.satisfaction_n else None
        avg_response_days = (self.response_days_sum / self.response_days_n) if self.response_days_n else None

        return {
            "display_name": self.display_name,
            "complaints_total": self.complaints_total,
            "complaints_finalizadas": self.complaints_finalizadas,
            "responded_rate": responded_rate,
            "resolution_rate": resolution_rate,
            "satisfaction_avg": satisfaction_avg,
            "avg_response_days": avg_response_days,
        }


def _norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s.strip()


def _to_int(x: Any) -> int:
    try:
        return int(str(x).strip())
    except Exception:
        return 0


def _to_float(x: Any) -> float:
    try:
        s = str(x).strip().replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


def discover_month_csv_urls(index_url: str) -> list[str]:
    """
    O índice do Consumidor.gov (dados abertos) costuma publicar links para CSV.GZ mensais.
    Esta função tenta coletar todos os URLs .csv.gz a partir de um HTML/JSON simples.
    """
    r = requests.get(index_url, timeout=60)
    r.raise_for_status()
    content_type = (r.headers.get("content-type") or "").lower()

    urls: list[str] = []
    if "application/json" in content_type:
        data = r.json()
        text = json.dumps(data, ensure_ascii=False)
        urls = re.findall(r"https?://[^\s\"']+?\.csv\.gz", text, flags=re.I)
    else:
        html = r.text
        urls = re.findall(r"https?://[^\s\"']+?\.csv\.gz", html, flags=re.I)

    # De-dup preservando ordem
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def download_month_csv_gz(url: str) -> bytes:
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    return r.content


def _pick_col(row: dict[str, Any], candidates: list[str]) -> Any:
    for k in candidates:
        if k in row:
            return row.get(k)
    return None


def _bool_from_pt(x: Any) -> bool:
    s = str(x or "").strip().lower()
    return s in {"1", "true", "sim", "s", "yes", "y"}


def aggregate_month(gz_bytes: bytes) -> dict[str, Agg]:
    """
    Lê um CSV.GZ mensal do Consumidor.gov e agrega por fornecedor (name_key).
    Retorna: dict[name_key] -> Agg
    """
    buf = io.BytesIO(gz_bytes)
    with gzip.GzipFile(fileobj=buf, mode="rb") as gz:
        raw = gz.read()

    # Tentativa de encoding: muitos arquivos vêm em UTF-8, mas alguns podem ter variações.
    text = raw.decode("utf-8", errors="replace")
    f = io.StringIO(text)

    # Sniff delimitador com fallback
    sample = f.read(4096)
    f.seek(0)
    delim = ";" if sample.count(";") >= sample.count(",") else ","

    reader = csv.DictReader(f, delimiter=delim)
    aggs: dict[str, Agg] = {}

    for row in reader:
        # Campos típicos (variáveis conforme layout do mês)
        fornecedor = _pick_col(
            row,
            [
                "fornecedor",
                "nome_fornecedor",
                "razao_social",
                "nomefantasia",
                "nome_fantasia",
                "empresa",
                "nomeempresa",
            ],
        )
        display_name = str(fornecedor or "").strip()
        if not display_name:
            continue

        name_key = _norm_key(display_name)
        if not name_key:
            continue

        # Métricas “mínimas” — você pode expandir depois, mas sem quebrar compatibilidade.
        finalizada = _pick_col(row, ["finalizada", "foi_finalizada", "status_finalizada", "finalizada_flag"])
        respondida = _pick_col(row, ["respondida", "foi_respondida", "status_respondida", "respondida_flag"])
        resolvida = _pick_col(row, ["resolvida", "foi_resolvida", "status_resolvida", "resolvida_flag"])
        nota = _pick_col(row, ["nota_consumidor", "nota", "satisfacao", "satisfacao_consumidor"])
        dias = _pick_col(row, ["tempo_resposta_dias", "dias_resposta", "tempo_resposta", "prazo_resposta_dias"])

        a = aggs.get(name_key)
        if not a:
            a = Agg(display_name=display_name)
            aggs[name_key] = a

        a.complaints_total += 1
        if _bool_from_pt(finalizada):
            a.complaints_finalizadas += 1
        if _bool_from_pt(respondida):
            a.responded_total += 1
        if _bool_from_pt(resolvida):
            a.resolved_total += 1

        n = _to_float(nota)
        if n > 0:
            a.satisfaction_sum += n
            a.satisfaction_n += 1

        d = _to_float(dias)
        if d > 0:
            a.response_days_sum += d
            a.response_days_n += 1

    return aggs


def aggregate_window(month_urls: list[str], max_months: int = 12) -> tuple[dict[str, Agg], dict[str, Any]]:
    """
    Baixa e agrega uma janela de meses (mais recentes primeiro, se você passar assim).
    Retorna: (aggs_by_name_key, meta)
    """
    months_used: list[str] = []
    merged: dict[str, Agg] = {}

    for i, url in enumerate(month_urls[:max_months]):
        gz_bytes = download_month_csv_gz(url)
        aggs = aggregate_month(gz_bytes)

        for k, a in aggs.items():
            if k not in merged:
                merged[k] = a
            else:
                merged[k].merge(a)

        months_used.append(url)

        # Hard cap defensivo contra listas enormes
        if i >= max_months - 1:
            break

    meta = {
        "as_of": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "window_months": max_months,
        "months": months_used,
    }
    return merged, meta


def to_payload(aggs: dict[str, Agg], meta: dict[str, Any]) -> dict[str, Any]:
    by_name_key: dict[str, Any] = {k: a.to_public() for k, a in aggs.items()}
    return {"meta": meta, "by_name_key": by_name_key}
