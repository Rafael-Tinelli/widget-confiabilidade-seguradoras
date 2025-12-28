from __future__ import annotations

import csv
import gzip
import hashlib
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

CKAN_BASE = "https://dados.mj.gov.br"
DATASET_ID = "reclamacoes-do-consumidor-gov-br"
UA = {"User-Agent": "Mozilla/5.0 (compatible; SanidaBot/1.0)"}

_SESSION = requests.Session()
_SESSION.trust_env = False  # garante bypass de proxy do ambiente


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


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s.strip()


def _digits(s: Any) -> str:
    return re.sub(r"\D+", "", str(s or ""))


def _to_float(x: Any) -> float:
    try:
        return float(str(x).strip().replace(",", "."))
    except Exception:
        return 0.0


def _pick_col(row: dict[str, Any], candidates: list[str]) -> Any:
    for k in candidates:
        if k in row:
            return row.get(k)
    return None


def _bool_from_pt(x: Any) -> bool:
    s = str(x or "").strip().lower()
    return s in {"1", "true", "sim", "s", "yes", "y"}


def _sniff_delimiter(sample: str) -> str:
    return ";" if sample.count(";") >= sample.count(",") else ","


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _extract_ym(text: str) -> str | None:
    """
    Extrai somente YYYY-MM válido (20xx e mês 01..12).
    Aceita padrões como 2025-11, 2025_11, 202511.
    """
    t = (text or "").lower()
    m = re.search(r"(20\d{2})[-_]?([01]\d)", t)
    if not m:
        return None
    y = int(m.group(1))
    mo = int(m.group(2))
    if y < 2000 or y > 2099:
        return None
    if mo < 1 or mo > 12:
        return None
    return f"{y:04d}-{mo:02d}"


def discover_basecompleta_urls(months: int = 12) -> dict[str, str]:
    api = f"{CKAN_BASE}/api/3/action/package_show"
    found: dict[str, str] = {}

    # 1) CKAN API
    try:
        r = _SESSION.get(api, params={"id": DATASET_ID}, headers=UA, timeout=60)
        r.raise_for_status()
        pkg = r.json().get("result") or {}
        resources = pkg.get("resources") or []
        for res in resources:
            if not isinstance(res, dict):
                continue
            url = str(res.get("url") or "")
            name = str(res.get("name") or "")
            hay = f"{name} {url}".lower()
            if "basecompleta" not in hay and "base completa" not in hay:
                continue
            if not re.search(r"\.csv(\.gz)?($|\?)", url, flags=re.I):
                continue
            ym = _extract_ym(hay)
            if not ym:
                continue
            found[ym] = url
    except Exception:
        pass

    # 2) fallback HTML
    if not found:
        try:
            page = f"{CKAN_BASE}/dataset/{DATASET_ID}"
            r2 = _SESSION.get(page, headers=UA, timeout=60)
            r2.raise_for_status()
            urls = re.findall(r"https?://[^\s\"']+?\.csv(?:\.gz)?", r2.text, flags=re.I)
            for u in urls:
                lu = u.lower()
                if "basecompleta" not in lu:
                    continue
                ym = _extract_ym(lu)
                if not ym:
                    continue
                found[ym] = u
        except Exception:
            return {}

    if not found:
        return {}

    yms_sorted = sorted(found.keys(), reverse=True)[: max(1, months)]
    return {ym: found[ym] for ym in sorted(yms_sorted)}


def download_csv_to_gz(url: str, out_gz_path: str) -> dict[str, Any]:
    os.makedirs(os.path.dirname(out_gz_path), exist_ok=True)
    with _SESSION.get(url, headers=UA, timeout=600, stream=True) as r:
        r.raise_for_status()
        ct = (r.headers.get("content-type") or "").lower()
        is_gz = url.lower().endswith(".gz") or "gzip" in ct

        if is_gz:
            with open(out_gz_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        else:
            with gzip.open(out_gz_path, "wb") as gz:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        gz.write(chunk)

    return {
        "url": url,
        "bytes": os.path.getsize(out_gz_path),
        "sha256": _sha256_file(out_gz_path),
        "downloaded_at": _utc_now(),
    }


def aggregate_month_dual(gz_path: str) -> tuple[dict[str, Agg], dict[str, Agg]]:
    by_name: dict[str, Agg] = {}
    by_cnpj: dict[str, Agg] = {}

    with gzip.open(gz_path, "rt", encoding="latin-1", errors="replace") as f:
        sample = f.read(4096)
        f.seek(0)
        delim = _sniff_delimiter(sample)
        reader = csv.DictReader(f, delimiter=delim)

        for row in reader:
            if not isinstance(row, dict):
                continue

            fornecedor = str(
                _pick_col(
                    row,
                    [
                        "fornecedor",
                        "nome_fornecedor",
                        "razao_social",
                        "nomefantasia",
                        "nome_fantasia",
                        "empresa",
                        "nomeempresa",
                        "nome_empresa",
                    ],
                )
                or ""
            ).strip()
            if not fornecedor:
                continue

            name_key = _norm_key(fornecedor)
            if not name_key:
                continue

            cnpj_raw = _pick_col(row, ["cnpj", "cnpj_fornecedor", "cnpjempresa", "cnpj_empresa", "documento"])
            cnpj_key = _digits(cnpj_raw)
            if len(cnpj_key) != 14:
                cnpj_key = ""

            finalizada = _pick_col(row, ["finalizada", "foi_finalizada", "status_finalizada"])
            respondida = _pick_col(row, ["respondida", "foi_respondida", "status_respondida"])
            resolvida = _pick_col(row, ["resolvida", "foi_resolvida", "status_resolvida"])
            nota = _pick_col(row, ["nota_consumidor", "nota", "satisfacao"])
            dias = _pick_col(row, ["tempo_resposta_dias", "dias_resposta", "tempo_resposta"])

            def _apply(a: Agg) -> None:
                if not a.display_name:
                    a.display_name = fornecedor
                a.total += 1
                if _bool_from_pt(finalizada):
                    a.finalizadas += 1
                if _bool_from_pt(respondida):
                    a.respondidas += 1
                if _bool_from_pt(resolvida):
                    a.resolvidas_indicador += 1
                n = _to_float(nota)
                if n > 0:
                    a.nota_sum += n
                    a.nota_count += 1
                d = _to_float(dias)
                if d > 0:
                    a.tempo_sum += d
                    a.tempo_count += 1

            a1 = by_name.get(name_key)
            if not a1:
                a1 = Agg(display_name=fornecedor)
                by_name[name_key] = a1
            _apply(a1)

            if cnpj_key:
                a2 = by_cnpj.get(cnpj_key)
                if not a2:
                    a2 = Agg(display_name=fornecedor)
                    by_cnpj[cnpj_key] = a2
                _apply(a2)

    return by_name, by_cnpj
