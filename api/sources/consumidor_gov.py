from __future__ import annotations

import csv
import gzip
import hashlib
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

CKAN_BASE = "https://dados.mj.gov.br"
DATASET_ID = "reclamacoes-do-consumidor-gov-br"
UA = {"User-Agent": "Mozilla/5.0 (compatible; SanidaBot/1.0)"}

_SESSION = requests.Session()
_SESSION.trust_env = False


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
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s.strip()


def _digits(x: Any) -> str:
    return re.sub(r"\D+", "", str(x or ""))


def _to_float(x: Any) -> float:
    try:
        s = str(x).strip().replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


def _pick_col(row: dict[str, Any], candidates: list[str]) -> Any:
    for k in candidates:
        if k in row:
            return row.get(k)
    return None


def _bool_from_pt(x: Any) -> bool:
    s = str(x or "").strip().lower()
    return s in {"1", "true", "sim", "s", "yes", "y", "finalizada", "respondida", "resolvida"}


def _sniff_delimiter(sample: str) -> str:
    return ";" if sample.count(";") >= sample.count(",") else ","


def discover_basecompleta_urls(months: int = 12) -> dict[str, str]:
    """
    Descobre URLs mensais da Base Completa do Consumidor.gov via CKAN (package_show).
    Robusto a mudanças de naming: usa scoring e infere YYYY-MM por:
      (1) regex no name/desc/url
      (2) fallback em res['last_modified'] / res['created'] (YYYY-MM)
    """
    api = f"{CKAN_BASE}/api/3/action/package_show"

    def _score(text: str) -> int:
        t = text.lower()
        score = 0
        if "base completa" in t or "base_completa" in t or "basecompleta" in t:
            score += 25
        if "base" in t and "complet" in t:
            score += 12
        if "reclama" in t:
            score += 3
        if "consumidor" in t:
            score += 2
        if any(bad in t for bad in ["dicion", "gloss", "layout", "manual", "metadad", "pdf"]):
            score -= 20
        if any(good in t for good in ["/download/", ".csv", ".zip", ".gz"]):
            score += 2
        return score

    def _infer_ym(text: str, res: dict[str, Any]) -> str | None:
    t = text.lower()
    m = re.search(r"(\d{4})[^\d]?(\d{2})", t)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    for k in ("last_modified", "created"):
        v = res.get(k)
        if isinstance(v, str) and len(v) >= 7 and v[4] == "-":
            # "YYYY-MM-..." -> "YYYY-MM"
            return v[:7]
    return None

    try:
        r = _SESSION.get(api, params={"id": DATASET_ID}, headers=UA, timeout=60)
        r.raise_for_status()
        pkg = r.json().get("result") or {}
        resources = pkg.get("resources") or []

        best_by_ym: dict[str, tuple[int, str]] = {}
        for res in resources:
            if not isinstance(res, dict):
                continue
            url = str(res.get("url") or "").strip()
            name = str(res.get("name") or "").strip()
            desc = str(res.get("description") or "").strip()
            fmt = str(res.get("format") or "").strip()
            text = f"{name} {desc} {url} {fmt}"

            sc = _score(text)
            if sc <= 0 or not url:
                continue

            ym = _infer_ym(text, res)
            if not ym:
                continue

            prev = best_by_ym.get(ym)
            if prev is None or sc > prev[0]:
                best_by_ym[ym] = (sc, url)

        if best_by_ym:
            yms = sorted(best_by_ym.keys(), reverse=True)[:months]
            return {ym: best_by_ym[ym][1] for ym in sorted(yms)}

    except Exception:
        pass

    # fallback HTML (mantém compat, mas normalmente o CKAN já resolve)
    page = f"{CKAN_BASE}/dataset/{DATASET_ID}"
    r2 = _SESSION.get(page, headers=UA, timeout=60)
    r2.raise_for_status()
    html = r2.text

    links = set(re.findall(r'href="([^"]+)"', html, flags=re.I))
    links |= set(re.findall(r'data-url="([^"]+)"', html, flags=re.I))

    urls2: dict[str, str] = {}
    for u in links:
        lu = u.lower()
        if "/download/" not in lu and not any(ext in lu for ext in [".csv", ".zip", ".gz"]):
            continue

        m = re.search(r"(\d{4})[^\d]?(\d{2})", lu)
        if not m:
            continue

        ym = f"{m.group(1)}-{m.group(2)}"
        if u.startswith("/"):
            u = CKAN_BASE + u
        if u.startswith("/"):
            u = CKAN_BASE + u
            if ym not in urls2:
                urls2[ym] = u
        if not urls2:
        return {}

    yms2 = sorted(urls2.keys(), reverse=True)[:months]
    return {ym: urls2[ym] for ym in sorted(yms2)}



def download_csv_to_gz(url: str, out_gz_path: str) -> dict[str, Any]:
    os.makedirs(os.path.dirname(out_gz_path), exist_ok=True)

    with _SESSION.get(url, headers=UA, timeout=180, stream=True) as r:
        r.raise_for_status()
        sha = hashlib.sha256()
        size = 0
        with gzip.open(out_gz_path, "wb") as gz:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                sha.update(chunk)
                gz.write(chunk)
                size += len(chunk)

    return {"url": url, "bytes": size, "sha256": sha.hexdigest(), "generated_at": _utc_now()}


def aggregate_month_dual(gz_path: str) -> tuple[dict[str, Agg], dict[str, Agg]]:
    by_name: dict[str, Agg] = {}
    by_cnpj: dict[str, Agg] = {}

    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f:
        sample = f.read(4096)
        delim = _sniff_delimiter(sample)
        f.seek(0)

        reader = csv.DictReader(f, delimiter=delim)
        for row in reader:
            if not isinstance(row, dict):
                continue

            fornecedor = str(
                _pick_col(
                    row,
                    [
                        "nomeFantasia",
                        "nome_fantasia",
                        "fornecedor",
                        "nome_fornecedor",
                        "Fornecedor",
                        "nomeFantasiaFornecedor",
                    ],
                )
                or ""
            ).strip()

            cnpj = _digits(
                _pick_col(
                    row,
                    [
                        "cnpjFornecedor",
                        "cnpj_fornecedor",
                        "CNPJ",
                        "cnpj",
                        "cnpjFornecedorPrincipal",
                    ],
                )
            )
            cnpj = cnpj if len(cnpj) == 14 else ""

            key_name = _norm_key(fornecedor) if fornecedor else ""
            key_cnpj = cnpj if cnpj else ""

            finalizada = _bool_from_pt(_pick_col(row, ["finalizada", "Finalizada", "status"]))
            respondida = _bool_from_pt(_pick_col(row, ["respondida", "Respondida"]))
            resolvida = _bool_from_pt(_pick_col(row, ["resolvida", "Resolvida", "indicadorResolucao"]))

            nota = _to_float(_pick_col(row, ["notaConsumidor", "nota_consumidor", "nota"]))
            tempo = _to_float(_pick_col(row, ["tempoResposta", "tempo_resposta", "tempoRespostaDias"]))

            def _apply(target: dict[str, Agg], k: str) -> None:
                if not k:
                    return
                if k not in target:
                    target[k] = Agg(display_name=fornecedor)
                a = target[k]
                a.total += 1
                if finalizada:
                    a.finalizadas += 1
                if respondida:
                    a.respondidas += 1
                if resolvida:
                    a.resolvidas_indicador += 1
                if nota > 0:
                    a.nota_sum += nota
                    a.nota_count += 1
                if tempo > 0:
                    a.tempo_sum += tempo
                    a.tempo_count += 1

            _apply(by_name, key_name)
            _apply(by_cnpj, key_cnpj)

    return by_name, by_cnpj


def aggregate_month(gz_path: str) -> dict[str, Agg]:
    by_name, _ = aggregate_month_dual(gz_path)
    return by_name


def to_payload(meta: dict[str, Any], by_name: dict[str, Agg], by_cnpj: dict[str, Agg] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"meta": meta, "by_name_key": {k: v.to_public() for k, v in by_name.items()}}
    if by_cnpj:
        payload["by_cnpj_key"] = {k: v.to_public() for k, v in by_cnpj.items()}
    return payload
