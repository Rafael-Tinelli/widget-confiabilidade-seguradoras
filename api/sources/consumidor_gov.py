import csv
import gzip
import os
import re
import unicodedata
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any

import requests


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

    def merge_raw(self, raw: dict[str, Any]) -> None:
        self.display_name = self.display_name or str(raw.get("display_name") or "")
        self.total += int(raw.get("total") or 0)
        self.finalizadas += int(raw.get("finalizadas") or 0)
        self.respondidas += int(raw.get("respondidas") or 0)
        self.resolvidas_indicador += int(raw.get("resolvidas_indicador") or 0)
        self.nota_sum += float(raw.get("nota_sum") or 0.0)
        self.nota_count += int(raw.get("nota_count") or 0)
        self.tempo_sum += float(raw.get("tempo_sum") or 0.0)
        self.tempo_count += int(raw.get("tempo_count") or 0)

    def to_public(self) -> dict[str, Any]:
        nota_avg = (self.nota_sum / self.nota_count) if self.nota_count else None
        tempo_avg = (self.tempo_sum / self.tempo_count) if self.tempo_count else None
        return {
            "display_name": self.display_name,
            "total": self.total,
            "finalizadas": self.finalizadas,
            "respondidas": self.respondidas,
            "resolvidas_indicador": self.resolvidas_indicador,
            "nota_avg": nota_avg,
            "tempo_resposta_avg_dias": tempo_avg,
        }


@dataclass
class ParseStats:
    """Metadados de parsing por mês (para diagnóstico e guardrails)."""

    delimiter: str = ","
    cnpj_col: str | None = None
    cnpj_col_norm: str | None = None
    rows_total: int = 0
    rows_with_cnpj_valid: int = 0
    unique_cnpj_keys: int = 0


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s.strip()


def _norm_header(s: str) -> str:
    """Normaliza cabeçalhos de CSV para facilitar detecção de colunas."""
    return _norm_key(s).replace(" ", "")


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
    # tenta variantes normalizadas
    for k in candidates:
        nk = _norm_key(k).replace(" ", "")
        for rk in row.keys():
            if _norm_key(rk).replace(" ", "") == nk:
                return row.get(rk)
    return None


def _bool_from_pt(x: Any) -> bool:
    s = str(x or "").strip().lower()
    return s in {"sim", "s", "true", "1", "yes", "y"}


def _sniff_delimiter(sample: str) -> str:
    # tenta heurística simples: ; vs ,
    sc = sample.count(";")
    cc = sample.count(",")
    return ";" if sc >= cc else ","


def _detect_cnpj_col(fieldnames: list[str]) -> str | None:
    """Tenta detectar a coluna de CNPJ do FORNECEDOR de forma robusta.

    Regras:
    - precisa conter 'cnpj'
    - penaliza colunas que pareçam ser do consumidor/reclamante
    - dá preferência a colunas com indícios de fornecedor/empresa
    """

    best: str | None = None
    best_score: float = -1e9

    for name in fieldnames:
        n = _norm_header(name)
        if not n:
            continue
        if "cnpj" not in n:
            continue
        if "cpf" in n:
            continue

        score: float = 10.0

        if any(t in n for t in ("fornecedor", "empresa", "instituicao", "prestador", "razaosocial", "fantasia")):
            score += 6.0
        if any(t in n for t in ("consumidor", "reclamante", "cidadao", "usuario")):
            score -= 8.0
        if any(t in n for t in ("documento", "doc")):
            score -= 2.0
        if n in {"cnpj", "cnpjfornecedor"}:
            score += 2.0

        score -= len(n) / 50.0

        if score > best_score:
            best_score = score
            best = name

    return best


def download_month_csv_gz(url: str, out_path: str) -> dict[str, Any]:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()

    size = 0
    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if not chunk:
                continue
            f.write(chunk)
            size += len(chunk)

    return {"url": url, "bytes": size, "saved_to": out_path, "fetched_at": _utc_now()}


def _aggregate_month_dual_core(gz_path: str) -> tuple[dict[str, Agg], dict[str, Agg], dict[str, Any]]:
    by_name: dict[str, Agg] = {}
    by_cnpj: dict[str, Agg] = {}
    stats = ParseStats()

    with gzip.open(gz_path, "rt", encoding="latin-1", errors="replace") as f:
        sample = f.read(4096)
        f.seek(0)
        delim = _sniff_delimiter(sample)
        stats.delimiter = delim
        reader = csv.DictReader(f, delimiter=delim)

        fieldnames = list(reader.fieldnames or [])
        cnpj_col = _detect_cnpj_col(fieldnames)
        if cnpj_col:
            stats.cnpj_col = cnpj_col
            stats.cnpj_col_norm = _norm_header(cnpj_col)

        for row in reader:
            if not isinstance(row, dict):
                continue
            stats.rows_total += 1

            fornecedor = str(
                _pick_col(
                    row,
                    [
                        "fornecedor",
                        "nome_fornecedor",
                        "nome fornecedor",
                        "nomefornecedor",
                        "razao_social",
                        "razao social",
                        "razaosocial",
                        "nome_fantasia",
                        "nome fantasia",
                        "nomefantasia",
                    ],
                )
                or ""
            ).strip()
            if not fornecedor:
                continue

            if cnpj_col:
                cnpj_raw = row.get(cnpj_col)
            else:
                cnpj_raw = _pick_col(
                    row,
                    [
                        "cnpj_fornecedor",
                        "cnpj fornecedor",
                        "cnpjfornecedor",
                        "cnpj",
                        "cnpj empresa",
                        "cnpj_empresa",
                    ],
                )

            cnpj = _digits(cnpj_raw)
            cnpj_key = cnpj if len(cnpj) == 14 else ""

            name_key = _norm_key(fornecedor)
            if not name_key:
                continue

            def _apply(a: Agg) -> None:
                a.display_name = a.display_name or fornecedor

                status = str(_pick_col(row, ["situacao", "status", "status da reclamacao", "status_reclamacao"]) or "").strip().lower()
                if status:
                    a.total += 1
                    if "final" in status:
                        a.finalizadas += 1

                # Respondida
                resp = str(_pick_col(row, ["respondida", "respondida?", "foi respondida", "resposta"]) or "").strip().lower()
                if resp in {"sim", "s", "1", "true", "t"}:
                    a.respondidas += 1

                # Resolvida
                res = str(
                    _pick_col(
                        row,
                        ["resolvida", "resolvida?", "indicador de resolucao", "resolucao", "resolucao_indicador"],
                    )
                    or ""
                ).strip().lower()
                if res in {"sim", "s", "1", "true", "t"}:
                    a.resolvidas_indicador += 1

                # Nota / Tempo de resposta
                nota_raw = _pick_col(row, ["nota", "nota do consumidor", "avaliacao", "nota_consumidor"])
                try:
                    nota = float(str(nota_raw).replace(",", "."))
                except Exception:
                    nota = None
                if nota is not None:
                    a.nota_sum += nota
                    a.nota_count += 1

                tempo_raw = _pick_col(row, ["tempo de resposta", "tempo_resposta", "tempo resposta", "tempo resposta dias"])
                try:
                    tempo = float(str(tempo_raw).replace(",", "."))
                except Exception:
                    tempo = None
                if tempo is not None:
                    a.tempo_sum += tempo
                    a.tempo_count += 1

            a1 = by_name.get(name_key)
            if not a1:
                a1 = Agg(display_name=fornecedor)
                by_name[name_key] = a1
            _apply(a1)

            if cnpj_key:
                stats.rows_with_cnpj_valid += 1
                a2 = by_cnpj.get(cnpj_key)
                if not a2:
                    a2 = Agg(display_name=fornecedor)
                    by_cnpj[cnpj_key] = a2
                _apply(a2)

    stats.unique_cnpj_keys = len(by_cnpj)
    return by_name, by_cnpj, asdict(stats)


def aggregate_month_dual(gz_path: str) -> tuple[dict[str, Agg], dict[str, Agg]]:
    """Compat: retorna apenas os mapas por nome e por CNPJ."""
    by_name, by_cnpj, _stats = _aggregate_month_dual_core(gz_path)
    return by_name, by_cnpj


def aggregate_month_dual_with_stats(gz_path: str) -> tuple[dict[str, Agg], dict[str, Agg], dict[str, Any]]:
    """Retorna mapas e metadados do parsing (para diagnóstico e guardrails)."""
    return _aggregate_month_dual_core(gz_path)
