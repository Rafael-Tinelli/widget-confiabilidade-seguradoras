# api/sources/consumidor_gov.py
from __future__ import annotations

import csv
import gzip
import os
import re
import unicodedata
from dataclasses import asdict, dataclass, field
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
    # Apenas para diagnóstico: top colunas candidatas e quantos CNPJs válidos (14 dígitos) apareceram no sample
    cnpj_candidates_top: list[list[Any]] = field(default_factory=list)  # [["Coluna", 123], ...]
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
    # não converter para int: preserva zeros à esquerda
    return re.sub(r"\D+", "", str(s or ""))


def _sniff_delimiter(sample: str) -> str:
    # heurística simples: ; vs ,
    sc = sample.count(";")
    cc = sample.count(",")
    return ";" if sc >= cc else ","


def _pick_col(row: dict[str, Any], candidates: list[str]) -> Any:
    # 1) tentativa direta
    for k in candidates:
        if k in row:
            return row.get(k)

    # 2) tentativa via normalização de chaves
    cand_norm = {_norm_header(k) for k in candidates}
    for rk in row.keys():
        if _norm_header(rk) in cand_norm:
            return row.get(rk)

    return None


def _header_score_for_cnpj_col(name: str) -> float:
    n = _norm_header(name)
    if not n:
        return -1e9
    if "cnpj" not in n:
        return -1e9
    if "cpf" in n:
        return -1e9

    score = 10.0

    # Preferir fornecedor/empresa, penalizar consumidor/reclamante
    if any(t in n for t in ("fornecedor", "empresa", "instituicao", "prestador", "razaosocial", "fantasia")):
        score += 6.0
    if any(t in n for t in ("consumidor", "reclamante", "cidadao", "usuario")):
        score -= 8.0

    # Nomes muito genéricos são OK, mas sem super bônus
    if n in {"cnpj", "cnpjfornecedor", "cnpjempresa", "cnpj_fornecedor", "cnpj_empresa"}:
        score += 2.0

    # preferir cabeçalhos “curtos”
    score -= len(n) / 50.0
    return score


def _detect_cnpj_col(fieldnames: list[str], sample_rows: list[dict[str, Any]]) -> tuple[str | None, list[list[Any]]]:
    """Detecta coluna de CNPJ combinando heurística por cabeçalho + evidência por valores (sample)."""
    candidates = [fn for fn in fieldnames if "cnpj" in _norm_header(fn) and "cpf" not in _norm_header(fn)]
    if not candidates:
        return None, []

    valid_counts: dict[str, int] = {c: 0 for c in candidates}
    for row in sample_rows:
        for c in candidates:
            v = row.get(c)
            if len(_digits(v)) == 14:
                valid_counts[c] += 1

    def _score(c: str) -> float:
        # Peso de valor: até 200 linhas de evidência já “decidem” bastante
        vc = valid_counts.get(c, 0)
        return _header_score_for_cnpj_col(c) + min(vc, 200) * 0.25

    best = max(candidates, key=_score)

    top = sorted(candidates, key=lambda c: valid_counts.get(c, 0), reverse=True)[:5]
    candidates_top = [[c, int(valid_counts.get(c, 0))] for c in top]
    return best, candidates_top


def download_month_csv_gz(url: str, out_path: str) -> dict[str, Any]:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    timeout = float(os.environ.get("CONSUMIDOR_GOV_HTTP_TIMEOUT", "120"))
    headers = {"User-Agent": os.environ.get("CONSUMIDOR_GOV_UA", "widget-confiabilidade-seguradoras/1.0")}

    r = requests.get(url, stream=True, timeout=timeout, headers=headers)
    r.raise_for_status()

    size = 0
    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if not chunk:
                continue
            f.write(chunk)
            size += len(chunk)

    return {"url": url, "bytes": size, "saved_to": out_path, "fetched_at": _utc_now()}


FORNECEDOR_COLS = [
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
]

STATUS_COLS = ["situacao", "status", "status da reclamacao", "status_reclamacao"]
RESPONDIDA_COLS = ["respondida", "respondida?", "foi respondida", "resposta"]
RESOLVIDA_COLS = ["resolvida", "resolvida?", "indicador de resolucao", "resolucao", "resolucao_indicador"]
NOTA_COLS = ["nota", "nota do consumidor", "avaliacao", "nota_consumidor"]
TEMPO_COLS = ["tempo de resposta", "tempo_resposta", "tempo resposta", "tempo resposta dias"]


def _aggregate_month_dual_core(gz_path: str) -> tuple[dict[str, Agg], dict[str, Agg], dict[str, Any]]:
    by_name: dict[str, Agg] = {}
    by_cnpj: dict[str, Agg] = {}
    stats = ParseStats()

    sample_limit = int(os.environ.get("CONSUMIDOR_GOV_CNPJ_SAMPLE_ROWS", "400") or 400)

    with gzip.open(gz_path, "rt", encoding="latin-1", errors="replace") as f:
        sample = f.read(4096)
        f.seek(0)

        delim = _sniff_delimiter(sample)
        stats.delimiter = delim
        reader = csv.DictReader(f, delimiter=delim)

        fieldnames = list(reader.fieldnames or [])
        buffer: list[dict[str, Any]] = []

        for i, row in enumerate(reader):
            if not isinstance(row, dict):
                continue
            buffer.append(row)
            if i + 1 >= sample_limit:
                break

        cnpj_col, candidates_top = _detect_cnpj_col(fieldnames, buffer)
        if cnpj_col:
            stats.cnpj_col = cnpj_col
            stats.cnpj_col_norm = _norm_header(cnpj_col)
        stats.cnpj_candidates_top = candidates_top

        if str(os.environ.get("CONSUMIDOR_GOV_DEBUG", "")).strip().lower() in {"1", "true", "yes"}:
            print(
                f"Consumidor.gov: delimiter={delim!r} cnpj_col={cnpj_col!r} candidates_top={candidates_top!r}",
                flush=True,
            )

        def _apply(agg: Agg, fornecedor: str, row: dict[str, Any]) -> None:
            agg.display_name = agg.display_name or fornecedor

            # Total: conta toda linha com fornecedor identificável
            agg.total += 1

            status = str(_pick_col(row, STATUS_COLS) or "").strip().lower()
            if status and "final" in status:
                agg.finalizadas += 1

            resp = str(_pick_col(row, RESPONDIDA_COLS) or "").strip().lower()
            if resp in {"sim", "s", "1", "true", "t", "yes", "y"}:
                agg.respondidas += 1

            res = str(_pick_col(row, RESOLVIDA_COLS) or "").strip().lower()
            if res in {"sim", "s", "1", "true", "t", "yes", "y"}:
                agg.resolvidas_indicador += 1

            nota_raw = _pick_col(row, NOTA_COLS)
            try:
                nota = float(str(nota_raw).strip().replace(",", "."))
            except Exception:
                nota = None
            if nota is not None:
                agg.nota_sum += nota
                agg.nota_count += 1

            tempo_raw = _pick_col(row, TEMPO_COLS)
            try:
                tempo = float(str(tempo_raw).strip().replace(",", "."))
            except Exception:
                tempo = None
            if tempo is not None:
                agg.tempo_sum += tempo
                agg.tempo_count += 1

        def _process_row(row: dict[str, Any]) -> None:
            fornecedor = str(_pick_col(row, FORNECEDOR_COLS) or "").strip()
            if not fornecedor:
                return

            name_key = _norm_key(fornecedor)
            if not name_key:
                return

            a1 = by_name.get(name_key)
            if not a1:
                a1 = Agg(display_name=fornecedor)
                by_name[name_key] = a1
            _apply(a1, fornecedor, row)

            cnpj_raw = row.get(cnpj_col) if cnpj_col else _pick_col(
                row,
                [
                    "cnpj_fornecedor",
                    "cnpj fornecedor",
                    "cnpjfornecedor",
                    "cnpj empresa",
                    "cnpj_empresa",
                    "cnpj",
                ],
            )
            cnpj = _digits(cnpj_raw)
            if len(cnpj) == 14:
                stats.rows_with_cnpj_valid += 1
                a2 = by_cnpj.get(cnpj)
                if not a2:
                    a2 = Agg(display_name=fornecedor)
                    by_cnpj[cnpj] = a2
                _apply(a2, fornecedor, row)

        # processa buffer
        for row in buffer:
            stats.rows_total += 1
            _process_row(row)

        # continua stream após o buffer
        for row in reader:
            if not isinstance(row, dict):
                continue
            stats.rows_total += 1
            _process_row(row)

    stats.unique_cnpj_keys = len(by_cnpj)
    return by_name, by_cnpj, asdict(stats)


def aggregate_month_dual(gz_path: str) -> tuple[dict[str, Agg], dict[str, Agg]]:
    """Compat: retorna apenas os mapas por nome e por CNPJ."""
    by_name, by_cnpj, _stats = _aggregate_month_dual_core(gz_path)
    return by_name, by_cnpj


def aggregate_month_dual_with_stats(gz_path: str) -> tuple[dict[str, Agg], dict[str, Agg], dict[str, Any]]:
    """Retorna mapas e metadados do parsing (para diagnóstico e guardrails)."""
    return _aggregate_month_dual_core(gz_path)
