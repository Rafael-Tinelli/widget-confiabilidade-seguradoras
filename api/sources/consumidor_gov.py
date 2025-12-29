import csv
import gzip
import io
import os
import re
import unicodedata
from dataclasses import dataclass
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
    s = str(s or "").strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _best_col(
    fieldnames: list[str],
    *,
    must_contain: list[str],
    prefer: dict[str, int],
    avoid: dict[str, int],
) -> str | None:
    scored: list[tuple[int, int, str]] = []
    for h in fieldnames:
        nh = _norm_header(h)
        if not nh:
            continue
        if any(tok not in nh for tok in must_contain):
            continue

        score = 0
        for tok, w in prefer.items():
            if tok in nh:
                score += w
        for tok, w in avoid.items():
            if tok in nh:
                score -= w

        scored.append((score, len(nh), h))

    if not scored:
        return None
    scored.sort(key=lambda x: (-x[0], x[1]))
    return scored[0][2]


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


def aggregate_month_dual_with_stats(
    gz_path: str,
) -> tuple[dict[str, Agg], dict[str, Agg], dict[str, Any]]:
    stats: dict[str, Any] = {
        "delimiter": None,
        "columns": {"fornecedor": None, "cnpj": None},
        "counts": {"rows": 0, "rows_with_valid_cnpj": 0},
    }

    by_name: dict[str, Agg] = {}
    by_cnpj: dict[str, Agg] = {}

    debug = str(os.environ.get("CONSUMIDOR_GOV_DEBUG", "")).strip().lower() in {"1", "true", "yes"}

    with gzip.open(gz_path, "rt", encoding="latin-1", errors="replace") as f:
        sample = f.read(4096)
        f.seek(0)
        delim = _sniff_delimiter(sample)
        stats["delimiter"] = delim
        reader = csv.DictReader(f, delimiter=delim)

        fieldnames = list(reader.fieldnames or [])
        fornecedor_col = _best_col(
            fieldnames,
            must_contain=["fornecedor"],
            prefer={"fornecedor": 10, "nome": 1, "empresa": 1, "razao": 1, "fantasia": 1},
            avoid={"consumidor": 10, "reclamante": 10, "usuario": 10},
        )
        cnpj_col = _best_col(
            fieldnames,
            must_contain=["cnpj"],
            prefer={"fornecedor": 10, "empresa": 5, "prestador": 5, "razao": 2, "nome": 1},
            avoid={"consumidor": 10, "reclamante": 10, "usuario": 10},
        )

        stats["columns"]["fornecedor"] = fornecedor_col
        stats["columns"]["cnpj"] = cnpj_col

        if debug:
            print(f"CG: delimiter={delim} fornecedor_col={fornecedor_col} cnpj_col={cnpj_col}", flush=True)
            print(f"CG: headers_sample={fieldnames[:25]}", flush=True)

        sample_logged = 0

        for row in reader:
            if not isinstance(row, dict):
                continue
            stats["counts"]["rows"] += 1

            fornecedor = ""
            if fornecedor_col and fornecedor_col in row:
                fornecedor = str(row.get(fornecedor_col) or "").strip()
            if not fornecedor:
                fornecedor = str(
                    _pick_col(
                        row,
                        [
                            "fornecedor",
                            "nome_fornecedor",
                            "razao_social",
                            "razao",
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

            cnpj_raw = ""
            if cnpj_col and cnpj_col in row:
                cnpj_raw = str(row.get(cnpj_col) or "")
            if not cnpj_raw:
                cnpj_raw = _pick_col(row, ["cnpj", "cnpj_fornecedor", "cnpjempresa", "cnpj_empresa", "documento"])

            cnpj_key = _digits(cnpj_raw)
            if len(cnpj_key) != 14:
                cnpj_key = ""
            else:
                stats["counts"]["rows_with_valid_cnpj"] += 1

            if debug and sample_logged < 3:
                print(f"CG: sample fornecedor='{fornecedor}' cnpj_raw='{cnpj_raw}' cnpj_key='{cnpj_key}'", flush=True)
                sample_logged += 1

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

    return by_name, by_cnpj, stats


def aggregate_month_dual(gz_path: str) -> tuple[dict[str, Agg], dict[str, Agg]]:
    by_name, by_cnpj, _stats = aggregate_month_dual_with_stats(gz_path)
    return by_name, by_cnpj
