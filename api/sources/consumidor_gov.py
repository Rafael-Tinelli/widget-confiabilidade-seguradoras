from __future__ import annotations

import csv
import gzip
import io
import json
import re
import tempfile
import time
import unicodedata
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

CKAN_BASE = "https://dados.mj.gov.br/api/3/action"
CKAN_PACKAGE_SHOW = f"{CKAN_BASE}/package_show"
DATASET_ID = "reclamacoes-do-consumidor-gov-br"

DEFAULT_WINDOW_MONTHS = 12


def _now_iso() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for p in [here, *here.parents]:
        if (p / "api").exists() and (p / "data").exists():
            return p
    return here.parents[1]


def _norm(s: str) -> str:
    s = (s or "").strip().strip('"').strip("'")
    s = s.replace("\ufeff", "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s


def _digits(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    d = re.sub(r"\D+", "", str(s))
    return d or None


def _detect_delimiter(sample: str) -> str:
    first = (sample.splitlines()[:1] or [""])[0]
    cand = [(";", first.count(";")), (",", first.count(",")), ("\t", first.count("\t"))]
    cand.sort(key=lambda x: x[1], reverse=True)
    return cand[0][0] if cand[0][1] > 0 else ";"


def _parse_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    s = str(v).strip().lower()
    if not s:
        return None
    if s in {"s", "sim", "y", "yes", "true", "1"}:
        return True
    if s in {"n", "nao", "não", "no", "false", "0"}:
        return False
    return None


def _parse_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^0-9\.\-]+", "", s)
    try:
        return float(s)
    except Exception:
        return None


def _parse_int(v: Any) -> Optional[int]:
    f = _parse_float(v)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def _name_key(name: str) -> Optional[str]:
    n = _norm(name)
    n = re.sub(r"_+", "_", n).strip("_")
    return n or None


@dataclass
class Agg:
    total: int = 0
    responded: int = 0
    resolved: int = 0
    rating_sum: float = 0.0
    rating_count: int = 0
    response_days_sum: float = 0.0
    response_days_count: int = 0
    name_counts: Dict[str, int] = field(default_factory=dict)

    def update(
        self,
        *,
        display_name: Optional[str],
        responded: Optional[bool],
        resolved: Optional[bool],
        rating: Optional[float],
        response_days: Optional[int],
    ) -> None:
        self.total += 1
        if responded is True:
            self.responded += 1
        if resolved is True:
            self.resolved += 1
        if rating is not None:
            self.rating_sum += float(rating)
            self.rating_count += 1
        if response_days is not None:
            self.response_days_sum += float(response_days)
            self.response_days_count += 1
        if display_name:
            self.name_counts[display_name] = self.name_counts.get(display_name, 0) + 1

    def best_display_name(self, fallback: str) -> str:
        if not self.name_counts:
            return fallback
        return max(self.name_counts.items(), key=lambda x: x[1])[0]

    def to_public(self, display_name: str) -> Dict[str, Any]:
        resp_rate = self.responded / self.total if self.total else 0.0
        res_rate = self.resolved / self.total if self.total else 0.0
        sat = (self.rating_sum / self.rating_count) if self.rating_count else None
        avg_days = (self.response_days_sum / self.response_days_count) if self.response_days_count else None
        return {
            "display_name": display_name,
            "complaints_total": self.total,
            "response_rate": round(resp_rate, 6),
            "resolution_rate": round(res_rate, 6),
            "satisfaction_avg": (round(float(sat), 6) if sat is not None else None),
            "avg_response_days": (round(float(avg_days), 6) if avg_days is not None else None),
        }


def _download_to_temp(url: str, timeout: int = 180) -> Path:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
    tmp_path = Path(tmp.name)
    tmp.close()

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    }

    with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
        r.raise_for_status()
        with tmp_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

    return tmp_path


def _open_as_text(path: Path) -> io.TextIOBase:
    head = path.read_bytes()[:8]

    if zipfile.is_zipfile(path):
        z = zipfile.ZipFile(path)
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise RuntimeError("ZIP não contém CSV.")
        fbin = z.open(names[0], "r")
        wrapper = io.TextIOWrapper(fbin, encoding="utf-8", errors="replace", newline="")
        wrapper._zip_ref = z  # type: ignore[attr-defined]
        return wrapper

    if len(head) >= 2 and head[0] == 0x1F and head[1] == 0x8B:
        fbin = gzip.open(path, "rb")
        return io.TextIOWrapper(fbin, encoding="utf-8", errors="replace", newline="")

    return path.open("r", encoding="utf-8", errors="replace", newline="")


def _ckan_package() -> Dict[str, Any]:
    r = requests.get(CKAN_PACKAGE_SHOW, params={"id": DATASET_ID}, timeout=60)
    r.raise_for_status()
    payload = r.json()
    if not payload.get("success"):
        raise RuntimeError(f"CKAN package_show falhou: {payload}")
    return payload["result"]


def _extract_ym_from_text(txt: str) -> Optional[str]:
    if not txt:
        return None
    m = re.search(r"\b(20\d{2})[-_/\.](0[1-9]|1[0-2])\b", txt)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.search(r"\b(20\d{2})(0[1-9]|1[0-2])\b", txt)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return None


def _list_monthly_resources(pkg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for res in pkg.get("resources", []):
        url = res.get("url") or ""
        name = res.get("name") or res.get("title") or ""
        fmt = (res.get("format") or "").lower()

        if "csv" not in fmt and not url.lower().endswith((".csv", ".csv.gz", ".zip", ".gz")):
            continue

        ym = (
            _extract_ym_from_text(name)
            or _extract_ym_from_text(url)
            or _extract_ym_from_text(res.get("description") or "")
            or _extract_ym_from_text(res.get("last_modified") or "")
            or _extract_ym_from_text(res.get("created") or "")
        )
        if not ym:
            continue

        cur = out.get(ym)
        if not cur:
            out[ym] = res
            continue

        def _ts(r: Dict[str, Any]) -> str:
            return str(r.get("last_modified") or r.get("created") or "")

        if _ts(res) > _ts(cur):
            out[ym] = res

    return out


def _latest_months(months: List[str], window_months: int) -> List[str]:
    months_sorted = sorted(months)
    return months_sorted[-window_months:]


ALIASES = {
    "supplier_name": [
        "nome_fantasia",
        "nomefantasia",
        "fornecedor",
        "nome_fornecedor",
        "nomefantasiafornecedor",
        "razao_social",
        "razaosocial",
    ],
    "supplier_cnpj": [
        "cnpj",
        "cnpjdofornecedor",
        "cnpj_do_fornecedor",
        "cnpjfornecedor",
        "cnpj_fornecedor",
        "cpf_cnpj",
        "cpfcnpj",
    ],
    "responded": [
        "respondida",
        "respondida_pelo_fornecedor",
        "respondidapelo_fornecedor",
        "fornecedor_respondeu",
        "resposta_fornecedor",
    ],
    "resolved": [
        "resolvida",
        "reclamacao_resolvida",
        "reclamacaoresolvida",
        "solucionada",
        "solucao",
    ],
    "rating": [
        "nota",
        "avaliacao",
        "avaliacao_reclamacao",
        "avaliacaoreclamacao",
        "nota_do_consumidor",
        "notadoconsumidor",
    ],
    "response_days": [
        "tempo_resposta",
        "temposresposta",
        "tempo_resposta_em_dias",
        "tempoderesposta",
        "dias_resposta",
    ],
}


def _header_norm_map(fieldnames: List[str]) -> Dict[str, str]:
    return {_norm(h): h for h in (fieldnames or []) if h}


def _get(row: Dict[str, Any], hdrmap: Dict[str, str], aliases: List[str]) -> Optional[str]:
    for a in aliases:
        k = hdrmap.get(_norm(a))
        if k and k in row:
            v = row.get(k)
            if v is not None and str(v).strip() != "":
                return str(v)
    for a in aliases:
        na = _norm(a)
        for hk_norm, hk in hdrmap.items():
            if na in hk_norm:
                v = row.get(hk)
                if v is not None and str(v).strip() != "":
                    return str(v)
    return None


def build_consumidor_gov_agg(
    *,
    window_months: int = DEFAULT_WINDOW_MONTHS,
    raw_dir: Optional[Path] = None,
    out_path: Optional[Path] = None,
) -> Path:
    root = _repo_root()
    raw_dir = raw_dir or (root / "data" / "raw" / "consumidor_gov")
    out_path = out_path or (root / "data" / "derived" / "consumidor_gov" / "consumidor_gov_agg_latest.json")
    raw_dir.mkdir(parents=True, exist_ok=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    pkg = _ckan_package()
    monthly = _list_monthly_resources(pkg)
    if not monthly:
        raise RuntimeError("Nenhum recurso mensal CSV identificado no dataset.")

    months = _latest_months(list(monthly.keys()), window_months)

    by_name: Dict[str, Agg] = {}
    by_cnpj: Dict[str, Agg] = {}

    for ym in months:
        res = monthly[ym]
        url = res.get("url")
        if not url:
            continue

        cache_path = raw_dir / f"{ym}.bin"
        if not cache_path.exists() or cache_path.stat().st_size == 0:
            tmp = _download_to_temp(url)
            try:
                cache_path.write_bytes(tmp.read_bytes())
            finally:
                try:
                    tmp.unlink()
                except Exception:
                    pass

        ftxt = _open_as_text(cache_path)
        try:
            sample = ftxt.read(65536)
            delim = _detect_delimiter(sample)
            ftxt.seek(0)

            reader = csv.DictReader(ftxt, delimiter=delim)
            hdrmap = _header_norm_map(reader.fieldnames or [])

            for row in reader:
                supplier_name = _get(row, hdrmap, ALIASES["supplier_name"]) or ""
                supplier_cnpj = _get(row, hdrmap, ALIASES["supplier_cnpj"])

                responded = _parse_bool(_get(row, hdrmap, ALIASES["responded"]))
                resolved = _parse_bool(_get(row, hdrmap, ALIASES["resolved"]))
                rating = _parse_float(_get(row, hdrmap, ALIASES["rating"]))
                resp_days = _parse_int(_get(row, hdrmap, ALIASES["response_days"]))

                nk = _name_key(supplier_name)
                if nk:
                    by_name.setdefault(nk, Agg()).update(
                        display_name=supplier_name,
                        responded=responded,
                        resolved=resolved,
                        rating=rating,
                        response_days=resp_days,
                    )

                cnpj = _digits(supplier_cnpj)
                if cnpj:
                    if len(cnpj) <= 14:
                        cnpj = cnpj.zfill(14)
                    by_cnpj.setdefault(cnpj, Agg()).update(
                        display_name=supplier_name or cnpj,
                        responded=responded,
                        resolved=resolved,
                        rating=rating,
                        response_days=resp_days,
                    )
        finally:
            try:
                z = getattr(ftxt, "_zip_ref", None)
                ftxt.close()
                if z:
                    z.close()
            except Exception:
                pass

    by_name_public: Dict[str, Any] = {}
    for k, agg in by_name.items():
        disp = agg.best_display_name(k)
        by_name_public[k] = agg.to_public(disp)

    by_cnpj_public: Dict[str, Any] = {}
    for k, agg in by_cnpj.items():
        disp = agg.best_display_name(k)
        by_cnpj_public[k] = agg.to_public(disp) | {"cnpj": k}

    payload = {
        "meta": {
            "as_of": _now_iso(),
            "window_months": window_months,
            "months": months,
            "dataset_id": DATASET_ID,
            "raw_cache_dir": str(raw_dir).replace(str(root), "").lstrip("/"),
        },
        "by_name_key": by_name_public,
        "by_cnpj_key": by_cnpj_public,
        "stats": {
            "companies_by_name": len(by_name_public),
            "companies_by_cnpj": len(by_cnpj_public),
        },
    }

    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def main() -> None:
    out = build_consumidor_gov_agg()
    print(f"Consumidor.gov agregado gerado em: {out}")


if __name__ == "__main__":
    main()
