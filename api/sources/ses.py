from __future__ import annotations

import csv
import io
import re
import tempfile
import time
import unicodedata
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import requests

SES_URL = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"
SES_BASE_ZIP_URL = "https://www2.susep.gov.br/download/estatisticas/BaseCompleta.zip"

DEBUG_DIR = Path("ses_debug")
_MAX_SAMPLE_ROWS = 50_000

# Heurísticas de colunas (headers normalizados)
_PREMIUM_HINTS = [
    "premio_direto",
    "premio_retido",
    "premio_emitido",
    "premio_ganho",
    "premio",
    "premios",
]
_CLAIMS_HINTS = [
    "sinistro_direto",
    "sinistro_retido",
    "sinistro_ocorrido",
    "sinistro",
    "sinistros",
]


@dataclass(frozen=True)
class SesExtractionMeta:
    zip_url: str
    cias_file: str
    seguros_file: str
    period_from: str
    period_to: str


# ----------------------------
# Utilitários
# ----------------------------

def _ensure_debug_dir() -> None:
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)


def _norm(s: str) -> str:
    s = (s or "").strip().strip('"').strip("'").replace("\ufeff", "")
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


def _read_head(path: Path, n: int = 4096) -> bytes:
    with path.open("rb") as f:
        return f.read(n)


def _is_zip_signature(head8: bytes) -> bool:
    return (
        head8.startswith(b"PK\x03\x04")
        or head8.startswith(b"PK\x05\x06")
        or head8.startswith(b"PK\x07\x08")
    )


def _classify_payload(head: bytes) -> str:
    txt = head.decode("utf-8", errors="ignore").lower().strip()
    if txt.startswith("<!doctype html") or txt.startswith("<html") or txt.startswith("<"):
        markers = ["cloudflare", "access denied", "forbidden", "captcha", "turnstile"]
        hit = [m for m in markers if m in txt]
        return f"HTML (provável bloqueio/erro). markers={hit[:5]}"
    if txt.startswith("{") or txt.startswith("["):
        return "JSON (provável erro)"
    if b";" in head or b"," in head:
        return "Provável CSV/Texto"
    return "Binário desconhecido (não-ZIP)"


def _validate_zip_or_raise(zip_path: Path, url_hint: str) -> None:
    size = zip_path.stat().st_size if zip_path.exists() else 0
    if size == 0:
        raise RuntimeError("Arquivo baixado veio com 0 bytes (timeout/bloqueio).")

    head = _read_head(zip_path, 4096)
    if _is_zip_signature(head[:8]) and zipfile.is_zipfile(zip_path):
        return

    _ensure_debug_dir()
    ts = int(time.time())
    (DEBUG_DIR / f"invalid_download_head_{ts}.bin").write_bytes(head)
    snippet = head[:1200].decode("utf-8", errors="ignore")
    kind = _classify_payload(head)

    raise RuntimeError(
        "Arquivo baixado não é ZIP válido. "
        f"kind={kind} size={size} url={url_hint}\n"
        f"snippet:\n{snippet}"
    )


def _download_to_temp(url: str, timeout: int = 180) -> Path:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    tmp_path = Path(tmp.name)
    tmp.close()

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/zip,application/octet-stream,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Referer": SES_URL,
    }

    with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
        r.raise_for_status()
        with tmp_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

    _validate_zip_or_raise(tmp_path, url)
    return tmp_path


def _discover_zip_url_from_page() -> Optional[str]:
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        }
        html = requests.get(SES_URL, timeout=60, headers=headers).text
    except Exception:
        return None

    hits = re.findall(r'href="([^"]+\.zip)"', html, flags=re.IGNORECASE)
    if not hits:
        return None

    cand = hits[-1]
    if cand.startswith("http"):
        return cand
    if cand.startswith("/"):
        return f"https://www2.susep.gov.br{cand}"
    return f"https://www2.susep.gov.br/{cand.lstrip('./')}"


def _download_zip() -> Tuple[Path, str]:
    try:
        return _download_to_temp(SES_BASE_ZIP_URL), SES_BASE_ZIP_URL
    except Exception as e1:
        print(f"SES: falha ao baixar BaseCompleta.zip (URL canônica): {e1}")

    url = _discover_zip_url_from_page()
    if not url:
        raise RuntimeError("SES: não foi possível descobrir URL do ZIP a partir do HTML.")
    return _download_to_temp(url), url


def _parse_brl_number(raw: Any) -> float:
    if raw is None:
        return 0.0
    s = str(raw).strip()
    if not s:
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^0-9\.\-]+", "", s)
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_ym(value: Any) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    m = re.search(r"\b(\d{4})\D?(\d{2})\b", s)
    if m:
        return int(m.group(1)) * 100 + int(m.group(2))

    m = re.search(r"\b(\d{2})\D+(\d{4})\b", s)
    if m:
        mm = int(m.group(1))
        yy = int(m.group(2))
        if 1 <= mm <= 12:
            return yy * 100 + mm
    return None


def _ym_to_iso_01(ym: int) -> str:
    return f"{ym // 100:04d}-{ym % 100:02d}-01"


def _month_index(ym: int) -> int:
    return (ym // 100) * 12 + (ym % 100) - 1


def _from_month_index(idx: int) -> int:
    yy = idx // 12
    mm = idx % 12 + 1
    return yy * 100 + mm


def _detect_dialect(z: zipfile.ZipFile, fname: str) -> tuple[str, str]:
    with z.open(fname) as f:
        head = f.read(65536)

    encoding = "utf-8-sig" if head.startswith(b"\xef\xbb\xbf") else "latin-1"
    txt = head.decode(encoding, errors="ignore")
    first = (txt.splitlines()[:1] or [""])[0]
    delims = [";", ",", "\t", "|"]
    delim = max(delims, key=lambda d: first.count(d))
    if first.count(delim) == 0:
        delim = ";"
    return encoding, delim


def _iter_rows(z: zipfile.ZipFile, fname: str) -> Iterable[list[str]]:
    enc, delim = _detect_dialect(z, fname)
    with z.open(fname) as f:
        wrapper = io.TextIOWrapper(f, encoding=enc, errors="replace", newline="")
        reader = csv.reader(wrapper, delimiter=delim)
        for row in reader:
            yield row


def _read_header(z: zipfile.ZipFile, fname: str) -> list[str]:
    it = _iter_rows(z, fname)
    return next(it, [])


def _find_idx_exact(h_norm: list[str], keys: list[str]) -> Optional[int]:
    for k in keys:
        nk = _norm(k)
        if nk in h_norm:
            return h_norm.index(nk)
    return None


def _find_idx_contains(h_norm: list[str], needle: str) -> Optional[int]:
    n = _norm(needle)
    for i, col in enumerate(h_norm):
        if n in col:
            return i
    return None


def _pick_best_csv(
    z: zipfile.ZipFile,
    candidates: list[str],
    required_groups: list[list[str]],
) -> str:
    best = None
    best_score = -1

    for fname in candidates:
        hdr = _read_header(z, fname)
        if not hdr:
            continue
        h = [_norm(x) for x in hdr]
        score = 0
        for group in required_groups:
            if any(_norm(k) in h for k in group):
                score += 1
        if score > best_score:
            best = fname
            best_score = score

    if not best or best_score < len(required_groups):
        raise RuntimeError(f"Não foi possível identificar CSV correto. candidates={candidates}")
    return best


def _select_best_numeric_column(sample_rows: list[list[str]], idx_candidates: list[int]) -> Optional[int]:
    if not idx_candidates:
        return None
    sums = {i: 0.0 for i in idx_candidates}
    nnz = {i: 0 for i in idx_candidates}

    for row in sample_rows:
        for i in idx_candidates:
            if i >= len(row):
                continue
            v = _parse_brl_number(row[i])
            if v != 0.0:
                nnz[i] += 1
                sums[i] += abs(v)

    idx_candidates.sort(key=lambda i: (sums.get(i, 0.0), nnz.get(i, 0)), reverse=True)
    return idx_candidates[0] if idx_candidates else None


# ----------------------------
# Extração principal
# ----------------------------

def extract_ses_master_and_financials(
    zip_url: Optional[str] = None,
) -> Tuple[SesExtractionMeta, Dict[str, Dict[str, Any]]]:
    """
    Extrai:
      - Ses_cias.csv: id, nome, cnpj
      - Ses_seguros.csv: prêmios e sinistros (rolling_12m)

    Nota: sem Playwright. Isso resolve o seu cenário do Codex (403 ao baixar Chromium).
    """
    _ = zip_url  # compatibilidade

    zip_path, used_url = _download_zip()
    try:
        with zipfile.ZipFile(zip_path) as z:
            csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if not csvs:
                raise RuntimeError("ZIP do SES não contém CSV.")

            cias_candidates = [n for n in csvs if "ses_cias" in n.lower()] or csvs
            seg_candidates = [n for n in csvs if "ses_seguros" in n.lower() or "seguros" in n.lower()] or csvs

            cias = _pick_best_csv(
                z,
                cias_candidates,
                required_groups=[["coenti", "cod_enti", "cod_cia", "co_enti"], ["noenti", "nome"]],
            )

            seguros = _pick_best_csv(
                z,
                seg_candidates,
                required_groups=[["coenti", "cod_enti", "cod_cia", "co_enti"], ["damesano", "anomes", "competencia"], ["premio"]],
            )

            hdr_cias = _read_header(z, cias)
            hdr_seg = _read_header(z, seguros)
            h_cias = [_norm(x) for x in hdr_cias]
            h_seg = [_norm(x) for x in hdr_seg]

            id_i = _find_idx_exact(h_cias, ["coenti", "cod_enti", "cod_cia", "co_enti"])
            nm_i = _find_idx_exact(h_cias, ["noenti", "nome", "nome_cia"])
            cn_i = _find_idx_contains(h_cias, "cnpj")

            sid_i = _find_idx_exact(h_seg, ["coenti", "cod_enti", "cod_cia", "co_enti"])
            ym_i = _find_idx_exact(h_seg, ["damesano", "anomes", "competencia", "damesaano"])
            if sid_i is None or ym_i is None:
                raise RuntimeError(f"Colunas obrigatórias ausentes em '{seguros}'.")

            prem_idxs = [i for i, col in enumerate(h_seg) if ("premio" in col) or any(_norm(x) in col for x in _PREMIUM_HINTS)]
            sin_idxs = [i for i, col in enumerate(h_seg) if ("sinistro" in col) or any(_norm(x) in col for x in _CLAIMS_HINTS)]

            max_ym = 0
            sample: list[list[str]] = []
            rows1 = _iter_rows(z, seguros)
            next(rows1, None)
            for row in rows1:
                if ym_i < len(row):
                    ym = _parse_ym(row[ym_i])
                    if ym and ym > max_ym:
                        max_ym = ym
                if len(sample) < _MAX_SAMPLE_ROWS:
                    sample.append(row)

            if max_ym <= 0:
                raise RuntimeError("Não foi possível determinar max_ym (competência).")

            pr_i = _select_best_numeric_column(sample, prem_idxs)
            sn_i = _select_best_numeric_column(sample, sin_idxs) if sin_idxs else None
            if pr_i is None:
                raise RuntimeError("Não foi possível identificar coluna de prêmios no Ses_seguros.csv.")

            start_ym = _from_month_index(_month_index(max_ym) - 11)

            print(
                "SES: colunas selecionadas | "
                f"sid={h_seg[sid_i]} ym={h_seg[ym_i]} premium={h_seg[pr_i]} "
                f"claims={(h_seg[sn_i] if sn_i is not None else None)} "
                f"window={start_ym}-{max_ym}"
            )

            companies: Dict[str, Dict[str, Any]] = {}
            if id_i is not None and nm_i is not None:
                rows_c = _iter_rows(z, cias)
                next(rows_c, None)
                for row in rows_c:
                    if id_i >= len(row) or nm_i >= len(row):
                        continue
                    sid = _digits(row[id_i])
                    if not sid:
                        continue
                    sid = sid.zfill(6)
                    cnpj = None
                    if cn_i is not None and cn_i < len(row):
                        d = _digits(row[cn_i])
                        if d:
                            cnpj = d.zfill(14) if len(d) <= 14 else d
                    companies[sid] = {"name": (row[nm_i] or "").strip(), "cnpj": cnpj}

            agg: Dict[str, Dict[str, float]] = {}
            rows2 = _iter_rows(z, seguros)
            next(rows2, None)
            for row in rows2:
                if sid_i >= len(row) or ym_i >= len(row) or pr_i >= len(row):
                    continue
                ym = _parse_ym(row[ym_i])
                if not ym or ym < start_ym or ym > max_ym:
                    continue
                sid = _digits(row[sid_i])
                if not sid:
                    continue
                sid = sid.zfill(6)

                prem = _parse_brl_number(row[pr_i])
                sin = _parse_brl_number(row[sn_i]) if (sn_i is not None and sn_i < len(row)) else 0.0

                bucket = agg.setdefault(sid, {"p": 0.0, "c": 0.0})
                bucket["p"] += prem
                bucket["c"] += sin

            out: Dict[str, Dict[str, Any]] = {}
            for sid, v in agg.items():
                if v["p"] <= 0:
                    continue
                base = companies.get(sid) or {"name": f"SES_{sid}", "cnpj": None}
                out[sid] = {
                    "name": base.get("name") or f"SES_{sid}",
                    "cnpj": base.get("cnpj"),
                    "premiums": round(v["p"], 2),
                    "claims": round(v["c"], 2),
                }

            meta = SesExtractionMeta(
                used_url,
                cias,
                seguros,
                _ym_to_iso_01(start_ym),
                _ym_to_iso_01(max_ym),
            )
            return meta, out

    finally:
        try:
            zip_path.unlink()
        except Exception:
            pass
