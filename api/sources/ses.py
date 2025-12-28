# api/sources/ses.py
from __future__ import annotations

import csv
import gzip
import io
import json
import os
import re
import warnings
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from requests.exceptions import RequestException, SSLError
from urllib3.exceptions import InsecureRequestWarning

SES_HOME_DEFAULT = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}

# SUSEP: tentamos primeiro respeitar o ambiente (proxy/rotas), e depois direto.
_SESSION_ENV = requests.Session()  # trust_env=True (default)
_SESSION_DIRECT = requests.Session()
_SESSION_DIRECT.trust_env = False

ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = ROOT / "data" / "raw"
DATA_SNAPSHOTS = ROOT / "data" / "snapshots"


@dataclass(frozen=True)
class SesMeta:
    zip_url: str
    cias_file: str
    seguros_file: str
    period_from: str
    period_to: str


def _digits(x: Any) -> str:
    return re.sub(r"\D+", "", str(x or ""))


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _parse_brl_num(x: Any) -> float:
    s = str(x or "").strip()
    if not s:
        return 0.0
    s = re.sub(r"[^0-9,.\-]", "", s)
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _parse_ym(v: Any) -> tuple[int, int] | None:
    s = str(v or "").strip()
    if not s:
        return None
    d = _digits(s)
    if len(d) >= 6:
        y = int(d[0:4])
        m = int(d[4:6])
        if 2000 <= y <= 2100 and 1 <= m <= 12:
            return y, m
    m2 = re.search(r"\b(20\d{2})\D{0,3}([01]\d)\b", s)
    if m2:
        y = int(m2.group(1))
        mo = int(m2.group(2))
        if 1 <= mo <= 12:
            return y, mo
    return None


def _ym_to_int(ym: tuple[int, int]) -> int:
    y, m = ym
    return y * 12 + m


def _add_months(ym: tuple[int, int], delta: int) -> tuple[int, int]:
    base = _ym_to_int(ym) + delta
    y = base // 12
    m = base % 12
    if m == 0:
        y -= 1
        m = 12
    return y, m


def _ym_to_date_str(ym: tuple[int, int]) -> str:
    y, m = ym
    return f"{y:04d}-{m:02d}-01"


def _allow_insecure_ssl() -> bool:
    return str(os.environ.get("SES_ALLOW_INSECURE_SSL", "")).strip() in {"1", "true", "True", "yes", "YES"}


def _request(url: str, timeout: int) -> requests.Response:
    """
    Tenta obter a URL respeitando o ambiente (proxy/rotas) e, se falhar, tenta direto.
    Se SSL falhar e SES_ALLOW_INSECURE_SSL=1, tenta novamente com verify=False.
    """
    allow_insecure = _allow_insecure_ssl()
    last: Exception | None = None

    sessions = [_SESSION_ENV, _SESSION_DIRECT]

    for sess in sessions:
        try:
            r = sess.get(url, headers=UA, timeout=timeout, allow_redirects=True)
            r.raise_for_status()
            return r
        except SSLError as exc:
            last = exc
            if allow_insecure:
                # Evita spam de warning no log quando verify=False estiver habilitado por env.
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", InsecureRequestWarning)
                    try:
                        print(
                            "WARN: SES SSL verify falhou; tentando verify=False (SES_ALLOW_INSECURE_SSL=1).",
                            flush=True,
                        )
                        r2 = sess.get(url, headers=UA, timeout=timeout, allow_redirects=True, verify=False)
                        r2.raise_for_status()
                        return r2
                    except Exception as exc2:
                        last = exc2
                        continue
            continue
        except RequestException as exc:
            last = exc
            continue

    if last:
        raise last
    raise RuntimeError("SES: falha desconhecida ao requisitar URL.")


def _fetch_text(url: str) -> str:
    r = _request(url, timeout=60)
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def _extract_urls(html: str, base_url: str) -> list[str]:
    """
    Extrai URLs de href/src e também strings JS que pareçam URL/paths.
    """
    urls: set[str] = set()

    # href/src padrão
    for m in re.findall(r"""(?is)(?:href|src)\s*=\s*['"]([^'"]+)['"]""", html):
        u = m.strip()
        if not u:
            continue
        urls.add(urljoin(base_url, u))

    # strings com .zip / download em JS
    for m in re.findall(r"""(?is)['"]([^'"]+?(?:\.zip|download)[^'"]*)['"]""", html):
        u = m.strip()
        if not u:
            continue
        urls.add(urljoin(base_url, u))

    # urls absolutas soltas
    for m in re.findall(r"""(?is)https?://[^\s"'<>]+""", html):
        u = m.strip()
        if u:
            urls.add(u)

    return sorted(urls)


def _score_zip(u: str) -> int:
    lu = u.lower()
    s = 0
    if ".zip" in lu:
        s += 10
    if "base" in lu:
        s += 3
    if "completa" in lu or "completo" in lu:
        s += 4
    if "ses" in lu:
        s += 2
    if "download" in lu:
        s += 3
    if "menuestatistica" in lu:
        s += 1
    # penalidades simples (evita docs aleatórios)
    if lu.endswith(".pdf"):
        s -= 5
    return s


def _pick_best_zip(candidates: list[str]) -> str | None:
    zips = [u for u in candidates if ".zip" in u.lower()]
    if not zips:
        return None
    zips.sort(key=lambda x: _score_zip(x), reverse=True)
    return zips[0]


def discover_ses_zip_url() -> str:
    """
    Descobre a URL do ZIP da Base Completa do SES.

    Ordem:
    1) SES_ZIP_URL (override)
    2) Extrair .zip do principal.aspx
    3) Identificar páginas candidatas (download/base/ses) e buscar .zip nelas (crawl curto)
    """
    override = str(os.environ.get("SES_ZIP_URL", "")).strip()
    if override:
        return override

    home = str(os.environ.get("SES_HOME_URL", "")).strip() or SES_HOME_DEFAULT

    html = _fetch_text(home)
    urls = _extract_urls(html, home)

    best = _pick_best_zip(urls)
    if best:
        return best

    # Tentativa especial: âncora “Base do SES para Download” pode estar com HTML aninhado
    m = re.search(
        r"""(?is)<a[^>]+href\s*=\s*['"]([^'"]+)['"][^>]*>.*?Base\s+do\s+SES\s+para\s+Download""",
        html,
    )
    if m:
        candidate_page = urljoin(home, m.group(1))
        html2 = _fetch_text(candidate_page)
        urls2 = _extract_urls(html2, candidate_page)
        best2 = _pick_best_zip(urls2)
        if best2:
            return best2

    # Crawl curto: pegue links “prováveis” e tente achar .zip
    likely_pages = []
    for u in urls:
        lu = u.lower()
        if any(tok in lu for tok in ["download", "base", "ses"]) and ".zip" not in lu:
            likely_pages.append(u)

    # limita o crawl para não explodir tempo
    for page in likely_pages[:8]:
        try:
            htmlp = _fetch_text(page)
        except Exception:
            continue
        up = _extract_urls(htmlp, page)
        bestp = _pick_best_zip(up)
        if bestp:
            return bestp

    snippet = re.sub(r"\s+", " ", html[:500]).strip()
    raise RuntimeError(
        "SES: não encontrei link .zip nem página de download no principal. "
        f"home={home} snippet={snippet!r}"
    )


def _download_bytes(url: str) -> bytes:
    r = _request(url, timeout=180)
    b = r.content
    # ZIP signatures
    if not (b.startswith(b"PK\x03\x04") or b.startswith(b"PK\x05\x06") or b.startswith(b"PK\x07\x08")):
        snippet = b[:200].decode("latin-1", errors="replace").strip()
        raise RuntimeError(f"SES: download não parece ZIP (head={snippet!r}) url={url}")
    return b


def _find_zip_member(z: zipfile.ZipFile, prefer_exact_lower: str, contains_any: list[str]) -> str:
    names = z.namelist()
    lower = [n.lower() for n in names]

    for i, ln in enumerate(lower):
        if ln.endswith(prefer_exact_lower):
            return names[i]

    scored: list[tuple[int, int, str]] = []
    for n in names:
        ln = n.lower()
        score = 0
        for tok in contains_any:
            if tok in ln:
                score += 1
        if score > 0 and ln.endswith(".csv"):
            info = z.getinfo(n)
            scored.append((score, info.file_size, n))

    if not scored:
        raise RuntimeError(f"SES: não encontrei CSV compatível para {prefer_exact_lower}.")

    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return scored[0][2]


def _read_csv_from_zip(z: zipfile.ZipFile, member: str) -> tuple[list[str], list[list[str]]]:
    raw = z.read(member)
    text = raw.decode("latin-1", errors="replace")
    f = io.StringIO(text)
    head = f.readline()
    delim = ";" if head.count(";") >= head.count(",") else ","
    f.seek(0)
    reader = csv.reader(f, delimiter=delim)
    rows = list(reader)
    if not rows:
        return [], []
    return rows[0], rows[1:]


def _idx(header: list[str], keys: list[str]) -> int | None:
    hn = [_norm(h) for h in header]
    for k in keys:
        kk = _norm(k)
        if kk in hn:
            return hn.index(kk)
    return None


def _find_cnpj_idx(header: list[str]) -> int | None:
    hn = [_norm(h) for h in header]
    prefer = ["cnpj", "nucnpj", "nu_cnpj", "numcnpj", "cnpjempresa", "cnpjentidade"]
    for k in prefer:
        kk = _norm(k)
        for i, col in enumerate(hn):
            if col == kk:
                return i
    for i, col in enumerate(hn):
        if "cnpj" in col:
            return i
    return None


def _infer_cnpj_idx_by_sampling(header: list[str], rows: list[list[str]], exclude: set[int] | None = None) -> int | None:
    exclude = exclude or set()
    if not header or not rows:
        return None
    sample = rows[:200]
    best_i: int | None = None
    best_hits = 0
    for i in range(len(header)):
        if i in exclude:
            continue
        hits = 0
        for r in sample:
            if i >= len(r):
                continue
            d = _digits(r[i])
            if len(d) == 14:
                hits += 1
        if hits > best_hits:
            best_hits = hits
            best_i = i
    if best_i is None:
        return None
    min_hits = max(3, int(len(sample) * 0.2))
    return best_i if best_hits >= min_hits else None


def _load_cached_insurers_payload() -> tuple[SesMeta, dict[str, dict[str, Any]]] | None:
    """
    Fallback "evergreen": usa data/raw/insurers_full.json.gz ou o snapshot mais recente.
    Isso impede o workflow de cair quando a SUSEP oscila (SSL/HTML/layout).
    """
    candidates: list[Path] = []

    raw = DATA_RAW / "insurers_full.json.gz"
    if raw.exists():
        candidates.append(raw)

    snaps = sorted(DATA_SNAPSHOTS.glob("insurers_full_*.json.gz"), reverse=True)
    candidates.extend(snaps)

    for path in candidates:
        try:
            with gzip.open(path, "rt", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            continue

        insurers = payload.get("insurers") or []
        period = payload.get("period") or {}
        sources = payload.get("sources") or {}
        ses_source = sources.get("ses") or {}

        if not isinstance(insurers, list) or not insurers:
            continue

        companies: dict[str, dict[str, Any]] = {}
        for item in insurers:
            iid = str(item.get("id") or "").strip()
            if iid.startswith("ses:"):
                iid = iid.split(":", 1)[1]
            sid = _digits(iid).zfill(6)
            if not sid:
                continue
            data = item.get("data") or {}
            companies[sid] = {
                "name": str(item.get("name") or "").strip() or f"SES_ENTIDADE_{sid}",
                "cnpj": _digits(item.get("cnpj")) or None,
                "premiums": float(data.get("premiums") or 0.0),
                "claims": float(data.get("claims") or 0.0),
            }

        if not companies:
            continue

        period_from = str(period.get("from") or "") or "1970-01-01"
        period_to = str(period.get("to") or "") or "1970-01-01"
        files = ses_source.get("files") or []
        cias_file = str(files[0]) if files else "cached_ses_cias.csv"
        seguros_file = str(files[1]) if len(files) > 1 else "cached_ses_seguros.csv"

        meta = SesMeta(
            zip_url=str(ses_source.get("url") or path),
            cias_file=cias_file,
            seguros_file=seguros_file,
            period_from=period_from,
            period_to=period_to,
        )
        return meta, companies

    return None


def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, dict[str, Any]]]:
    """
    Retorna:
      - SesMeta com URL/nomes de arquivos e janela (12 meses)
      - companies: sid -> {name, cnpj, premiums, claims}

    Se SUSEP falhar por rede/SSL/layout e houver cache/snapshot, usa o cache para manter o build verde.
    """
    try:
        zip_url = discover_ses_zip_url()
        zip_bytes = _download_bytes(zip_url)

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            cias_member = _find_zip_member(z, "ses_cias.csv", ["ses_cias", "cias", "companhias"])
            seg_member = _find_zip_member(z, "ses_seguros.csv", ["ses_seguros", "seguros"])
            h_cias, rows_cias = _read_csv_from_zip(z, cias_member)
            h_seg, rows_seg = _read_csv_from_zip(z, seg_member)

        if not h_cias or not h_seg:
            raise RuntimeError("SES: CSVs vazios ou não carregados corretamente.")

        sid_i = _idx(h_cias, ["coenti", "id", "codigo", "codigo_entidade"])
        name_i = _idx(h_cias, ["noenti", "nome", "nome_entidade", "razao_social", "no_entidade"])

        cnpj_i = _find_cnpj_idx(h_cias)
        exclude_indices = {i for i in (sid_i, name_i) if i is not None}
        if cnpj_i is None:
            cnpj_i = _infer_cnpj_idx_by_sampling(h_cias, rows_cias, exclude=exclude_indices)

        if sid_i is None or name_i is None:
            raise RuntimeError("SES: não foi possível localizar colunas de ID/Nome em Ses_cias.csv.")

        cias: dict[str, dict[str, Any]] = {}
        for r in rows_cias:
            if sid_i >= len(r) or name_i >= len(r):
                continue
            sid = _digits(r[sid_i]).zfill(6)
            if not sid:
                continue
            name = str(r[name_i] or "").strip()
            cnpj = None
            if cnpj_i is not None and cnpj_i < len(r):
                c = _digits(r[cnpj_i])
                if len(c) == 14:
                    cnpj = c
            cias[sid] = {"name": name or f"SES_ENTIDADE_{sid}", "cnpj": cnpj}

        seg_sid_i = _idx(h_seg, ["coenti", "id", "codigo", "codigo_entidade"])
        ym_i = _idx(h_seg, ["damesano", "ano_mes", "competencia", "mes_ano", "mesano"])
        prem_i = _idx(h_seg, ["premio_direto", "premio", "premio_emitido", "premios", "premio_total"])
        clm_i = _idx(h_seg, ["sinistro_direto", "sinistro", "sinistros", "sinistro_total"])

        if seg_sid_i is None or ym_i is None or prem_i is None or clm_i is None:
            raise RuntimeError("SES: não foi possível localizar colunas-chave em Ses_seguros.csv.")

        max_ym: tuple[int, int] | None = None
        for r in rows_seg:
            if ym_i >= len(r):
                continue
            ym = _parse_ym(r[ym_i])
            if not ym:
                continue
            if (max_ym is None) or (_ym_to_int(ym) > _ym_to_int(max_ym)):
                max_ym = ym

        if not max_ym:
            raise RuntimeError("SES: não consegui determinar o mês mais recente (max_ym).")

        start_ym = _add_months(max_ym, -11)
        max_i = _ym_to_int(max_ym)
        start_i = _ym_to_int(start_ym)

        buckets: dict[str, dict[str, float]] = {}
        for r in rows_seg:
            if seg_sid_i >= len(r) or ym_i >= len(r) or prem_i >= len(r) or clm_i >= len(r):
                continue
            sid = _digits(r[seg_sid_i]).zfill(6)
            if not sid:
                continue
            ym = _parse_ym(r[ym_i])
            if not ym:
                continue
            yi = _ym_to_int(ym)
            if yi < start_i or yi > max_i:
                continue
            p = _parse_brl_num(r[prem_i])
            c = _parse_brl_num(r[clm_i])
            if sid not in buckets:
                buckets[sid] = {"premiums": 0.0, "claims": 0.0}
            buckets[sid]["premiums"] += p
            buckets[sid]["claims"] += c

        companies: dict[str, dict[str, Any]] = {}
        for sid, base in cias.items():
            vals = buckets.get(sid) or {"premiums": 0.0, "claims": 0.0}
            companies[sid] = {
                "name": base.get("name"),
                "cnpj": base.get("cnpj"),
                "premiums": float(vals.get("premiums") or 0.0),
                "claims": float(vals.get("claims") or 0.0),
            }

        meta = SesMeta(
            zip_url=zip_url,
            cias_file=str(cias_member),
            seguros_file=str(seg_member),
            period_from=_ym_to_date_str(start_ym),
            period_to=_ym_to_date_str(max_ym),
        )
        return meta, companies

    except Exception as exc:
        if str(os.environ.get("SES_DISABLE_CACHE_FALLBACK", "")).strip() == "1":
            raise
        cached = _load_cached_insurers_payload()
        if cached:
            print(f"WARN: SES falhou ({exc}); usando cache/snapshot existente.", flush=True)
            return cached
        raise
