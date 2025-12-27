# api/sources/ses.py
from __future__ import annotations

import csv
import io
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import requests

SES_HOME = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"
UA = {"User-Agent": "Mozilla/5.0 (compatible; SanidaBot/1.0; +https://example.com)"}


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
    # padrão BR: 1.234.567,89
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
    # yyyymm (ou yyyymmdd -> usa yyyymm)
    if len(d) >= 6:
        y = int(d[0:4])
        m = int(d[4:6])
        if 1 <= m <= 12:
            return y, m
    m = re.search(r"\b(\d{4})\D{0,3}(\d{2})\b", s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
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


def _fetch_text(url: str) -> str:
    r = requests.get(url, headers=UA, timeout=60)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def discover_ses_zip_url() -> str:
    """
    Localiza um link .zip relacionado à 'Base Completa' do SES.
    Estratégia:
      1) tenta achar .zip diretamente no principal
      2) se não achar, busca link para uma página de download e tenta achar .zip lá
    """
    html = _fetch_text(SES_HOME)

    zips = re.findall(r'href="([^"]+?\.zip)"', html, flags=re.I)
    if zips:
        best = None
        for u in zips:
            lu = u.lower()
            if "base" in lu and ("completa" in lu or "completo" in lu):
                best = u
                break
        return urljoin(SES_HOME, best or zips[0])

    m = re.search(r'href="([^"]+)"[^>]*>\s*Base\s+do\s+SES\s+para\s+Download', html, flags=re.I)
    if not m:
        links = re.findall(r'href="([^"]+)"', html, flags=re.I)
        cand = None
        for u in links:
            lu = u.lower()
            if "download" in lu and "ses" in lu:
                cand = u
                break
        if not cand:
            raise RuntimeError("SES: não encontrei link .zip nem página de download no principal.")
        download_page = urljoin(SES_HOME, cand)
    else:
        download_page = urljoin(SES_HOME, m.group(1))

    html2 = _fetch_text(download_page)
    zips2 = re.findall(r'href="([^"]+?\.zip)"', html2, flags=re.I)
    if not zips2:
        raise RuntimeError("SES: página de download encontrada, mas nenhum link .zip foi localizado.")
    best2 = None
    for u in zips2:
        lu = u.lower()
        if "base" in lu and ("completa" in lu or "completo" in lu):
            best2 = u
            break
    return urljoin(download_page, best2 or zips2[0])


def _download_bytes(url: str) -> bytes:
    r = requests.get(url, headers=UA, timeout=180)
    r.raise_for_status()
    return r.content


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


def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, dict[str, Any]]]:
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
    for sid, vals in buckets.items():
        premiums = float(vals.get("premiums") or 0.0)
        claims = float(vals.get("claims") or 0.0)
        if premiums <= 0:
            continue

        base = cias.get(sid) or {"name": f"SES_ENTIDADE_{sid}", "cnpj": None}
        companies[sid] = {
            "name": base.get("name"),
            "cnpj": base.get("cnpj"),
            "premiums": premiums,
            "claims": claims,
        }

    meta = SesMeta(
        zip_url=zip_url,
        cias_file=str(cias_member),
        seguros_file=str(seg_member),
        period_from=_ym_to_date_str(start_ym),
        period_to=_ym_to_date_str(max_ym),
    )
    return meta, companies
