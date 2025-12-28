# api/sources/ses.py
from __future__ import annotations

import csv
import io
import re
import zipfile
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import requests

SES_HOME = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"
UA = {"User-Agent": "Mozilla/5.0 (compatible; SanidaBot/1.0; +https://sanida.com.br)"}

# Blindagem contra proxy do ambiente (voltava ProxyError/403 no CI)
_SESSION = requests.Session()
_SESSION.trust_env = False


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
    r = _SESSION.get(url, headers=UA, timeout=60)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def _best_zip_link(candidates: list[str]) -> str:
    """
    Escolhe o melhor link de ZIP por scoring (evita pegar zip errado).
    """
    best = None
    best_score = -1
    for u in candidates:
        lu = u.lower()
        score = 0
        if "base" in lu:
            score += 3
        if "completa" in lu or "completo" in lu:
            score += 6
        if "ses" in lu:
            score += 1
        if ".zip" in lu:
            score += 1
        if score > best_score:
            best_score = score
            best = u
    return best or candidates[0]


def discover_ses_zip_url() -> str:
    """
    Localiza um link ZIP relacionado à 'Base Completa' do SES.
    Estratégia:
      1) tenta achar ZIP diretamente no principal
      2) se não achar, busca a página "Base do SES para Download" e acha ZIP lá
    """
    html = _fetch_text(SES_HOME)

    hrefs = re.findall(r'href="([^"]+)"', html, flags=re.I)
    zips = [u for u in hrefs if ".zip" in u.lower()]
    if zips:
        chosen = _best_zip_link(zips)
        return urljoin(SES_HOME, chosen)

    m = re.search(
        r'href="([^"]+)"[^>]*>\s*Base\s+do\s+SES\s+para\s+Download',
        html,
        flags=re.I,
    )
    download_page = None
    if m:
        download_page = urljoin(SES_HOME, m.group(1))
    else:
        # fallback: qualquer link que pareça levar a download do SES
        for u in hrefs:
            lu = u.lower()
            if "download" in lu and "ses" in lu:
                download_page = urljoin(SES_HOME, u)
                break

    if not download_page:
        raise RuntimeError("SES: não encontrei link .zip nem página de download no principal.")

    html2 = _fetch_text(download_page)
    hrefs2 = re.findall(r'href="([^"]+)"', html2, flags=re.I)
    zips2 = [u for u in hrefs2 if ".zip" in u.lower()]
    if not zips2:
        raise RuntimeError("SES: página de download encontrada, mas nenhum link .zip foi localizado.")

    chosen2 = _best_zip_link(zips2)
    return urljoin(download_page, chosen2)


def _download_bytes(url: str) -> bytes:
    r = _SESSION.get(url, headers=UA, timeout=240, allow_redirects=True)
    r.raise_for_status()
    b = r.content

    # evita BadZipFile silencioso: às vezes vem HTML de erro/bloqueio
    if not (
        b.startswith(b"PK\x03\x04")
        or b.startswith(b"PK\x05\x06")
        or b.startswith(b"PK\x07\x08")
    ):
        head = b[:200].decode("latin-1", errors="replace").strip()
        raise RuntimeError(f"SES: download não parece ZIP (head={head!r})")

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
        if not ln.endswith(".csv"):
            continue
        score = 0
        for tok in contains_any:
            if tok in ln:
                score += 1
        if score > 0:
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


def _hits_14_digits(rows: list[list[str]], col_i: int, sample_n: int = 200) -> int:
    hits = 0
    for r in rows[:sample_n]:
        if col_i >= len(r):
            continue
        d = _digits(r[col_i])
        if len(d) == 14:
            hits += 1
    return hits


def _infer_cnpj_idx_by_sampling(
    header: list[str], rows: list[list[str]], exclude: set[int] | None = None
) -> int | None:
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

    # mínimo: 3 ocorrências e 20% do sample (pra não pegar coluna errada)
    min_hits = max(3, int(len(sample) * 0.2))
    return best_i if best_hits >= min_hits else None


def _infer_metric_col(
    header: list[str],
    rows: list[list[str]],
    kind: str,
    prefer_keys: list[str],
    must_tokens: list[str],
    avoid_tokens: list[str] | None = None,
) -> int | None:
    """
    Tenta achar coluna de métrica (prêmio/sinistro) de forma resiliente:
    - 1) tenta prefer_keys (match exato)
    - 2) se parecer ruim (quase tudo zero), varre header por tokens e escolhe a melhor por amostragem
    """
    avoid_tokens = avoid_tokens or []
    hn = [_norm(h) for h in header]

    def nonzero_hits(col_i: int, sample_n: int = 400) -> int:
        hits = 0
        for r in rows[:sample_n]:
            if col_i >= len(r):
                continue
            if _parse_brl_num(r[col_i]) != 0.0:
                hits += 1
        return hits

    cand = _idx(header, prefer_keys)
    if cand is not None:
        # se a coluna candidata for “morta”, tenta fallback
        if nonzero_hits(cand) >= 5:
            return cand

    candidates: list[tuple[int, int, int]] = []
    # (score_tokens, hits_nonzero, idx)
    for i, col in enumerate(hn):
        if any(a in col for a in avoid_tokens):
            continue
        if not all(t in col for t in must_tokens):
            continue
        score = sum(1 for t in must_tokens if t in col)
        hits = nonzero_hits(i)
        candidates.append((score, hits, i))

    if not candidates:
        return cand

    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    # exige “vida mínima”
    _, best_hits, best_i = candidates[0]
    return best_i if best_hits >= 5 else cand


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

    # valida CNPJ encontrado por header; se ruim, tenta inferência por amostragem
    if cnpj_i is not None:
        if _hits_14_digits(rows_cias, cnpj_i) < 3:
            cnpj_i = None
    if cnpj_i is None:
        cnpj_i = _infer_cnpj_idx_by_sampling(h_cias, rows_cias, exclude={sid_i, name_i})

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
    ym_i = _idx(h_seg, ["damesano", "ano_mes", "competencia", "mes_ano", "mesano", "ano_mes_competencia"])

    # inferência resiliente das colunas de prêmio e sinistro
    prem_i = _infer_metric_col(
        h_seg,
        rows_seg,
        kind="premiums",
        prefer_keys=[
            "premio_direto",
            "premio",
            "premio_emitido",
            "premios",
            "premio_total",
            "vl_premio_direto",
            "vlpremiodireto",
            "vlpremio",
        ],
        must_tokens=["premio"],
        avoid_tokens=["sinistro"],
    )
    clm_i = _infer_metric_col(
        h_seg,
        rows_seg,
        kind="claims",
        prefer_keys=[
            "sinistro_direto",
            "sinistro",
            "sinistros",
            "sinistro_total",
            "vl_sinistro_direto",
            "vlsinistrodireto",
            "vlsinistro",
        ],
        must_tokens=["sinistro"],
        avoid_tokens=["premio"],
    )

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
