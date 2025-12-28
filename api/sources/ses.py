from __future__ import annotations

import csv
import gzip
import io
import json
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests

SES_HOME = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"
UA = {"User-Agent": "Mozilla/5.0 (compatible; SanidaBot/1.0)"}

# 1) DIRECT: ignora proxy do ambiente
_SESSION_DIRECT = requests.Session()
_SESSION_DIRECT.trust_env = False

# 2) ENV: usa rede/proxy do ambiente (quando existir)
_SESSION_ENV = requests.Session()

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


def _request(url: str, timeout: int) -> requests.Response:
    """
    Estratégia:
      - tenta ENV+verify=True
      - tenta DIRECT+verify=True
      - tenta ENV+verify=certifi.where()
      - tenta DIRECT+verify=certifi.where()
      - se SES_ALLOW_INSECURE_SSL=1 => tenta ENV/DIRECT com verify=False (último recurso)
    """
    allow_insecure = str((__import__("os").environ.get("SES_ALLOW_INSECURE_SSL") or "")).strip() == "1"

    verify_candidates: list[object] = [True]
    try:
        import certifi  # type: ignore

        verify_candidates.append(certifi.where())
    except Exception:
        pass

    last_exc: Exception | None = None

    for verify in verify_candidates:
        for sess in (_SESSION_ENV, _SESSION_DIRECT):
            try:
                r = sess.get(url, headers=UA, timeout=timeout, verify=verify)
                r.raise_for_status()
                return r
            except requests.exceptions.SSLError as exc:
                last_exc = exc
                continue
            except requests.RequestException as exc:
                last_exc = exc
                continue

    if allow_insecure:
        for sess in (_SESSION_ENV, _SESSION_DIRECT):
            try:
                print("WARN: SES SSL verify falhou; tentando verify=False (SES_ALLOW_INSECURE_SSL=1).")
                r = sess.get(url, headers=UA, timeout=timeout, verify=False)
                r.raise_for_status()
                return r
            except requests.RequestException as exc:
                last_exc = exc
                continue

    if last_exc:
        raise last_exc
    raise RuntimeError("SES: falha desconhecida na requisição.")


def _fetch_text(url: str) -> str:
    r = _request(url, timeout=60)
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def discover_ses_zip_url() -> str:
    html = _fetch_text(SES_HOME)

    # 1) Se já houver link .zip direto
    zips = re.findall(r'href="([^"]+?\.zip)"', html, flags=re.I)
    if zips:
        best = None
        for u in zips:
            lu = u.lower()
            if "base" in lu and ("completa" in lu or "completo" in lu):
                best = u
                break
        return urljoin(SES_HOME, best or zips[0])

    # 2) Caso exista uma página intermediária de download
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
    r = _request(url, timeout=180)
    b = r.content
    if not (b.startswith(b"PK\x03\x04") or b.startswith(b"PK\x05\x06") or b.startswith(b"PK\x07\x08")):
        snippet = b[:200].decode("latin-1", errors="replace").strip()
        raise RuntimeError(f"SES: download não parece ZIP (head={snippet!r})")
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
    try:
        zip_url = discover_ses_zip_url()
        zip_bytes = _download_bytes(zip_url)
    except requests.RequestException as exc:
        cached = _load_cached_insurers_payload()
        if cached:
            print("WARN: SES download/SSL falhou, usando payload cacheado (data/raw/snapshots).")
            return cached
        raise RuntimeError("SES: falha de rede/SSL e nenhum cache disponível.") from exc

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
