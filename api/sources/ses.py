# api/sources/ses.py
from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import time
import unicodedata
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
from requests.exceptions import SSLError

SES_HOME_DEFAULT = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"

SES_DOWNLOAD_HINTS = [
    "download",
    "base",
    "ses",
    "estatisticas",
    "estatística",
    "estatistica",
    "base completa",
    "basecompleta",
]


# ----------------------------
# Dataclasses
# ----------------------------

@dataclass
class SesFetchResult:
    zip_url: str
    fetched_at: str
    bytes_len: int
    sha256: str
    saved_to: str


@dataclass
class SesExtractMeta:
    # Campos esperados por api/build_insurers.py
    zip_url: str
    cias_file: str
    seguros_file: str
    period_from: str  # "YYYY-MM"
    period_to: str    # "YYYY-MM"

    # Campos adicionais (diagnóstico)
    fetched_at: str
    bytes_len: int
    sha256: str
    saved_to: str
    notes: dict[str, Any]


# ----------------------------
# Helpers (time/hash/http)
# ----------------------------

def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _allow_insecure_ssl() -> bool:
    return str(os.environ.get("SES_ALLOW_INSECURE_SSL", "")).strip().lower() in {"1", "true", "yes"}


def _ua_headers() -> dict[str, str]:
    ua = os.environ.get("SES_UA", "widget-confiabilidade-seguradoras/1.0")
    return {"User-Agent": ua}


def _clean_url(u: str) -> str:
    u = u.strip()
    return u.replace(" ", "%20")


def _is_zip_signature(b: bytes) -> bool:
    return len(b) >= 2 and b[:2] == b"PK"


def _fetch_text(url: str) -> str:
    timeout = float(os.environ.get("SES_HTTP_TIMEOUT", "30"))
    headers = _ua_headers()
    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, verify=True)
        r.raise_for_status()
        return r.text
    except SSLError:
        if not _allow_insecure_ssl():
            raise
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, verify=False)
        r.raise_for_status()
        return r.text


def _extract_urls(html: str, base_url: str) -> list[str]:
    urls: set[str] = set()

    for m in re.finditer(r'href=["\']([^"\']+)["\']', html, flags=re.I):
        u = m.group(1).strip()
        if not u:
            continue
        if u.startswith("#") or u.startswith("javascript:"):
            continue
        urls.add(u)

    out: list[str] = []
    for u in urls:
        if u.startswith("http://") or u.startswith("https://"):
            out.append(u)
        elif u.startswith("//"):
            out.append("https:" + u)
        else:
            if base_url.endswith("/"):
                out.append(base_url + u.lstrip("/"))
            else:
                out.append(base_url.rsplit("/", 1)[0] + "/" + u.lstrip("/"))

    return [_clean_url(x) for x in out]


def _score_zip(url: str) -> int:
    u = url.lower()
    score = 0
    if "basecompleta" in u:
        score += 50
    if u.endswith(".zip"):
        score += 10
    if "estatistic" in u or "estatistica" in u:
        score += 5
    return score


def _pick_best_zip(urls: list[str]) -> str | None:
    zips = [u for u in urls if u.lower().endswith(".zip")]
    if not zips:
        return None
    zips.sort(key=_score_zip, reverse=True)
    return zips[0]


# ----------------------------
# Public: discovery + download
# ----------------------------

def discover_ses_zip_url() -> str:
    """
    Resolve a URL do ZIP da Base Completa do SES.

    Ordem:
    1) SES_ZIP_URL (override; caminho feliz)
    2) Extrair .zip do principal.aspx
    3) Crawl curto em páginas candidatas (download/base/ses) buscando .zip
    """
    override = str(os.environ.get("SES_ZIP_URL", "")).strip()
    if override:
        print(f"SES: usando override SES_ZIP_URL={override}", flush=True)
        return override

    home = str(os.environ.get("SES_HOME_URL", "")).strip() or SES_HOME_DEFAULT
    html = _fetch_text(home)

    urls = _extract_urls(html, home)
    best = _pick_best_zip(urls)
    if best:
        return best

    candidates: list[str] = []
    for u in urls:
        ul = u.lower()
        if any(h in ul for h in SES_DOWNLOAD_HINTS):
            candidates.append(u)

    for page in candidates[:10]:
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


def fetch_ses_zip_head_signature(zip_url: str) -> bytes:
    timeout = float(os.environ.get("SES_HTTP_TIMEOUT", "30"))
    headers = _ua_headers()
    headers["Range"] = "bytes=0-3"

    def _get(verify: bool) -> bytes:
        r = requests.get(zip_url, headers=headers, timeout=timeout, stream=True, verify=verify, allow_redirects=True)
        r.raise_for_status()
        return r.raw.read(4)

    try:
        return _get(verify=True)
    except SSLError:
        if not _allow_insecure_ssl():
            raise
        return _get(verify=False)


def wait_until_ses_zip_is_available(max_wait_seconds: int = 30) -> str:
    start = time.time()
    last: Exception | None = None

    while time.time() - start < max_wait_seconds:
        try:
            url = discover_ses_zip_url()
            head = fetch_ses_zip_head_signature(url)
            if _is_zip_signature(head):
                return url
        except Exception as e:
            last = e
        time.sleep(2)

    raise RuntimeError(f"SES zip não ficou disponível/validável em {max_wait_seconds}s") from last


def _download_zip_streaming(zip_url: str, out_path: Path) -> SesFetchResult:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")

    timeout = float(os.environ.get("SES_HTTP_TIMEOUT", "120"))
    headers = _ua_headers()

    def _do_get(verify: bool) -> SesFetchResult:
        r = requests.get(
            zip_url,
            headers=headers,
            timeout=timeout,
            stream=True,
            verify=verify,
            allow_redirects=True,
        )
        r.raise_for_status()

        h = hashlib.sha256()
        size = 0
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                if size == 0 and len(chunk) >= 2 and not _is_zip_signature(chunk[:2]):
                    raise RuntimeError(f"SES: download não parece ZIP (assinatura != PK). url={zip_url!r}")
                f.write(chunk)
                h.update(chunk)
                size += len(chunk)

        tmp.replace(out_path)
        return SesFetchResult(
            zip_url=zip_url,
            fetched_at=_utc_now(),
            bytes_len=size,
            sha256=h.hexdigest(),
            saved_to=str(out_path),
        )

    try:
        return _do_get(verify=True)
    except SSLError:
        if not _allow_insecure_ssl():
            raise
        return _do_get(verify=False)


def download_and_validate_ses_zip() -> SesFetchResult:
    zip_url = discover_ses_zip_url()
    cache = Path(os.environ.get("SES_ZIP_CACHE_PATH", "data/raw/ses_basecompleta.zip"))
    force = str(os.environ.get("SES_FORCE_DOWNLOAD", "")).strip().lower() in {"1", "true", "yes"}

    if cache.exists() and cache.stat().st_size > 1024 * 1024 and not force:
        # valida assinatura
        with cache.open("rb") as f:
            head = f.read(4)
        if _is_zip_signature(head):
            # sha256 de arquivo (rápido o suficiente; mas pode ser grande)
            h = hashlib.sha256()
            with cache.open("rb") as f2:
                for chunk in iter(lambda: f2.read(1024 * 1024), b""):
                    h.update(chunk)
            return SesFetchResult(
                zip_url=zip_url,
                fetched_at=_utc_now(),
                bytes_len=int(cache.stat().st_size),
                sha256=h.hexdigest(),
                saved_to=str(cache),
            )

    info = _download_zip_streaming(zip_url, cache)

    # sanity: abre o zip
    try:
        with zipfile.ZipFile(cache) as zf:
            _ = zf.namelist()[:1]
    except zipfile.BadZipFile as e:
        raise RuntimeError(f"SES: BadZipFile ao abrir ZIP baixado. url={zip_url!r}") from e

    return info


# ----------------------------
# Helpers (CSV parsing + heuristics)
# ----------------------------

def _norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s.strip()


def _norm_header(s: str) -> str:
    return _norm_key(s).replace(" ", "")


def _digits(s: Any) -> str:
    return re.sub(r"\D+", "", str(s or ""))


def _to_float(x: Any) -> float:
    try:
        return float(str(x).strip().replace(".", "").replace(",", "."))
    except Exception:
        return 0.0


def _sniff_delimiter(sample: str) -> str:
    sc = sample.count(";")
    cc = sample.count(",")
    tc = sample.count("\t")
    if tc > sc and tc > cc:
        return "\t"
    return ";" if sc >= cc else ","


def _ym_to_index(ym: str) -> int:
    # ym: "YYYY-MM"
    y, m = ym.split("-", 1)
    return int(y) * 12 + int(m)


def _months_back(ym: str, months: int) -> str:
    y, m = map(int, ym.split("-"))
    idx = (y * 12 + (m - 1)) - months
    ny = idx // 12
    nm = (idx % 12) + 1
    return f"{ny:04d}-{nm:02d}"


def _parse_ym_from_row(row: dict[str, Any], fieldnames: list[str]) -> str | None:
    # tenta: ano+mes
    fn = {k: _norm_header(k) for k in fieldnames}
    year_key = None
    month_key = None

    for k, nk in fn.items():
        if nk in {"ano", "anocompetencia", "anoref", "anobase"} or nk.endswith("ano"):
            year_key = year_key or k
        if nk in {"mes", "mescompetencia", "mesref", "mesbase"} or nk.endswith("mes"):
            month_key = month_key or k

    if year_key and month_key:
        y = _digits(row.get(year_key))
        m = _digits(row.get(month_key))
        if len(y) == 4 and m.isdigit():
            mm = int(m)
            if 1 <= mm <= 12:
                return f"{int(y):04d}-{mm:02d}"

    # tenta: competencia / periodo / anomes
    candidates = []
    for k, nk in fn.items():
        if any(t in nk for t in ("competencia", "periodo", "anomes", "mesano", "dataref", "dtref")):
            candidates.append(k)

    for k in candidates:
        raw = str(row.get(k) or "").strip()
        d = _digits(raw)
        if len(d) >= 6:
            y = d[:4]
            m = d[4:6]
            if y.isdigit() and m.isdigit():
                mm = int(m)
                if 1 <= mm <= 12:
                    return f"{int(y):04d}-{mm:02d}"

        # formatos: YYYY-MM, MM/YYYY
        m1 = re.search(r"(\d{4})[/-](\d{1,2})", raw)
        if m1:
            yy = int(m1.group(1))
            mm = int(m1.group(2))
            if 1 <= mm <= 12:
                return f"{yy:04d}-{mm:02d}"
        m2 = re.search(r"(\d{1,2})[/-](\d{4})", raw)
        if m2:
            mm = int(m2.group(1))
            yy = int(m2.group(2))
            if 1 <= mm <= 12:
                return f"{yy:04d}-{mm:02d}"

    return None


def _score_master_file(name: str) -> float:
    n = name.lower()
    score = 0.0
    if "cias" in n:
        score += 50.0
    if "companh" in n or "entidad" in n:
        score += 20.0
    if n.endswith(".csv"):
        score += 5.0
    score -= len(n) / 2000.0
    return score


def _detect_master_file(names: list[str]) -> str | None:
    csvs = [x for x in names if x.lower().endswith(".csv")]
    if not csvs:
        return None
    ranked = sorted(csvs, key=_score_master_file, reverse=True)
    # pega o top que contenha pista real
    for n in ranked[:25]:
        nl = n.lower()
        if "cias" in nl or "companh" in nl or "entidad" in nl:
            return n
    return ranked[0]


def _detect_company_id_col(fieldnames: list[str]) -> str | None:
    best = None
    best_score = -1e9
    for fn in fieldnames:
        n = _norm_header(fn)
        if not n:
            continue
        score = 0.0
        if "cia" in n and "cod" in n:
            score += 20.0
        if n in {"codcia", "cod_cia", "codigo_cia", "codigocia"}:
            score += 15.0
        if "cia" in n and ("id" in n or "codigo" in n):
            score += 10.0
        if "cnpj" in n:
            score -= 20.0
        score -= len(n) / 100.0
        if score > best_score:
            best_score = score
            best = fn
    return best if best_score > 0 else None


def _detect_name_col(fieldnames: list[str]) -> str | None:
    best = None
    best_score = -1e9
    for fn in fieldnames:
        n = _norm_header(fn)
        if not n:
            continue
        score = 0.0
        if any(t in n for t in ("nomefantasia", "fantasia")):
            score += 20.0
        if any(t in n for t in ("razaosocial", "razao", "denominacao")):
            score += 15.0
        if "nome" == n or n.startswith("nome"):
            score += 10.0
        if any(t in n for t in ("sigla", "uf", "pais")):
            score -= 5.0
        if "cnpj" in n:
            score -= 10.0
        score -= len(n) / 200.0
        if score > best_score:
            best_score = score
            best = fn
    return best if best_score > 0 else None


def _detect_cnpj_col(fieldnames: list[str]) -> str | None:
    for fn in fieldnames:
        if "cnpj" in _norm_header(fn):
            return fn
    return None


def _detect_amount_cols(fieldnames: list[str]) -> tuple[str | None, str | None]:
    prem_best = None
    prem_score = -1e9
    sin_best = None
    sin_score = -1e9

    for fn in fieldnames:
        n = _norm_header(fn)
        if not n:
            continue

        # Premium
        if "prem" in n or "premio" in n:
            s = 10.0
            if any(t in n for t in ("emit", "diret", "total")):
                s += 5.0
            if any(t in n for t in ("ganho", "custo", "tarifa", "taxa")):
                s -= 4.0
            s -= len(n) / 120.0
            if s > prem_score:
                prem_score = s
                prem_best = fn

        # Claims / Sinistros
        if "sinist" in n or "indeniz" in n:
            s = 10.0
            if any(t in n for t in ("ocorr", "total", "bruto")):
                s += 4.0
            if any(t in n for t in ("recup", "ressarc")):
                s -= 4.0
            s -= len(n) / 120.0
            if s > sin_score:
                sin_score = s
                sin_best = fn

    if prem_score < 0:
        prem_best = None
    if sin_score < 0:
        sin_best = None
    return prem_best, sin_best


def _open_csv_from_zip(zf: zipfile.ZipFile, member: str) -> tuple[csv.DictReader, list[str], str]:
    # 1) sample para sniff delimiter
    with zf.open(member) as bf:
        sample_bytes = bf.read(4096)
    try:
        sample = sample_bytes.decode("latin-1", errors="replace")
    except Exception:
        sample = str(sample_bytes)

    delim = _sniff_delimiter(sample)

    # 2) abre de novo para o reader completo
    raw = zf.open(member)
    text = io.TextIOWrapper(raw, encoding="latin-1", errors="replace", newline="")
    reader = csv.DictReader(text, delimiter=delim)
    fieldnames = list(reader.fieldnames or [])
    return reader, fieldnames, delim


def _iter_relevant_csv_members(names: list[str]) -> Iterable[str]:
    # heurística de relevância: evita ler tudo se o zip for gigantesco
    for n in names:
        nl = n.lower()
        if not nl.endswith(".csv"):
            continue
        if any(x in nl for x in ("readme", "leia", "dicion", "doc", "layout")):
            continue
        # pistas financeiras
        if any(x in nl for x in ("prem", "sinist", "prov", "segur", "ramos", "produt")):
            yield n


# ----------------------------
# Public: B1 extraction (expected by build_insurers)
# ----------------------------

def extract_ses_master_and_financials() -> tuple[SesExtractMeta, dict[str, dict[str, Any]]]:
    """
    Retorna:
      meta_ses: SesExtractMeta (tem .zip_url, .cias_file, .seguros_file, .period_from, .period_to)
      companies: dict[ses_id] => {name, cnpj, premiums, claims}

    Estratégia:
      - Baixa/usa cache do BaseCompleta.zip (streaming)
      - Detecta arquivo mestre (cias)
      - Varre CSVs financeiros relevantes e agrega prêmios/sinistros
      - Calcula rolling_12m usando o último mês observado
    """
    info = download_and_validate_ses_zip()

    cache_path = Path(info.saved_to)
    if not cache_path.exists() or cache_path.stat().st_size < 1024 * 1024:
        raise RuntimeError(f"SES: cache ZIP inválido/pequeno demais: {cache_path}")

    with zipfile.ZipFile(cache_path) as zf:
        names = zf.namelist()

        cias_member = _detect_master_file(names)
        if not cias_member:
            raise RuntimeError("SES: não consegui detectar arquivo mestre (CIAS) dentro do ZIP.")

        # --- Parse master (CIAS) ---
        companies: dict[str, dict[str, Any]] = {}
        master_rows = 0

        reader, fieldnames, _delim = _open_csv_from_zip(zf, cias_member)
        id_col = _detect_company_id_col(fieldnames)
        name_col = _detect_name_col(fieldnames)
        cnpj_col = _detect_cnpj_col(fieldnames)

        if not id_col or not name_col:
            # ainda assim tentamos seguir, mas isso é bem ruim
            # id_col é essencial para "ses_id"
            raise RuntimeError(
                f"SES: mestre CIAS sem colunas mínimas. "
                f"id_col={id_col!r} name_col={name_col!r} file={cias_member!r}"
            )

        for row in reader:
            if not isinstance(row, dict):
                continue
            master_rows += 1
            ses_id = _digits(row.get(id_col))
            if not ses_id:
                continue
            nm = str(row.get(name_col) or "").strip()
            if not nm:
                nm = f"SES_ENTIDADE_{ses_id}"
            cnpj = _digits(row.get(cnpj_col)) if cnpj_col else ""
            companies[ses_id] = {
                "name": nm,
                "cnpj": cnpj if len(cnpj) == 14 else None,
                "premiums": 0.0,
                "claims": 0.0,
            }

        # --- Parse financial CSVs (rolling_12m base) ---
        prem_by_id: dict[str, dict[str, float]] = {}
        claim_by_id: dict[str, dict[str, float]] = {}
        all_months: set[str] = set()

        processed_files: dict[str, int] = {}
        processed_rows_total = 0

        for member in _iter_relevant_csv_members(names):
            try:
                fin_reader, fin_fields, _d = _open_csv_from_zip(zf, member)
            except Exception:
                continue

            fin_id_col = _detect_company_id_col(fin_fields)
            prem_col, sin_col = _detect_amount_cols(fin_fields)

            # sem id ou sem nenhum valor financeiro, pula
            if not fin_id_col or (prem_col is None and sin_col is None):
                continue

            # precisa conseguir data/competência também
            # (checagem leve: tenta extrair YM de uma linha rápida)
            ym_probe = None
            try:
                probe_row = next(fin_reader)
                if isinstance(probe_row, dict):
                    ym_probe = _parse_ym_from_row(probe_row, fin_fields)
            except StopIteration:
                continue
            except Exception:
                continue

            if not ym_probe:
                continue

            # Reabrir reader (porque consumimos 1 linha no probe)
            try:
                fin_reader2, fin_fields2, _d2 = _open_csv_from_zip(zf, member)
            except Exception:
                continue

            file_rows = 0
            for row in fin_reader2:
                if not isinstance(row, dict):
                    continue

                ses_id = _digits(row.get(fin_id_col))
                if not ses_id:
                    continue

                ym = _parse_ym_from_row(row, fin_fields2)
                if not ym:
                    continue

                file_rows += 1
                processed_rows_total += 1
                all_months.add(ym)

                if prem_col is not None:
                    v = _to_float(row.get(prem_col))
                    if v:
                        prem_by_id.setdefault(ses_id, {}).setdefault(ym, 0.0)
                        prem_by_id[ses_id][ym] += v

                if sin_col is not None:
                    v = _to_float(row.get(sin_col))
                    if v:
                        claim_by_id.setdefault(ses_id, {}).setdefault(ym, 0.0)
                        claim_by_id[ses_id][ym] += v

            if file_rows > 0:
                processed_files[member] = file_rows

        # --- Determine rolling window ---
        if all_months:
            latest = max(all_months, key=_ym_to_index)
            start = _months_back(latest, 11)
            period_from = start
            period_to = latest
        else:
            # fallback defensivo: não quebra build_insurers
            period_to = _utc_now()[:7]
            period_from = _months_back(period_to, 11)

        start_idx = _ym_to_index(period_from)
        end_idx = _ym_to_index(period_to)

        def in_window(ym: str) -> bool:
            i = _ym_to_index(ym)
            return start_idx <= i <= end_idx

        # --- Apply totals to companies (ensure universe from master) ---
        # 1) soma para empresas do mestre
        for ses_id, base in companies.items():
            p = 0.0
            c = 0.0
            for ym, v in prem_by_id.get(ses_id, {}).items():
                if in_window(ym):
                    p += v
            for ym, v in claim_by_id.get(ses_id, {}).items():
                if in_window(ym):
                    c += v
            base["premiums"] = float(p)
            base["claims"] = float(c)

        # 2) inclui IDs que apareceram nos financeiros mas não estavam no mestre
        # (melhor do que perder entidade e derrubar count em casos de layout)
        extra_ids = set(prem_by_id.keys()) | set(claim_by_id.keys())
        for ses_id in extra_ids:
            if ses_id in companies:
                continue
            p = 0.0
            c = 0.0
            for ym, v in prem_by_id.get(ses_id, {}).items():
                if in_window(ym):
                    p += v
            for ym, v in claim_by_id.get(ses_id, {}).items():
                if in_window(ym):
                    c += v
            companies[ses_id] = {
                "name": f"SES_ENTIDADE_{ses_id}",
                "cnpj": None,
                "premiums": float(p),
                "claims": float(c),
            }

        # Escolhe 1 "arquivo financeiro" representativo para o sources.files
        seguros_file = ""
        if processed_files:
            # maior volume de linhas processadas
            seguros_file = max(processed_files, key=processed_files.get)

        meta = SesExtractMeta(
            zip_url=info.zip_url,
            cias_file=cias_member,
            seguros_file=seguros_file or "(auto-detected multiple files)",
            period_from=period_from,
            period_to=period_to,
            fetched_at=info.fetched_at,
            bytes_len=info.bytes_len,
            sha256=info.sha256,
            saved_to=info.saved_to,
            notes={
                "master_rows": master_rows,
                "master_detected_cols": {"id_col": id_col, "name_col": name_col, "cnpj_col": cnpj_col},
                "financial_files_processed": len(processed_files),
                "financial_rows_processed": processed_rows_total,
                "financial_top_files": sorted(processed_files.items(), key=lambda x: x[1], reverse=True)[:5],
            },
        )

        return meta, companies
