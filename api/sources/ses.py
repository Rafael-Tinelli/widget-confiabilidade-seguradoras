from __future__ import annotations

import csv
import io
import re
import tempfile
import unicodedata
import zipfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Desabilita avisos de certificado inseguro
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Páginas para varredura
SES_PAGES = [
    "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx",
    "http://www2.susep.gov.br/menuestatistica/ses/principal.aspx",
]

# Fallbacks explícitos
FALLBACK_URLS = [
    "https://www2.susep.gov.br/menuestatistica/ses/download/BaseCompleta.zip",
    "http://www2.susep.gov.br/menuestatistica/ses/download/BaseCompleta.zip",
    "https://www2.susep.gov.br/safe/menuestatistica/ses/download/BaseCompleta.zip",
    "https://www2.susep.gov.br/menuestatistica/ses/download/ses_base_completa.zip",
]

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


@dataclass(frozen=True)
class SesExtractionMeta:
    zip_url: str
    cias_file: str
    seguros_file: str
    period_from: str
    period_to: str


def _session() -> requests.Session:
    """Cria sessão com retry automático para lidar com instabilidade da SUSEP."""
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": USER_AGENT})
    return s


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
    d = re.sub(r"\D+", "", s)
    return d or None


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


def _ym_add(ym: int, delta_months: int) -> int:
    y = ym // 100
    m = ym % 100
    total = y * 12 + (m - 1) + delta_months
    y2 = total // 12
    m2 = total % 12 + 1
    return y2 * 100 + m2


def _ym_to_iso_01(ym: int) -> str:
    y = ym // 100
    m = ym % 100
    return f"{y:04d}-{m:02d}-01"


def _parse_ym(value: Any) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    m = re.search(r"\b(\d{4})\D?(\d{2})\b", s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        if 1 <= mo <= 12:
            return y * 100 + mo
    m = re.search(r"\b(\d{2})\D+(\d{4})\b", s)
    if m:
        mo = int(m.group(1))
        y = int(m.group(2))
        if 1 <= mo <= 12:
            return y * 100 + mo
    return None


def _score_zip_link(link: str) -> int:
    """Pontua links para decidir qual é a Base Completa."""
    link_lower = link.lower()
    score = 0
    if "base" in link_lower:
        score += 10
    if "completa" in link_lower:
        score += 10
    if "ses" in link_lower:
        score += 5
    if "download" in link_lower:
        score += 2
    if "dados" in link_lower:
        score += 2
    return score


def _discover_ses_zip_url() -> str:
    session = _session()
    candidates = []

    for page in SES_PAGES:
        print(f"Crawling {page}...")
        try:
            r = session.get(page, timeout=30, verify=False)
            if r.status_code != 200:
                print(f"  Status {r.status_code}, skipping.")
                continue
                
            html = r.text
            links = re.findall(r'href=["\']?([^"\'>\s]+\.zip)["\']?', html, re.IGNORECASE)
            
            for link in links:
                full_url = urljoin(page, link)
                candidates.append(full_url)
                
        except Exception as e:
            print(f"  Error crawling {page}: {e}")

    if candidates:
        candidates.sort(key=_score_zip_link, reverse=True)
        best = candidates[0]
        print(f"Crawler found {len(candidates)} ZIPs. Best match: {best}")
        return best

    print("Crawler failed to find any ZIP links. Trying hardcoded fallbacks...")
    
    for fallback in FALLBACK_URLS:
        try:
            print(f"Testing fallback: {fallback}")
            r = session.head(fallback, timeout=15, verify=False, allow_redirects=True)
            if r.status_code == 200:
                print(f"Fallback confirmed: {fallback}")
                return fallback
        except Exception:
            pass

    return FALLBACK_URLS[0]


def _download_zip_to_tempfile(zip_url: Optional[str] = None, timeout_s: int = 300) -> Tuple[Path, str]:
    if not zip_url:
        zip_url = _discover_ses_zip_url()

    print(f"Downloading from: {zip_url}")
    session = _session()
    
    # Tenta baixar com stream
    r = session.get(zip_url, timeout=timeout_s, stream=True, verify=False)
    r.raise_for_status()

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    try:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                tmp.write(chunk)
        tmp.flush()
        return Path(tmp.name), zip_url
    finally:
        tmp.close()


def _detect_encoding_in_zip(z: zipfile.ZipFile, member: str) -> str:
    candidates = ("utf-8-sig", "cp1252", "latin-1", "utf-8")
    with z.open(member) as bf:
        sample = bf.read(8192)
    for enc in candidates:
        try:
            sample.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _read_csv_rows_from_zip(z: zipfile.ZipFile, member: str) -> Tuple[List[str], Iterable[List[str]]]:
    enc = _detect_encoding_in_zip(z, member)
    with z.open(member) as bf:
        text = io.TextIOWrapper(bf, encoding=enc, errors="replace", newline="")
        reader = csv.reader(text, delimiter=";")
        headers = next(reader, [])
        headers_norm = [_norm(h) for h in headers]

    def _rows_iter() -> Iterable[List[str]]:
        with z.open(member) as bf2:
            text2 = io.TextIOWrapper(bf2, encoding=enc, errors="replace", newline="")
            reader2 = csv.reader(text2, delimiter=";")
            _ = next(reader2, None)
            for row in reader2:
                yield row

    return headers_norm, _rows_iter()


def _find_member_case_insensitive(z: zipfile.ZipFile, expected: str) -> Optional[str]:
    expected_l = expected.lower()
    for name in z.namelist():
        if name.lower() == expected_l:
            return name
    return None


def _find_member_by_contains(z: zipfile.ZipFile, needles: List[str]) -> Optional[str]:
    needles_l = [n.lower() for n in needles]
    for name in z.namelist():
        nl = name.lower()
        if not nl.endswith(".csv"):
            continue
        if all(n in nl for n in needles_l):
            return name
    return None


def _find_member_by_header(z: zipfile.ZipFile, required_any: List[List[str]], required_all: List[str]) -> Optional[str]:
    required_all_n = [_norm(x) for x in required_all]
    required_any_n = [[_norm(x) for x in group] for group in required_any]

    for name in z.namelist():
        if not name.lower().endswith(".csv"):
            continue
        try:
            headers_norm, _ = _read_csv_rows_from_zip(z, name)
        except Exception:
            continue
        hs = set(headers_norm)
        if any(h not in hs for h in required_all_n):
            continue
        ok = True
        for group in required_any_n:
            if not any(h in hs for h in group):
                ok = False
                break
        if ok:
            return name
    return None


def extract_ses_master_and_financials(
    zip_url: Optional[str] = None,
) -> Tuple[SesExtractionMeta, Dict[str, Dict[str, Any]]]:
    
    tmp_zip, used_url = _download_zip_to_tempfile(zip_url)
    
    try:
        with zipfile.ZipFile(tmp_zip) as z:
            cias = _find_member_case_insensitive(z, "Ses_cias.csv") or _find_member_case_insensitive(z, "SES_cias.csv")
            seguros = _find_member_case_insensitive(z, "Ses_seguros.csv") or _find_member_case_insensitive(z, "SES_seguros.csv")

            if not cias:
                cias = _find_member_by_contains(z, ["ses", "cias"])
            if not seguros:
                seguros = _find_member_by_contains(z, ["ses", "seguros"])

            if not cias:
                cias = _find_member_by_header(
                    z,
                    required_any=[
                        ["coenti", "cod_enti", "codcia", "cod_cia", "codigo_cia"],
                        ["noenti", "nome", "razao_social"],
                    ],
                    required_all=[],
                )
            if not seguros:
                seguros = _find_member_by_header(
                    z,
                    required_any=[
                        ["coenti", "cod_enti", "codcia", "cod_cia", "codigo_cia"],
                        ["damesano", "anomes", "ano_mes", "competencia", "mesano"],
                        ["premio", "premio_emitido", "vl_premio", "vl_premio_emitido", "premio_total"],
                    ],
                    required_all=[],
                )

            if not cias or not seguros:
                raise RuntimeError(f"ZIP baixado de {used_url}, mas não contém CSVs esperados. Cias={cias}, Seguros={seguros}")

            # --- Lê CIAS ---
            h_cias, rows_cias = _read_csv_rows_from_zip(z, cias)
            idx_cias = {h: i for i, h in enumerate(h_cias)}

            def pick_idx(possibles: List[str]) -> Optional[int]:
                for p in possibles:
                    pn = _norm(p)
                    if pn in idx_cias:
                        return idx_cias[pn]
                return None

            id_i = pick_idx(["coenti", "cod_enti", "codcia", "cod_cia", "codigo_cia", "cd_entidade"])
            name_i = pick_idx(["noenti", "nome", "razao_social", "nome_cia", "nome_entidade"])
            cnpj_i = pick_idx(["cnpj", "numcnpj", "nr_cnpj", "cpf_cnpj", "cnpj_cia"])

            companies: Dict[str, Dict[str, Any]] = {}
            for row in rows_cias:
                if not row or len(row) <= max(id_i, name_i):
                    continue
                ses_id = _digits((row[id_i] or "").strip())
                if not ses_id:
                    continue
                ses_id = ses_id.zfill(6)
                nm = (row[name_i] or "").strip()
                if not nm:
                    continue
                cnpj = _digits(row[cnpj_i]) if cnpj_i is not None and len(row) > cnpj_i else None
                companies[ses_id] = {"name": nm, "cnpj": cnpj}

            # --- Lê SEGUROS ---
            h_seg, _ = _read_csv_rows_from_zip(z, seguros)
            idx_seg = {h: i for i, h in enumerate(h_seg)}

            def pick_idx_seg(possibles: List[str]) -> Optional[int]:
                for p in possibles:
                    pn = _norm(p)
                    if pn in idx_seg:
                        return idx_seg[pn]
                return None

            seg_id_i = pick_idx_seg(["coenti", "cod_enti", "codcia", "cod_cia", "codigo_cia", "cd_entidade"])
            seg_ym_i = pick_idx_seg(["damesano", "anomes", "ano_mes", "competencia", "mesano"])
            premio_i = pick_idx_seg(["premio_emitido", "premio", "vl_premio_emitido", "vl_premio", "premio_total"])
            sin_i = pick_idx_seg(["sinistros", "sinistro", "sinistro_ocorrido", "vl_sinistro", "sinistro_total", "vl_sinistros"])

            def iter_seguros_rows():
                _, rows = _read_csv_rows_from_zip(z, seguros)
                return rows

            max_ym: Optional[int] = None
            for row in iter_seguros_rows():
                if not row or len(row) <= seg_ym_i:
                    continue
                ym = _parse_ym(row[seg_ym_i])
                if ym and (max_ym is None or ym > max_ym):
                    max_ym = ym

            if max_ym is None:
                raise RuntimeError(f"Sem competência (AAAAMM) em {seguros}.")

            start_ym = _ym_add(max_ym, -11)
            agg: Dict[str, Dict[str, float]] = {}
            for row in iter_seguros_rows():
                if not row or len(row) <= max(seg_id_i, seg_ym_i, premio_i):
                    continue
                ym = _parse_ym(row[seg_ym_i])
                if ym is None or ym < start_ym or ym > max_ym:
                    continue
                ses_id = _digits((row[seg_id_i] or "").strip())
                if not ses_id:
                    continue
                ses_id = ses_id.zfill(6)
                premio = _parse_brl_number(row[premio_i])
                sin = _parse_brl_number(row[sin_i]) if sin_i is not None and len(row) > sin_i else 0.0
                cur = agg.setdefault(ses_id, {"premiums": 0.0, "claims": 0.0})
                cur["premiums"] += premio
                cur["claims"] += sin

            period_from = _ym_to_iso_01(start_ym)
            period_to = _ym_to_iso_01(max_ym)

            out: Dict[str, Dict[str, Any]] = {}
            for ses_id, fin in agg.items():
                if fin.get("premiums", 0.0) <= 0:
                    continue
                base = companies.get(ses_id) or {"name": f"SES_ENTIDADE_{ses_id}", "cnpj": None}
                out[ses_id] = {
                    "sesId": ses_id,
                    "name": base.get("name"),
                    "cnpj": base.get("cnpj"),
                    "premiums": round(float(fin.get("premiums", 0.0)), 2),
                    "claims": round(float(fin.get("claims", 0.0)), 2),
                }

            meta = SesExtractionMeta(
                zip_url=used_url,
                cias_file=cias,
                seguros_file=seguros,
                period_from=period_from,
                period_to=period_to,
            )
            return meta, out
    finally:
        try:
            tmp_zip.unlink(missing_ok=True)
        except Exception:
            pass
