# api/sources/consumidor_gov.py
from __future__ import annotations

import csv
import gzip
import io
import json
import os
import re
import shutil
import tempfile
import unicodedata
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Optional, Tuple
from urllib.parse import quote, urljoin

from curl_cffi import requests

# --- CONFIGURAÇÕES GERAIS ---
CKAN_API_BASE = os.getenv("CG_CKAN_API_BASE", "https://dados.mj.gov.br/api/3/action/")
CKAN_QUERY = os.getenv("CG_CKAN_QUERY", "consumidor.gov")
ALLOW_BASECOMPLETA = os.getenv("CG_ALLOW_BASECOMPLETA", "0") == "1"

TIMEOUT = int(os.getenv("CG_TIMEOUT", "600"))
MIN_BYTES = int(os.getenv("CG_MIN_BYTES", "50000"))
CHUNK_SIZE = int(os.getenv("CG_CHUNK_SIZE", "1048576"))  # 1MB

# Diretórios
RAW_DIR = Path(os.getenv("CG_RAW_DIR", "data/raw/consumidor_gov"))
# Alias para compatibilidade com código legado que importe CACHE_DIR
CACHE_DIR = RAW_DIR

# Nota: DERIVED_DIR é usado apenas como default/fallback; as funções honram output_dir
DEFAULT_DERIVED_DIR = Path(os.getenv("CG_DERIVED_DIR", "data/derived/consumidor_gov"))

# Filtro (mantido por compat, mas o parser aplica filtro flexível "seguros" por segurança)
TARGET_SEGMENT = "Seguros, Capitalização e Previdência"

_CNPJ_RE = re.compile(r"\D+")
_FILE_RE = re.compile(r"\.(csv|zip|gz)(\?|$)", re.I)
_FINALIZADAS_OR_BASE_RE = re.compile(r"(finalizadas|basecompleta)", re.I)
_YM_ANY_RE = re.compile(r"(20\d{2})[^\d]?(0[1-9]|1[0-2])")

DIRECT_DOWNLOAD_PAGE = "https://www.consumidor.gov.br/pages/dadosabertos/externo/"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# --- VALIDAÇÃO E NORMALIZAÇÃO ---


def _safe_float(v: Any) -> float:
    try:
        if v in (None, "", "NA", "N/A", "-"):
            return 0.0
        s = str(v).strip()
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            s = s.replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


def _safe_int(v: Any) -> int:
    try:
        return int(_safe_float(v))
    except Exception:
        return 0


def _validate_entry(entry: dict[str, Any]) -> bool:
    """Valida se a entrada possui estatísticas mínimas para ser útil."""
    st = entry.get("statistics") or {}
    keys_to_check = [
        "complaintsCount",
        "total_claims",
        "resolvedCount",
        "respondedCount",
        "finalizedCount",
    ]
    return any(_safe_int(st.get(k, 0)) > 0 for k in keys_to_check)


# --- HELPERS LEGADOS ---


def _norm_col(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.lower().strip()


def _norm_key(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def _bool_from_pt(val: Any) -> bool | None:
    if not val:
        return None
    s = str(val).strip().lower()
    if s in ("s", "sim", "si", "yes", "y", "1", "true"):
        return True
    if s in ("n", "nao", "não", "no", "0", "false"):
        return False
    return None


def _pick_col(row: dict[str, Any], candidates: list[str]) -> str:
    if not row:
        return ""
    keys = list(row.keys())
    lower_map = {k.lower().strip(): k for k in keys if isinstance(k, str)}
    norm_map = {_norm_key(k): k for k in keys if isinstance(k, str)}

    for c in candidates:
        if not c:
            continue
        if c in row:
            return str(row[c])
        cl = c.lower().strip()
        if cl in lower_map:
            return str(row[lower_map[cl]])
        cn = _norm_key(c)
        if cn in norm_map:
            return str(row[norm_map[cn]])
    return ""


def normalize_cnpj(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    d = _CNPJ_RE.sub("", str(v))
    return d if len(d) == 14 else None


# --- INFRAESTRUTURA DE URL/SCORE ---


def _blob(url: str, meta: Optional[dict] = None) -> str:
    b = (url or "").lower()
    if meta:
        b += " " + str(meta.get("name") or "").lower()
        b += " " + str(meta.get("description") or "").lower()
    return b


def _ym_variants(ym: str) -> set[str]:
    y, m = ym.split("-")
    return {ym, f"{y}{m}", f"{y}_{m}", f"{y}.{m}", f"{y}/{m}", f"{y}-{m}"}


def _blob_has_ym(b: str, ym: str) -> bool:
    bb = (b or "").lower()
    return any(v in bb for v in _ym_variants(ym))


def _is_monthly_dump_candidate(url: str, meta: Optional[dict] = None) -> bool:
    b = _blob(url, meta)
    if not _FINALIZADAS_OR_BASE_RE.search(b):
        return False
    if "basecompleta" in b and not ALLOW_BASECOMPLETA:
        return False
    return ("finalizadas" in b) or ("basecompleta" in b)


def _score_url(url: str, meta: Optional[dict] = None) -> int:
    u = (url or "").lower()
    score = 0
    if "finalizadas" in u:
        score += 1_000_000
    if "basecompleta" in u:
        score -= 250_000
    if u.endswith(".zip"):
        score += 50_000
    elif u.endswith(".csv"):
        score += 40_000

    m = _YM_ANY_RE.search(u)
    if m:
        score += int(m.group(1)) * 100 + int(m.group(2))

    if meta:
        lm = meta.get("last_modified") or meta.get("created") or ""
        mm = _YM_ANY_RE.search(str(lm))
        if mm:
            score += int(mm.group(1)) * 100 + int(mm.group(2))
    return score


@dataclass
class Agg:
    display_name: str
    cnpj_key: Optional[str] = None

    total_claims: int = 0
    evaluated_claims: int = 0
    resolved_claims: int = 0
    responded_claims: int = 0
    finalized_claims: int = 0

    score_sum: float = 0.0
    response_time_sum: float = 0.0
    response_time_count: int = 0

    def merge_raw(self, *args: Any, **__: Any) -> None:
        """
        Compat: usado pelo build_consumidor_gov ao agregar vários meses.
        Aceita chamadas como merge_raw(raw) ou merge_raw(ym, raw).
        """
        raw: Any = None
        for a in args:
            if isinstance(a, dict):
                raw = a
                break
        if not isinstance(raw, dict):
            return

        disp = raw.get("display_name") or raw.get("name") or raw.get("displayName")
        if disp and not self.display_name:
            self.display_name = str(disp).strip()

        cnpj = normalize_cnpj(raw.get("cnpj") or raw.get("cnpjKey") or raw.get("cnpj_key"))
        if cnpj and not self.cnpj_key:
            self.cnpj_key = cnpj

        st = raw.get("statistics") if isinstance(raw.get("statistics"), dict) else raw
        if not isinstance(st, dict):
            return

        self.total_claims += _safe_int(
            st.get("complaintsCount") or st.get("total_claims") or st.get("totalClaims") or 0
        )
        self.finalized_claims += _safe_int(st.get("finalizedCount") or st.get("finalized_claims") or 0)
        self.responded_claims += _safe_int(st.get("respondedCount") or st.get("responded_claims") or 0)
        self.resolved_claims += _safe_int(st.get("resolvedCount") or st.get("resolved_claims") or 0)

        ec = _safe_int(st.get("evaluatedCount") or st.get("evaluated_claims") or st.get("satisfaction_count") or 0)
        self.evaluated_claims += ec

        score_sum = _safe_float(st.get("scoreSum") or st.get("score_sum") or 0)
        if score_sum <= 0 and ec > 0:
            ov_f = _safe_float(st.get("overallSatisfaction"))
            if ov_f > 0:
                score_sum = ov_f * ec
        self.score_sum += score_sum

        rt_sum = _safe_float(st.get("responseTimeSum") or st.get("response_time_sum") or 0)
        rt_count = _safe_int(st.get("responseTimeCount") or st.get("response_time_count") or 0)

        if rt_sum <= 0 and rt_count <= 0:
            avg = _safe_float(st.get("averageResponseTime") or 0)
            if avg > 0:
                rt_count = _safe_int(st.get("responseTimeCount") or st.get("respondedCount") or 0)
                if rt_count > 0:
                    rt_sum = avg * rt_count

        self.response_time_sum += rt_sum
        self.response_time_count += rt_count

    def to_public(self) -> dict:
        ec = self.evaluated_claims
        tc = self.total_claims

        sat_avg = round(self.score_sum / ec, 2) if ec > 0 else None

        denom_sol = ec if ec > 0 else (self.finalized_claims if self.finalized_claims > 0 else tc)
        sol_idx = round(self.resolved_claims / denom_sol, 2) if denom_sol > 0 else None

        rc = self.response_time_count
        resp_time = round(self.response_time_sum / rc, 1) if rc > 0 else None

        return {
            "display_name": self.display_name,
            "name": self.display_name,
            "cnpj": self.cnpj_key,
            "statistics": {
                "complaintsCount": tc,
                "finalizedCount": self.finalized_claims,
                "evaluatedCount": ec,
                "satisfaction_count": ec,
                "respondedCount": self.responded_claims,
                "resolvedCount": self.resolved_claims,
                "overallSatisfaction": sat_avg,
                "solutionIndex": sol_idx,
                "averageResponseTime": resp_time,
                "scoreSum": self.score_sum,
                "responseTimeSum": self.response_time_sum,
                "responseTimeCount": self.response_time_count,
            },
            "indexes": {"b": {"nota": sat_avg}},
        }


# --- REDE ---


def _ckan_resource_search(client: requests.Session, term: str, limit: int = 50) -> list[dict]:
    api = urljoin(CKAN_API_BASE, "resource_search")
    url = f"{api}?query={quote(term)}&limit={limit}"
    try:
        r = client.get(url, timeout=30)
        if r.status_code != 200:
            return []
        data = r.json()
        if not data.get("success"):
            return []
        return (data.get("result") or {}).get("results") or []
    except Exception:
        return []


def _get_dump_url_for_month(client: requests.Session, ym: str) -> Optional[str]:
    try:
        terms = [
            f"finalizadas {ym}",
            f"finalizadas_{ym}",
            f"basecompleta{ym}",
            f"basecompleta {ym}",
            f"{CKAN_QUERY} {ym}",
        ]
        best: Tuple[int, str] | None = None

        for term in terms:
            print(f"CG: CKAN resource_search -> {term}")
            resources = _ckan_resource_search(client, term)
            for res in resources:
                u = res.get("url") or ""
                if not u or not _FILE_RE.search(u):
                    continue
                if not _is_monthly_dump_candidate(u, res):
                    continue
                b = _blob(u, res)
                if not _blob_has_ym(b, ym):
                    continue
                sc = _score_url(u, res)
                if best is None or sc > best[0]:
                    best = (sc, u)

        if best:
            print(f"CG: CKAN candidate {ym}: {best[1]}")
            return best[1]
    except Exception as e:
        print(f"CG: CKAN falhou ({ym}): {e}")

    print(f"CG: Fallback HTML ({ym}) ...")
    try:
        r = client.get(DIRECT_DOWNLOAD_PAGE, timeout=30)
        if r.status_code != 200:
            return None
        html = r.text or ""
        hrefs = re.findall(r'href\s*=\s*["\']([^"\']+)["\']', html, flags=re.I)
        candidates: list[Tuple[int, str]] = []
        for h in hrefs:
            if not h:
                continue
            full = urljoin("https://www.consumidor.gov.br", h)
            if not _FILE_RE.search(full):
                continue
            if not _is_monthly_dump_candidate(full):
                continue
            if not _blob_has_ym(full, ym):
                continue
            candidates.append((_score_url(full), full))
        if not candidates:
            return None
        candidates.sort(reverse=True)
        return candidates[0][1]
    except Exception as e:
        print(f"CG: HTML scrape falhou ({ym}): {e}")
        return None


def download_dump_to_file(url: str, client: requests.Session) -> Optional[Path]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DIR / "dump_latest.bin"
    print(f"CG: Baixando {url} para {out_path}...")
    try:
        r = client.get(url, stream=True, timeout=TIMEOUT)
        if r.status_code != 200:
            if out_path.exists():
                out_path.unlink()
            return None
        total_bytes = 0
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
                    total_bytes += len(chunk)
        if total_bytes < MIN_BYTES:
            if out_path.exists():
                out_path.unlink()
            return None
        return out_path
    except Exception as e:
        print(f"CG: Exceção download: {e}")
        if out_path.exists():
            out_path.unlink()
        return None


def open_dump_file(path: Path) -> BinaryIO:
    with open(path, "rb") as f:
        sig = f.read(4)
    if sig.startswith(b"\x1f\x8b"):
        print("CG: GZIP detectado.")
        return gzip.open(path, "rb")
    if sig.startswith(b"PK\x03\x04"):
        print("CG: ZIP detectado.")
        z = zipfile.ZipFile(path, "r")
        try:
            csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if not csvs:
                raise ValueError("ZIP sem CSV")
            target = max(csvs, key=lambda x: z.getinfo(x).file_size)
            print(f"CG: Extraindo {target}...")
            RAW_DIR.mkdir(parents=True, exist_ok=True)
            tmp = tempfile.NamedTemporaryFile(
                delete=False,
                suffix=".csv",
                prefix="cg_extract_",
                dir=str(RAW_DIR),
            )
            tmp_path = Path(tmp.name)
            with z.open(target) as zfh:
                shutil.copyfileobj(zfh, tmp)
            tmp.close()
        finally:
            z.close()
        return open(tmp_path, "rb")
    print("CG: Assumindo CSV direto.")
    return open(path, "rb")


# --- PROCESSAMENTO PRINCIPAL ---


def process_dump_to_monthly(dump_path: Path, target_yms: list[str], output_dir: str) -> None:
    """
    Processa o dump e gera JSONs mensais.
    Obedece estritamente ao `output_dir` e usa filtro de segmento flexível.
    """
    target_set = set(target_yms)

    out_base_path = Path(output_dir)
    out_base_path.mkdir(parents=True, exist_ok=True)

    try:
        csv_stream = open_dump_file(dump_path)
    except Exception as e:
        print(f"CG: Falha ao abrir dump: {e}")
        return

    encodings = ["utf-8-sig", "utf-8", "latin1", "cp1252"]
    delimiter = ";"
    encoding_used: str | None = None

    sample_bytes = csv_stream.read(16384)
    csv_stream.seek(0)

    valid_setup = False
    for enc in encodings:
        try:
            sample_str = sample_bytes.decode(enc)
            delim = ";" if sample_str.count(";") >= sample_str.count(",") else ","
            first_line = sample_str.splitlines()[0] if sample_str else ""
            headers = [h.strip().lower() for h in first_line.split(delim)]
            h_norm = [_norm_col(h) for h in headers]
            if any("segmento" in h for h in h_norm) and any(
                ("nome" in h or "fornecedor" in h or "empresa" in h) for h in h_norm
            ):
                encoding_used = enc
                delimiter = delim
                valid_setup = True
                break
        except Exception:
            continue

    if not valid_setup or not encoding_used:
        csv_stream.close()
        raise RuntimeError(f"CG: Encoding/headers não detectados em {dump_path.name}")

    print(f"CG: Processando CSV (Enc: {encoding_used}, Delim: {delimiter!r})...")

    monthly_data: dict[str, dict[str, Agg]] = {ym: {} for ym in target_set}
    rows_total = 0
    rows_eligible = 0

    try:
        text_stream = io.TextIOWrapper(csv_stream, encoding=encoding_used, errors="replace", newline="")
        reader = csv.DictReader(text_stream, delimiter=delimiter)

        for row in reader:
            rows_total += 1

            seg = _pick_col(row, ["Segmento de Mercado", "Segmento", "Area"])
            seg_norm = _norm_key(seg)

            # Aceita se contiver "seguros" (reduz risco de dataset vazio)
            if "seguros" not in seg_norm:
                continue

            rows_eligible += 1

            date_str = _pick_col(row, ["Data Finalizacao", "Data Abertura", "Data da Reclamacao", "Data"])
            ym: str | None = None
            m1 = re.search(r"(\d{2})[/-](\d{2})[/-](\d{4})", date_str)
            if m1:
                ym = f"{m1.group(3)}-{m1.group(2)}"
            else:
                m2 = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", date_str)
                if m2:
                    ym = f"{m2.group(1)}-{m2.group(2)}"

            if not ym or ym not in target_set:
                continue

            name_raw = _pick_col(row, ["Nome Fantasia", "Nome do Fornecedor", "Fornecedor", "Empresa", "Nome"])
            if not name_raw:
                continue

            nk = _norm_key(name_raw)
            cnpj_key = normalize_cnpj(_pick_col(row, ["CNPJ", "CNPJ do Fornecedor", "Documento", "CPF/CNPJ"]))

            situ = _pick_col(row, ["Situação", "Situacao", "Status", "Situação da Reclamação"])
            is_finalized = "finaliz" in _norm_col(situ)

            resp_val = _pick_col(row, ["Respondida", "Respondida?", "Empresa Respondeu", "Respondeu"])
            is_respondida = _bool_from_pt(resp_val)

            aval = _pick_col(row, ["Avaliacao Reclamacao", "Avaliação", "Resolvida", "Resolvida?"])
            aval_norm = _norm_col(aval)
            is_resolved = (
                ("resolvida" in aval_norm)
                and ("nao resolvida" not in aval_norm)
                and (not aval_norm.startswith("nao "))
            )

            nota = _safe_float(_pick_col(row, ["Nota do Consumidor", "Nota Consumidor", "Nota"]))
            tempo = _safe_float(_pick_col(row, ["Tempo Resposta", "Tempo de Resposta", "Dias Resposta"]))

            if (is_respondida is None or not is_respondida) and tempo > 0:
                is_respondida = True

            is_finalized_b = bool(is_finalized)
            is_respondida_b = bool(is_respondida)
            is_resolved_b = bool(is_resolved)

            if nk not in monthly_data[ym]:
                monthly_data[ym][nk] = Agg(display_name=name_raw.strip(), cnpj_key=cnpj_key)

            agg = monthly_data[ym][nk]
            agg.total_claims += 1

            if is_finalized_b:
                agg.finalized_claims += 1
            if is_respondida_b:
                agg.responded_claims += 1
            if is_resolved_b:
                agg.resolved_claims += 1

            if nota > 0:
                agg.score_sum += nota
                agg.evaluated_claims += 1

            if tempo > 0:
                agg.response_time_sum += tempo
                agg.response_time_count += 1

            if cnpj_key and not agg.cnpj_key:
                agg.cnpj_key = cnpj_key

    except Exception as e:
        print(f"CG: Erro loop linhas: {e}")
    finally:
        try:
            csv_stream.close()
        except Exception:
            pass
        try:
            p = Path(csv_stream.name) if hasattr(csv_stream, "name") else None
            if p and p.exists() and "cg_extract_" in p.name:
                p.unlink()
        except Exception:
            pass

    count_files = 0
    for ym, data_map in monthly_data.items():
        if not data_map:
            continue

        by_name_raw: dict[str, Any] = {}
        by_cnpj_raw: dict[str, Any] = {}

        for k, agg in data_map.items():
            raw_obj = agg.to_public()
            if not _validate_entry(raw_obj):
                continue

            by_name_raw[k] = raw_obj
            if agg.cnpj_key:
                by_cnpj_raw[agg.cnpj_key] = raw_obj

        if by_name_raw:
            out_p = out_base_path / f"consumidor_gov_{ym}.json"
            payload = {
                "meta": {
                    "ym": ym,
                    "generated_at": _utc_now(),
                    "parse": {"rows_total": rows_total, "rows_eligible": rows_eligible},
                },
                "by_name": by_name_raw,
                "by_name_key_raw": by_name_raw,
                "by_cnpj_key_raw": by_cnpj_raw,
            }
            out_p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            count_files += 1

    print(f"CG: Processamento concluído. {count_files} meses gerados em {out_base_path}.")


def sync_monthly_cache_from_dump_if_needed(target_yms: list[str], monthly_dir: str) -> None:
    output_path = Path(monthly_dir)
    missing = [ym for ym in target_yms if not (output_path / f"consumidor_gov_{ym}.json").exists()]

    if not missing:
        print("CG: Cache completo.")
        return

    print(f"CG: Baixando dados para: {missing}")
    client = requests.Session(impersonate="chrome110")

    env_url = os.getenv("CG_DUMP_URL")
    if env_url:
        dump_path = download_dump_to_file(env_url, client)
        if dump_path:
            process_dump_to_monthly(dump_path, target_yms, str(output_path))
            try:
                dump_path.unlink()
            except Exception:
                pass
        return

    for ym in missing:
        url = _get_dump_url_for_month(client, ym)
        if not url:
            continue
        dump_path = download_dump_to_file(url, client)
        if not dump_path:
            continue
        process_dump_to_monthly(dump_path, [ym], str(output_path))
        try:
            dump_path.unlink()
        except Exception:
            pass


sync_monthly_cache_from_dump = sync_monthly_cache_from_dump_if_needed
