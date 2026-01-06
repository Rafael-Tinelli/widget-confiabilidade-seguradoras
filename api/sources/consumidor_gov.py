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
from typing import Any, BinaryIO, Dict, List, Optional, Tuple
from urllib.parse import urljoin, quote

from curl_cffi import requests

# --- CONFIGURAÇÕES ---
CKAN_API_BASE = os.getenv("CG_CKAN_API_BASE", "https://dados.mj.gov.br/api/3/action/")
CKAN_QUERY = os.getenv("CG_CKAN_QUERY", "consumidor.gov")
ALLOW_BASECOMPLETA = os.getenv("CG_ALLOW_BASECOMPLETA", "0") == "1"

TIMEOUT = int(os.getenv("CG_TIMEOUT", "600"))
MIN_BYTES = int(os.getenv("CG_MIN_BYTES", "50000"))
CHUNK_SIZE = int(os.getenv("CG_CHUNK_SIZE", "1048576")) # 1MB
CACHE_DIR = Path("data/raw/consumidor_gov")
DIRECT_DOWNLOAD_PAGE = "https://www.consumidor.gov.br/pages/dadosabertos/externo/"

# Filtro Rígido
TARGET_SEGMENT = "Seguros, Capitalização e Previdência"

_CNPJ_RE = re.compile(r"\D+")
_FILE_RE = re.compile(r"\.(csv|zip|gz)(\?|$)", re.I)
_FINALIZADAS_OR_BASE_RE = re.compile(r"(finalizadas|basecompleta)", re.I)
_YM_ANY_RE = re.compile(r"(20\d{2})[^\d]?(0[1-9]|1[0-2])")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _norm_col(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.lower().strip()


def _norm_key(s: str) -> str:
    """Normaliza strings para chave de dicionário (sem acentos, lower, alphanum)."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def _bool_from_pt(val: Any) -> bool | None:
    """Converte 'S', 'Sim' para True. Retorna None se vazio/inconclusivo."""
    if not val:
        return None
    s = str(val).strip().lower()
    if s in ("s", "sim", "si", "yes", "y", "1", "true"):
        return True
    if s in ("n", "nao", "não", "no", "0", "false"):
        return False
    return None


def _to_float(x: Any) -> float:
    """
    Converte string numérica para float de forma defensiva (BR/US).
    Ex: '1.234,56' -> 1234.56
        '1,234.56' -> 1234.56
        '10' -> 10.0
    """
    try:
        s = str(x or "").strip()
        if not s:
            return 0.0
        
        # Mantém apenas dígitos, sinal, ponto e vírgula
        s = re.sub(r"[^0-9,\.\-]+", "", s)
        if not s or s in {"-", ",", "."}:
            return 0.0

        # Heurística para decidir separador decimal
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                # Formato BR: 1.234,56 -> remove ponto, troca vírgula
                s = s.replace(".", "").replace(",", ".")
            else:
                # Formato US: 1,234.56 -> remove vírgula
                s = s.replace(",", "")
        else:
            # Se só tem vírgula, assume decimal BR
            if "," in s:
                s = s.replace(",", ".")
        
        return float(s)
    except Exception:
        return 0.0


def _pick_col(row: dict[str, Any], candidates: list[str]) -> str:
    """
    Encontra valor na row usando matching flexível.
    """
    if not row:
        return ""

    # Otimização: maps construídos on-the-fly
    keys = list(row.keys())
    lower_map = {}
    norm_map = {}
    
    for k in keys:
        if not isinstance(k, str):
            continue
        lower_map[k.lower().strip()] = k
        norm_map[_norm_key(k)] = k

    for c in candidates:
        if not c:
            continue
        
        # 1. Exact
        if c in row:
            return str(row[c])
        
        # 2. Lower
        cl = c.lower().strip()
        if cl in lower_map:
            return str(row[lower_map[cl]])
            
        # 3. Normalized
        cn = _norm_key(c)
        if cn in norm_map:
            return str(row[norm_map[cn]])
            
        # 4. Substring (fallback)
        if cn:
            for nk, ok in norm_map.items():
                if cn in nk:
                    return str(row[ok])
                
    return ""


def normalize_cnpj(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    d = _CNPJ_RE.sub("", str(v))
    return d if len(d) == 14 else None


def normalize_key_name(raw: str) -> str:
    return _norm_key(raw)


# --- Infraestrutura de URL/Score (MANTIDA) ---

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
    
    # Contadores
    total_claims: int = 0
    evaluated_claims: int = 0
    resolved_claims: int = 0
    responded_claims: int = 0
    finalized_claims: int = 0
    
    # Somas
    score_sum: float = 0.0
    response_time_sum: float = 0.0
    
    # Contadores auxiliares para médias
    satisfaction_count: int = 0
    response_time_count: int = 0

    def merge_raw(self, raw: dict) -> None:
        # Mantido para compatibilidade se usado externamente
        pass 

    def to_public(self) -> dict:
        ec = self.satisfaction_count
        tc = self.total_claims
        
        sat_avg = round(self.score_sum / ec, 2) if ec > 0 else None
        
        # Índice de Solução
        denom_sol = ec if ec > 0 else (self.finalized_claims if self.finalized_claims > 0 else tc)
        sol_idx = round(self.resolved_claims / denom_sol, 2) if denom_sol > 0 else None
        
        rc = self.response_time_count
        resp_time = round(self.response_time_sum / rc, 1) if rc > 0 else None

        return {
            "display_name": self.display_name,
            "name": self.display_name, # Compatibilidade
            "cnpj": self.cnpj_key,
            "statistics": {
                "complaintsCount": tc,
                "finalizedCount": self.finalized_claims,
                "evaluatedCount": ec,
                "respondedCount": self.responded_claims,
                "resolvedCount": self.resolved_claims,
                "overallSatisfaction": sat_avg,
                "solutionIndex": sol_idx,
                "averageResponseTime": resp_time,
                "responseTimeCount": rc,
                "scoreSum": self.score_sum,
                "responseTimeSum": self.response_time_sum
            },
            "indexes": {"b": {"nota": sat_avg}}
        }


# --- Funções de Rede (MANTIDAS) ---

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
            f"finalizadas {ym}", f"finalizadas_{ym}",
            f"basecompleta{ym}", f"basecompleta {ym}",
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

    # Fallback HTML
    print(f"CG: Fallback HTML ({ym}) ...")
    try:
        r = client.get(DIRECT_DOWNLOAD_PAGE, timeout=30)
        if r.status_code != 200:
            return None
        html = r.text or ""
        hrefs = re.findall(r'href\s*=\s*["\']([^"\']+)["\']', html, flags=re.I)
        candidates = []
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
        print(f"CG: HTML candidate {ym}: {candidates[0][1]}")
        return candidates[0][1]
    except Exception as e:
        print(f"CG: HTML scrape falhou ({ym}): {e}")
        return None


def download_dump_to_file(url: str, client: requests.Session) -> Optional[Path]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CACHE_DIR / "dump_latest.bin"
    print(f"CG: Baixando {url} para {out_path}...")
    try:
        r = client.get(url, stream=True, timeout=TIMEOUT)
        try:
            if r.status_code != 200:
                print(f"CG: Erro HTTP {r.status_code}")
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
        finally:
            if hasattr(r, 'close'):
                r.close()
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
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", prefix="cg_extract_", dir=str(CACHE_DIR))
            tmp_path = Path(tmp.name)
            with z.open(target) as zfh:
                shutil.copyfileobj(zfh, tmp)
            tmp.close()
        finally:
            z.close()
        return open(tmp_path, "rb")
    print("CG: Assumindo CSV direto.")
    return open(path, "rb")


# --- PARSER PRINCIPAL (ROBUSTO) ---

def process_dump_to_monthly(dump_path: Path, target_yms: List[str], output_dir: str):
    """
    Processa o dump (CSV/ZIP) e gera JSONs mensais agregados.
    Implementa: detecção de encoding, fail-fast em headers, filtro de segmento.
    """
    target_set = set(target_yms)

    try:
        csv_stream = open_dump_file(dump_path)
    except Exception as e:
        print(f"CG: Falha ao abrir dump: {e}")
        return

    # 1. Detecção de Encoding/Delimiter (Heurística Fail-Fast)
    encodings = ["utf-8-sig", "utf-8", "latin1", "cp1252"]
    delimiter = ";"
    encoding_used = None

    # Lê amostra
    sample_bytes = csv_stream.read(16384)
    csv_stream.seek(0)

    valid_setup = False

    for enc in encodings:
        try:
            sample_str = sample_bytes.decode(enc)
            # Sniff delimiter
            delim = ";" if sample_str.count(";") >= sample_str.count(",") else ","

            # Testa headers
            first_line = sample_str.splitlines()[0] if sample_str else ""
            headers = [h.strip().lower() for h in first_line.split(delim)]
            h_norm = [_norm_col(h) for h in headers]

            # Validação mínima: tem 'segmento' e ('nome' ou 'empresa')?
            has_seg = any("segmento" in h for h in h_norm)
            has_name = any(("nome" in h or "fornecedor" in h or "empresa" in h) for h in h_norm)

            if has_seg and has_name:
                encoding_used = enc
                delimiter = delim
                valid_setup = True
                break
        except UnicodeDecodeError:
            continue
        except Exception:
            continue

    if not valid_setup:
        csv_stream.close()
        # Fail-fast: não gerar dados vazios se não conseguimos ler
        raise RuntimeError(f"CG: Não foi possível detectar encoding/headers válidos em {dump_path.name}")

    print(f"CG: Processando CSV (Enc: {encoding_used}, Delim: {delimiter!r})...")

    monthly_data: Dict[str, Dict[str, Agg]] = {ym: {} for ym in target_set}

    rows_total = 0
    rows_eligible = 0
    rows_cnpj = 0
    has_cnpj_col = False

    target_segment_norm = _norm_key(TARGET_SEGMENT)

    try:
        # Abre DictReader com o encoding descoberto e errors='replace' para segurança
        text_stream = io.TextIOWrapper(csv_stream, encoding=encoding_used, errors="replace", newline="")
        reader = csv.DictReader(text_stream, delimiter=delimiter)

        # Check para log
        if reader.fieldnames and any("cnpj" in _norm_col(h) for h in reader.fieldnames):
            has_cnpj_col = True

        for row in reader:
            rows_total += 1

            # 1. Filtro de Segmento (Obrigatório)
            seg = _pick_col(row, ["Segmento de Mercado", "Segmento", "Area"])
            seg_norm = _norm_key(seg)

            if seg_norm != target_segment_norm:
                # Tolerância: deve conter "seguros" E "previdencia"
                if not ("seguros" in seg_norm and "previdencia" in seg_norm):
                    continue

            rows_eligible += 1

            # 2. Data (Mês)
            date_str = _pick_col(row, ["Data Finalizacao", "Data Abertura", "Data da Reclamacao", "Data"])
            ym = None
            # DD/MM/YYYY
            m1 = re.search(r"(\d{2})[\/-](\d{2})[\/-](\d{4})", date_str)
            if m1:
                ym = f"{m1.group(3)}-{m1.group(2)}"
            else:
                # YYYY-MM-DD
                m2 = re.search(r"(\d{4})[\/-](\d{2})[\/-](\d{2})", date_str)
                if m2:
                    ym = f"{m2.group(1)}-{m2.group(2)}"

            if not ym or ym not in target_set:
                continue

            # 3. Identificadores
            name_raw = _pick_col(row, ["Nome Fantasia", "Nome do Fornecedor", "Fornecedor", "Empresa", "Nome"])
            if not name_raw:
                continue
            nk = _norm_key(name_raw)

            cnpj_key = normalize_cnpj(
                _pick_col(row, ["CNPJ", "CNPJ do Fornecedor", "Documento", "CPF/CNPJ"])
            )
            if cnpj_key:
                rows_cnpj += 1

            # 4. Métricas (Parse Robusto)

            # Situação
            situ = _pick_col(row, ["Situação", "Situacao", "Status", "Situação da Reclamação"])
            is_finalized = "finaliz" in _norm_col(situ)

            # Respondida
            resp = _pick_col(row, ["Respondida", "Respondida?", "Empresa Respondeu", "Respondeu"])
            is_respondida = _bool_from_pt(resp)

            # Resolvida
            aval = _pick_col(row, ["Avaliacao Reclamacao", "Avaliação", "Resolvida", "Resolvida?"])
            aval_norm = _norm_col(aval)
            # Cuidado com "Não Resolvida"
            is_resolved = ("resolvida" in aval_norm) and ("nao resolvida" not in aval_norm) and (not aval_norm.startswith("nao "))

            # Nota (1-5)
            nota = _to_float(_pick_col(row, ["Nota do Consumidor", "Nota Consumidor", "Nota"]))

            # Tempo (dias)
            tempo = _to_float(_pick_col(row, ["Tempo Resposta", "Tempo de Resposta", "Dias Resposta"]))

            # Fallback
            if (is_respondida is None or not is_respondida) and tempo > 0:
                is_respondida = True

            # Default bools
            is_finalized = bool(is_finalized)
            is_respondida = bool(is_respondida)
            is_resolved = bool(is_resolved)

            # 5. Agregação
            k = nk
            if k not in monthly_data[ym]:
                monthly_data[ym][k] = Agg(display_name=name_raw.strip(), cnpj_key=cnpj_key)

            agg = monthly_data[ym][k]
            agg.total_claims += 1

            if is_finalized:
                agg.finalized_claims += 1
            if is_respondida:
                agg.responded_claims += 1
            if is_resolved:
                agg.resolved_claims += 1

            if nota > 0:
                agg.score_sum += nota
                agg.satisfaction_count += 1

            if tempo > 0:
                agg.response_time_sum += tempo
                agg.response_time_count += 1

            # Link CNPJ se disponível
            if cnpj_key:
                if not agg.cnpj_key:
                    agg.cnpj_key = cnpj_key

    except Exception as e:
        print(f"CG: Erro durante leitura das linhas: {e}")
    finally:
        try:
            if hasattr(csv_stream, "name"):
                fname = csv_stream.name
                csv_stream.close()
                p = Path(fname)
                if p.exists() and p.parent == CACHE_DIR and ("cg_extract_" in p.name or "dump_latest" in p.name):
                    p.unlink(missing_ok=True)
        except Exception:
            pass

    # Stats / Fail-safe
    print(f"CG: Stats Parse -> Total: {rows_total}, Elegíveis (Seguros): {rows_eligible}, Com CNPJ: {rows_cnpj}")

    # Se leu muitas linhas mas zero elegíveis, o filtro de segmento pode estar quebrado
    if rows_total > 1000 and rows_eligible == 0:
        raise RuntimeError(f"CG: CRÍTICO - Nenhuma linha elegível encontrada para Seguros em {dump_path.name}. Possível mudança no nome do segmento.")

    # Exportação
    os.makedirs(output_dir, exist_ok=True)
    count_files = 0

    for ym, data_map in monthly_data.items():
        if not data_map:
            continue
        out_p = os.path.join(output_dir, f"consumidor_gov_{ym}.json")
        by_name_raw = {}
        by_cnpj_raw = {}

        for k, agg in data_map.items():
            raw_obj = agg.to_public()
            # Adiciona sums brutas para re-agregação segura
            raw_obj["statistics"]["scoreSum"] = agg.score_sum
            raw_obj["statistics"]["responseTimeSum"] = agg.response_time_sum

            by_name_raw[k] = raw_obj
            if agg.cnpj_key:
                by_cnpj_raw[agg.cnpj_key] = raw_obj

        payload = {
            "meta": {
                "ym": ym,
                "generated_at": _utc_now(),
                "parse": {
                    "rows_total": rows_total,
                    "rows_eligible": rows_eligible,
                    "has_cnpj_col": has_cnpj_col,
                    "encoding": encoding_used
                }
            },
            "by_name_key_raw": by_name_raw,
            "by_cnpj_key_raw": by_cnpj_raw
        }
        with open(out_p, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        count_files += 1

    print(f"CG: Processamento concluído. {count_files} meses gerados.")


def sync_monthly_cache_from_dump_if_needed(target_yms: List[str], monthly_dir: str):
    missing = [ym for ym in target_yms if not os.path.exists(os.path.join(monthly_dir, f"consumidor_gov_{ym}.json"))]
    if not missing:
        print("CG: Cache mensal completo. Pula download do dump.")
        return

    print(f"CG: Faltam dados para: {missing}. Iniciando download do dump...")
    client = requests.Session(impersonate="chrome110")

    env_url = os.getenv("CG_DUMP_URL")
    if env_url:
        dump_path = download_dump_to_file(env_url, client)
        if dump_path:
            process_dump_to_monthly(dump_path, target_yms, monthly_dir)
            if dump_path.exists():
                os.remove(dump_path)
        return

    for ym in missing:
        url = _get_dump_url_for_month(client, ym)
        if not url:
            print(f"CG: WARN - Não encontrei dump mensal para {ym}.")
            continue
        dump_path = download_dump_to_file(url, client)
        if not dump_path:
            continue
        process_dump_to_monthly(dump_path, [ym], monthly_dir)
        if dump_path.exists():
            os.remove(dump_path)

sync_monthly_cache_from_dump = sync_monthly_cache_from_dump_if_needed
