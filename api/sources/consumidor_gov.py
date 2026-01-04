# api/sources/consumidor_gov.py
from __future__ import annotations

import gzip
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

import pandas as pd
from curl_cffi import requests

# --- CONFIGURAÇÕES ---
CKAN_API_BASE = os.getenv("CG_CKAN_API_BASE", "https://dados.mj.gov.br/api/3/action/")
CKAN_QUERY = os.getenv("CG_CKAN_QUERY", "consumidor.gov")

# Controle de custos/tempo: Bloqueia base completa por padrão
ALLOW_BASECOMPLETA = os.getenv("CG_ALLOW_BASECOMPLETA", "0") == "1"

TIMEOUT = int(os.getenv("CG_TIMEOUT", "600"))
MIN_BYTES = int(os.getenv("CG_MIN_BYTES", "50000"))
CHUNK_SIZE = int(os.getenv("CG_CHUNK_SIZE", "100000"))
CACHE_DIR = Path("data/raw/consumidor_gov")
DIRECT_DOWNLOAD_PAGE = "https://www.consumidor.gov.br/pages/dadosabertos/externo/"

_CNPJ_RE = re.compile(r"\D+")
_FILE_RE = re.compile(r"\.(csv|zip|gz)(\?|$)", re.I)
_FINALIZADAS_OR_BASE_RE = re.compile(r"(finalizadas|basecompleta)", re.I)
# Regex flexível para mês: 2025-12, 202512, 2025_12, 2025.12
_YM_ANY_RE = re.compile(r"(20\d{2})[^\d]?(0[1-9]|1[0-2])") 
_YM_RE = re.compile(r"(20\d{2})[-_/\.](0[1-9]|1[0-2])")
_Y_RE = re.compile(r"(20\d{2})")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _norm_col(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.lower().strip()


def _blob(url: str, meta: Optional[dict] = None) -> str:
    b = (url or "").lower()
    if meta:
        b += " " + str(meta.get("name") or "").lower()
        b += " " + str(meta.get("description") or "").lower()
    return b


def _ym_variants(ym: str) -> set[str]:
    """Gera variantes de YYYY-MM para busca flexível."""
    y, m = ym.split("-")
    return {
        ym,
        f"{y}{m}",
        f"{y}_{m}",
        f"{y}.{m}",
        f"{y}/{m}",
        f"{y}-{m}",
    }


def _blob_has_ym(b: str, ym: str) -> bool:
    """Verifica se o blob contem o mês em algum formato."""
    bb = (b or "").lower()
    return any(v in bb for v in _ym_variants(ym))


def _is_monthly_dump_candidate(url: str, meta: Optional[dict] = None) -> bool:
    b = _blob(url, meta)
    if not _FINALIZADAS_OR_BASE_RE.search(b):
        return False
    
    # Política de Custo: Evita base completa se não permitido explicitamente
    if "basecompleta" in b and not ALLOW_BASECOMPLETA:
        return False
        
    return ("finalizadas" in b) or ("basecompleta" in b)


@dataclass
class Agg:
    display_name: str
    total_claims: int = 0
    evaluated_claims: int = 0
    score_sum: float = 0.0
    resolved_claims: int = 0
    cnpj: Optional[str] = None

    def merge_raw(self, raw: dict) -> None:
        if not self.display_name:
            self.display_name = raw.get("display_name", "")

        stats = raw.get("statistics", {})
        tc = int(stats.get("complaintsCount", 0))
        ec = int(stats.get("evaluatedCount", 0))

        self.total_claims += tc
        self.evaluated_claims += ec

        avg_sat = float(stats.get("overallSatisfaction", 0.0))
        if ec > 0:
            self.score_sum += avg_sat * ec

        rc = stats.get("resolvedCount")
        if rc is not None:
            self.resolved_claims += int(rc)
        else:
            idx_sol = float(stats.get("solutionIndex", 0.0))
            self.resolved_claims += int((idx_sol if idx_sol <= 1.0 else idx_sol / 100.0) * tc)

        if not self.cnpj and raw.get("cnpj"):
            self.cnpj = raw.get("cnpj")

    def to_public(self) -> dict:
        avg_sat = 0.0
        if self.evaluated_claims > 0:
            avg_sat = round(self.score_sum / self.evaluated_claims, 2)

        sol_idx = 0.0
        if self.total_claims > 0:
            sol_idx = round(self.resolved_claims / self.total_claims, 2)

        return {
            "display_name": self.display_name,
            "name": self.display_name,
            "cnpj": self.cnpj,
            "statistics": {
                "overallSatisfaction": avg_sat,
                "solutionIndex": sol_idx,
                "complaintsCount": self.total_claims,
                "evaluatedCount": self.evaluated_claims,
                "resolvedCount": self.resolved_claims
            },
            "indexes": {"b": {"nota": avg_sat}}
        }


def normalize_cnpj(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    d = _CNPJ_RE.sub("", str(v))
    return d if len(d) == 14 else None


def normalize_key_name(raw: str) -> str:
    s = (raw or "").strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


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
    elif u.endswith(".gz"):
        score += 30_000

    m = _YM_ANY_RE.search(u)
    if m:
        score += int(m.group(1)) * 100 + int(m.group(2))
    else:
        y = _Y_RE.search(u)
        if y:
            score += int(y.group(1)) * 10

    if meta:
        lm = meta.get("last_modified") or meta.get("created") or ""
        mm = _YM_ANY_RE.search(str(lm))
        if mm:
            score += int(mm.group(1)) * 100 + int(mm.group(2))
            
    return score


def _ckan_resource_search(client: requests.Session, term: str, limit: int = 50) -> list[dict]:
    """Busca direta por resources no CKAN."""
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
        # Termos específicos para o mês
        terms = [
            f"finalizadas {ym}",
            f"finalizadas_{ym}",
            f"basecompleta{ym}",
            f"basecompleta {ym}",
            f"{CKAN_QUERY} {ym}",
        ]
        best: Tuple[int, str] | None = None

        # 1. Busca Direta por Resource (Mais preciso)
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

        # 2. Busca por Package (Fallback)
        if not best:
            api = urljoin(CKAN_API_BASE, "package_search")
            url = f"{api}?q={quote(CKAN_QUERY)}&rows=50"
            r = client.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if data.get("success"):
                    results = (data.get("result") or {}).get("results") or []
                    for pkg in results:
                        for res in (pkg.get("resources") or []):
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
            print(f"CG: CKAN candidate {ym} (score={best[0]}): {best[1]}")
            return best[1]
    except Exception as e:
        print(f"CG: CKAN falhou ({ym}): {e}")

    # 3. Fallback HTML
    print(f"CG: Fallback HTML ({ym}) -> {DIRECT_DOWNLOAD_PAGE} ...")
    try:
        r = client.get(DIRECT_DOWNLOAD_PAGE, timeout=30)
        if r.status_code != 200:
            return None

        html = r.text or ""
        hrefs = re.findall(r'href\s*=\s*["\']([^"\']+)["\']', html, flags=re.I)
        candidates: list[tuple[int, str]] = []

        for h in hrefs:
            if not h:
                continue
            
            full = urljoin("https://www.consumidor.gov.br", h)
            
            if not _FILE_RE.search(full):
                continue
            if not _is_monthly_dump_candidate(full):
                continue
            
            # Check flexível de mês
            if not _blob_has_ym(full, ym):
                continue
            
            candidates.append((_score_url(full), full))

        if not candidates:
            return None

        candidates.sort(reverse=True)
        print(f"CG: HTML candidate {ym} (score={candidates[0][0]}): {candidates[0][1]}")
        return candidates[0][1]
    except Exception as e:
        print(f"CG: Erro no scraping HTML ({ym}): {e}")
        return None


# --- TRANSPORTE ---

def _get_latest_dump_url(client: requests.Session) -> Optional[str]:
    env_url = os.getenv("CG_DUMP_URL")
    if env_url:
        print(f"CG: Usando URL forçada via ENV: {env_url}")
        return env_url
    
    # Fallback genérico se precisasse, mas o fluxo agora é por mês
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
                # Cleanup imediato em caso de erro
                if out_path.exists():
                    out_path.unlink()
                return None

            total_bytes = 0
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=128 * 1024):
                    if chunk:
                        f.write(chunk)
                        total_bytes += len(chunk)

            if total_bytes < MIN_BYTES:
                print(f"CG: Arquivo muito pequeno ({total_bytes}b).")
                # Cleanup de arquivo inválido
                if out_path.exists():
                    out_path.unlink()
                return None

            print(f"CG: Download OK ({total_bytes / 1024 / 1024:.2f} MB).")
            return out_path
            
        finally:
            if hasattr(r, 'close'):
                r.close()
                
    except Exception as e:
        print(f"CG: Exceção download: {e}")
        # Cleanup em exceção
        if out_path.exists():
            out_path.unlink()
        return None


def open_dump_file(path: Path) -> BinaryIO:
    with open(path, "rb") as f:
        sig = f.read(4)

    if sig.startswith(b"\x1f\x8b"):
        print("CG: Formato GZIP detectado.")
        return gzip.open(path, "rb")

    if sig.startswith(b"PK\x03\x04"):
        print("CG: Formato ZIP detectado.")
        z = zipfile.ZipFile(path, "r")
        try:
            csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if not csvs:
                raise ValueError("ZIP sem CSV")
            
            target = max(csvs, key=lambda x: z.getinfo(x).file_size)
            print(f"CG: Extraindo {target} do ZIP para temp...")

            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            
            # Patch E: Usa NamedTemporaryFile para extração segura e única
            tmp = tempfile.NamedTemporaryFile(
                delete=False, 
                suffix=".csv", 
                prefix="cg_extract_", 
                dir=str(CACHE_DIR)
            )
            tmp_path = Path(tmp.name)
            
            with z.open(target) as zfh:
                shutil.copyfileobj(zfh, tmp)
            
            tmp.close() # Fecha o handle de escrita

        finally:
            z.close()

        # Retorna handle de leitura do arquivo extraído
        return open(tmp_path, "rb")

    print("CG: Assumindo CSV direto.")
    return open(path, "rb")


# --- PROCESSAMENTO ---

def pick_columns(cols: list[str]) -> Tuple[Any, Any, Any, Any, Any]:
    c_map = {_norm_col(c): c for c in cols}

    def find(targets):
        for t in targets:
            for k in c_map:
                if t in k:
                    return c_map[k]
        return None

    c_cnpj = find(["cnpj", "cpf/cnpj", "cpf cnpj"])
    c_name = find(["nome fantasia", "fantasia", "nome do fornecedor", "fornecedor"])
    c_score = find(["nota do consumidor", "nota", "avaliacao"])
    c_date = find(["data finalizacao", "data finalizacao", "data abertura", "data"])
    c_resolved = find(["avaliacao reclamacao", "respondida", "resolvida", "situacao", "status"])

    return c_cnpj, c_name, c_score, c_date, c_resolved


def process_dump_to_monthly(dump_path: Path, target_yms: List[str], output_dir: str):
    target_set = set(target_yms)
    if not target_set:
        print("CG: Nenhum mês alvo definido.")
        return

    try:
        csv_stream = open_dump_file(dump_path)
    except Exception as e:
        print(f"CG: Falha ao abrir dump: {e}")
        return

    # Tenta detectar encoding
    enc = "utf-8"
    try:
        sample = csv_stream.read(4096)
        sample.decode("utf-8")
        csv_stream.seek(0)
    except Exception:
        enc = "latin1"
        csv_stream.seek(0)

    print(f"CG: Processando CSV (Encoding: {enc})...")
    
    monthly_data: Dict[str, Dict[str, Agg]] = {ym: {} for ym in target_set}

    try:
        reader = pd.read_csv(
            csv_stream,
            sep=";",
            encoding=enc,
            dtype=str,
            chunksize=CHUNK_SIZE,
            on_bad_lines="skip"
        )
    except Exception as e:
        print(f"CG: Erro ao iniciar leitura CSV: {e}")
        csv_stream.close()
        return

    first = True
    cols = {}
    has_cnpj_col = False

    for chunk in reader:
        chunk.columns = [c.strip() for c in chunk.columns]

        if first:
            cn, nm, sc, dt, rs = pick_columns(list(chunk.columns))
            cols = {'cnpj': cn, 'name': nm, 'score': sc, 'date': dt, 'resolved': rs}
            print(f"CG: Colunas Mapeadas -> {cols}")
            if not cols['name'] or not cols['date']:
                print("CG: CRÍTICO - Colunas obrigatórias não encontradas.")
                break 
            
            if not cols['cnpj']:
                print("CG: AVISO - Coluna CNPJ não encontrada. Prosseguindo sem CNPJ.")
                # Patch D: Log das colunas para debug definitivo
                print(f"CG: Colunas disponíveis (sample): {list(chunk.columns)[:20]}")
            has_cnpj_col = bool(cols['cnpj'])
                
            first = False

        dates = chunk[cols['date']].fillna("")
        
        # Patch de Data Robusto: dd/mm/yyyy, dd-mm-yyyy, dd.mm.yyyy ou ISO
        # Captura (\d{2})SEP(\d{2})SEP(\d{4})
        extracted = dates.str.extract(r'(\d{2})[\/\-\.](\d{2})[\/\-\.](\d{4})')
        if not extracted.empty and extracted[2].notna().any():
            chunk['ym'] = extracted[2] + "-" + extracted[1]
        else:
            # Tenta ISO YYYY-MM-DD
            iso = dates.str.extract(r'(20\d{2})-(\d{2})-(\d{2})')
            if not iso.empty and iso[0].notna().any():
                chunk['ym'] = iso[0] + "-" + iso[1]
            else:
                # Fallback slice YYYY-MM
                chunk['ym'] = dates.str.slice(0, 7)

        valid_chunk = chunk[chunk['ym'].isin(target_set)].copy()
        if valid_chunk.empty:
            continue

        if cols['score']:
            s = valid_chunk[cols['score']].astype(str).str.replace(',', '.', regex=False)
            valid_chunk['score_val'] = pd.to_numeric(s, errors='coerce').fillna(0.0)
        else:
            valid_chunk['score_val'] = 0.0

        if cols['resolved']:
            col_norm = _norm_col(cols['resolved'])
            s = valid_chunk[cols['resolved']].astype(str).fillna("")
            s_low = s.str.lower()

            if "avaliacao reclamacao" in col_norm:
                is_res = s_low.str.contains("resolvida", na=False) & ~s_low.str.contains("nao resolvida", na=False) & ~s_low.str.contains("não resolvida", na=False)
                valid_chunk['res_val'] = is_res.astype(int)
            elif "respondida" in col_norm:
                valid_chunk['res_val'] = s_low.str.startswith('s').astype(int)
            else:
                valid_chunk['res_val'] = s_low.str.startswith('s').astype(int)
        else:
            valid_chunk['res_val'] = 0

        valid_chunk['key'] = valid_chunk[cols['name']].astype(str).apply(normalize_key_name)

        grp = valid_chunk.groupby(['ym', 'key', cols['name']])

        for (ym, key, display_name), g in grp:
            if not key:
                continue

            if key not in monthly_data[ym]:
                monthly_data[ym][key] = Agg(display_name=str(display_name))

            agg = monthly_data[ym][key]
            agg.total_claims += len(g)

            evals = g[g['score_val'] > 0]
            if not evals.empty:
                agg.evaluated_claims += len(evals)
                agg.score_sum += float(evals['score_val'].sum())

            agg.resolved_claims += int(g['res_val'].sum())

            if not agg.cnpj and cols['cnpj']:
                c_vals = g[cols['cnpj']].dropna()
                if not c_vals.empty:
                    for raw_c in c_vals.astype(str).tolist():
                        norm = normalize_cnpj(raw_c)
                        if norm:
                            agg.cnpj = norm
                            break

    # Limpeza de recursos (temp file)
    try:
        file_name = getattr(csv_stream, "name", None)
        csv_stream.close()
        if file_name:
            p = Path(file_name)
            # Patch E: Limpa temporários gerados por nós
            if p.exists() and p.parent == CACHE_DIR and ("cg_extract_" in p.name or "dump_latest" in p.name):
                p.unlink(missing_ok=True)
    except Exception:
        pass

    os.makedirs(output_dir, exist_ok=True)
    count_files = 0
    for ym, data_map in monthly_data.items():
        if not data_map:
            continue

        out_p = os.path.join(output_dir, f"consumidor_gov_{ym}.json")

        by_name_raw = {}
        by_cnpj_raw = {}

        for k, agg in data_map.items():
            avg = 0.0
            if agg.evaluated_claims > 0:
                avg = round(agg.score_sum / agg.evaluated_claims, 2)

            raw_obj = {
                "display_name": agg.display_name,
                "cnpj": agg.cnpj,
                "statistics": {
                    "complaintsCount": agg.total_claims,
                    "evaluatedCount": agg.evaluated_claims,
                    "overallSatisfaction": avg,
                    "resolvedCount": agg.resolved_claims
                }
            }
            by_name_raw[k] = raw_obj
            if agg.cnpj:
                by_cnpj_raw[agg.cnpj] = raw_obj

        payload = {
            "meta": {
                "ym": ym,
                "generated_at": _utc_now(),
                "parse": {"rows": len(by_name_raw), "has_cnpj_col": has_cnpj_col}
            },
            "by_name_key_raw": by_name_raw,
            "by_cnpj_key_raw": by_cnpj_raw
        }

        with open(out_p, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        count_files += 1

    print(f"CG: Processamento concluído. {count_files} meses atualizados.")


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

# --- BACKWARD COMPATIBILITY ---
sync_monthly_cache_from_dump = sync_monthly_cache_from_dump_if_needed
