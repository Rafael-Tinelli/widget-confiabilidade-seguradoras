from __future__ import annotations

import csv
import io
import logging
import re
import tempfile
import unicodedata
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3

# Desabilita avisos SSL (padrão em gov.br)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Portal de Dados Abertos da SUSEP (CKAN)
CKAN_API_URL = "https://dados.susep.gov.br/api/3/action/package_search"
CKAN_QUERY = "ses"  # Termo de busca genérico para encontrar o dataset

USER_AGENT = "widget-confiabilidade-seguradoras/0.1 (GitHub Actions ETL)"

@dataclass(frozen=True)
class SesExtractionMeta:
    zip_url: str
    cias_file: str
    seguros_file: str
    period_from: str
    period_to: str

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
    if not s: return None
    d = re.sub(r"\D+", "", s)
    return d or None

def _parse_brl_number(raw: Any) -> float:
    if raw is None: return 0.0
    s = str(raw).strip()
    if not s: return 0.0
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^0-9\.\-]+", "", s)
    try: return float(s)
    except ValueError: return 0.0

def _ym_to_iso_01(ym: int) -> str:
    return f"{ym // 100:04d}-{ym % 100:02d}-01"

def _parse_ym(value: Any) -> Optional[int]:
    if not value: return None
    s = str(value).strip()
    m = re.search(r"\b(\d{4})\D?(\d{2})\b", s) or re.search(r"\b(\d{2})\D+(\d{4})\b", s)
    if not m: return None
    v1, v2 = int(m.group(1)), int(m.group(2))
    return v1 * 100 + v2 if v1 > 12 else v2 * 100 + v1

def _find_ckan_resource_url() -> str:
    """Busca a URL do ZIP no portal de dados abertos (CKAN)."""
    print(f"Querying CKAN API: {CKAN_API_URL}?q={CKAN_QUERY}")
    try:
        r = requests.get(
            CKAN_API_URL, 
            params={"q": CKAN_QUERY, "rows": 20},
            headers={"User-Agent": USER_AGENT},
            verify=False,
            timeout=30
        )
        r.raise_for_status()
        data = r.json()
        
        if not data.get("success"):
            raise RuntimeError("CKAN API retornou erro.")
            
        results = data.get("result", {}).get("results", [])
        
        # Procura dataset que parece ser o SES completo
        candidates = []
        for pkg in results:
            title = pkg.get("title", "").lower()
            if "ses" in title and ("base" in title or "completa" in title or "estatistica" in title):
                # Procura recurso ZIP dentro do pacote
                for res in pkg.get("resources", []):
                    if res.get("format", "").upper() == "ZIP" or res.get("url", "").lower().endswith(".zip"):
                        candidates.append(res["url"])
        
        if candidates:
            print(f"Found CKAN candidate: {candidates[0]}")
            return candidates[0]
            
        # Fallback Hardcoded para o link direto do portal de dados abertos (se a busca falhar)
        # Este link costuma ser mais estável e permissivo que o www2
        print("CKAN search empty. Trying direct Open Data URL fallback...")
        return "https://dados.susep.gov.br/dataset/sistema-de-estatisticas-da-susep-ses/resource/base-completa-zip"

    except Exception as e:
        print(f"CKAN Discovery failed: {e}")
        # Último recurso: tenta o link antigo, vai que...
        return "https://www2.susep.gov.br/menuestatistica/ses/download/BaseCompleta.zip"

def _download_zip_to_tempfile(url: str = None) -> Path:
    if not url:
        url = _find_ckan_resource_url()
        
    print(f"Downloading from: {url}")
    # Nota: verify=False ainda é necessário para gov.br
    r = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        stream=True,
        verify=False,
        timeout=300
    )
    r.raise_for_status()
    
    # Verifica se é HTML de erro antes de salvar
    content_type = r.headers.get("Content-Type", "").lower()
    if "text/html" in content_type:
        sample = r.content[:500].decode("utf-8", errors="ignore")
        raise RuntimeError(f"URL retornou HTML em vez de ZIP. Content-Type: {content_type}. Snippet: {sample}")

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    for chunk in r.iter_content(chunk_size=1024*1024):
        tmp.write(chunk)
    tmp.close()
    return Path(tmp.name), url

def extract_ses_master_and_financials(zip_url: Optional[str] = None) -> Tuple[SesExtractionMeta, Dict[str, Dict[str, Any]]]:
    zip_path, used_url = _download_zip_to_tempfile(zip_url)
    
    try:
        with zipfile.ZipFile(zip_path) as z:
            # Lógica de detecção de arquivos (mantida idêntica para robustez)
            def find(names):
                for n in z.namelist():
                    if any(x.lower() in n.lower() for x in names) and n.lower().endswith(".csv"): return n
                return None
            
            cias = find(["ses_cias", "cias"])
            seguros = find(["ses_seguros", "seguros"])
            
            if not cias or not seguros:
                raise RuntimeError(f"ZIP baixado, mas CSVs não encontrados. Conteúdo: {z.namelist()}")

            def read_csv(fname):
                with z.open(fname) as f:
                    content = io.TextIOWrapper(f, encoding="latin-1", errors="replace", newline="")
                    return list(csv.reader(content, delimiter=";"))

            rows_cias = read_csv(cias)
            rows_seguros = read_csv(seguros)
            
            h_cias = [_norm(x) for x in rows_cias[0]]
            h_seg = [_norm(x) for x in rows_seguros[0]]
            
            # Mapeamento e índices (mantido idêntico)
            def g_idx(h, keys):
                for k in keys: 
                    if _norm(k) in h: return h.index(_norm(k))
                return None

            id_i = g_idx(h_cias, ["cod_enti", "coenti", "cod_cia"])
            nm_i = g_idx(h_cias, ["noenti", "nome", "nome_cia"])
            cn_i = g_idx(h_cias, ["cnpj", "numcnpj"])
            
            sid_i = g_idx(h_seg, ["cod_enti", "coenti", "cod_cia"])
            ym_i = g_idx(h_seg, ["damesano", "anomes", "competencia"])
            pr_i = g_idx(h_seg, ["premio", "premio_emitido"])
            sn_i = g_idx(h_seg, ["sinistros", "sinistro"])

            companies = {}
            if id_i is not None and nm_i is not None:
                for row in rows_cias[1:]:
                    if len(row) <= max(id_i, nm_i): continue
                    sid = _digits(row[id_i])
                    if not sid: continue
                    companies[sid.zfill(6)] = {
                        "name": row[nm_i].strip(),
                        "cnpj": _digits(row[cn_i]) if cn_i is not None and len(row) > cn_i else None
                    }

            agg = {}
            max_ym = 0
            
            for row in rows_seguros[1:]:
                if len(row) <= max(sid_i, ym_i, pr_i): continue
                ym = _parse_ym(row[ym_i])
                if not ym: continue
                if ym > max_ym: max_ym = ym
                
                sid = _digits(row[sid_i])
                if not sid: continue
                sid = sid.zfill(6)
                
                prem = _parse_brl_number(row[pr_i])
                sin = _parse_brl_number(row[sn_i]) if sn_i is not None and len(row) > sn_i else 0.0
                
                if sid not in agg: agg[sid] = {"p": 0.0, "c": 0.0}
                agg[sid]["p"] += prem
                agg[sid]["c"] += sin

            start_ym = (max_ym // 100 * 12 + max_ym % 100 - 1 - 11)
            start_ym = (start_ym // 12) * 100 + (start_ym % 12 + 1)
            
            out = {}
            for sid, val in agg.items():
                if val["p"] <= 0: continue
                base = companies.get(sid) or {"name": f"SES_{sid}", "cnpj": None}
                out[sid] = {
                    "name": base["name"],
                    "cnpj": base["cnpj"],
                    "premiums": round(val["p"], 2),
                    "claims": round(val["c"], 2)
                }

            return SesExtractionMeta(used_url, cias, seguros, _ym_to_iso_01(start_ym), _ym_to_iso_01(max_ym)), out

    finally:
        try: zip_path.unlink()
        except: pass
