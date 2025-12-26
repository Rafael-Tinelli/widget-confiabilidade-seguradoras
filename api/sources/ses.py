from __future__ import annotations

import csv
import io
import os
import re
import time
import unicodedata
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from playwright.sync_api import sync_playwright

# URL Principal da SUSEP
SES_URL = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"


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
    if not value:
        return None
    s = str(value).strip()
    m = re.search(r"\b(\d{4})\D?(\d{2})\b", s) or re.search(r"\b(\d{2})\D+(\d{4})\b", s)
    if not m:
        return None
    v1 = int(m.group(1))
    v2 = int(m.group(2))
    if v1 > 12:
        return v1 * 100 + v2
    return v2 * 100 + v1


def _validate_downloaded_file(path: Path) -> None:
    """
    Análise forense do arquivo baixado conforme relatório técnico.
    Verifica se é um ZIP real ou um HTML de bloqueio (WAF).
    """
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    size = path.stat().st_size
    if size == 0:
        raise RuntimeError("Download resultou em arquivo vazio (0 bytes). Timeout de rede provável.")

    with open(path, "rb") as f:
        header = f.read(4)

    # Assinatura mágica do ZIP: PK..
    if header == b"\x50\x4b\x03\x04":
        print(f"Sucesso: Assinatura ZIP válida detectada. Tamanho: {size / 1024 / 1024:.2f} MB")
        return

    # Diagnóstico de erro
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read(1000)
    except Exception:
        content = "Conteúdo binário desconhecido"

    if "<html" in content.lower() or "<!doctype" in content.lower():
        if "access denied" in content.lower() or "forbidden" in content.lower():
            raise PermissionError("Bloqueio WAF detectado! O servidor retornou HTML 403/406 em vez do ZIP.")
        raise ValueError(f"O arquivo baixado é uma página HTML, não um ZIP. Snippet: {content[:200]}")

    raise ValueError(f"Formato de arquivo desconhecido. Header Hex: {header.hex()}")


def _download_via_browser() -> Path:
    print(f"Browser: Initializing Stealth Mode to evade WAF at {SES_URL}...")

    with sync_playwright() as p:
        # Launch com argumentos de evasão
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",  # Remove flag principal de bot
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-infobars",
                "--window-size=1920,1080",
            ],
        )

        # Contexto com User-Agent real e viewport de desktop
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            accept_downloads=True,
            ignore_https_errors=True,  # Ignora erros de SSL da SUSEP
        )

        # Evasão via JavaScript: Esconde navigator.webdriver
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        page = context.new_page()

        try:
            print("Browser: Navigating...")
            # Timeout estendido para 90s
            page.goto(SES_URL, timeout=90000, wait_until="domcontentloaded")

            print("Browser: Searching for download link...")
            # Tenta encontrar o link pelo texto "Base" e "Completa" (ou variações)
            # O relatório indica que o link pode ser dinâmico (Postback)
            
            # Estratégia: Espera download após clique
            with page.expect_download(timeout=120000) as download_info:
                # Tenta seletores diferentes em ordem de especificidade
                
                # 1. Link com texto exato (mais comum)
                loc = page.get_by_role("link").filter(has_text=re.compile(r"Base\s*(do)?\s*SES", re.IGNORECASE))
                
                # 2. Se falhar, procura qualquer ZIP na página
                if loc.count() == 0:
                    print("Browser: Text match failed. Trying generic ZIP selector...")
                    loc = page.locator("a[href$='.zip']")
                
                if loc.count() > 0:
                    print(f"Browser: Found {loc.count()} candidates. Clicking the first one...")
                    # Força clique via JS se o elemento estiver oculto/obscurecido
                    loc.first.click(force=True)
                else:
                    raise RuntimeError("Nenhum link de download (Base SES ou .zip) encontrado na página.")

            download = download_info.value
            tmp_path = Path(f"temp_ses_{int(time.time())}.zip")
            print(f"Browser: Download started. Saving to {tmp_path}...")
            download.save_as(tmp_path)

            _validate_downloaded_file(tmp_path)
            
            browser.close()
            return tmp_path

        except Exception as e:
            # Tira screenshot em caso de erro para debug futuro (se configurado upload de artefatos)
            try:
                page.screenshot(path="debug_error.png")
            except Exception:
                pass
            browser.close()
            raise RuntimeError(f"Playwright download failed: {e}")


def extract_ses_master_and_financials(
    zip_url: Optional[str] = None,
) -> Tuple[SesExtractionMeta, Dict[str, Dict[str, Any]]]:
    # Ignora zip_url e usa browser (Estratégia Blindada)
    zip_path = _download_via_browser()
    used_url = "browser_downloaded_artifact"

    try:
        with zipfile.ZipFile(zip_path) as z:
            
            def find(names: List[str]) -> Optional[str]:
                for n in z.namelist():
                    if any(x.lower() in n.lower() for x in names) and n.lower().endswith(".csv"):
                        return n
                return None

            cias = find(["ses_cias", "cias"])
            seguros = find(["ses_seguros", "seguros"])

            if not cias:
                # Fallback por header
                for n in z.namelist():
                    if not n.endswith(".csv"):
                        continue
                    with z.open(n) as f:
                        l1 = f.readline().decode("latin-1", errors="ignore").lower()
                        if "cod_cia" in l1 and "nome" in l1:
                            cias = n
                            break
            
            if not seguros:
                # Fallback por header
                for n in z.namelist():
                    if not n.endswith(".csv"):
                        continue
                    with z.open(n) as f:
                        l1 = f.readline().decode("latin-1", errors="ignore").lower()
                        if "premio" in l1 and "sinistro" in l1:
                            seguros = n
                            break

            if not cias or not seguros:
                raise RuntimeError(f"CSVs não encontrados no ZIP. Conteúdo: {z.namelist()}")

            def read_csv(fname: str) -> List[List[str]]:
                with z.open(fname) as f:
                    content = io.TextIOWrapper(f, encoding="latin-1", errors="replace", newline="")
                    return list(csv.reader(content, delimiter=";"))

            rows_cias = read_csv(cias)
            rows_seguros = read_csv(seguros)

            h_cias = [_norm(x) for x in rows_cias[0]]
            h_seg = [_norm(x) for x in rows_seguros[0]]

            def g_idx(h: List[str], keys: List[str]) -> Optional[int]:
                for k in keys:
                    if _norm(k) in h:
                        return h.index(_norm(k))
                return None

            id_i = g_idx(h_cias, ["cod_enti", "coenti", "cod_cia"])
            nm_i = g_idx(h_cias, ["noenti", "nome", "nome_cia"])
            cn_i = g_idx(h_cias, ["cnpj", "numcnpj"])

            sid_i = g_idx(h_seg, ["cod_enti", "coenti", "cod_cia"])
            ym_i = g_idx(h_seg, ["damesano", "anomes", "competencia"])
            pr_i = g_idx(h_seg, ["premio", "premio_emitido"])
            sn_i = g_idx(h_seg, ["sinistros", "sinistro"])

            companies: Dict[str, Dict[str, Any]] = {}
            if id_i is not None and nm_i is not None:
                for row in rows_cias[1:]:
                    if len(row) <= max(id_i, nm_i):
                        continue
                    sid = _digits(row[id_i])
                    if not sid:
                        continue
                    companies[sid.zfill(6)] = {
                        "name": row[nm_i].strip(),
                        "cnpj": _digits(row[cn_i]) if cn_i is not None and len(row) > cn_i else None,
                    }

            agg: Dict[str, Dict[str, float]] = {}
            max_ym = 0

            for row in rows_seguros[1:]:
                if sid_i is None or ym_i is None or pr_i is None:
                    break
                if len(row) <= max(sid_i, ym_i, pr_i):
                    continue
                
                ym = _parse_ym(row[ym_i])
                if not ym:
                    continue
                if ym > max_ym:
                    max_ym = ym

                sid = _digits(row[sid_i])
                if not sid:
                    continue
                sid = sid.zfill(6)

                prem = _parse_brl_number(row[pr_i])
                sin = _parse_brl_number(row[sn_i]) if sn_i is not None and len(row) > sn_i else 0.0

                if sid not in agg:
                    agg[sid] = {"p": 0.0, "c": 0.0}
                agg[sid]["p"] += prem
                agg[sid]["c"] += sin

            start_ym = _ym_add(max_ym, -11)

            out: Dict[str, Dict[str, Any]] = {}
            for sid, val in agg.items():
                if val["p"] <= 0:
                    continue
                base = companies.get(sid) or {"name": f"SES_{sid}", "cnpj": None}
                out[sid] = {
                    "name": base["name"],
                    "cnpj": base["cnpj"],
                    "premiums": round(val["p"], 2),
                    "claims": round(val["c"], 2),
                }

            return (
                SesExtractionMeta(used_url, cias, seguros, _ym_to_iso_01(start_ym), _ym_to_iso_01(max_ym)),
                out,
            )

    finally:
        try:
            zip_path.unlink()
        except Exception:
            pass
