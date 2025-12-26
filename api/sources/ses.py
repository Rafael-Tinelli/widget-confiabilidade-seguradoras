from __future__ import annotations

import csv
import io
import re
import tempfile
import time
import unicodedata
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from playwright.sync_api import TimeoutError as PwTimeoutError
from playwright.sync_api import sync_playwright

SES_URL = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"
DEBUG_DIR = Path("ses_debug")


@dataclass(frozen=True)
class SesExtractionMeta:
    zip_url: str
    cias_file: str
    seguros_file: str
    period_from: str
    period_to: str


def _ensure_debug_dir() -> None:
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)


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


def _read_head(path: Path, n: int = 4096) -> bytes:
    with path.open("rb") as f:
        return f.read(n)


def _is_zip_signature(head8: bytes) -> bool:
    return (
        head8.startswith(b"PK\x03\x04")
        or head8.startswith(b"PK\x05\x06")
        or head8.startswith(b"PK\x07\x08")
    )


def _classify_payload(head: bytes) -> str:
    txt = head.decode("utf-8", errors="ignore").lower().strip()
    if txt.startswith("<!doctype html") or txt.startswith("<html") or txt.startswith("<"):
        markers = ["cloudflare", "access denied", "forbidden", "attention required", "captcha", "turnstile"]
        hit = [m for m in markers if m in txt]
        return f"HTML (provável bloqueio/erro). markers={hit[:5]}"
    if txt.startswith("{") or txt.startswith("["):
        return "JSON (provável erro)"
    if b";" in head or b"," in head:
        return "Provável CSV/Texto"
    return "Binário desconhecido (não-ZIP)"


def _validate_zip_or_raise(zip_path: Path, url_hint: str) -> None:
    if not zip_path.exists():
        raise FileNotFoundError(f"Download não gerou arquivo: {zip_path}")

    size = zip_path.stat().st_size
    if size == 0:
        raise RuntimeError("Arquivo baixado veio com 0 bytes (timeout/bloqueio).")

    head = _read_head(zip_path, 4096)
    head8 = head[:8]
    if _is_zip_signature(head8) and zipfile.is_zipfile(zip_path):
        return

    _ensure_debug_dir()
    try:
        (DEBUG_DIR / "download_head.bin").write_bytes(head)
    except Exception:
        pass

    snippet = head[:1200].decode("utf-8", errors="ignore")
    kind = _classify_payload(head)

    raise RuntimeError(
        "Arquivo baixado não é ZIP válido. "
        f"kind={kind} size={size} url={url_hint}\n"
        f"snippet:\n{snippet}"
    )


def _save_page_evidence(page, label: str) -> None:
    _ensure_debug_dir()
    ts = int(time.time())
    try:
        page.screenshot(path=str(DEBUG_DIR / f"{label}_{ts}.png"), full_page=True)
    except Exception:
        pass
    try:
        (DEBUG_DIR / f"{label}_{ts}.html").write_text(page.content(), encoding="utf-8", errors="ignore")
    except Exception:
        pass


def _download_zip_via_browser() -> Tuple[Path, str]:
    print(f"Browser: Navigating to {SES_URL}...")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = browser.new_context(
            accept_downloads=True,
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        page = context.new_page()

        try:
            page.goto(SES_URL, timeout=90_000, wait_until="domcontentloaded")

            print("Browser: Searching for download link via Text Match...")

            # CORREÇÃO: Usar regex flexível para pegar o texto que vimos no log.
            # O texto real é 'Base de Dados do SES, atualizada até YYYYMM'
            # Usamos get_by_text para pegar spans/divs clicáveis, não só links.
            target_text = re.compile(r"Base\s*de\s*Dados\s*do\s*SES", re.IGNORECASE)
            
            # Tenta encontrar qualquer elemento com esse texto visível
            link = page.get_by_text(target_text)

            if link.count() == 0:
                # Fallback: Tenta achar qualquer link que contenha "Download" ou ".zip"
                print("Browser: Exact text not found. Trying generic 'Download' locator...")
                link = page.get_by_role("link").filter(has_text="Download")

            if link.count() == 0:
                visible_text = page.locator("body").inner_text()
                raise RuntimeError(f"Link 'Base de Dados do SES' NÃO encontrado. Texto visível: {visible_text[:300]}")

            print("Browser: Found target element. Clicking...")

            # Timeout alto para o download começar (ASP.NET é lento)
            with page.expect_download(timeout=180_000) as dlinfo:
                # Clica no primeiro elemento encontrado
                link.first.click(timeout=30_000)

            download = dlinfo.value
            dl_url = getattr(download, "url", "") or "playwright_download"
            
            print(f"Browser: Download started from {dl_url}")

            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
            tmp_path = Path(tmp.name)
            tmp.close()

            download.save_as(tmp_path)

            _validate_zip_or_raise(tmp_path, dl_url)

            browser.close()
            return tmp_path, dl_url

        except (PwTimeoutError, Exception) as e:
            _save_page_evidence(page, "ses_failure")
            browser.close()
            raise RuntimeError(f"Playwright SES download failed: {e}") from e


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


def _ym_to_iso_01(ym: int) -> str:
    return f"{ym // 100:04d}-{ym % 100:02d}-01"


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


def extract_ses_master_and_financials(
    zip_url: Optional[str] = None,
) -> Tuple[SesExtractionMeta, Dict[str, Dict[str, Any]]]:
    zip_path, used_url = _download_zip_via_browser()

    try:
        with zipfile.ZipFile(zip_path) as z:

            def find(names: list[str]) -> Optional[str]:
                for n in z.namelist():
                    if any(x.lower() in n.lower() for x in names) and n.lower().endswith(".csv"):
                        return n
                return None

            cias = find(["ses_cias", "cias"])
            seguros = find(["ses_seguros", "seguros"])

            if not cias or not seguros:
                raise RuntimeError(f"ZIP baixado, mas CSVs não encontrados. Conteúdo: {z.namelist()}")

            def read_csv(fname: str) -> list[list[str]]:
                with z.open(fname) as f:
                    content = io.TextIOWrapper(f, encoding="latin-1", errors="replace", newline="")
                    return list(csv.reader(content, delimiter=";"))

            rows_cias = read_csv(cias)
            rows_seguros = read_csv(seguros)

            h_cias = [_norm(x) for x in rows_cias[0]]
            h_seg = [_norm(x) for x in rows_seguros[0]]

            def g_idx(h: list[str], keys: list[str]) -> Optional[int]:
                for k in keys:
                    nk = _norm(k)
                    if nk in h:
                        return h.index(nk)
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
                    cn = None
                    if cn_i is not None and len(row) > cn_i:
                        cn = _digits(row[cn_i])
                    companies[sid.zfill(6)] = {"name": row[nm_i].strip(), "cnpj": cn}

            agg: Dict[str, Dict[str, float]] = {}
            max_ym = 0

            for row in rows_seguros[1:]:
                if None in (sid_i, ym_i, pr_i):
                    break
                if len(row) <= max(sid_i, ym_i, pr_i):
                    continue

                ym = _parse_ym(row[ym_i])
                if not ym:
                    continue
                max_ym = max(max_ym, ym)

                sid = _digits(row[sid_i])
                if not sid:
                    continue
                sid = sid.zfill(6)

                prem = _parse_brl_number(row[pr_i])
                sin = 0.0
                if sn_i is not None and len(row) > sn_i:
                    sin = _parse_brl_number(row[sn_i])

                bucket = agg.setdefault(sid, {"p": 0.0, "c": 0.0})
                bucket["p"] += prem
                bucket["c"] += sin

            start_ym = (max_ym // 100 * 12 + max_ym % 100 - 1 - 11)
            start_ym = (start_ym // 12) * 100 + (start_ym % 12 + 1)

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

            meta = SesExtractionMeta(used_url, cias, seguros, _ym_to_iso_01(start_ym), _ym_to_iso_01(max_ym))
            return meta, out

    finally:
        try:
            zip_path.unlink()
        except Exception:
            pass
