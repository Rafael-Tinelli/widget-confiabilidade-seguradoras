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

SES_URL = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"
DEBUG_DIR = Path("ses_debug")
_DELIMS = [";", ",", "\t", "|"]


@dataclass(frozen=True)
class SesExtractionMeta:
    zip_url: str
    cias_file: str
    seguros_file: str
    period_from: str
    period_to: str


def _ensure_debug_dir() -> None:
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    keep = DEBUG_DIR / "_keep.txt"
    if not keep.exists():
        keep.write_text(
            f"SES debug evidence folder. created_at={int(time.time())}\n",
            encoding="utf-8",
        )


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
    d = re.sub(r"\D+", "", str(s))
    return d or None


def _cnpj14(raw: Any) -> Optional[str]:
    """
    Normaliza CNPJ para exatamente 14 dígitos.
    - Remove não-dígitos
    - zfill(14) se vier com zeros à esquerda omitidos
    - descarta se não conseguir ficar em 14 dígitos
    """
    d = _digits(str(raw) if raw is not None else None)
    if not d:
        return None
    if len(d) < 14:
        d = d.zfill(14)
    if len(d) != 14:
        return None
    return d


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
        markers = [
            "cloudflare",
            "access denied",
            "forbidden",
            "attention required",
            "captcha",
            "turnstile",
        ]
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

    try:
        (DEBUG_DIR / f"invalid_download_{int(time.time())}.txt").write_bytes(head)
    except Exception:
        pass

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
        (DEBUG_DIR / f"{label}_{ts}.html").write_text(
            page.content(), encoding="utf-8", errors="ignore"
        )
    except Exception:
        pass


def _first_visible(locator):
    n = locator.count()
    for i in range(n):
        it = locator.nth(i)
        try:
            if it.is_visible():
                return it
        except Exception:
            continue
    return None


def _find_ses_download_trigger(page):
    target = re.compile(
        r"Base[\s\u00A0]*de[\s\u00A0]*Dados[\s\u00A0]*do[\s\u00A0]*SES",
        re.IGNORECASE,
    )

    try:
        page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        pass

    for fr in page.frames:
        clickables = fr.locator(
            "a, button, input[type=submit], input[type=button], [role=link], "
            "[onclick*='__doPostBack'], a[href*='__doPostBack']"
        ).filter(has_text=target)

        cand = _first_visible(clickables)
        if cand:
            print("Browser: Found direct clickable element.")
            return cand

        text_hit = fr.get_by_text(target)
        th = _first_visible(text_hit)
        if th:
            ancestor = th.locator(
                "xpath=ancestor-or-self::a[1] | "
                "ancestor-or-self::button[1] | "
                "ancestor-or-self::input[1] | "
                "ancestor-or-self::*[contains(@onclick,'__doPostBack')][1]"
            )
            anc = _first_visible(ancestor)
            if anc:
                print("Browser: Found clickable ancestor.")
                return anc

    return None


def _download_zip_via_browser() -> Tuple[Path, str]:
    from playwright.sync_api import TimeoutError as PwTimeoutError
    from playwright.sync_api import sync_playwright

    _ensure_debug_dir()

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
            extra_http_headers={
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Upgrade-Insecure-Requests": "1",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

        context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        )

        page = context.new_page()

        try:
            page.goto(SES_URL, timeout=90_000, wait_until="domcontentloaded")

            print("Browser: Locating ASP.NET download trigger...")
            trigger = _find_ses_download_trigger(page)

            if not trigger:
                visible_text = page.locator("body").inner_text()
                raise RuntimeError(
                    "Trigger 'Base de Dados do SES' NÃO encontrado. "
                    f"Texto visível (head 500): {visible_text[:500]}"
                )

            print("Browser: Found trigger. Clicking...")
            trigger.scroll_into_view_if_needed()

            with page.expect_download(timeout=180_000) as dlinfo:
                trigger.click(timeout=30_000)

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


def _detect_csv_dialect(z: zipfile.ZipFile, fname: str) -> tuple[str, str]:
    with z.open(fname) as f:
        head = f.read(65536)

    encoding = "utf-8-sig" if head.startswith(b"\xef\xbb\xbf") else "latin-1"
    try:
        txt = head.decode(encoding, errors="ignore")
    except Exception:
        txt = head.decode("latin-1", errors="ignore")

    first_line = (txt.splitlines()[:1] or [""])[0]

    delim = max(_DELIMS, key=lambda d: first_line.count(d))
    if first_line.count(delim) == 0:
        delim = ";"

    print(f"Reading {fname} | Enc: {encoding} | Delim: {delim!r}")
    return encoding, delim


def _read_csv(z: zipfile.ZipFile, fname: str) -> list[list[str]]:
    enc, delim = _detect_csv_dialect(z, fname)
    with z.open(fname) as f:
        content = io.TextIOWrapper(f, encoding=enc, errors="replace", newline="")
        return list(csv.reader(content, delimiter=delim))


def _pick_best_csv(
    z: zipfile.ZipFile,
    candidates: list[str],
    required_groups: list[list[str]],
) -> str:
    best: str | None = None
    best_score = -1
    best_rows = -1
    best_hdr = None

    for fname in candidates:
        rows = _read_csv(z, fname)
        if not rows:
            continue
        h = [_norm(x) for x in rows[0]]

        score = 0
        for group in required_groups:
            if any(_norm(k) in h for k in group):
                score += 1

        n_rows = len(rows)
        # desempate: prefira o arquivo que tem MAIS linhas
        if (score > best_score) or (score == best_score and n_rows > best_rows):
            best = fname
            best_score = score
            best_rows = n_rows
            best_hdr = rows[0][:80]

    if not best or best_score < len(required_groups):
        _ensure_debug_dir()
        try:
            (DEBUG_DIR / "csv_candidates_failed.txt").write_text(
                f"Candidates: {candidates}\nLast Header: {best_hdr}", encoding="utf-8"
            )
        except Exception:
            pass
        raise RuntimeError(
            "Não foi possível identificar o CSV correto no ZIP. "
            f"Candidatos={candidates}. MelhorScore={best_score}. "
            f"Requisitos={required_groups}. HeaderExemplo={best_hdr}"
        )

    return best


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
    if zip_url:
        print(
            f"SES: zip_url={zip_url} informado, mas ignorado em favor do crawler Playwright."
        )

    zip_path, used_url = _download_zip_via_browser()

    try:
        with zipfile.ZipFile(zip_path) as z:
            all_files = z.namelist()
            print(f"DEBUG: ZIP Contents: {all_files}")

            csvs = [n for n in all_files if n.lower().endswith(".csv")]

            # Preferência canônica
            cias_candidates = [n for n in csvs if re.fullmatch(r"ses_cias\.csv", n.lower())]
            seg_candidates = [n for n in csvs if re.fullmatch(r"ses_seguros\.csv", n.lower())]

            # Fallback (se um dia mudarem capitalização/nome)
            if not cias_candidates:
                cias_candidates = [n for n in csvs if "ses_cias" in n.lower()]
            if not seg_candidates:
                # "seguros" pode vir em outros arquivos — deixe o picker escolher pelo header + volume
                seg_candidates = [
                    n for n in csvs
                    if "ses_seguros" in n.lower()
                    or "seguros" in n.lower()
                    or "valoresmov" in n.lower()
                ]

            if not cias_candidates or not seg_candidates:
                raise RuntimeError(f"CSVs válidos não identificados. Conteúdo: {all_files}")

            cias = _pick_best_csv(
                z,
                cias_candidates,
                required_groups=[
                    ["cod_enti", "coenti", "cod_cia", "co_enti"],
                    ["noenti", "nome", "nome_cia"],
                ],
            )

            seguros = _pick_best_csv(
                z,
                seg_candidates,
                required_groups=[
                    ["cod_enti", "coenti", "cod_cia", "co_enti"],
                    ["damesano", "anomes", "competencia", "damesaano"],
                    [
                        "premio_direto",
                        "premio_de_seguros",
                        "premio_retido",
                        "premio_ganho",
                        "premio_emitido2",
                        "premio_emitido_cap",
                        "premio_direto_cap",
                        "premio",
                        "premio_emitido",
                        "premios",
                    ],
                ],
            )

            print(f"SES: CSVs selecionados: cias={cias} | seguros={seguros}")

            rows_cias = _read_csv(z, cias)
            rows_seguros = _read_csv(z, seguros)

            h_cias = [_norm(x) for x in rows_cias[0]]
            h_seg = [_norm(x) for x in rows_seguros[0]]

            def g_idx(h: list[str], keys: list[str]) -> Optional[int]:
                for k in keys:
                    nk = _norm(k)
                    if nk in h:
                        return h.index(nk)
                return None

            id_i = g_idx(h_cias, ["cod_enti", "coenti", "cod_cia", "co_enti"])
            nm_i = g_idx(h_cias, ["noenti", "nome", "nome_cia"])

            # CNPJ muda bastante de nome (nu_cnpj, cnpj_cia, num_cnpj, etc)
            cn_i = g_idx(
                h_cias,
                ["cnpj", "numcnpj", "nu_cnpj", "num_cnpj", "cnpj_cia", "cgc", "nu_cgc"],
            )
            if cn_i is None:
                for i, col in enumerate(h_cias):
                    if "cnpj" in col or col.endswith("cgc") or "cgc" in col:
                        cn_i = i
                        break

            sid_i = g_idx(h_seg, ["cod_enti", "coenti", "cod_cia", "co_enti"])
            ym_i = g_idx(h_seg, ["damesano", "anomes", "competencia", "damesaano"])

            pr_i = g_idx(
                h_seg,
                [
                    "premio_direto",
                    "premio_de_seguros",
                    "premio_retido",
                    "premio_ganho",
                    "premio_emitido2",
                    "premio_emitido_cap",
                    "premio_direto_cap",
                    "premio",
                    "premio_emitido",
                    "premios",
                ],
            )

            sn_i = g_idx(
                h_seg,
                [
                    "sinistro_direto",
                    "sinistro_ocorrido",
                    "sinistro_retido",
                    "sinistro_ocorrido_cap",
                    "sinistros_ocorridos_cap",
                    "sinistros",
                    "sinistro",
                ],
            )

            if sid_i is None or ym_i is None or pr_i is None:
                _ensure_debug_dir()
                try:
                    (DEBUG_DIR / "headers_failed_seguros.txt").write_text(str(h_seg), encoding="utf-8")
                    (DEBUG_DIR / "headers_failed_cias.txt").write_text(str(h_cias), encoding="utf-8")
                except Exception:
                    pass
                raise RuntimeError(
                    f"Colunas obrigatórias ausentes em '{seguros}'. "
                    f"sid_i={sid_i}, ym_i={ym_i}, pr_i={pr_i}. Header={rows_seguros[0][:80]}"
                )

            print(
                "SES: Colunas selecionadas: "
                f"sid={h_seg[sid_i]}, ym={h_seg[ym_i]}, premium={h_seg[pr_i]}, "
                f"claims={(h_seg[sn_i] if sn_i is not None else None)}, "
                f"cnpj={(h_cias[cn_i] if (cn_i is not None and cn_i < len(h_cias)) else None)}"
            )

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
                        cn = _cnpj14(row[cn_i])

                    companies[sid.zfill(6)] = {"name": row[nm_i].strip(), "cnpj": cn}

            agg: Dict[str, Dict[str, float]] = {}
            max_ym = 0

            for row in rows_seguros[1:]:
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
                sin = 0.0
                if sn_i is not None and len(row) > sn_i:
                    sin = _parse_brl_number(row[sn_i])

                bucket = agg.setdefault(sid, {"p": 0.0, "c": 0.0})
                bucket["p"] += prem
                bucket["c"] += sin

            if max_ym <= 0:
                raise RuntimeError("Não foi possível determinar o período (max_ym).")

            # rolling 12m metadata (mantém compatibilidade com build atual; filtragem pode ser aplicada em versão futura)
            max_idx = (max_ym // 100) * 12 + (max_ym % 100) - 1
            start_idx = max_idx - 11
            start_year = start_idx // 12
            start_month = start_idx % 12 + 1
            start_ym = start_year * 100 + start_month

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

            meta = SesExtractionMeta(
                used_url,
                cias,
                seguros,
                _ym_to_iso_01(start_ym),
                _ym_to_iso_01(max_ym),
            )
            return meta, out

    finally:
        try:
            zip_path.unlink()
        except Exception:
            pass
