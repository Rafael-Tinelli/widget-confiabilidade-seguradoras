import os
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO

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


@dataclass
class SesFetchResult:
    zip_url: str
    fetched_at: str
    bytes_len: int
    sha256: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256(b: bytes) -> str:
    import hashlib

    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def _clean_url(u: str) -> str:
    u = u.strip()
    u = u.replace(" ", "%20")
    return u


def _extract_urls(html: str, base_url: str) -> list[str]:
    # extrai urls absolutas e relativas simples
    urls = set()

    for m in re.finditer(r'href=["\']([^"\']+)["\']', html, flags=re.I):
        u = m.group(1).strip()
        if not u:
            continue
        if u.startswith("#") or u.startswith("javascript:"):
            continue
        urls.add(u)

    # normaliza relativa -> absoluta (bem simples, suficiente para o SES)
    out: list[str] = []
    for u in urls:
        if u.startswith("http://") or u.startswith("https://"):
            out.append(u)
        elif u.startswith("//"):
            out.append("https:" + u)
        else:
            # junta com base_url
            if base_url.endswith("/"):
                out.append(base_url + u.lstrip("/"))
            else:
                out.append(base_url.rsplit("/", 1)[0] + "/" + u.lstrip("/"))
    return [_clean_url(x) for x in out]


def _fetch_text(url: str, *, allow_insecure: bool | None = None) -> str:
    timeout = float(os.environ.get("SES_HTTP_TIMEOUT", "30"))
    ua = os.environ.get("SES_UA", "widget-confiabilidade-seguradoras/1.0")
    headers = {"User-Agent": ua}

    if allow_insecure is None:
        allow_insecure = str(os.environ.get("SES_ALLOW_INSECURE_SSL", "")).strip().lower() in {"1", "true", "yes"}

    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, verify=True)
        r.raise_for_status()
        return r.text
    except SSLError:
        if not allow_insecure:
            raise
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, verify=False)
        r.raise_for_status()
        return r.text


def _fetch_bytes(url: str, *, allow_insecure: bool | None = None) -> bytes:
    timeout = float(os.environ.get("SES_HTTP_TIMEOUT", "60"))
    ua = os.environ.get("SES_UA", "widget-confiabilidade-seguradoras/1.0")
    headers = {"User-Agent": ua}

    if allow_insecure is None:
        allow_insecure = str(os.environ.get("SES_ALLOW_INSECURE_SSL", "")).strip().lower() in {"1", "true", "yes"}

    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, verify=True)
        r.raise_for_status()
        return r.content
    except SSLError:
        if not allow_insecure:
            raise
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, verify=False)
        r.raise_for_status()
        return r.content


def _is_zip_signature(b: bytes) -> bool:
    return len(b) >= 2 and b[:2] == b"PK"


def fetch_ses_zip(zip_url: str) -> SesFetchResult:
    b = _fetch_bytes(zip_url)
    if not _is_zip_signature(b[:4]):
        raise RuntimeError(f"SES: download não parece ZIP (assinatura != PK). url={zip_url!r}")

    # valida zipfile (rápido) sem extrair tudo
    try:
        with zipfile.ZipFile(BytesIO(b)) as zf:
            # lista um item (se existir) só para confirmar estrutura
            _ = zf.namelist()[:1]
    except zipfile.BadZipFile as e:
        raise RuntimeError(f"SES: BadZipFile ao abrir conteúdo baixado. url={zip_url!r}") from e

    return SesFetchResult(zip_url=zip_url, fetched_at=_utc_now(), bytes_len=len(b), sha256=_sha256(b))


def _score_zip(url: str) -> int:
    u = url.lower()
    score = 0
    if "basecompleta" in u:
        score += 50
    if u.endswith(".zip"):
        score += 10
    if "estatistic" in u or "estatistic" in u:
        score += 5
    return score


def _pick_best_zip(urls: list[str]) -> str | None:
    zips = [u for u in urls if u.lower().endswith(".zip")]
    if not zips:
        return None
    zips.sort(key=lambda x: _score_zip(x), reverse=True)
    return zips[0]


def discover_ses_zip_url() -> str:
    """
    Descobre a URL do ZIP da Base Completa do SES.

    Ordem:
    1) SES_ZIP_URL (override)
    2) Extrair .zip do principal.aspx
    3) Identificar páginas candidatas (download/base/ses) e buscar .zip nelas (crawl curto)
    """
    override = str(os.environ.get("SES_ZIP_URL", "")).strip()
    if override:
        print(f"SES: usando override SES_ZIP_URL={override}", flush=True)
        return override

    home = str(os.environ.get("SES_HOME_URL", "")).strip() or SES_HOME_DEFAULT

    html = _fetch_text(home)

    # 1) tenta extrair direto do principal
    urls = _extract_urls(html, home)
    best = _pick_best_zip(urls)
    if best:
        return best

    # 2) crawl curto em páginas candidatas
    candidates = []
    for u in urls:
        ul = u.lower()
        if any(h in ul for h in SES_DOWNLOAD_HINTS):
            candidates.append(u)

    # limita pra evitar scraping grande
    candidates = candidates[:10]

    for page in candidates:
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


def download_and_validate_ses_zip() -> SesFetchResult:
    """
    Resolve a URL do zip e baixa o conteúdo completo (com validação PK + zipfile).
    """
    zip_url = discover_ses_zip_url()
    return fetch_ses_zip(zip_url)


def fetch_ses_zip_head_signature(zip_url: str) -> bytes:
    """
    Busca apenas os primeiros bytes (Range) para validar assinatura PK sem baixar tudo.
    """
    timeout = float(os.environ.get("SES_HTTP_TIMEOUT", "30"))
    ua = os.environ.get("SES_UA", "widget-confiabilidade-seguradoras/1.0")
    headers = {"User-Agent": ua, "Range": "bytes=0-3"}

    allow_insecure = str(os.environ.get("SES_ALLOW_INSECURE_SSL", "")).strip().lower() in {"1", "true", "yes"}

    def _get(verify: bool) -> bytes:
        r = requests.get(zip_url, headers=headers, timeout=timeout, stream=True, verify=verify, allow_redirects=True)
        r.raise_for_status()
        return r.raw.read(4)

    try:
        return _get(verify=True)
    except SSLError:
        if not allow_insecure:
            raise
        return _get(verify=False)


def wait_until_ses_zip_is_available(max_wait_seconds: int = 30) -> str:
    """
    Usa a discovery e valida a assinatura PK via Range.
    Retorna a URL do ZIP quando OK.
    """
    start = time.time()
    last_err: Exception | None = None
    while time.time() - start < max_wait_seconds:
        try:
            url = discover_ses_zip_url()
            head = fetch_ses_zip_head_signature(url)
            if _is_zip_signature(head):
                return url
        except Exception as e:
            last_err = e
        time.sleep(2)

    raise RuntimeError(f"SES zip não ficou disponível/validável em {max_wait_seconds}s") from last_err
