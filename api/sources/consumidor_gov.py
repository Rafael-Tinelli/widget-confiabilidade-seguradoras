from __future__ import annotations

import csv
import gzip
import hashlib
import io
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, Optional

import requests

DATASET_URL = "https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br"
CKAN_PACKAGE_ID = "reclamacoes-do-consumidor-gov-br"
CKAN_PACKAGE_SHOW = "https://dados.mj.gov.br/api/3/action/package_show"

# Captura "basecompletaYYYY-MM.csv" em URLs
BASECOMPLETA_URL_RE = re.compile(r"basecompleta(?P<ym>\d{4}-\d{2})\.csv", re.IGNORECASE)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}


def _build_session(*, trust_env: bool) -> requests.Session:
    s = requests.Session()
    # Se True: respeita HTTP(S)_PROXY do ambiente; se False: ignora proxy/env.
    s.trust_env = trust_env
    s.headers.update(DEFAULT_HEADERS)
    return s


def _http_get(
    url: str,
    *,
    stream: bool,
    timeout: int,
    params: Optional[dict] = None,
) -> requests.Response:
    """
    GET robusto:
    1) tenta com trust_env=True (respeita proxy do runner)
    2) se retornar 403, tenta novamente com trust_env=False (bypass de proxy)
    """
    last_exc: Optional[Exception] = None

    # Tenta primeiro COM proxy (padrão), depois SEM proxy (bypass)
    for trust_env in (True, False):
        try:
            s = _build_session(trust_env=trust_env)
            resp = s.get(
                url,
                params=params,
                stream=stream,
                timeout=timeout,
                allow_redirects=True,
            )

            # Proxy negando (403) é o caso clássico: retry sem proxy resolve.
            if resp.status_code == 403 and trust_env is True:
                resp.close()
                continue

            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e

    raise RuntimeError(f"Falha ao acessar {url}: {last_exc}") from last_exc


# --- Normalização robusta ---


def _norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return re.sub(r"\s+", " ", s)


def _norm_header(h: str) -> str:
    return _norm_text(h).replace(" ", "")


HEADER_ALIASES = {
    "nomefantasia": "nome_fantasia",
    "respondida": "respondida",
    "situacao": "situacao",
    "avaliacaoreclamacao": "avaliacao",
    "notadoconsumidor": "nota",
    "notaconsumidor": "nota",
    "temporesposta": "tempo_resposta",
}


def _get_field(row: dict, canonical: str) -> str:
    return (row.get(canonical) or "").strip()


def _safe_int(s: str) -> Optional[int]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _safe_float(s: str) -> Optional[float]:
    s = (s or "").strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


@dataclass
class Agg:
    display_name: str = ""
    total: int = 0
    finalizadas: int = 0
    respondidas: int = 0
    resolvidas_indicador: int = 0
    nota_sum: float = 0.0
    nota_count: int = 0
    tempo_sum: int = 0
    tempo_count: int = 0

    def to_public(self) -> dict:
        sat = (self.nota_sum / self.nota_count) if self.nota_count else None
        avg_tempo = (self.tempo_sum / self.tempo_count) if self.tempo_count else None

        responded_rate = (self.respondidas / self.finalizadas) if self.finalizadas else None
        resolution_rate = (
            (self.resolvidas_indicador / self.finalizadas) if self.finalizadas else None
        )

        return {
            "display_name": self.display_name,
            "complaints_total": self.total,
            "complaints_finalizadas": self.finalizadas,
            "responded_rate": round(responded_rate, 4) if responded_rate is not None else None,
            "resolution_rate": round(resolution_rate, 4) if resolution_rate is not None else None,
            "satisfaction_avg": round(sat, 2) if sat is not None else None,
            "avg_response_days": round(avg_tempo, 1) if avg_tempo is not None else None,
        }


def discover_basecompleta_urls(
    months: int = 12,
    *,
    dataset_url: str = DATASET_URL,
    package_id: str = CKAN_PACKAGE_ID,
) -> Dict[str, str]:
    """
    Retorna dict { 'YYYY-MM': '<url csv>' } do(s) mês(es) mais recentes.
    """
    # 1) CKAN API
    try:
        resp = _http_get(
            CKAN_PACKAGE_SHOW,
            params={"id": package_id},
            stream=False,
            timeout=60,
        )
        data = resp.json()
        if data.get("success"):
            resources = data["result"]["resources"]
            matches: Dict[str, str] = {}

            for r in resources:
                url = (r.get("url") or "").strip()
                if not url:
                    continue
                m = BASECOMPLETA_URL_RE.search(url)
                if not m:
                    continue
                ym = m.group("ym")
                matches[ym] = url

            yms = sorted(matches.keys(), reverse=True)[:months]
            return {ym: matches[ym] for ym in yms}
    except Exception:
        pass

    # 2) fallback: HTML direto
    try:
        html_resp = _http_get(dataset_url, stream=False, timeout=60)
        html = html_resp.text

        href_re = re.compile(
            r'href="(?P<url>https?://[^"]+/download/basecompleta(?P<ym>\d{4}-\d{2})\.csv)"',
            re.IGNORECASE,
        )

        matches2: Dict[str, str] = {}
        for m in href_re.finditer(html):
            matches2[m.group("ym")] = m.group("url")

        yms2 = sorted(matches2.keys(), reverse=True)[:months]
        return {ym: matches2[ym] for ym in yms2}
    except Exception as e:
        print(f"AVISO: Falha no fallback HTML: {e}")
        return {}


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def download_csv_to_gz(url: str, out_gz_path: str, timeout: int = 300) -> dict:
    dirpath = os.path.dirname(out_gz_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

    tmp_path = out_gz_path + ".tmp"

    try:
        resp = _http_get(url, stream=True, timeout=timeout)
        with resp:
            with gzip.open(tmp_path, "wb") as gz:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        gz.write(chunk)

        os.replace(tmp_path, out_gz_path)

        return {
            "url": url,
            "path": out_gz_path,
            "sha256": _sha256_file(out_gz_path),
            "downloaded_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "bytes": os.path.getsize(out_gz_path),
        }
    except Exception as e:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise RuntimeError(f"Falha no download de {url}: {e}") from e


def iter_rows_from_gz_csv(path: str, encoding: str = "latin-1") -> Iterable[dict]:
    with gzip.open(path, "rb") as f:
        text = io.TextIOWrapper(f, encoding=encoding, errors="replace", newline="")
        reader = csv.reader(text, delimiter=";")

        try:
            headers = next(reader)
        except StopIteration:
            return

        norm_headers = [_norm_header(h) for h in headers]
        fieldnames = [HEADER_ALIASES.get(h, h) for h in norm_headers]

        for row in reader:
            if not row:
                continue
            if len(row) != len(fieldnames):
                continue
            yield dict(zip(fieldnames, row))


def aggregate_month(path_gz: str) -> Dict[str, Agg]:
    aggs: Dict[str, Agg] = {}

    for row in iter_rows_from_gz_csv(path_gz):
        nome = _get_field(row, "nome_fantasia")
        if not nome:
            continue

        key = _norm_text(nome)
        if key not in aggs:
            aggs[key] = Agg(display_name=nome)

        a = aggs[key]
        a.total += 1

        situacao = _get_field(row, "situacao").lower()
        is_finalizada = situacao.startswith("finalizada")

        if not is_finalizada:
            continue

        a.finalizadas += 1

        respondida = _get_field(row, "respondida").upper()
        if respondida == "S":
            a.respondidas += 1

        avaliacao = _get_field(row, "avaliacao").lower()

        if "nao avaliada" in situacao or "não avaliada" in situacao:
            a.resolvidas_indicador += 1
        elif "resolvida" in avaliacao:
            a.resolvidas_indicador += 1

        nota = _safe_float(_get_field(row, "nota"))
        if nota is not None:
            a.nota_sum += nota
            a.nota_count += 1

        tempo = _safe_int(_get_field(row, "tempo_resposta"))
        if tempo is not None and respondida == "S":
            a.tempo_sum += tempo
            a.tempo_count += 1

    return aggs
