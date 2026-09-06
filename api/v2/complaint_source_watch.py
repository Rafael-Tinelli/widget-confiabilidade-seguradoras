from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from api.sources import consumer_gov_direct

WATCH_VERSION = "complaint-source-watch-v1"
LAST_VALIDATED_CONSUMER_MONTH = "2026-06"

CONSUMER_CKAN_URL = (
    "https://dados.mj.gov.br/api/3/action/package_show"
    "?id=reclamacoes-do-consumidor-gov-br"
)
CONSUMER_DIRECT_URL = "https://www.consumidor.gov.br/pages/dadosabertos/externo/"
SUSEPCON_URL = (
    "https://www.gov.br/susep/pt-br/central-de-conteudos/central-de-paineis/"
    "painel-susepcon"
)
SUSEP_PANELS_URL = "https://www.gov.br/susep/pt-br/central-de-conteudos/central-de-paineis"
BDR_GUIDANCE_URL = (
    "https://www.gov.br/susep/pt-br/servicos/mercado/enviar-dados/"
    "roteiro-de-envio-dos-arquivos-de-dados-de-reclamacao-de-ouvidorias"
)

SUSEPCON_FREEZE_MARKER = "dados ficarao congelados no 4 trimestre de 2025"


@dataclass(frozen=True)
class WatchEvent:
    key: str
    title: str
    source: str
    summary: str
    evidence: dict[str, Any]
    urls: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _norm(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _session() -> requests.Session:
    retry = Retry(
        total=1,
        connect=1,
        read=1,
        status=1,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "Sanida-RK2-Complaint-Source-Watch/1.0",
            "Accept": "text/html,application/json,*/*;q=0.8",
        }
    )
    return session


def _fetch_text(session: requests.Session, url: str) -> tuple[str | None, str | None]:
    try:
        response = session.get(url, timeout=(8, 20))
        if response.status_code != 200:
            return None, f"HTTP {response.status_code}"
        if len(response.content) < 500:
            return None, f"response_too_small:{len(response.content)}"
        return response.text, None
    except requests.RequestException as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _fetch_json(session: requests.Session, url: str) -> tuple[Any | None, str | None]:
    try:
        response = session.get(url, timeout=(8, 20))
        if response.status_code != 200:
            return None, f"HTTP {response.status_code}"
        return response.json(), None
    except (requests.RequestException, ValueError) as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _months_after(last_month: str) -> list[str]:
    year, month = (int(part) for part in last_month.split("-"))
    now = datetime.now(timezone.utc)
    out: list[str] = []
    while (year, month) < (now.year, now.month):
        month += 1
        if month == 13:
            year += 1
            month = 1
        out.append(f"{year:04d}-{month:02d}")
    return out


def evaluate_consumer_ckan(payload: Any) -> WatchEvent | None:
    if not isinstance(payload, dict) or payload.get("success") is not True:
        return None
    result = payload.get("result")
    if not isinstance(result, dict):
        return None
    resources = result.get("resources")
    if not isinstance(resources, list):
        return None
    usable = [
        item
        for item in resources
        if isinstance(item, dict)
        and str(item.get("url") or "").startswith(("http://", "https://"))
    ]
    if not usable:
        return None
    return WatchEvent(
        key="consumer-gov-ckan-operational",
        title="Fontes de Reclamações Disponíveis — Consumer.gov",
        source="Consumer.gov / dados.mj.gov.br",
        summary="O catálogo CKAN oficial voltou a responder com recursos utilizáveis.",
        evidence={
            "package_title": result.get("title") or result.get("name"),
            "metadata_modified": result.get("metadata_modified"),
            "resource_count": len(usable),
        },
        urls=(CONSUMER_CKAN_URL,),
    )


def _is_explicit_basecompleta(publication: consumer_gov_direct.Publication) -> bool:
    blob = _norm(f"{publication.title} {publication.filename or ''}")
    return "base completa" in blob or "basecompleta" in blob


def evaluate_consumer_publications(
    publications: list[consumer_gov_direct.Publication],
) -> WatchEvent | None:
    newer = sorted(
        {
            publication.month
            for publication in publications
            if publication.month
            and publication.month > LAST_VALIDATED_CONSUMER_MONTH
            and _is_explicit_basecompleta(publication)
        }
    )
    if not newer:
        return None
    return WatchEvent(
        key="consumer-gov-new-basecompleta-after-2026-06",
        title="Fontes de Reclamações Disponíveis — Consumer.gov",
        source="Consumer.gov / Dados Abertos",
        summary="Foram encontradas publicações Base Completa posteriores a junho de 2026.",
        evidence={
            "last_validated_month": LAST_VALIDATED_CONSUMER_MONTH,
            "newer_months": newer,
        },
        urls=(CONSUMER_DIRECT_URL,),
    )


def evaluate_susepcon_html(html: str) -> WatchEvent | None:
    normalized = _norm(html)
    if "susepcon" not in normalized or "reclamacoes" not in normalized:
        return None
    if SUSEPCON_FREEZE_MARKER in normalized:
        return None
    return WatchEvent(
        key="susepcon-no-longer-frozen-at-2025-q4",
        title="Fontes de Reclamações Disponíveis — SusepCon",
        source="SUSEP / SusepCon",
        summary=(
            "O aviso oficial de congelamento no 4º trimestre de 2025 não está mais "
            "presente na página do SusepCon."
        ),
        evidence={"previous_state": "frozen_at_2025_q4"},
        urls=(SUSEPCON_URL,),
    )


class _LinkCollector(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        self._href = urljoin(self.base_url, href) if href else None
        self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._href is None:
            return
        self.links.append((self._href, " ".join(self._text).strip()))
        self._href = None
        self._text = []


def _bdr_candidate_links(html: str, base_url: str) -> list[dict[str, str]]:
    parser = _LinkCollector(base_url)
    parser.feed(html)
    candidates: list[dict[str, str]] = []
    seen: set[str] = set()
    for href, text in parser.links:
        parsed = urlparse(href)
        if parsed.scheme not in {"http", "https"}:
            continue
        blob = _norm(f"{text} {href}")
        if "jsonschema" in blob or "manual" in blob or "painel susepcon" in blob:
            continue
        if "painel-susepcon" in href:
            continue
        subject = " bdr " in f" {blob} " or "reclamacoes" in blob
        access = any(
            term in blob
            for term in (
                "dados abertos",
                "acesso aos dados",
                "consultar dados",
                "consulta de dados",
                "download da base",
                "base para download",
                "painel",
            )
        )
        if not (subject and access):
            continue
        if href in seen:
            continue
        seen.add(href)
        candidates.append({"text": text, "url": href})
    return candidates


def evaluate_bdr_html(pages: list[tuple[str, str]]) -> WatchEvent | None:
    candidates: list[dict[str, str]] = []
    for url, html in pages:
        candidates.extend(_bdr_candidate_links(html, url))
    if not candidates:
        return None
    unique = {item["url"]: item for item in candidates}
    ordered = [unique[url] for url in sorted(unique)]
    digest = hashlib.sha256(
        "\n".join(item["url"] for item in ordered).encode("utf-8")
    ).hexdigest()[:16]
    return WatchEvent(
        key=f"bdr-public-access-candidate-{digest}",
        title="Fontes de Reclamações Disponíveis — BDR/SUSEP",
        source="SUSEP / BDR",
        summary="Foi encontrado um novo candidato oficial de acesso público aos dados da BDR.",
        evidence={"candidate_links": ordered},
        urls=tuple(item["url"] for item in ordered),
    )


def _probe_once() -> tuple[list[WatchEvent], dict[str, str]]:
    session = _session()
    events: list[WatchEvent] = []
    diagnostics: dict[str, str] = {}

    payload, error = _fetch_json(session, CONSUMER_CKAN_URL)
    diagnostics["consumer_ckan"] = error or "reachable"
    if payload is not None:
        event = evaluate_consumer_ckan(payload)
        if event:
            events.append(event)

    wanted_months = set(_months_after(LAST_VALIDATED_CONSUMER_MONTH))
    if wanted_months:
        old_retries = consumer_gov_direct.HTTP_RETRIES
        old_connect = consumer_gov_direct.CONNECT_TIMEOUT
        old_read = consumer_gov_direct.READ_TIMEOUT
        consumer_gov_direct.HTTP_RETRIES = 1
        consumer_gov_direct.CONNECT_TIMEOUT = 8
        consumer_gov_direct.READ_TIMEOUT = 20
        try:
            publications = consumer_gov_direct.discover_structured_publications(
                wanted_months
            )
            diagnostics["consumer_direct"] = f"reachable:{len(publications)}"
            event = evaluate_consumer_publications(publications)
            if event:
                events.append(event)
        except Exception as exc:  # noqa: BLE001 - observational sensor must not abort
            diagnostics["consumer_direct"] = f"{type(exc).__name__}: {exc}"
        finally:
            consumer_gov_direct.HTTP_RETRIES = old_retries
            consumer_gov_direct.CONNECT_TIMEOUT = old_connect
            consumer_gov_direct.READ_TIMEOUT = old_read

    html, error = _fetch_text(session, SUSEPCON_URL)
    diagnostics["susepcon"] = error or "reachable"
    if html:
        event = evaluate_susepcon_html(html)
        if event:
            events.append(event)

    bdr_pages: list[tuple[str, str]] = []
    for label, url in (("bdr_guidance", BDR_GUIDANCE_URL), ("susep_panels", SUSEP_PANELS_URL)):
        page, error = _fetch_text(session, url)
        diagnostics[label] = error or "reachable"
        if page:
            bdr_pages.append((url, page))
    event = evaluate_bdr_html(bdr_pages)
    if event:
        events.append(event)

    deduped = {event.key: event for event in events}
    return [deduped[key] for key in sorted(deduped)], diagnostics


def run_watch(confirm_delay: float = 0.0) -> dict[str, Any]:
    first, diagnostics = _probe_once()
    confirmed = first
    confirmation: dict[str, str] = {}
    if first and confirm_delay > 0:
        time.sleep(confirm_delay)
        second, second_diagnostics = _probe_once()
        second_by_key = {event.key: event for event in second}
        confirmed = [event for event in first if event.key in second_by_key]
        confirmation = second_diagnostics

    return {
        "watch_version": WATCH_VERSION,
        "checked_at": _utc_now(),
        "baseline": {
            "consumer_gov_last_validated_month": LAST_VALIDATED_CONSUMER_MONTH,
            "susepcon_state": "frozen_at_2025_q4",
            "bdr_public_raw_access": "not_established",
        },
        "event_count": len(confirmed),
        "events": [event.as_dict() for event in confirmed],
        "diagnostics": diagnostics,
        "confirmation_diagnostics": confirmation,
        "effects": {
            "production_data_write": False,
            "methodology_change": False,
            "full_generation_dispatch": False,
            "hostgator_publication": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--confirm-delay", type=float, default=0.0)
    args = parser.parse_args()

    payload = run_watch(confirm_delay=max(args.confirm_delay, 0.0))
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
