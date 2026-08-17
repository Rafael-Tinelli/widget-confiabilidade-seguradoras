from __future__ import annotations

import html
import os
import re
from typing import Any

import requests

SPECIAL_REGIME_URLS = {
    "fiscal_direction": "https://www2.susep.gov.br/menuatendimento/regimesespeciais/direcao_fiscal_2011.asp",
    "intervention": "https://www2.susep.gov.br/menuatendimento/regimesespeciais/intervencao_2011.asp",
    "extrajudicial_liquidation": "https://www2.susep.gov.br/menuatendimento/regimesespeciais/liq_extrajudicial_2011.asp",
    "ordinary_liquidation": "https://www2.susep.gov.br/menuatendimento/regimesespeciais/liq_ordinaria_2011.asp",
    "bankruptcy": "https://www2.susep.gov.br/menuatendimento/regimesespeciais/falencia_2011.asp",
}

_SECTION_TYPES = {
    "I. SOCIEDADES SEGURADORAS": "insurer",
    "II. ENTIDADES DE PREVIDÊNCIA COMPLEMENTAR ABERTA": "open_pension_entity",
    "III. SOCIEDADES DE CAPITALIZAÇÃO": "capitalization_company",
}

_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SanidaResearch/1.0; +https://sanida.com.br)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.5",
}


class SpecialRegimeSourceError(RuntimeError):
    """Raised when the official special-regime source cannot be trusted."""


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _decode(content: bytes) -> str:
    for encoding in ("utf-8", "cp1252", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _clean_text(fragment: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", fragment)
    return html.unescape(re.sub(r"\s+", " ", no_tags)).strip()


def _normalize_fip(raw: Any) -> str:
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _section_type_at(document: str, position: int) -> str:
    prefix = document[:position].upper()
    candidates = [
        (prefix.rfind(marker.upper()), entity_type)
        for marker, entity_type in _SECTION_TYPES.items()
    ]
    found = [(pos, entity_type) for pos, entity_type in candidates if pos >= 0]
    if not found:
        return "unknown"
    return max(found, key=lambda item: item[0])[1]


def parse_special_regime_html(document: str, regulatory_status: str) -> list[dict[str, Any]]:
    """Parse FIP-stable records from one SUSEP special-regime list page."""
    if regulatory_status not in SPECIAL_REGIME_URLS:
        raise ValueError(f"Unsupported regulatory status: {regulatory_status}")

    records: list[dict[str, Any]] = []
    pattern = re.compile(
        r"<a\b[^>]*href=[\"'][^\"']*codempresa=(\d+)[^\"']*[\"'][^>]*>(.*?)</a>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(document):
        fip_code = _normalize_fip(match.group(1))
        legal_name = _clean_text(match.group(2))
        if not fip_code or not legal_name:
            continue

        records.append(
            {
                "fip_code": fip_code,
                "legal_name": legal_name,
                "entity_type": _section_type_at(document, match.start()),
                "regulatory_regime": "special",
                "regulatory_status": regulatory_status,
                "source": SPECIAL_REGIME_URLS[regulatory_status],
            }
        )

    return records


def fetch_special_regime_records(
    *,
    session: requests.Session | None = None,
    timeout: int = 45,
    verify_ssl: bool | None = None,
) -> list[dict[str, Any]]:
    """Fetch current SUSEP lists for all published special-regime categories."""
    verify = _env_bool("SUSEP_SPECIAL_VERIFY_SSL", True) if verify_ssl is None else verify_ssl
    client = session or requests.Session()
    output: list[dict[str, Any]] = []

    for status, url in SPECIAL_REGIME_URLS.items():
        response = client.get(url, headers=_DEFAULT_HEADERS, timeout=timeout, verify=verify)
        if response.status_code != 200:
            raise SpecialRegimeSourceError(
                f"SUSEP special-regime {status} returned HTTP {response.status_code}"
            )
        document = _decode(response.content)
        if "regime" not in document.lower() and status not in {
            "fiscal_direction",
            "intervention",
        }:
            raise SpecialRegimeSourceError(
                f"SUSEP special-regime {status} returned an unexpected document"
            )
        output.extend(parse_special_regime_html(document, status))

    by_fip: dict[str, dict[str, Any]] = {}
    for record in output:
        fip = record["fip_code"]
        previous = by_fip.get(fip)
        if previous and previous["regulatory_status"] != record["regulatory_status"]:
            raise SpecialRegimeSourceError(
                f"FIP {fip} appears in multiple current special-regime lists"
            )
        by_fip[fip] = record

    return [by_fip[key] for key in sorted(by_fip)]
