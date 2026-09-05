from __future__ import annotations

import html
import os
import re
from collections.abc import Iterable
from typing import Any

import requests

from api.utils.identifiers import normalize_cnpj_v2

LICENSED_ENTITIES_URL = "https://www2.susep.gov.br/menuatendimento/procura_2011.asp"

LICENSED_ENTITY_TYPES = {
    "1": "open_pension_entity",
    "2": "insurer",
    "3": "local_reinsurer",
    "4": "admitted_reinsurer",
    "5": "self_regulator",
    "6": "capitalization_company",
    "7": "occasional_reinsurer",
    "8": "reinsurance_broker",
}

_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SanidaResearch/1.0; +https://sanida.com.br)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.5",
}


class LicensedEntitiesSourceError(RuntimeError):
    """Raised when the official licensed-entities source cannot be trusted."""


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _normalize_fip(raw: Any) -> str:
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _text(fragment: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", fragment)
    return html.unescape(re.sub(r"\s+", " ", no_tags)).strip()


def decode_susep_html(content: bytes) -> str:
    """Decode the legacy page without allowing mojibake into identity fields."""
    for encoding in ("utf-8", "cp1252", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def parse_licensed_entity_type_options(document: str) -> dict[str, str]:
    """Read the type taxonomy exposed by SUSEP's own licensed-entity form."""
    select_match = re.search(
        r"<select\b[^>]*name=[\"']tiposempresas[\"'][^>]*>(.*?)</select>",
        document,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not select_match:
        return {}

    options: dict[str, str] = {}
    for value, label in re.findall(
        r"<option\b[^>]*value=[\"']([^\"']*)[\"'][^>]*>(.*?)</option>",
        select_match.group(1),
        flags=re.IGNORECASE | re.DOTALL,
    ):
        code = str(value or "").strip()
        if not code.isdigit():
            continue
        options[code] = _text(label)
    return options


def _assert_known_official_taxonomy(document: str) -> None:
    discovered = parse_licensed_entity_type_options(document)
    if not discovered:
        raise LicensedEntitiesSourceError(
            "SUSEP licensed-entities page did not expose a parseable type taxonomy"
        )
    unknown = sorted(set(discovered) - set(LICENSED_ENTITY_TYPES))
    if unknown:
        details = {code: discovered[code] for code in unknown}
        raise LicensedEntitiesSourceError(
            "SUSEP licensed-entity taxonomy changed; explicit classification is required "
            f"before publication: {details}"
        )


def parse_licensed_entities_html(document: str, type_code: str) -> list[dict[str, Any]]:
    """Parse one official SUSEP licensed-entity result page."""
    entity_type = LICENSED_ENTITY_TYPES.get(str(type_code))
    if not entity_type:
        raise ValueError(f"Unsupported SUSEP licensed entity type: {type_code}")

    records: list[dict[str, Any]] = []
    for table in re.findall(
        r"<table\b.*?</table>",
        document,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        row_texts: list[str] = []
        for row in re.findall(
            r"<tr\b.*?</tr>",
            table,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            cells = re.findall(
                r"<t[dh]\b.*?</t[dh]>",
                row,
                flags=re.IGNORECASE | re.DOTALL,
            )
            clean = " ".join(part for part in (_text(cell) for cell in cells) if part)
            if clean:
                row_texts.append(clean)

        if not row_texts:
            continue

        fip_match = next(
            (
                re.search(r"FIP\s*:\s*([0-9.]+)", row, flags=re.IGNORECASE)
                for row in row_texts
                if "FIP" in row.upper()
            ),
            None,
        )
        if not fip_match:
            continue

        fip_code = _normalize_fip(fip_match.group(1))
        if not fip_code:
            continue

        cnpj_raw = ""
        for row in row_texts:
            match = re.search(r"CNPJ\s*:\s*(.+)$", row, flags=re.IGNORECASE)
            if match:
                cnpj_raw = match.group(1).strip()
                break
        cnpj = normalize_cnpj_v2(cnpj_raw)

        legal_name = row_texts[0].strip()
        if not legal_name:
            continue

        records.append(
            {
                "fip_code": fip_code,
                "cnpj": cnpj,
                "legal_name": legal_name,
                "entity_type": entity_type,
                "source_type_code": str(type_code),
                "source": LICENSED_ENTITIES_URL,
            }
        )

    return records


def fetch_licensed_entities(
    type_codes: Iterable[str] | None = None,
    *,
    session: requests.Session | None = None,
    timeout: int = 60,
    verify_ssl: bool | None = None,
) -> list[dict[str, Any]]:
    """Fetch the current licensed universe from the official SUSEP service.

    When consuming the complete universe, validate the form taxonomy first. A
    newly introduced official category must fail closed until its semantics are
    explicitly modeled; it must never be ignored or silently mapped to insurer.
    """
    codes = [str(code) for code in (type_codes or LICENSED_ENTITY_TYPES.keys())]
    unknown = [code for code in codes if code not in LICENSED_ENTITY_TYPES]
    if unknown:
        raise ValueError(f"Unsupported SUSEP licensed entity types: {unknown}")

    verify = _env_bool("SUSEP_LICENSED_VERIFY_SSL", True) if verify_ssl is None else verify_ssl
    client = session or requests.Session()
    output: list[dict[str, Any]] = []

    if type_codes is None:
        taxonomy_response = client.get(
            LICENSED_ENTITIES_URL,
            headers=_DEFAULT_HEADERS,
            timeout=timeout,
            verify=verify,
        )
        if taxonomy_response.status_code != 200:
            raise LicensedEntitiesSourceError(
                "SUSEP licensed-entities taxonomy page returned HTTP "
                f"{taxonomy_response.status_code}"
            )
        _assert_known_official_taxonomy(decode_susep_html(taxonomy_response.content))

    for code in codes:
        response = client.post(
            LICENSED_ENTITIES_URL,
            headers=_DEFAULT_HEADERS,
            data={
                "Criteria": "",
                "Busca": "OK",
                "estado": "",
                "tiposempresas": code,
                "Procurar": "",
                "Limpar": "",
            },
            timeout=timeout,
            verify=verify,
        )
        if response.status_code != 200:
            raise LicensedEntitiesSourceError(
                f"SUSEP licensed-entities type {code} returned HTTP {response.status_code}"
            )

        records = parse_licensed_entities_html(decode_susep_html(response.content), code)
        if not records:
            raise LicensedEntitiesSourceError(
                f"SUSEP licensed-entities type {code} returned no parseable records"
            )
        output.extend(records)

    by_fip: dict[str, dict[str, Any]] = {}
    for record in output:
        fip = record["fip_code"]
        previous = by_fip.get(fip)
        if previous and previous["entity_type"] != record["entity_type"]:
            raise LicensedEntitiesSourceError(
                f"Official licensed source returned FIP {fip} in multiple entity types"
            )
        by_fip[fip] = record

    return [by_fip[key] for key in sorted(by_fip)]
