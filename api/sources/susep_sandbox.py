from __future__ import annotations

import html
import os
import re
from typing import Any

import requests

from api.utils.identifiers import normalize_cnpj_v2

SANDBOX_PARTICIPANTS_URL = (
    "https://www.gov.br/susep/pt-br/assuntos/sandbox-regulatorio/"
    "seguradoras-participantes-do-sandbox-1"
)
_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SanidaResearch/1.0; +https://sanida.com.br)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.5",
}
_BLOCK_END_RE = re.compile(
    r"</(?:p|div|li|h[1-6]|tr|td|th|section|article|dd|dt)>",
    flags=re.IGNORECASE,
)
_FIELD_LABELS = (
    "STATUS",
    "CNPJ",
    "PORTARIA",
    "DATA DE INICIO DA AUTORIZAÇÃO TEMPORÁRIA",
    "DATA FINAL DA AUTORIZAÇÃO TEMPORÁRIA",
    "MODALIDADES",
    "DIRETOR DO SANDBOX",
    "ENDEREÇO",
    "SITE",
    "TELEFONE",
    "E-MAIL",
)


class SandboxSourceError(RuntimeError):
    """Raised when the official Sandbox participant source cannot be trusted."""


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _normalize_label(value: str) -> str:
    text = html.unescape(value or "").casefold()
    replacements = {
        "á": "a",
        "à": "a",
        "â": "a",
        "ã": "a",
        "é": "e",
        "ê": "e",
        "í": "i",
        "ó": "o",
        "ô": "o",
        "õ": "o",
        "ú": "u",
        "ç": "c",
        "ª": "a",
        "º": "o",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _decode_content(content: bytes) -> str:
    for encoding in ("utf-8", "cp1252", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _html_lines(document: str) -> list[str]:
    text = re.sub(r"<script\b.*?</script>", "", document, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style\b.*?</style>", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = _BLOCK_END_RE.sub("\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    output: list[str] = []
    for raw in text.replace("\r", "\n").split("\n"):
        clean = re.sub(r"\s+", " ", raw).strip()
        if clean:
            output.append(clean)
    return output


def _is_label(line: str, label: str) -> bool:
    normalized = _normalize_label(line)
    normalized_label = _normalize_label(label)
    return normalized == normalized_label or normalized.startswith(f"{normalized_label} ")


def _is_any_field_label(line: str) -> bool:
    return any(_is_label(line, label) for label in _FIELD_LABELS)


def _inline_field_value(line: str, label: str) -> str | None:
    if not _is_label(line, label):
        return None
    normalized = _normalize_label(line)
    normalized_label = _normalize_label(label)
    if normalized == normalized_label:
        return ""
    if "|" in line:
        return line.split("|", 1)[1].strip()
    if ":" in line:
        return line.split(":", 1)[1].strip()

    # Same-line fallback for renderers that omit the visual separator. The
    # normalized value is sufficient for status interpretation, but identifiers
    # and dates are normally rendered with a separator or a separate value cell.
    return normalized[len(normalized_label) :].strip()


def _field_value_at(lines: list[str], index: int, label: str) -> str | None:
    inline = _inline_field_value(lines[index], label)
    if inline is None:
        return None
    if inline:
        return inline

    # Gov.br currently renders some tables as label and value in separate cells.
    # Skip standalone visual separators and return the next meaningful value.
    for candidate in lines[index + 1 : index + 4]:
        clean = candidate.strip().lstrip("|:–—-").strip()
        if not clean:
            continue
        if _is_any_field_label(clean):
            return None
        return clean
    return None


def _status_code(raw_status: str) -> str:
    normalized = _normalize_label(raw_status)
    if "autorizacao temporaria cancelada" in normalized:
        return "sandbox_authorization_cancelled"
    if "autorizada" in normalized:
        return "temporary_authorized"
    return "unknown"


def _extract_date(value: str | None) -> str | None:
    if not value:
        return None
    match = re.search(r"\b(\d{2})/(\d{2})/(\d{4})\b", value)
    if not match:
        return None
    day, month, year = match.groups()
    return f"{year}-{month}-{day}"


def _edition_marker(line: str) -> str | None:
    match = re.fullmatch(r"(\d+)a edicao do sandbox", _normalize_label(line))
    if not match:
        return None
    return f"{match.group(1)}ª edição do Sandbox"


def _participant_name_before_status(lines: list[str], status_index: int) -> str | None:
    for candidate in reversed(lines[max(0, status_index - 4) : status_index]):
        clean = candidate.strip().strip("|:–—-").strip()
        if not clean or _is_any_field_label(clean) or _edition_marker(clean):
            continue
        # Values from a preceding participant should not occur here because
        # STATUS is the first field of every official participant block.
        return clean
    return None


def parse_sandbox_participants_html(document: str) -> list[dict[str, Any]]:
    """Parse the current consolidated SUSEP Sandbox participant page."""
    lines = _html_lines(document)
    current_edition: str | None = None
    status_positions: list[tuple[int, str | None]] = []

    for index, line in enumerate(lines):
        edition = _edition_marker(line)
        if edition:
            current_edition = edition
        if _is_label(line, "STATUS"):
            status_positions.append((index, current_edition))

    records: list[dict[str, Any]] = []
    for position_index, (status_index, edition) in enumerate(status_positions):
        name = _participant_name_before_status(lines, status_index)
        if not name:
            continue
        end_index = (
            status_positions[position_index + 1][0]
            if position_index + 1 < len(status_positions)
            else len(lines)
        )
        block = lines[status_index:end_index]
        raw_status = _field_value_at(block, 0, "STATUS") or ""
        fields: dict[str, str] = {}
        for index, line in enumerate(block[1:], start=1):
            for label, key in (
                ("CNPJ", "cnpj"),
                ("DATA DE INICIO DA AUTORIZAÇÃO TEMPORÁRIA", "start_raw"),
                ("DATA FINAL DA AUTORIZAÇÃO TEMPORÁRIA", "end_raw"),
                ("MODALIDADES", "modalities"),
                ("PORTARIA", "authorization_act"),
            ):
                value = _field_value_at(block, index, label)
                if value is not None and key not in fields:
                    fields[key] = value

        cnpj = normalize_cnpj_v2(fields.get("cnpj"))
        if not cnpj:
            raise SandboxSourceError(f"Sandbox participant without valid CNPJ: {name}")
        records.append(
            {
                "legal_name": name,
                "cnpj": cnpj,
                "edition": edition,
                "regulatory_status": _status_code(raw_status),
                "raw_status": raw_status,
                "authorization_start": _extract_date(fields.get("start_raw")),
                "authorization_end": _extract_date(fields.get("end_raw")),
                "authorization_end_raw": fields.get("end_raw"),
                "modalities": fields.get("modalities"),
                "source": SANDBOX_PARTICIPANTS_URL,
            }
        )

    by_cnpj: dict[str, dict[str, Any]] = {}
    for record in records:
        cnpj = record["cnpj"]
        previous = by_cnpj.get(cnpj)
        if previous and previous != record:
            raise SandboxSourceError(f"Sandbox source duplicated CNPJ {cnpj}")
        by_cnpj[cnpj] = record
    return [by_cnpj[key] for key in sorted(by_cnpj)]


def fetch_sandbox_participants(
    *,
    session: requests.Session | None = None,
    timeout: int = 45,
    verify_ssl: bool | None = None,
) -> list[dict[str, Any]]:
    """Fetch and validate the current consolidated Sandbox participant page."""
    verify = _env_bool("SUSEP_SANDBOX_VERIFY_SSL", True) if verify_ssl is None else verify_ssl
    client = session or requests.Session()
    response = client.get(
        SANDBOX_PARTICIPANTS_URL,
        headers=_DEFAULT_HEADERS,
        timeout=timeout,
        verify=verify,
    )
    if response.status_code != 200:
        raise SandboxSourceError(
            f"SUSEP Sandbox participants returned HTTP {response.status_code}"
        )
    records = parse_sandbox_participants_html(_decode_content(response.content))
    if len(records) < 5:
        raise SandboxSourceError(
            f"SUSEP Sandbox participant page returned too few records: {len(records)}"
        )
    if not any(item["regulatory_status"] == "temporary_authorized" for item in records):
        raise SandboxSourceError("SUSEP Sandbox page has no active temporary authorization")
    unknown = [item["legal_name"] for item in records if item["regulatory_status"] == "unknown"]
    if unknown:
        raise SandboxSourceError(f"Unknown Sandbox status: {unknown[:5]}")
    return records
