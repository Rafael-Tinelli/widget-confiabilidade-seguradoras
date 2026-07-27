"""Collect the public participant/project lists from the SUSEP Sandbox pages.

The SUSEP pages do not publish CNPJ values in the lists used by this collector.
The output therefore keeps ``cnpj`` empty instead of inferring identifiers from
third-party sources.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URLS = [
    (
        "1ª edição do Sandbox",
        "https://www.gov.br/susep/pt-br/assuntos/sandbox-regulatorio/"
        "sandbox-regulatorio-1a-edicao",
    ),
    (
        "2ª edição do Sandbox",
        "https://www.gov.br/susep/pt-br/assuntos/sandbox-regulatorio/"
        "sandbox-regulatorio-2a-edicao",
    ),
    (
        "3ª edição do Sandbox",
        "https://www.gov.br/susep/pt-br/assuntos/sandbox-regulatorio/"
        "sandbox-regulatorio-3a-edicao",
    ),
]

OUT = Path("api/v1/susep-sandbox-participants.json")
MIN_TOTAL_ITEMS = int(os.getenv("SUSEP_SANDBOX_MIN_ITEMS", "5"))
MAX_DROP_PCT = float(os.getenv("SUSEP_SANDBOX_MAX_DROP_PCT", "0.35"))

_REQUIRED_EDITIONS = {"1ª edição do Sandbox", "2ª edição do Sandbox"}
_NAME_HEADER_MARKERS = (
    "empresa",
    "participante",
    "proponente",
    "sociedade",
    "insurtech",
    "projeto",
)

_BUSINESS_LINE_LABELS = {
    "bicicletas",
    "caminhoes",
    "celular",
    "celular bicicletas",
    "esportes fianca locaticia residencial",
    "fianca locaticia",
    "microsseguros de danos para pequeno empreendedor",
    "passagens aereas e hoteis",
    "pets",
}


class _TableParser(HTMLParser):
    """Extract text from HTML table rows without adding a parser dependency."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell_parts: list[str] | None = None
        self._cell_depth = 0

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        del attrs
        if tag == "table":
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell_parts = []
            self._cell_depth = 1
        elif self._cell_parts is not None:
            self._cell_depth += 1
            if tag == "br":
                self._cell_parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._cell_parts is not None:
            if self._row is not None:
                self._row.append(clean_text("".join(self._cell_parts)))
            self._cell_parts = None
            self._cell_depth = 0
        elif tag == "tr" and self._row is not None:
            if self._table is not None and any(self._row):
                self._table.append(self._row)
            self._row = None
        elif tag == "table" and self._table is not None:
            if self._table:
                self.tables.append(self._table)
            self._table = None
        elif self._cell_parts is not None and self._cell_depth > 1:
            self._cell_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(value or "")).strip()


def _normalized_label(value: str) -> str:
    text = clean_text(value).casefold()
    text = (
        text.replace("á", "a")
        .replace("à", "a")
        .replace("â", "a")
        .replace("ã", "a")
        .replace("é", "e")
        .replace("ê", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ô", "o")
        .replace("õ", "o")
        .replace("ú", "u")
        .replace("ç", "c")
    )
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def clean_lines(html: str) -> list[str]:
    text = unescape(html or "")
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(
        r"</(p|div|li|h\d|tr|td|th|ul|ol|table)>",
        "\n",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return [clean_text(line) for line in text.split("\n") if clean_text(line)]


def make_item(name: str, edition: str) -> dict[str, str]:
    return {
        "name": clean_text(name),
        "edition": edition,
        "status": "Projeto selecionado",
        # A lista pública da SUSEP usada aqui não fornece CNPJ ou datas.
        "cnpj": "",
        "start": "",
        "end": "",
    }


def is_noise(line: str) -> bool:
    normalized = _normalized_label(line)
    if not normalized:
        return True

    noise_exact = {
        "projeto",
        "empresa",
        "participante",
        "participantes",
        "linhas de negocio",
        "seguros a serem ofertados",
        "links de compartilhamento",
        "servicos",
        "manuais",
        "normas",
        "editais",
        "inscricoes",
        "perguntas e respostas",
    }
    if normalized in noise_exact:
        return True
    if normalized.startswith(("http ", "https ")):
        return True
    if "arquivo" in normalized and "json" in normalized:
        return True
    if "manual" in normalized and "envio" in normalized:
        return True
    return any(
        marker in normalized
        for marker in ("circular susep", "resolucao", "edital")
    )


def is_business_line_label(name: str) -> bool:
    return _normalized_label(name) in _BUSINESS_LINE_LABELS


def _looks_like_name(name: str) -> bool:
    value = clean_text(name)
    if not value or len(value) > 160 or is_noise(value):
        return False
    if is_business_line_label(value):
        return False
    return bool(re.search(r"[A-Za-zÀ-ÿ]", value))


def _parse_named_tables(html: str, edition: str) -> list[dict[str, str]]:
    parser = _TableParser()
    parser.feed(html)
    items: list[dict[str, str]] = []

    for table in parser.tables:
        header_index = None
        name_column = None
        for row_index, row in enumerate(table[:5]):
            normalized_cells = [_normalized_label(cell) for cell in row]
            for marker in _NAME_HEADER_MARKERS:
                matching_column = next(
                    (
                        column_index
                        for column_index, cell in enumerate(normalized_cells)
                        if marker in cell
                    ),
                    None,
                )
                if matching_column is not None:
                    header_index = row_index
                    name_column = matching_column
                    break
            if name_column is not None:
                break

        if header_index is None or name_column is None:
            continue

        for row in table[header_index + 1 :]:
            if name_column >= len(row):
                continue
            name = clean_text(row[name_column])
            if _looks_like_name(name):
                items.append(make_item(name, edition))

    return items


def _parse_first_edition_fallback(
    lines: list[str],
    edition: str,
) -> list[dict[str, str]]:
    start = next(
        (
            index + 1
            for index, line in enumerate(lines)
            if "selecionados foram" in line.casefold()
        ),
        None,
    )
    if start is None:
        return []

    block: list[str] = []
    for line in lines[start:]:
        if "links de compartilhamento" in line.casefold():
            break
        block.append(line)

    items: list[dict[str, str]] = []
    index = 0
    while index < len(block):
        if block[index].isdigit() and index + 1 < len(block):
            name = block[index + 1]
            if _looks_like_name(name):
                items.append(make_item(name, edition))
            index += 2
        else:
            index += 1
    return items


def parse_page(html: str, edition: str) -> list[dict[str, str]]:
    table_items = _parse_named_tables(html, edition)
    if table_items:
        return dedup(table_items)

    if edition.startswith("1ª"):
        return dedup(_parse_first_edition_fallback(clean_lines(html), edition))

    # Para a 2ª edição não usamos mais o antigo pareamento de linhas.
    # Sem uma tabela que identifique explicitamente a coluna de empresa,
    # a extração é considerada inconclusiva e o arquivo anterior é preservado.
    return []


def dedup(items: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    output: list[dict[str, str]] = []
    for item in items:
        name = clean_text(item.get("name", ""))
        edition = clean_text(item.get("edition", ""))
        key = (edition.casefold(), name.casefold())
        if not name or key in seen:
            continue
        seen.add(key)
        output.append({**item, "name": name, "edition": edition})
    output.sort(key=lambda item: (item["edition"], item["name"].casefold()))
    return output


def _previous_count(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    items = payload.get("items") if isinstance(payload, dict) else None
    return len(items) if isinstance(items, list) else None


def validate_items(
    items: list[dict[str, str]],
    *,
    previous_count: int | None = None,
) -> list[str]:
    errors: list[str] = []
    if len(items) < MIN_TOTAL_ITEMS:
        errors.append(
            f"quantidade insuficiente: {len(items)} < mínimo {MIN_TOTAL_ITEMS}"
        )

    editions = {item.get("edition", "") for item in items}
    missing_editions = sorted(_REQUIRED_EDITIONS - editions)
    if missing_editions:
        errors.append(f"edições obrigatórias sem itens: {missing_editions}")

    invalid_names = [
        item.get("name", "")
        for item in items
        if not _looks_like_name(item.get("name", ""))
    ]
    if invalid_names:
        errors.append(f"nomes inválidos ou linhas de negócio: {invalid_names[:10]}")

    keys = [
        (
            clean_text(item.get("edition", "")).casefold(),
            clean_text(item.get("name", "")).casefold(),
        )
        for item in items
    ]
    if len(keys) != len(set(keys)):
        errors.append("há participantes duplicados na mesma edição")

    if previous_count and previous_count > 0:
        min_allowed = int(previous_count * (1.0 - MAX_DROP_PCT))
        if len(items) < min_allowed:
            errors.append(
                "queda excessiva na quantidade de itens: "
                f"atual={len(items)}, anterior={previous_count}, mínimo={min_allowed}"
            )
    return errors


def _build_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update(
        {
            "User-Agent": "SanidaBot/1.0 (+https://sanida.com.br/)",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
        }
    )
    return session


def _fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=(10, 30))
    response.raise_for_status()
    return response.text


def _write_atomic(payload: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(path)


def main() -> int:
    all_items: list[dict[str, str]] = []
    fetch_errors: list[str] = []
    session = _build_session()

    for edition, url in URLS:
        try:
            html = _fetch_html(session, url)
        except requests.RequestException as exc:
            fetch_errors.append(f"{edition}: {exc}")
            print(f"ERROR: falha ao buscar {edition}: {exc}")
            continue

        items = parse_page(html, edition)
        all_items.extend(items)
        print(f"OK: {edition}: {len(items)} item(ns) extraído(s)")

    all_items = dedup(all_items)
    errors = fetch_errors + validate_items(
        all_items,
        previous_count=_previous_count(OUT),
    )
    if errors:
        print("ERROR: coleta rejeitada; arquivo existente foi preservado.")
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    payload: dict[str, object] = {
        "source": " | ".join(url for _, url in URLS),
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
        "items": all_items,
    }
    _write_atomic(payload, OUT)
    print(f"OK: generated {OUT} ({len(all_items)} item(ns))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
