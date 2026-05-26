# tools/fetch_susep_sandbox.py
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from html import unescape

import requests


URLS = [
    ("1ª edição do Sandbox", "https://www.gov.br/susep/pt-br/assuntos/sandbox-regulatorio/sandbox-regulatorio-1a-edicao"),
    ("2ª edição do Sandbox", "https://www.gov.br/susep/pt-br/assuntos/sandbox-regulatorio/sandbox-regulatorio-2a-edicao"),
    ("3ª edição do Sandbox", "https://www.gov.br/susep/pt-br/assuntos/sandbox-regulatorio/sandbox-regulatorio-3a-edicao"),
]

OUT = Path("api/v1/susep-sandbox-participants.json")


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", unescape(s or "")).strip()


def clean_lines(html: str) -> list[str]:
    h = unescape(html or "")
    h = re.sub(r"<br\s*/?>", "\n", h, flags=re.I)
    h = re.sub(r"</(p|div|li|h\d|tr|td|th|ul|ol|table)>", "\n", h, flags=re.I)
    text = re.sub(r"<[^>]+>", "", h)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return [clean_text(ln) for ln in text.split("\n") if clean_text(ln)]


def make_item(name: str, edition: str) -> dict:
    return {
        "name": clean_text(name),
        "edition": edition,
        "status": "Projeto selecionado",
        "cnpj": "",
        "start": "",
        "end": "",
    }


def is_noise(line: str) -> bool:
    low = (line or "").strip().lower()

    if not low:
        return True

    noise_exact = {
        "projeto",
        "empresa",
        "linhas de negócio",
        "seguros a serem ofertados",
        "links de compartilhamento",
        "serviços",
        "manuais",
        "normas",
        "editais",
        "inscrições",
        "perguntas e respostas",
    }

    if low in noise_exact:
        return True

    if low.startswith(("http://", "https://")):
        return True

    if "arquivo" in low and "json" in low:
        return True

    if "manual" in low and "envio" in low:
        return True

    if "circular susep" in low or "resolução" in low or "edital" in low:
        return True

    return False


def parse_1a_edicao(lines: list[str], edition: str) -> list[dict]:
    """
    Página da 1ª edição costuma trazer lista numerada:
    1 / 88i / descrição
    2 / COOVER / descrição
    etc.
    """
    items = []

    start = None
    for i, line in enumerate(lines):
        if "selecionados foram" in line.lower():
            start = i + 1
            break

    if start is None:
        return items

    block = []
    for line in lines[start:]:
        low = line.lower()
        if "links de compartilhamento" in low:
            break
        block.append(line)

    i = 0
    while i < len(block):
        if block[i].isdigit() and i + 1 < len(block):
            name = block[i + 1]
            if not is_noise(name):
                items.append(make_item(name, edition))
            i += 2
        else:
            i += 1

    return items


def parse_2a_edicao(lines: list[str], edition: str) -> list[dict]:
    """
    Página da 2ª edição costuma trazer:
    Empresa / Linhas de negócio
    Clubfix / Garantia estendida
    Darwin / ...
    etc.
    """
    items = []

    start = None
    for i, line in enumerate(lines):
        low = line.lower()
        if "lista dos participantes selecionados" in low or "projetos selecionados" in low:
            start = i + 1
            break

    if start is None:
        return items

    block = []
    for line in lines[start:]:
        low = line.lower()

        if "links de compartilhamento" in low:
            break

        if is_noise(line):
            continue

        block.append(line)

    # Em geral vem em pares: empresa / linha de negócio.
    # Para o uso auxiliar de busca, basta capturar os nomes.
    i = 0
    while i < len(block):
        name = block[i]

        # pula descrições longas que não parecem nome de player
        if len(name) <= 80 and not re.search(r"[.;:]", name):
            items.append(make_item(name, edition))

        i += 2

    return items


def parse_generic(lines: list[str], edition: str) -> list[dict]:
    """
    Fallback conservador.
    Só tenta capturar linhas em tabelas/listas com cara de nome curto.
    Não inventa CNPJ, datas ou status regulatório.
    """
    items = []

    for line in lines:
        if is_noise(line):
            continue

        if len(line) > 80:
            continue

        if re.search(r"[.;:]", line):
            continue

        # Evita transformar títulos da página em participantes.
        low = line.lower()
        if "sandbox" in low or "susep" in low or "regulatório" in low:
            continue

        # Captura somente nomes plausíveis.
        if re.search(r"[a-zA-ZÀ-ÿ]", line):
            items.append(make_item(line, edition))

    return items


def parse_page(html: str, edition: str) -> list[dict]:
    lines = clean_lines(html)

    if edition.startswith("1ª"):
        return parse_1a_edicao(lines, edition)

    if edition.startswith("2ª"):
        return parse_2a_edicao(lines, edition)

    # 3ª edição pode ainda não ter lista pública de selecionados.
    return []


def dedup(items: list[dict]) -> list[dict]:
    seen = set()
    out = []

    for item in items:
        name = item.get("name", "").strip()
        edition = item.get("edition", "").strip()

        if not name:
            continue

        key = (edition.lower(), name.lower())
        if key in seen:
            continue

        seen.add(key)
        out.append(item)

    out.sort(key=lambda x: (x.get("edition", ""), x.get("name", "").lower()))
    return out


def main() -> int:
    all_items = []
    errors = []

    for edition, url in URLS:
        try:
            r = requests.get(
                url,
                timeout=20,
                headers={
                    "User-Agent": "SanidaBot/1.0 (+https://sanida.com.br/)",
                    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
                },
            )
            r.raise_for_status()

            items = parse_page(r.text, edition)
            all_items.extend(items)

            print(f"OK: {edition}: {len(items)} item(ns) extraído(s)")

        except requests.RequestException as e:
            errors.append({
                "edition": edition,
                "url": url,
                "error": str(e),
            })
            print(f"WARN: falha ao buscar {edition}: {e}")

    all_items = dedup(all_items)

    # Blindagem: se a SUSEP mudar de novo e nada for extraído,
    # não derruba o build se já houver arquivo anterior no repositório.
    if not all_items and OUT.exists():
        print(f"WARN: nenhum item extraído. Mantendo arquivo existente: {OUT}")
        return 0

    payload = {
        "source": " | ".join(url for _, url in URLS),
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
        "items": all_items,
    }

    if errors:
        payload["warnings"] = errors

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"OK: generated {OUT} ({len(all_items)} item(ns))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
