# tools/fetch_susep_sandbox.py
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from html import unescape

import requests

URL = "https://www.gov.br/susep/pt-br/assuntos/sandbox-regulatorio/copy_of_sociedades-seguradoras-participantes-do-sandbox"
OUT = Path("api/v1/susep-sandbox-participants.json")  # alinhado ao /api/v1/

RE_EDICAO = re.compile(r"^\s*\d+ª\s+edi[cç][aã]o\s+do\s+Sandbox\s*$", re.I)

def extract_date(s: str) -> str:
    m = re.search(r"\b\d{2}/\d{2}/\d{4}\b", s or "")
    return m.group(0) if m else (s or "").strip()

def status_short(s: str) -> str:
    x = (s or "").strip().lower()
    if "cancelad" in x:
        return "Autorização cancelada"
    if "autorizad" in x:
        return "Autorizada"
    return (s or "").strip()

def clean_lines(html: str) -> list[str]:
    h = unescape(html or "")
    # normaliza quebras
    h = re.sub(r"<br\s*/?>", "\n", h, flags=re.I)
    h = re.sub(r"</(p|div|li|h\d|tr|td|th|ul|ol)>", "\n", h, flags=re.I)
    # remove tags
    text = re.sub(r"<[^>]+>", "", h)
    text = (text.replace("\r\n", "\n").replace("\r", "\n"))
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    return lines

def looks_like_company(line: str, lines: list[str], i: int) -> bool:
    if not line:
        return False
    up = line.upper()
    # evita rótulos/headers
    if up.startswith(("STATUS", "CNPJ", "PORTARIA", "DATA DE", "MODALIDADES", "DIRETOR", "ENDEREÇO", "SITE", "TELEFONE", "E-MAIL")):
        return False
    if RE_EDICAO.match(line):
        return False
    # “cara” de empresa de seguros
    if not re.search(r"\bsegur", line, flags=re.I):
        return False
    # valida: deve existir STATUS logo adiante (mesma seção)
    for j in range(i + 1, min(len(lines), i + 15)):
        if lines[j].upper().startswith("STATUS"):
            return True
        if RE_EDICAO.match(lines[j]):
            break
    return False

def parse(html: str) -> list[dict]:
    lines = clean_lines(html)

    items = []
    edition = None
    current = None

    def flush():
        nonlocal current
        if current and current.get("name"):
            items.append(current)
        current = None

    i = 0
    while i < len(lines):
        line = lines[i]

        if RE_EDICAO.match(line):
            edition = line.strip()
            i += 1
            continue

        if looks_like_company(line, lines, i):
            flush()
            current = {
                "name": line.strip(),
                "edition": edition or "",
                "status": "",
                "cnpj": "",
                "start": "",
                "end": "",
            }
            i += 1
            continue

        if current:
            up = line.upper()

            if up.startswith("STATUS"):
                rest = line[6:].strip()
                if not rest and i + 1 < len(lines):
                    rest = lines[i + 1].strip()
                current["status"] = status_short(rest)

            elif up.startswith("CNPJ"):
                rest = line[4:].strip()
                if not rest and i + 1 < len(lines):
                    rest = lines[i + 1].strip()
                current["cnpj"] = rest

            elif up.startswith("DATA DE INICIO DA AUTORIZAÇÃO TEMPORÁRIA"):
                rest = line[len("DATA DE INICIO DA AUTORIZAÇÃO TEMPORÁRIA"):].strip()
                if not rest and i + 1 < len(lines):
                    rest = lines[i + 1].strip()
                current["start"] = extract_date(rest)

            elif up.startswith("DATA FINAL DA AUTORIZAÇÃO TEMPORÁRIA"):
                rest = line[len("DATA FINAL DA AUTORIZAÇÃO TEMPORÁRIA"):].strip()
                if not rest and i + 1 < len(lines):
                    rest = lines[i + 1].strip()
                current["end"] = extract_date(rest)

        i += 1

    flush()

    # dedup simples por nome+cnpj (se houver)
    seen = set()
    dedup = []
    for it in items:
        k = (it["name"].upper(), (it.get("cnpj") or "").strip())
        if k in seen:
            continue
        seen.add(k)
        dedup.append(it)

    dedup.sort(key=lambda x: x["name"].lower())
    return dedup

def main():
    r = requests.get(
        URL,
        timeout=20,
        headers={
            "User-Agent": "SanidaBot/1.0 (+https://sanida.com.br/)",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
        },
    )
    r.raise_for_status()

    payload = {
        "source": URL,
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
        "items": parse(r.text),
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
