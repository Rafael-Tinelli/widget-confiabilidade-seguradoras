from __future__ import annotations

import csv
import gzip
import io
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import requests

CKAN_API = "https://dados.mj.gov.br/api/3/action/package_show"
PACKAGE_ID = "reclamacoes-do-consumidor-gov-br"

# Timeouts (connect, read)
_TIMEOUT = (10, 60)

_MONTH_RE = re.compile(r"(20\d{2})[-_](0[1-9]|1[0-2])")


def _norm_text(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()


def _norm_header(h: str) -> str:
    s = _norm_text(h)
    return re.sub(r"[^a-z0-9]+", "", s)


# Aliases para colunas (variações históricas e/ou alternativas)
_HEADER_ALIASES: Dict[str, str] = {
    # Nome / Identificação
    "nomefantasia": "nome_fantasia",
    "nomefantasiaconsumidor": "nome_fantasia",
    "nomefantasiaprestador": "nome_fantasia",
    "nomefantasiafornecedor": "nome_fantasia",
    "fornecedor": "nome_fantasia",
    "empresa": "nome_fantasia",
    # Situação
    "situacao": "situacao",
    "situacaoreclamacao": "situacao",
    "status": "situacao",
    "statusreclamacao": "situacao",
    "statusdareclamacao": "situacao",
    # Respondida
    "respondida": "respondida",
    "reclamacaorespondida": "respondida",
    "respondida?": "respondida",
    "respondeu": "respondida",
    # Avaliação / Resolução
    "avaliacaoreclamacao": "avaliacao",
    "avaliacaodareclamacao": "avaliacao",
    "avaliacao": "avaliacao",
    "resolvida": "avaliacao",
    "indicadoresolucao": "avaliacao",
    # Nota
    "notadoconsumidor": "nota",
    "nota": "nota",
    "notadocliente": "nota",
    "avaliacaodoconsumidor": "nota",
    # Tempo de resposta
    "temporesposta": "tempo_resposta",
    "tempoderesposta": "tempo_resposta",
    "tempomedioresposta": "tempo_resposta",
    "tempomedioderesposta": "tempo_resposta",
}

_FINAL_PREFIX = "finalizada"
_TRUE_SET = {"s", "sim", "1", "true", "t", "y", "yes"}


def _utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _extract_ym(name_or_url: str) -> Optional[str]:
    m = _MONTH_RE.search(name_or_url or "")
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; QSeguradorasBot/1.0; +https://github.com/)",
            "Accept": "*/*",
        }
    )
    return s


def discover_basecompleta_urls(months: int = 12) -> Dict[str, str]:
    """
    Descobre URLs de recursos "Base Completa" do dataset do Consumidor.gov via CKAN.

    Returns: dict { 'YYYY-MM': url }
    """
    resp = _session().get(CKAN_API, params={"id": PACKAGE_ID}, timeout=_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError("CKAN API returned success=False")

    resources = data.get("result", {}).get("resources") or []
    out: Dict[str, str] = {}

    for r in resources:
        fmt = str(r.get("format") or "").lower()
        if fmt != "csv":
            continue

        name = str(r.get("name") or "")
        url = str(r.get("url") or "")
        hint = f"{name} {url}".lower()

        if ("basecompleta" not in hint) and ("base completa" not in hint):
            continue

        ym = _extract_ym(hint)
        if not ym:
            created = str(r.get("created") or "")
            ym = _extract_ym(created)
        if not ym:
            continue

        out[ym] = url

    if not out:
        return {}

    yms_sorted = sorted(out.keys())
    return {ym: out[ym] for ym in yms_sorted[-months:]}


def download_csv_to_gz(url: str, out_gz_path: str, *, retries: int = 5) -> Dict[str, Any]:
    """
    Baixa um CSV (HTTP) e salva como .csv.gz, streaming, com retries.
    """
    out_path = Path(out_gz_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    sess = _session()
    last_err: Optional[Exception] = None
    started = time.time()

    for attempt in range(1, retries + 1):
        try:
            with sess.get(url, stream=True, timeout=_TIMEOUT) as r:
                r.raise_for_status()
                etag = r.headers.get("ETag")
                lm = r.headers.get("Last-Modified")

                nbytes = 0
                with gzip.open(out_path, "wb") as gz:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if not chunk:
                            continue
                        gz.write(chunk)
                        nbytes += len(chunk)

            secs = round(time.time() - started, 3)
            return {
                "url": url,
                "path": str(out_path),
                "bytes": nbytes,
                "seconds": secs,
                "etag": etag,
                "last_modified": lm,
                "downloaded_at": _utc_now(),
            }

        except Exception as e:
            last_err = e
            if attempt >= retries:
                break
            time.sleep(min(30.0, 2.0**attempt))

    raise RuntimeError(f"download failed after {retries} attempts: {last_err}")


def _detect_encoding_gz(path: str) -> str:
    p = Path(path)
    with gzip.open(p, "rb") as f:
        head = f.read(16_384)

    if head.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    try:
        head.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def _detect_delimiter(header_line: str) -> str:
    sc = header_line.count(";")
    cc = header_line.count(",")
    return ";" if sc >= cc else ","


def _mk_key(name: str) -> str:
    s = _norm_text(name)
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    return _norm_text(str(value)) in _TRUE_SET


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

    def to_public(self) -> Dict[str, Any]:
        responded_rate = (self.respondidas / self.finalizadas) if self.finalizadas else None
        resolution_rate = (self.resolvidas_indicador / self.finalizadas) if self.finalizadas else None
        satisfaction_avg = (self.nota_sum / self.nota_count) if self.nota_count else None
        avg_response_days = (self.tempo_sum / self.tempo_count) if self.tempo_count else None

        return {
            "display_name": self.display_name or None,
            "complaints_total": self.total,
            "complaints_finalizadas": self.finalizadas,
            "responded_rate": round(responded_rate, 6) if responded_rate is not None else None,
            "resolution_rate": round(resolution_rate, 6) if resolution_rate is not None else None,
            "satisfaction_avg": round(satisfaction_avg, 6) if satisfaction_avg is not None else None,
            "avg_response_days": round(avg_response_days, 6) if avg_response_days is not None else None,
        }


def iter_rows_from_gz_csv(path: str) -> Iterator[Dict[str, str]]:
    """
    Stream de linhas (dict) a partir de um .csv.gz.

    - Detecta encoding e delimitador.
    - Mapeia headers para nomes canônicos via aliases.
    - Faz pad/truncate defensivo quando a linha vier com colunas fora do esperado.
    """
    encoding = _detect_encoding_gz(path)
    p = Path(path)

    with gzip.open(p, "rt", encoding=encoding, errors="replace", newline="") as f:
        header_line = f.readline()
        if not header_line:
            return

        delim = _detect_delimiter(header_line)
        header_reader = csv.reader(io.StringIO(header_line), delimiter=delim)
        headers_raw = next(header_reader, [])
        if not headers_raw:
            return

        headers = []
        for h in headers_raw:
            nh = _norm_header(h)
            headers.append(_HEADER_ALIASES.get(nh, nh))

        expected = len(headers)
        row_reader = csv.reader(f, delimiter=delim)

        for row in row_reader:
            if not row:
                continue

            # pad/truncate leve (evita descartar por pequenas inconsistências)
            if len(row) < expected:
                row = row + ([""] * (expected - len(row)))
            elif len(row) > expected:
                row = row[:expected]

            out: Dict[str, str] = {}
            for i, key in enumerate(headers):
                out[key] = row[i] if i < len(row) else ""
            yield out


def aggregate_month(path_gz: str) -> Dict[str, Agg]:
    """
    Agrega um mês por nome_fantasia normalizado.

    Retorna: dict { name_key: Agg }
    """
    out: Dict[str, Agg] = {}

    for row in iter_rows_from_gz_csv(path_gz):
        name_raw = (row.get("nome_fantasia") or "").strip()
        if not name_raw:
            continue

        key = _mk_key(name_raw)
        if not key:
            continue

        a = out.get(key)
        if a is None:
            a = Agg(display_name=name_raw)
            out[key] = a

        a.total += 1

        situacao = _norm_text(row.get("situacao") or "")
        is_finalizada = situacao.startswith(_FINAL_PREFIX)
        if is_finalizada:
            a.finalizadas += 1

        if is_finalizada and _truthy(row.get("respondida")):
            a.respondidas += 1

        # Avaliação da Reclamação: "Resolvida" OU "Não avaliada" conta como resolvida no indicador
        if is_finalizada:
            aval = _norm_text(row.get("avaliacao") or "")
            if aval.startswith("resolvida") or aval.startswith("nao avaliada") or aval.startswith("não avaliada"):
                a.resolvidas_indicador += 1

        if is_finalizada:
            n = _parse_float(row.get("nota"))
            if n is not None:
                a.nota_sum += float(n)
                a.nota_count += 1

        if is_finalizada:
            tr = _parse_float(row.get("tempo_resposta"))
            if tr is not None:
                a.tempo_sum += int(round(tr))
                a.tempo_count += 1

    return out
