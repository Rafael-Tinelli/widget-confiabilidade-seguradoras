# api/sources/ses.py
from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import unicodedata
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
from requests.exceptions import SSLError


# ----------------------------
# URLs "evergreen" (sem depender de discovery HTML)
# ----------------------------

# Página informativa (não é fonte estável de download)
SES_HOME_DEFAULT = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"

# Link direto para o BaseCompleta.zip (forma estável; evita depender de HTML)
SES_ZIP_URL_DEFAULT = "https://www2.susep.gov.br/redarq.asp?arq=BaseCompleta%2ezip"

# Cadastro mestre com CNPJ (fonte crítica para o projeto)
SES_LISTAEMPRESAS_URL_DEFAULT = "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv"


# ----------------------------
# Dataclasses
# ----------------------------

@dataclass
class SesFetchResult:
    url: str
    fetched_at: str
    bytes_len: int
    sha256: str
    saved_to: str


@dataclass
class SesExtractMeta:
    # Campos esperados por api/build_insurers.py
    zip_url: str
    cias_file: str
    seguros_file: str
    period_from: str  # "YYYY-MM"
    period_to: str    # "YYYY-MM"

    # Campos adicionais (diagnóstico)
    fetched_at: str
    bytes_len: int
    sha256: str
    saved_to: str
    notes: dict[str, Any]


# ----------------------------
# Helpers (time/hash/http)
# ----------------------------

def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _allow_insecure_ssl() -> bool:
    return str(os.environ.get("SES_ALLOW_INSECURE_SSL", "")).strip().lower() in {"1", "true", "yes"}


def _ua_headers() -> dict[str, str]:
    ua = os.environ.get("SES_UA", "widget-confiabilidade-seguradoras/1.0")
    return {"User-Agent": ua}


def _clean_url(u: str) -> str:
    u = u.strip()
    return u.replace(" ", "%20")


def _is_zip_signature(b: bytes) -> bool:
    return len(b) >= 2 and b[:2] == b"PK"


def _requests_get(url: str, *, timeout: float, stream: bool, verify: bool) -> requests.Response:
    # Centraliza GET para facilitar fallback SSL
    headers = _ua_headers()
    r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, stream=stream, verify=verify)
    r.raise_for_status()
    return r


def _fetch_text(url: str) -> str:
    timeout = float(os.environ.get("SES_HTTP_TIMEOUT", "30"))
    try:
        r = _requests_get(url, timeout=timeout, stream=False, verify=True)
        return r.text
    except SSLError:
        if not _allow_insecure_ssl():
            raise
        # fallback (ambiente com SSL quebrado)
        r = _requests_get(url, timeout=timeout, stream=False, verify=False)
        return r.text


# ----------------------------
# Normalização
# ----------------------------

def _norm_key(s: str) -> str:
    s = (s or "").replace("\ufeff", "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s.strip()


def _norm_header(s: str) -> str:
    return _norm_key(s).replace(" ", "")


def _digits(x: Any) -> str:
    return re.sub(r"\D+", "", str(x or ""))


def _to_float(x: Any) -> float:
    # típico do SES: 1.234.567,89
    try:
        return float(str(x).strip().replace(".", "").replace(",", "."))
    except Exception:
        return 0.0


# ----------------------------
# CSV parsing robusto
# ----------------------------

def _detect_delim_and_skip_first_line(lines: list[str]) -> tuple[str, int]:
    """
    Retorna (delimiter, skip_n).

    Trata caso Excel: primeira linha "sep=;"
    """
    for i, ln in enumerate(lines):
        if not ln:
            continue
        t = ln.strip().lower()

        if t.startswith("sep=") and len(t) >= 5:
            return (t[4:5] or ";"), i + 1

        counts = {d: ln.count(d) for d in [";", "|", ",", "\t"]}
        best = max(counts, key=counts.get)
        return (best if counts[best] > 0 else ";"), i

    return ";", 0


def _open_csv_bytes(data: bytes) -> tuple[csv.DictReader, list[str], str]:
    # tenta encodings comuns
    last_reader: csv.DictReader | None = None
    last_fields: list[str] = []
    last_delim = ";"

    for enc in ("utf-8-sig", "latin-1"):
        text = io.TextIOWrapper(io.BytesIO(data), encoding=enc, errors="replace", newline="")
        l1 = text.readline()
        l2 = text.readline()
        l3 = text.readline()
        probe = [l1, l2, l3]
        delim, skip_n = _detect_delim_and_skip_first_line(probe)

        head_lines = probe[skip_n:]
        lines_iter: Iterable[str] = iter(head_lines + list(text))
        reader = csv.DictReader(lines_iter, delimiter=delim)
        fieldnames = list(reader.fieldnames or [])
        if len(fieldnames) > 1:
            return reader, fieldnames, delim

        last_reader = reader
        last_fields = fieldnames
        last_delim = delim

    # fallback: devolve o que conseguiu
    return last_reader, last_fields, last_delim  # type: ignore[return-value]


def _open_csv_from_zip(zf: zipfile.ZipFile, member: str) -> tuple[csv.DictReader, list[str], str]:
    # abre com sniff de delimiter/encoding sem carregar arquivo inteiro em memória
    last_reader: csv.DictReader | None = None
    last_fields: list[str] = []
    last_delim = ";"

    for enc in ("utf-8-sig", "latin-1"):
        raw = zf.open(member)
        text = io.TextIOWrapper(raw, encoding=enc, errors="replace", newline="")

        l1 = text.readline()
        l2 = text.readline()
        l3 = text.readline()
        probe = [l1, l2, l3]
        delim, skip_n = _detect_delim_and_skip_first_line(probe)

        head_lines = probe[skip_n:]
        lines_iter = iter(head_lines + list(text))

        reader = csv.DictReader(lines_iter, delimiter=delim)
        fieldnames = list(reader.fieldnames or [])
        if len(fieldnames) > 1:
            return reader, fieldnames, delim

        last_reader = reader
        last_fields = fieldnames
        last_delim = delim

    return last_reader, last_fields, last_delim  # type: ignore[return-value]


# ----------------------------
# Detecção de colunas (robusta para SES)
# ----------------------------

def _detect_company_id_col(fieldnames: list[str]) -> str | None:
    """
    No SES, o código de empresa aparece como:
      - CodigoFIP (LISTAEMPRESAS.csv)
      - Coenti (Ses_cias.csv e várias tabelas)
      - variações com 'cod' + 'cia' etc
    """
    best = None
    best_score = -1e9

    for fn in fieldnames:
        n = _norm_header(fn)
        if not n:
            continue

        score = 0.0

        # Fonte mestre
        if n in {"codigofip", "codigo_fip"}:
            score += 50.0

        # Padrões SES
        if n in {"coenti", "coentidade", "codentidade"}:
            score += 45.0

        # Padrões legados
        if n in {"codcia", "cod_cia", "codigo_cia", "codigocia", "cdcia"}:
            score += 30.0
        if n in {"cia", "idcia"}:
            score += 18.0
        if "cia" in n and ("cod" in n or "codigo" in n or "id" in n):
            score += 12.0

        # penalidades
        if "cnpj" in n:
            score -= 30.0

        score -= len(n) / 100.0

        if score > best_score:
            best_score = score
            best = fn

    return best if best_score > 0 else None


def _detect_name_col(fieldnames: list[str]) -> str | None:
    best = None
    best_score = -1e9

    for fn in fieldnames:
        n = _norm_header(fn)
        if not n:
            continue

        score = 0.0

        # Fonte mestre
        if n in {"nomeentidade"}:
            score += 50.0

        # Padrões SES
        if n in {"noenti"}:
            score += 45.0

        # gerais
        if any(t in n for t in ("nomefantasia", "fantasia")):
            score += 20.0
        if any(t in n for t in ("razaosocial", "razao", "denominacao")):
            score += 15.0
        if n == "nome" or n.startswith("nome"):
            score += 10.0

        # penalidades
        if any(t in n for t in ("sigla", "uf", "pais")):
            score -= 5.0
        if "cnpj" in n:
            score -= 10.0

        score -= len(n) / 200.0

        if score > best_score:
            best_score = score
            best = fn

    return best if best_score > 0 else None


def _detect_cnpj_col(fieldnames: list[str]) -> str | None:
    for fn in fieldnames:
        if "cnpj" in _norm_header(fn):
            return fn
    return None


def _detect_amount_cols(fieldnames: list[str]) -> tuple[str | None, str | None]:
    """
    Detecta melhor coluna de prêmio e melhor coluna de sinistro.
    Heurística: combinações de "prem/premio/pr" com "emit/diret/total"
                e "sinist/indeniz/desp" com "ocorr/pago/total"
    """
    prem_best = None
    prem_score = -1e9
    sin_best = None
    sin_score = -1e9

    for fn in fieldnames:
        n = _norm_header(fn)
        if not n:
            continue

        # Premium / Prêmio
        if ("prem" in n) or ("premio" in n) or (n.startswith("pr") and any(t in n for t in ("emit", "diret", "total"))):
            s = 10.0
            if any(t in n for t in ("emit", "diret", "total", "bruto", "contab")):
                s += 6.0
            if any(t in n for t in ("ganho", "custo", "tarifa", "taxa")):
                s -= 4.0
            s -= len(n) / 120.0
            if s > prem_score:
                prem_score = s
                prem_best = fn

        # Sinistros / Indenizações
        if ("sinist" in n) or ("indeniz" in n) or (n.startswith("si") and any(t in n for t in ("ocorr", "pago", "total"))):
            s = 10.0
            if any(t in n for t in ("ocorr", "pago", "total", "bruto")):
                s += 5.0
            if any(t in n for t in ("recup", "ressarc")):
                s -= 4.0
            s -= len(n) / 120.0
            if s > sin_score:
                sin_score = s
                sin_best = fn

    if prem_score < 0:
        prem_best = None
    if sin_score < 0:
        sin_best = None

    return prem_best, sin_best


def _parse_ym_from_row(row: dict[str, Any], fieldnames: list[str]) -> str | None:
    fn = {k: _norm_header(k) for k in fieldnames}

    year_key = None
    month_key = None

    for k, nk in fn.items():
        if nk in {"ano", "anocompetencia", "anoref", "anobase"} or nk.endswith("ano"):
            year_key = year_key or k
        if nk in {"mes", "mescompetencia", "mesref", "mesbase"} or nk.endswith("mes"):
            month_key = month_key or k

    if year_key and month_key:
        y = _digits(row.get(year_key))
        m = _digits(row.get(month_key))
        if len(y) == 4 and m.isdigit():
            mm = int(m)
            if 1 <= mm <= 12:
                return f"{int(y):04d}-{mm:02d}"

    # tenta campos tipo "competencia"/"periodo"/"anomes"
    candidates = []
    for k, nk in fn.items():
        if any(t in nk for t in ("competencia", "periodo", "anomes", "mesano", "dataref", "dtref")):
            candidates.append(k)

    for k in candidates:
        raw = str(row.get(k) or "").strip()

        d = _digits(raw)
        if len(d) >= 6:
            y = d[:4]
            m = d[4:6]
            if y.isdigit() and m.isdigit():
                mm = int(m)
                if 1 <= mm <= 12:
                    return f"{int(y):04d}-{mm:02d}"

        m1 = re.search(r"(\d{4})[/-](\d{1,2})", raw)
        if m1:
            yy = int(m1.group(1))
            mm = int(m1.group(2))
            if 1 <= mm <= 12:
                return f"{yy:04d}-{mm:02d}"

        m2 = re.search(r"(\d{1,2})[/-](\d{4})", raw)
        if m2:
            mm = int(m2.group(1))
            yy = int(m2.group(2))
            if 1 <= mm <= 12:
                return f"{yy:04d}-{mm:02d}"

    return None


def _ym_to_index(ym: str) -> int:
    y, m = ym.split("-", 1)
    return int(y) * 12 + int(m)


def _months_back(ym: str, months: int) -> str:
    y, m = map(int, ym.split("-"))
    idx = (y * 12 + (m - 1)) - months
    ny = idx // 12
    nm = (idx % 12) + 1
    return f"{ny:04d}-{nm:02d}"


# ----------------------------
# Download "evergreen"
# ----------------------------

def discover_ses_zip_url() -> str:
    override = str(os.environ.get("SES_ZIP_URL", "")).strip()
    if override:
        print(f"SES: usando override SES_ZIP_URL={override}", flush=True)
        return override

    # default estável (evita discovery HTML)
    return SES_ZIP_URL_DEFAULT


def discover_listaempresas_url() -> str:
    override = str(os.environ.get("SES_LISTAEMPRESAS_URL", "")).strip()
    if override:
        print(f"SES: usando override SES_LISTAEMPRESAS_URL={override}", flush=True)
        return override
    return SES_LISTAEMPRESAS_URL_DEFAULT


def _download_streaming(url: str, out_path: Path, *, min_bytes: int = 1024) -> SesFetchResult:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")

    timeout = float(os.environ.get("SES_HTTP_TIMEOUT", "180"))

    def _do_get(verify: bool) -> SesFetchResult:
        r = _requests_get(url, timeout=timeout, stream=True, verify=verify)

        h = hashlib.sha256()
        size = 0
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                h.update(chunk)
                size += len(chunk)

        if size < min_bytes:
            raise RuntimeError(f"SES: download pequeno demais ({size} bytes). url={url!r}")

        tmp.replace(out_path)
        return SesFetchResult(
            url=url,
            fetched_at=_utc_now(),
            bytes_len=size,
            sha256=h.hexdigest(),
            saved_to=str(out_path),
        )

    try:
        return _do_get(verify=True)
    except SSLError:
        if not _allow_insecure_ssl():
            raise
        return _do_get(verify=False)


def download_and_validate_ses_zip() -> SesFetchResult:
    zip_url = discover_ses_zip_url()
    cache = Path(os.environ.get("SES_ZIP_CACHE_PATH", "data/raw/ses_basecompleta.zip"))
    force = str(os.environ.get("SES_FORCE_DOWNLOAD", "")).strip().lower() in {"1", "true", "yes"}

    if cache.exists() and cache.stat().st_size > 1024 * 1024 and not force:
        with cache.open("rb") as f:
            head = f.read(4)
        if _is_zip_signature(head):
            # ok: usa cache
            h = hashlib.sha256()
            with cache.open("rb") as f2:
                for chunk in iter(lambda: f2.read(1024 * 1024), b""):
                    h.update(chunk)
            return SesFetchResult(
                url=zip_url,
                fetched_at=_utc_now(),
                bytes_len=int(cache.stat().st_size),
                sha256=h.hexdigest(),
                saved_to=str(cache),
            )

    info = _download_streaming(zip_url, cache, min_bytes=5 * 1024 * 1024)

    # sanity: abre zip
    try:
        with zipfile.ZipFile(cache) as zf:
            _ = zf.namelist()[:1]
    except zipfile.BadZipFile as e:
        raise RuntimeError(f"SES: BadZipFile ao abrir ZIP baixado. url={zip_url!r}") from e

    return info


def download_listaempresas_csv() -> SesFetchResult:
    url = discover_listaempresas_url()
    cache = Path(os.environ.get("SES_LISTAEMPRESAS_CACHE_PATH", "data/raw/ses_listaempresas.csv"))
    force = str(os.environ.get("SES_FORCE_DOWNLOAD", "")).strip().lower() in {"1", "true", "yes"}

    if cache.exists() and cache.stat().st_size > 10_000 and not force:
        h = hashlib.sha256()
        with cache.open("rb") as f:
            data = f.read()
            h.update(data)
        return SesFetchResult(
            url=url,
            fetched_at=_utc_now(),
            bytes_len=int(cache.stat().st_size),
            sha256=h.hexdigest(),
            saved_to=str(cache),
        )

    # LISTAEMPRESAS é pequeno
    return _download_streaming(url, cache, min_bytes=10_000)


# ----------------------------
# Seleção de arquivos financeiros dentro do ZIP
# (evita varrer 40 CSVs "na força")
# ----------------------------

def _iter_csv_members(names: list[str]) -> Iterable[str]:
    for n in names:
        if n.lower().endswith(".csv"):
            yield n


def _score_financial_member(member: str) -> float:
    nl = member.lower()
    score = 0.0

    # preferências gerais
    if "valoresmovramos" in nl or "rmovram" in nl:
        score += 25.0
    if "seguros" in nl:
        score += 15.0
    if "sinist" in nl:
        score += 10.0
    if "prem" in nl:
        score += 10.0
    if "ramo" in nl:
        score += 8.0

    # penalidades leves (evita ficar preso só em prev/cap)
    if "prev" in nl:
        score -= 3.0
    if "cap" in nl and "cap_" in nl:
        score -= 2.0

    # arquivos muito "meta"
    if any(x in nl for x in ("readme", "leia", "dicion", "doc", "layout", "contatos", "administradores")):
        score -= 50.0

    score -= len(nl) / 3000.0
    return score


def _probe_member_schema(zf: zipfile.ZipFile, member: str) -> dict[str, Any] | None:
    try:
        # leitura leve: header + poucas linhas
        with zf.open(member) as raw:
            data = raw.read(200_000)  # suficiente para header + primeiras linhas
        reader, fields, delim = _open_csv_bytes(data)
    except Exception:
        return None

    if not fields or len(fields) < 2:
        return None

    id_col = _detect_company_id_col(fields)
    prem_col, sin_col = _detect_amount_cols(fields)
    if not id_col or (prem_col is None and sin_col is None):
        return None

    # tenta achar competência com uma linha de amostra
    ym = None
    try:
        for _ in range(5):
            row = next(reader)
            if isinstance(row, dict):
                ym = _parse_ym_from_row(row, fields)
                if ym:
                    break
    except Exception:
        ym = None

    if not ym:
        return None

    return {
        "member": member,
        "fields": fields,
        "delim": delim,
        "id_col": id_col,
        "prem_col": prem_col,
        "sin_col": sin_col,
        "score": _score_financial_member(member) + (15.0 if prem_col and sin_col else 0.0),
    }


def _pick_best_fact_files(zf: zipfile.ZipFile, names: list[str]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """
    Retorna (best_prem_file, best_sin_file).
    Se existir um arquivo "fato" com ambos (prem e sin), ele tende a ser escolhido para ambos.
    """
    candidates: list[dict[str, Any]] = []
    for m in sorted(_iter_csv_members(names), key=_score_financial_member, reverse=True)[:25]:
        probe = _probe_member_schema(zf, m)
        if probe:
            candidates.append(probe)

    if not candidates:
        return None, None

    # 1) procura "single fact" com prem e sin
    both = [c for c in candidates if c.get("prem_col") and c.get("sin_col")]
    if both:
        both.sort(key=lambda x: float(x["score"]), reverse=True)
        return both[0], both[0]

    # 2) separa melhor de prem e melhor de sin
    prem = [c for c in candidates if c.get("prem_col")]
    sin = [c for c in candidates if c.get("sin_col")]

    prem.sort(key=lambda x: float(x["score"]), reverse=True)
    sin.sort(key=lambda x: float(x["score"]), reverse=True)

    return (prem[0] if prem else None), (sin[0] if sin else None)


# ----------------------------
# Public: extraction (expected by build_insurers)
# ----------------------------

def extract_ses_master_and_financials() -> tuple[SesExtractMeta, dict[str, dict[str, Any]]]:
    """
    Retorna:
      meta_ses: SesExtractMeta
      companies: dict[codigoFIP] => {name, cnpj, premiums, claims}

    Fonte mestre (CNPJ): LISTAEMPRESAS.csv
    Fonte fatos: BaseCompleta.zip (1 ou 2 tabelas "fato" auto-detectadas)
    """
    zip_info = download_and_validate_ses_zip()
    master_info = download_listaempresas_csv()

    # --- 1) Carrega mestre LISTAEMPRESAS ---
    master_path = Path(master_info.saved_to)
    master_bytes = master_path.read_bytes()

    master_reader, master_fields, master_delim = _open_csv_bytes(master_bytes)
    id_col = _detect_company_id_col(master_fields) or "CodigoFIP"
    name_col = _detect_name_col(master_fields) or "NomeEntidade"
    cnpj_col = _detect_cnpj_col(master_fields) or "CNPJ"

    companies: dict[str, dict[str, Any]] = {}
    master_rows = 0

    for row in master_reader:
        if not isinstance(row, dict):
            continue
        master_rows += 1

        codigo = _digits(row.get(id_col))
        if not codigo:
            continue

        nome = str(row.get(name_col) or "").strip() or f"SES_ENTIDADE_{codigo}"
        cnpj = _digits(row.get(cnpj_col))

        companies[codigo] = {
            "name": nome,
            "cnpj": cnpj if len(cnpj) == 14 else None,
            "premiums": 0.0,
            "claims": 0.0,
        }

    if not companies:
        raise RuntimeError(
            f"SES: LISTAEMPRESAS não gerou nenhuma entidade. fields={master_fields[:10]} delim={master_delim!r}"
        )

    # --- 2) Abre zip e escolhe tabelas fato ---
    cache_path = Path(zip_info.saved_to)
    with zipfile.ZipFile(cache_path) as zf:
        names = zf.namelist()

        best_prem, best_sin = _pick_best_fact_files(zf, names)

        # Se não detectar nada, não derruba o projeto: mantém mestre com 0, mas registra diagnóstico.
        # (O guardrail MIN_INSURERS_COUNT + MAX_COUNT_DROP_PCT vai proteger a publicação.)
        processed_files: list[str] = []
        processed_rows_total = 0
        all_months: set[str] = set()

        prem_by_id: dict[str, dict[str, float]] = {}
        sin_by_id: dict[str, dict[str, float]] = {}

        def ingest(member_info: dict[str, Any], mode: str) -> None:
            nonlocal processed_rows_total

            member = str(member_info["member"])
            processed_files.append(member)

            reader, fields, _d = _open_csv_from_zip(zf, member)
            idc = str(member_info["id_col"])
            prem_col = member_info.get("prem_col")
            sin_col = member_info.get("sin_col")

            for row in reader:
                if not isinstance(row, dict):
                    continue

                codigo = _digits(row.get(idc))
                if not codigo:
                    continue

                ym = _parse_ym_from_row(row, fields)
                if not ym:
                    continue

                processed_rows_total += 1
                all_months.add(ym)

                if mode in {"prem", "both"} and prem_col:
                    v = _to_float(row.get(prem_col))
                    if v:
                        prem_by_id.setdefault(codigo, {}).setdefault(ym, 0.0)
                        prem_by_id[codigo][ym] += v

                if mode in {"sin", "both"} and sin_col:
                    v = _to_float(row.get(sin_col))
                    if v:
                        sin_by_id.setdefault(codigo, {}).setdefault(ym, 0.0)
                        sin_by_id[codigo][ym] += v

        if best_prem and best_sin and best_prem["member"] == best_sin["member"]:
            ingest(best_prem, "both")
            seguros_file = str(best_prem["member"])
        else:
            if best_prem:
                ingest(best_prem, "prem")
            if best_sin:
                ingest(best_sin, "sin")
            seguros_file = str(best_prem["member"] if best_prem else (best_sin["member"] if best_sin else ""))

        # --- 3) Define janela rolling 12m ---
        if all_months:
            latest = max(all_months, key=_ym_to_index)
            period_to = latest
            period_from = _months_back(latest, 11)
        else:
            # fallback defensivo: evita quebrar o build
            period_to = _utc_now()[:7]
            period_from = _months_back(period_to, 11)

        start_idx = _ym_to_index(period_from)
        end_idx = _ym_to_index(period_to)

        def in_window(ym: str) -> bool:
            i = _ym_to_index(ym)
            return start_idx <= i <= end_idx

        # --- 4) Aplica totais no mestre ---
        for codigo, base in companies.items():
            p = 0.0
            s = 0.0
            for ym, v in prem_by_id.get(codigo, {}).items():
                if in_window(ym):
                    p += v
            for ym, v in sin_by_id.get(codigo, {}).items():
                if in_window(ym):
                    s += v

            base["premiums"] = float(p)
            base["claims"] = float(s)

        meta = SesExtractMeta(
            zip_url=zip_info.url,
            cias_file=str(master_path.name),  # mestre real
            seguros_file=seguros_file or "(auto-detect failed; no fact file processed)",
            period_from=period_from,
            period_to=period_to,
            fetched_at=zip_info.fetched_at,
            bytes_len=zip_info.bytes_len,
            sha256=zip_info.sha256,
            saved_to=zip_info.saved_to,
            notes={
                "master_source": master_info.url,
                "master_cache": master_info.saved_to,
                "master_rows": master_rows,
                "master_detected_cols": {"id_col": id_col, "name_col": name_col, "cnpj_col": cnpj_col},
                "fact_files": processed_files,
                "fact_rows_processed": processed_rows_total,
                "auto_detect_best_prem": (best_prem or {}).get("member"),
                "auto_detect_best_sin": (best_sin or {}).get("member"),
            },
        )

        return meta, companies
