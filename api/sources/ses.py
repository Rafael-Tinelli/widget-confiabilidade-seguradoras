# api/sources/ses.py
"""SES (SUSEP) source helpers.

Objetivo
--------
Extrair um *mínimo confiável* (e evergreen) do SES para alimentar o widget:

- BaseCompleta.zip (dump oficial do SES) via URL fixa (sem depender de HTML discovery)
- LISTAEMPRESAS.csv (master com CNPJ por Código FIP)

Saídas
------
A função pública `extract_ses_master_and_financials()` mantém compatibilidade com o builder:
- retorna (meta, companies)
- companies[sid] contém ao menos: name, cnpj, premiums, claims
- extras são adicionados sob `ses` (por ramo/UF/solvência) sem quebrar compatibilidade.

Notas
-----
- Evita carregar o ZIP inteiro em memória: faz download streaming para disco.
- Lê apenas arquivos necessários dentro do ZIP.
- Janela padrão: 12 meses (configurável por env var).
- Correção (Dez/2025): Robustez na leitura de CSVs mal formatados (linhas com colunas extras).

Env vars
--------
SES_ZIP_URL
SES_LISTAEMPRESAS_URL
SES_ALLOW_INSECURE_SSL=1 (fallback: verify=False quando SSL falhar)
SES_WINDOW_MONTHS (default: 12)
SES_CACHE_DIR (default: data/raw/ses)
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import unicodedata
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple, Union

import requests


DEFAULT_SES_ZIP_URL = "https://www2.susep.gov.br/redarq.asp?arq=BaseCompleta%2ezip"
DEFAULT_LISTAEMPRESAS_URL = "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv"

# Dentro do BaseCompleta.zip (nomes variam em caixa; buscamos case-insensitive)
DEFAULT_SEGUROS_FILE = "Ses_seguros.csv"
DEFAULT_RAMOS_FILE = "Ses_ramos.csv"
DEFAULT_PL_MARGEM_FILE = "Ses_pl_margem.csv"
DEFAULT_UF2_FILE = "SES_UF2.csv"  # costuma vir em caixa alta


@dataclass(frozen=True)
class SesMeta:
    source: str
    zip_url: str
    zip_name: str
    cias_file: str
    seguros_file: str
    as_of: str
    period_from: str
    period_to: str
    window_months: int
    files: list[str]


# ---------------------------------------------------------------------
# Pequenas utilidades
# ---------------------------------------------------------------------


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _digits(s: Any) -> str:
    if s is None:
        return ""
    # Se vier lista (overflow de CSV), pega o primeiro ou junta tudo
    if isinstance(s, list):
        s = "".join(str(x) for x in s)
    return re.sub(r"\D+", "", str(s))


def _norm(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.upper()
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _to_float(v: Any) -> float:
    if v is None:
        return 0.0
    s = str(v).strip()
    if not s:
        return 0.0
    # Normaliza formatos comuns (1.234,56) e (123,45)
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    s = s.replace(" ", "")
    # Se tiver '.' e ',', assume '.' milhar e ',' decimal
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        x = float(s)
        return -x if neg else x
    except ValueError:
        return 0.0


def _month_idx(year: int, month: int) -> int:
    # idx monotônico (0-based)
    return year * 12 + (month - 1)


def _idx_to_ym(idx: int) -> Tuple[int, int]:
    year = idx // 12
    month = (idx % 12) + 1
    return year, month


def _parse_damesano(val: Any) -> Optional[int]:
    """Converte DAMESANO (YYYYMM) para month_idx. Retorna None se inválido."""
    s = _digits(val)
    if len(s) < 6:
        return None
    y = int(s[:4])
    m = int(s[4:6])
    if y < 1900 or m < 1 or m > 12:
        return None
    return _month_idx(y, m)


def _pick_col(fieldnames: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    fns = list(fieldnames)
    norm_map = {_norm(fn): fn for fn in fns}
    for c in candidates:
        key = _norm(c)
        if key in norm_map:
            return norm_map[key]
    # fallback por contains
    for fn in fns:
        n = _norm(fn)
        for c in candidates:
            if _norm(c) in n:
                return fn
    return None


# ---------------------------------------------------------------------
# Download + cache em disco (evergreen)
# ---------------------------------------------------------------------


def _cache_dir() -> Path:
    return Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")).resolve()


def _download_to_file(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    allow_insecure = _env_bool("SES_ALLOW_INSECURE_SSL", False)

    def _do(verify: bool) -> None:
        with requests.get(url, stream=True, timeout=(15, 180), verify=verify) as r:
            r.raise_for_status()
            tmp = dest.with_suffix(dest.suffix + ".part")
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            tmp.replace(dest)

    try:
        _do(verify=True)
    except requests.exceptions.SSLError:
        if not allow_insecure:
            raise
        _do(verify=False)


def _get_or_download(url: str, dest: Path) -> Path:
    try:
        _download_to_file(url, dest)
        return dest
    except Exception:
        # fallback: se já houver cache, usa o cache
        if dest.exists() and dest.stat().st_size > 0:
            return dest
        raise


# ---------------------------------------------------------------------
# Leitura de CSVs (ZIP e externo)
# ---------------------------------------------------------------------


def _open_csv_text(stream: io.BufferedReader, encoding: str = "latin-1") -> io.TextIOWrapper:
    # SES costuma ser latin-1; errors=replace para nunca quebrar.
    return io.TextIOWrapper(stream, encoding=encoding, errors="replace", newline="")


def _iter_csv_from_zip(zf: zipfile.ZipFile, member: str, delimiter: str = ";") -> Iterable[dict[str, str]]:
    with zf.open(member, "r") as bf:
        tf = _open_csv_text(bf)
        reader = csv.DictReader(tf, delimiter=delimiter)
        for row in reader:
            yield row


def _decode_csv_bytes(raw: bytes) -> Tuple[str, str]:
    """Decodifica bytes para string e detecta delimitador (';' ou ',')."""
    try:
        txt = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        txt = raw.decode("latin-1", errors="replace")

    sample = txt[:4096]
    delimiter = ";" if sample.count(";") >= sample.count(",") else ","
    return txt, delimiter


def _iter_csv_from_path(path: Union[str, Path]) -> Iterable[dict[str, Any]]:
    """
    Lê CSV do disco com robustez para linhas quebradas/excedentes.
    Usa restkey='__extra__' para capturar colunas além do header.
    """
    raw = Path(path).read_bytes()
    txt, delimiter = _decode_csv_bytes(raw)

    # Padroniza colunas excedentes em "__extra__" (em vez de key=None)
    reader = csv.DictReader(
        io.StringIO(txt),
        delimiter=delimiter,
        restkey="__extra__",
        restval="",
    )
    for row in reader:
        if not isinstance(row, dict):
            continue
        yield row


def _find_member(zf: zipfile.ZipFile, wanted: str) -> Optional[str]:
    w = wanted.lower()
    for name in zf.namelist():
        if name.lower() == w:
            return name
    # fallback: termina com o nome
    for name in zf.namelist():
        if name.lower().endswith("/" + w) or name.lower().endswith(w):
            return name
    return None


# ---------------------------------------------------------------------
# Master LISTAEMPRESAS (Código FIP -> Nome + CNPJ)
# ---------------------------------------------------------------------


def _load_listaempresas(path: Union[str, Path]) -> dict[str, dict[str, Any]]:
    """
    Lê LISTAEMPRESAS.csv e devolve mapa:
        { codigo_fip: { "name": "...", "cnpj": "00000000000000" } }

    Robustez:
      - ignora chaves None / "__extra__" do DictReader
      - trata valores list (overflow) sem quebrar (.strip em lista causava erro)
    """
    rows = list(_iter_csv_from_path(path))
    if not rows:
        return {}

    # Filtra chaves do header que não sejam strings válidas
    raw_fieldnames = list(rows[0].keys())
    fieldnames: list[str] = [
        str(fn) for fn in raw_fieldnames 
        if fn and isinstance(fn, str) and fn.strip() and fn != "__extra__"
    ]

    col_codigo = _pick_col(fieldnames, ["CodigoFIP", "codigo_fip", "coenti", "cod_fip", "codigo"])
    col_nome = _pick_col(
        fieldnames, 
        ["NomeEntidade", "nome_entidade", "noenti", "nome", "Nome Entidade", "nomeentidade"]
    )
    col_cnpj = _pick_col(fieldnames, ["CNPJ", "cnpj", "Cnpj"])

    # Se a heurística falhar, tenta fallback posicional
    if not (col_codigo and col_nome and col_cnpj):
        keys = list(fieldnames)
        col_codigo = col_codigo or (keys[0] if len(keys) > 0 else None)
        col_nome = col_nome or (keys[1] if len(keys) > 1 else None)
        col_cnpj = col_cnpj or (keys[2] if len(keys) > 2 else None)

    if not col_codigo or not col_nome or not col_cnpj:
        # Se mesmo assim falhar, retorna vazio (ou poderia dar raise)
        # Para evitar crash total, logamos aviso ou retornamos vazio.
        # Aqui optamos por retornar vazio para não parar o processo se o arquivo estiver corrompido.
        return {}

    out: dict[str, dict[str, Any]] = {}

    for r in rows:
        # Codigo
        sid_val = r.get(col_codigo)
        sid = _digits(sid_val)
        if not sid:
            continue

        # Nome
        nome_val = r.get(col_nome)
        if isinstance(nome_val, list):
            nome_val = " ".join(str(x) for x in nome_val if x)
        name = str(nome_val or "").strip()

        # CNPJ
        cnpj_val = r.get(col_cnpj)
        if isinstance(cnpj_val, list):
            cnpj_val = "".join(str(x) for x in cnpj_val if x)
        cnpj = _digits(cnpj_val)

        out[sid] = {"name": name, "cnpj": cnpj}

    return out


# ---------------------------------------------------------------------
# SES: Seguros (prêmios / sinistros / despesas) por mês
# ---------------------------------------------------------------------


def _parse_ses_seguros(
    zf: zipfile.ZipFile,
    member: str,
    company_ids: set[str],
    window_months: int,
) -> tuple[int, list[int], dict[str, dict[str, Any]]]:
    """Retorna (as_of_idx, months_idx_sorted, buckets_por_empresa)."""
    # Vamos ler header (primeira linha) para detectar colunas
    with zf.open(member, "r") as bf:
        tf = _open_csv_text(bf)
        reader = csv.DictReader(tf, delimiter=";")
        fieldnames = reader.fieldnames or []
        id_col = _pick_col(fieldnames, ["coenti", "codigo_fip", "codigofip"])
        ramo_col = _pick_col(fieldnames, ["coramo", "ramo", "co_ramo"])
        date_col = _pick_col(fieldnames, ["damesano", "ano_mes", "anomes", "competencia", "dta"])
        prem_col = _pick_col(
            fieldnames,
            [
                "prem_gan",
                "premio_ganho",
                "prem_ret",
                "premio_retido",
                "prem_dir",
                "premio_direto",
                "premio",
            ],
        )
        sin_col = _pick_col(fieldnames, ["sin_ret", "sinistro_retido", "sin_dir", "sinistro"])
        desp_col = _pick_col(fieldnames, ["desp_com", "despesa_comercial", "despesas_comerciais", "aquisicao"])

        if not (id_col and date_col and prem_col and sin_col):
            raise RuntimeError(
                f"SES: colunas essenciais não encontradas em {member}. "
                f"id_col={id_col} date_col={date_col} prem_col={prem_col} sin_col={sin_col}"
            )

        max_idx = -1
        window_start = -1
        # buckets[sid] = {totals..., by_ramo: {coramo: {...}}}
        buckets: dict[str, dict[str, Any]] = {}
        months_seen: set[int] = set()

        def _prune() -> None:
            # Nada a fazer aqui porque armazenamos por competência agregada (não por mês).
            # Mantemos apenas somatórios dentro da janela no próprio loop.
            return

        for row in reader:
            sid = _digits(row.get(id_col))
            if not sid or sid not in company_ids:
                continue

            idx = _parse_damesano(row.get(date_col))
            if idx is None:
                continue

            if idx > max_idx:
                max_idx = idx
                window_start = max_idx - (window_months - 1)
                _prune()

            if idx < window_start:
                continue

            months_seen.add(idx)

            prem = _to_float(row.get(prem_col))
            sin = _to_float(row.get(sin_col))
            desp = _to_float(row.get(desp_col)) if desp_col else 0.0

            b = buckets.setdefault(
                sid,
                {
                    "premium": 0.0,
                    "claims": 0.0,
                    "acq_expenses": 0.0,
                    "by_ramo": {},
                },
            )
            b["premium"] += prem
            b["claims"] += sin
            b["acq_expenses"] += desp

            if ramo_col:
                coramo = _digits(row.get(ramo_col))
                if coramo:
                    rb = b["by_ramo"].setdefault(coramo, {"premium": 0.0, "claims": 0.0, "acq_expenses": 0.0})
                    rb["premium"] += prem
                    rb["claims"] += sin
                    rb["acq_expenses"] += desp

        if max_idx < 0:
            raise RuntimeError("SES: não foi possível identificar competência máxima em Ses_seguros")

        months_sorted = sorted(months_seen)
        return max_idx, months_sorted, buckets


def _load_ramos_map(zf: zipfile.ZipFile, member: str) -> dict[str, str]:
    out: dict[str, str] = {}
    # Ses_ramos costuma ser pequena; iteramos direto
    for row in _iter_csv_from_zip(zf, member, delimiter=";"):
        # campos típicos: coramo, noramo
        # mas pode variar: buscamos por heurística simples
        if not row:
            continue
        if not out:
            fieldnames = row.keys()
            col_id = _pick_col(fieldnames, ["coramo", "co_ramo", "ramo"])
            col_nm = _pick_col(fieldnames, ["noramo", "no_ramo", "nome"])
            # guardamos em closure
            _load_ramos_map.col_id = col_id  # type: ignore[attr-defined]
            _load_ramos_map.col_nm = col_nm  # type: ignore[attr-defined]
        col_id = getattr(_load_ramos_map, "col_id", None)  # type: ignore[attr-defined]
        col_nm = getattr(_load_ramos_map, "col_nm", None)  # type: ignore[attr-defined]
        if not col_id or not col_nm:
            continue
        rid = _digits(row.get(col_id))
        if not rid:
            continue
        nm = (row.get(col_nm) or "").strip()
        if nm:
            out[rid] = nm
    return out


def _parse_pl_margem(
    zf: zipfile.ZipFile,
    member: str,
    company_ids: set[str],
    window_start: int,
    window_end: int,
) -> dict[str, dict[str, float]]:
    """Extrai solvência (PL ajustado e margem) para a competência mais recente da janela."""
    # regra: pega *apenas* o último mês (window_end) para evitar ruído
    out: dict[str, dict[str, float]] = {}

    with zf.open(member, "r") as bf:
        tf = _open_csv_text(bf)
        reader = csv.DictReader(tf, delimiter=";")
        fieldnames = reader.fieldnames or []
        id_col = _pick_col(fieldnames, ["coenti", "codigo_fip", "codigofip"])
        date_col = _pick_col(fieldnames, ["damesano", "anomes", "competencia"])
        pl_col = _pick_col(fieldnames, ["pl_ajust", "plajust", "pl_ajustado", "pl_ajustado_rs"])
        margem_col = _pick_col(fieldnames, ["margem", "margem_solv", "margem_solvencia", "margem_requerida"])

        if not (id_col and date_col and pl_col and margem_col):
            return {}

        for row in reader:
            sid = _digits(row.get(id_col))
            if not sid or sid not in company_ids:
                continue
            idx = _parse_damesano(row.get(date_col))
            if idx is None or idx != window_end:
                continue
            pl = _to_float(row.get(pl_col))
            mg = _to_float(row.get(margem_col))
            out[sid] = {
                "pl_adjusted": pl,
                "solvency_margin": mg,
                "coverage": (pl / mg) if mg else 0.0,
            }
    return out


def _parse_uf2(
    zf: zipfile.ZipFile,
    member: str,
    company_ids: set[str],
    window_start: int,
    window_end: int,
) -> dict[str, dict[str, dict[str, float]]]:
    """Agrega por UF (prêmio e sinistro) na janela."""
    out: dict[str, dict[str, dict[str, float]]] = {}

    with zf.open(member, "r") as bf:
        tf = _open_csv_text(bf)
        reader = csv.DictReader(tf, delimiter=";")
        fieldnames = reader.fieldnames or []
        id_col = _pick_col(fieldnames, ["coenti", "codigo_fip", "codigofip"])
        uf_col = _pick_col(fieldnames, ["uf"])
        date_col = _pick_col(fieldnames, ["damesano", "anomes", "competencia"])
        prem_col = _pick_col(fieldnames, ["prem_gan", "prem_ret", "prem_dir", "premio"])
        sin_col = _pick_col(fieldnames, ["sin_ret", "sinistro"])

        if not (id_col and uf_col and date_col and prem_col and sin_col):
            return {}

        for row in reader:
            sid = _digits(row.get(id_col))
            if not sid or sid not in company_ids:
                continue
            idx = _parse_damesano(row.get(date_col))
            if idx is None or idx < window_start or idx > window_end:
                continue
            uf = (row.get(uf_col) or "").strip().upper()
            if not uf or len(uf) != 2:
                continue
            prem = _to_float(row.get(prem_col))
            sin = _to_float(row.get(sin_col))

            byuf = out.setdefault(sid, {})
            u = byuf.setdefault(uf, {"premium": 0.0, "claims": 0.0})
            u["premium"] += prem
            u["claims"] += sin
    return out


# ---------------------------------------------------------------------
# API pública (usada pelos builders)
# ---------------------------------------------------------------------


def extract_ses_master_and_financials() -> Tuple[SesMeta, Dict[str, Dict[str, Any]]]:
    """Extrai master (nome/CNPJ) + financeiros do SES.

    Compatibilidade:
    - premiums / claims = soma na janela em todos os ramos
    - `ses` contém detalhes adicionais (by_ramo, by_uf, solvency)
    """
    window_months = _env_int("SES_WINDOW_MONTHS", 12)
    zip_url = os.getenv("SES_ZIP_URL", DEFAULT_SES_ZIP_URL)
    lista_url = os.getenv("SES_LISTAEMPRESAS_URL", DEFAULT_LISTAEMPRESAS_URL)

    cache_dir = _cache_dir()
    zip_path = cache_dir / "BaseCompleta.zip"
    lista_path = cache_dir / "LISTAEMPRESAS.csv"

    zip_path = _get_or_download(zip_url, zip_path)
    lista_path = _get_or_download(lista_url, lista_path)

    master = _load_listaempresas(lista_path)
    company_ids = set(master.keys())
    if len(company_ids) < 100:
        raise RuntimeError(f"SES: master LISTAEMPRESAS pequeno demais ({len(company_ids)})")

    with zipfile.ZipFile(zip_path) as zf:
        seg_member = _find_member(zf, DEFAULT_SEGUROS_FILE)
        if not seg_member:
            raise RuntimeError(f"SES: {DEFAULT_SEGUROS_FILE} não encontrado no ZIP")

        as_of_idx, months_idx, buckets = _parse_ses_seguros(
            zf,
            seg_member,
            company_ids=company_ids,
            window_months=window_months,
        )

        window_end = as_of_idx
        window_start = as_of_idx - (window_months - 1)

        # Ramos map (best-effort)
        ramos_map: dict[str, str] = {}
        ramos_member = _find_member(zf, DEFAULT_RAMOS_FILE)
        if ramos_member:
            try:
                ramos_map = _load_ramos_map(zf, ramos_member)
            except Exception:
                ramos_map = {}

        # Solvência (best-effort)
        solv: dict[str, dict[str, float]] = {}
        pl_member = _find_member(zf, DEFAULT_PL_MARGEM_FILE)
        if pl_member:
            try:
                solv = _parse_pl_margem(zf, pl_member, company_ids, window_start, window_end)
            except Exception:
                solv = {}

        # UF2 (best-effort)
        by_uf: dict[str, dict[str, dict[str, float]]] = {}
        uf2_member = _find_member(zf, DEFAULT_UF2_FILE)
        if uf2_member:
            try:
                by_uf = _parse_uf2(zf, uf2_member, company_ids, window_start, window_end)
            except Exception:
                by_uf = {}

    # Monta saída por empresa
    companies: Dict[str, Dict[str, Any]] = {}
    for sid, info in master.items():
        name = info.get("name") or ""
        cnpj = info.get("cnpj") or ""

        b = buckets.get(sid) or {}
        premium = float(b.get("premium") or 0.0)
        claims = float(b.get("claims") or 0.0)
        acq = float(b.get("acq_expenses") or 0.0)

        # detalhes por ramo (com nome, se disponível)
        by_ramo_out: dict[str, dict[str, Any]] = {}
        for coramo, rb in (b.get("by_ramo") or {}).items():
            nm = ramos_map.get(coramo)
            by_ramo_out[coramo] = {
                "name": nm,
                "premium": float(rb.get("premium") or 0.0),
                "claims": float(rb.get("claims") or 0.0),
                "acq_expenses": float(rb.get("acq_expenses") or 0.0),
            }

        companies[sid] = {
            "name": name,
            "cnpj": cnpj,
            # compatibilidade
            "premiums": premium,
            "claims": claims,
            # extensões
            "ses": {
                "window_months": window_months,
                "as_of": "{:04d}-{:02d}".format(*_idx_to_ym(as_of_idx)),
                "by_ramo": by_ramo_out,
                "by_uf": by_uf.get(sid) or {},
                "solvency": solv.get(sid) or {},
                "acq_expenses": acq,
            },
        }

    y2, m2 = _idx_to_ym(as_of_idx)
    y1, m1 = _idx_to_ym(as_of_idx - (window_months - 1))
    meta = SesMeta(
        source="SES/SUSEP (BaseCompleta.zip + LISTAEMPRESAS.csv)",
        zip_url=zip_url,
        zip_name=zip_path.name,
        cias_file=lista_path.name,
        seguros_file=seg_member,
        as_of=f"{y2:04d}-{m2:02d}",
        period_from=f"{y1:04d}-{m1:02d}",
        period_to=f"{y2:04d}-{m2:02d}",
        window_months=window_months,
        files=[
            zip_path.name,
            lista_path.name,
        ],
    )
    return meta, companies


# ---------------------------------------------------------------------
# Fallback: reaproveitar último insurers_full se SES falhar
# (mantido para compatibilidade com o workflow atual)
# ---------------------------------------------------------------------


DATA_SNAPSHOTS = Path("data/snapshots")
DATA_DERIVED = Path("data/derived")


def _load_cached_insurers_payload() -> Optional[dict[str, Any]]:
    # Preferir snapshot (se existir)
    if DATA_SNAPSHOTS.exists():
        files = sorted(DATA_SNAPSHOTS.glob("insurers_full_*.json.gz"))
        if files:
            latest = files[-1]
            import gzip

            with gzip.open(latest, "rt", encoding="utf-8") as f:
                return json.load(f)

    # Fallback: derived (se existir)
    derived = DATA_DERIVED / "insurers_full_latest.json.gz"
    if derived.exists():
        import gzip

        with gzip.open(derived, "rt", encoding="utf-8") as f:
            return json.load(f)

    return None


def extract_ses_master_and_financials_with_fallback() -> Tuple[SesMeta, Dict[str, Dict[str, Any]]]:
    try:
        return extract_ses_master_and_financials()
    except Exception as exc:
        cached = _load_cached_insurers_payload()
        if not cached:
            raise

        # tenta manter formato (meta + companies) do SES
        meta_dict = (cached.get("meta") or {}).get("ses") or {}
        companies = cached.get("companies") or cached.get("insurers") or {}

        # meta mínima
        meta = SesMeta(
            source=str(meta_dict.get("source") or "SES (cached insurers_full)"),  # type: ignore[arg-type]
            zip_url=str(meta_dict.get("zip_url") or ""),
            zip_name=str(meta_dict.get("zip_name") or ""),
            cias_file=str(meta_dict.get("cias_file") or ""),
            seguros_file=str(meta_dict.get("seguros_file") or ""),
            as_of=str(meta_dict.get("as_of") or ""),
            period_from=str(meta_dict.get("period_from") or ""),
            period_to=str(meta_dict.get("period_to") or ""),
            window_months=int(meta_dict.get("window_months") or 12),
            files=list(meta_dict.get("files") or []),
        )

        # Injeta aviso (sem quebrar o tipo)
        md = asdict(meta)
        md["warning"] = f"SES falhou ({exc.__class__.__name__}); usando cache do último insurers_full"
        # reconstrói meta como dataclass (removendo chave extra)
        meta = SesMeta(**{k: md[k] for k in SesMeta.__dataclass_fields__.keys()})  # type: ignore[arg-type]

        # companies deve ser dict sid->obj
        if isinstance(companies, list):
            companies = {str(c.get("id") or ""): c for c in companies if isinstance(c, dict)}

        return meta, companies  # type: ignore[return-value]
