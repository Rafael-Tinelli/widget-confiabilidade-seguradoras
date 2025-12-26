from __future__ import annotations

import csv
import io
import re
import tempfile
import unicodedata
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


DEFAULT_SES_ZIP_URL = "https://www2.susep.gov.br/safe/menuestatistica/ses/download/BaseCompleta.zip"
USER_AGENT = "widget-confiabilidade-seguradoras/0.1 (+https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras)"


@dataclass(frozen=True)
class SesExtractionMeta:
    zip_url: str
    cias_file: str
    seguros_file: str
    period_from: str  # YYYY-MM-01
    period_to: str    # YYYY-MM-01


def _norm(s: str) -> str:
    """Normaliza header/campos para matching robusto."""
    s = (s or "").strip().strip('"').strip("'")
    s = s.replace("\ufeff", "")  # BOM
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s


def _digits(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    d = re.sub(r"\D+", "", s)
    return d or None


def _parse_brl_number(raw: Any) -> float:
    if raw is None:
        return 0.0
    s = str(raw).strip()
    if not s:
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^0-9\.\-]+", "", s)
    try:
        return float(s)
    except ValueError:
        return 0.0


def _ym_add(ym: int, delta_months: int) -> int:
    y = ym // 100
    m = ym % 100
    total = y * 12 + (m - 1) + delta_months
    y2 = total // 12
    m2 = total % 12 + 1
    return y2 * 100 + m2


def _ym_to_iso_01(ym: int) -> str:
    y = ym // 100
    m = ym % 100
    return f"{y:04d}-{m:02d}-01"


def _parse_ym(value: Any) -> Optional[int]:
    """
    Tenta extrair AAAAMM de valores como:
      - 202510
      - 2025-10
      - 10/2025
      - 2025/10
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    m = re.search(r"\b(\d{4})\D?(\d{2})\b", s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        if 1 <= mo <= 12:
            return y * 100 + mo

    m = re.search(r"\b(\d{2})\D+(\d{4})\b", s)
    if m:
        mo = int(m.group(1))
        y = int(m.group(2))
        if 1 <= mo <= 12:
            return y * 100 + mo

    return None


def _download_zip_to_tempfile(zip_url: str, timeout_s: int = 300) -> Path:
    """
    Baixa o ZIP em streaming para evitar carregar tudo em memória (mais robusto no GitHub Actions).
    """
    r = requests.get(
        zip_url,
        timeout=timeout_s,
        headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
        stream=True,
    )
    r.raise_for_status()

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    try:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                tmp.write(chunk)
        tmp.flush()
        return Path(tmp.name)
    finally:
        tmp.close()


def _detect_encoding_in_zip(z: zipfile.ZipFile, member: str) -> str:
    """
    Detecta encoding por amostra (sem varrer arquivo todo).
    Prioriza utf-8-sig, depois cp1252/latin-1.
    """
    candidates = ("utf-8-sig", "cp1252", "latin-1", "utf-8")
    with z.open(member) as bf:
        sample = bf.read(8192)  # amostra pequena
    for enc in candidates:
        try:
            sample.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _read_csv_rows_from_zip(z: zipfile.ZipFile, member: str) -> Tuple[List[str], Iterable[List[str]]]:
    """
    Retorna (headers_norm, rows_iter) com parsing tolerante:
    - separador ';' (padrão governo)
    - encoding detectado por amostra
    """
    enc = _detect_encoding_in_zip(z, member)

    # Lê headers (abre uma vez)
    with z.open(member) as bf:
        text = io.TextIOWrapper(bf, encoding=enc, errors="replace", newline="")
        reader = csv.reader(text, delimiter=";")
        headers = next(reader, [])
        headers_norm = [_norm(h) for h in headers]

    # Iterador de linhas (abre novamente; mantém o arquivo aberto durante a iteração)
    def _rows_iter() -> Iterable[List[str]]:
        with z.open(member) as bf2:
            text2 = io.TextIOWrapper(bf2, encoding=enc, errors="replace", newline="")
            reader2 = csv.reader(text2, delimiter=";")
            _ = next(reader2, None)  # consome header
            for row in reader2:
                yield row

    return headers_norm, _rows_iter()


def _find_member_case_insensitive(z: zipfile.ZipFile, expected: str) -> Optional[str]:
    expected_l = expected.lower()
    for name in z.namelist():
        if name.lower() == expected_l:
            return name
    return None


def _find_member_by_contains(z: zipfile.ZipFile, needles: List[str]) -> Optional[str]:
    needles_l = [n.lower() for n in needles]
    for name in z.namelist():
        nl = name.lower()
        if not nl.endswith(".csv"):
            continue
        if all(n in nl for n in needles_l):
            return name
    return None


def _find_member_by_header(z: zipfile.ZipFile, required_any: List[List[str]], required_all: List[str]) -> Optional[str]:
    """
    required_all: headers que devem existir.
    required_any: lista de grupos, onde pelo menos 1 header de cada grupo deve existir.
    """
    required_all_n = [_norm(x) for x in required_all]
    required_any_n = [[_norm(x) for x in group] for group in required_any]

    for name in z.namelist():
        if not name.lower().endswith(".csv"):
            continue
        try:
            headers_norm, _ = _read_csv_rows_from_zip(z, name)
        except Exception:
            continue

        hs = set(headers_norm)

        if any(h not in hs for h in required_all_n):
            continue

        ok = True
        for group in required_any_n:
            if not any(h in hs for h in group):
                ok = False
                break
        if ok:
            return name

    return None


def extract_ses_master_and_financials(
    zip_url: str = DEFAULT_SES_ZIP_URL,
) -> Tuple[SesExtractionMeta, Dict[str, Dict[str, Any]]]:
    """
    B1:
      - lista mestre (id, nome, cnpj quando houver)
      - prêmios e sinistros (rolling_12m)
    Retorna:
      - meta
      - dict por ses_id (string) com {name, cnpj, premiums, claims}
    """
    tmp_zip = _download_zip_to_tempfile(zip_url)
    try:
        with zipfile.ZipFile(tmp_zip) as z:
            # Preferência: nomes “clássicos”
            cias = _find_member_case_insensitive(z, "Ses_cias.csv") or _find_member_case_insensitive(z, "SES_cias.csv")
            seguros = _find_member_case_insensitive(z, "Ses_seguros.csv") or _find_member_case_insensitive(z, "SES_seguros.csv")

            # Fallback por nome “contém”
            if not cias:
                cias = _find_member_by_contains(z, ["ses", "cias"])
            if not seguros:
                seguros = _find_member_by_contains(z, ["ses", "seguros"])

            # Fallback por header (mais evergreen)
            if not cias:
                cias = _find_member_by_header(
                    z,
                    required_any=[
                        ["coenti", "cod_enti", "codcia", "cod_cia", "codigo_cia"],
                        ["noenti", "nome", "razao_social"],
                    ],
                    required_all=[],
                )
            if not seguros:
                seguros = _find_member_by_header(
                    z,
                    required_any=[
                        ["coenti", "cod_enti", "codcia", "cod_cia", "codigo_cia"],
                        ["damesano", "anomes", "ano_mes", "competencia", "mesano"],
                        ["premio", "premio_emitido", "vl_premio", "vl_premio_emitido", "premio_total"],
                    ],
                    required_all=[],
                )

            if not cias or not seguros:
                raise RuntimeError(
                    f"Não foi possível localizar Ses_cias/Ses_seguros no ZIP. Encontrados? cias={cias} seguros={seguros}"
                )

            # --- Lê CIAS (mestre) ---
            h_cias, rows_cias = _read_csv_rows_from_zip(z, cias)
            idx_cias = {h: i for i, h in enumerate(h_cias)}

            def pick_idx(possibles: List[str]) -> Optional[int]:
                for p in possibles:
                    pn = _norm(p)
                    if pn in idx_cias:
                        return idx_cias[pn]
                return None

            id_i = pick_idx(["coenti", "cod_enti", "codcia", "cod_cia", "codigo_cia", "cd_entidade"])
            name_i = pick_idx(["noenti", "nome", "razao_social", "nome_cia", "nome_entidade"])
            cnpj_i = pick_idx(["cnpj", "numcnpj", "nr_cnpj", "cpf_cnpj", "cnpj_cia"])

            if id_i is None or name_i is None:
                raise RuntimeError(f"Ses_cias encontrado ({cias}), mas não consegui mapear colunas de id/nome: {h_cias[:30]}")

            companies: Dict[str, Dict[str, Any]] = {}
            for row in rows_cias:
                if not row or len(row) <= max(id_i, name_i):
                    continue
                ses_id = (row[id_i] or "").strip()
                ses_id = _digits(ses_id) or ses_id.strip()
                if not ses_id:
                    continue
                ses_id = ses_id.zfill(6)

                nm = (row[name_i] or "").strip()
                if not nm:
                    continue

                cnpj = None
                if cnpj_i is not None and len(row) > cnpj_i:
                    cnpj = _digits(row[cnpj_i])

                companies[ses_id] = {"name": nm, "cnpj": cnpj}

            # --- Lê SEGUROS (financeiro) ---
            h_seg, _ = _read_csv_rows_from_zip(z, seguros)
            idx_seg = {h: i for i, h in enumerate(h_seg)}

            def pick_idx_seg(possibles: List[str]) -> Optional[int]:
                for p in possibles:
                    pn = _norm(p)
                    if pn in idx_seg:
                        return idx_seg[pn]
                return None

            seg_id_i = pick_idx_seg(["coenti", "cod_enti", "codcia", "cod_cia", "codigo_cia", "cd_entidade"])
            seg_ym_i = pick_idx_seg(["damesano", "anomes", "ano_mes", "competencia", "mesano"])
            premio_i = pick_idx_seg(["premio_emitido", "premio", "vl_premio_emitido", "vl_premio", "premio_total"])
            sin_i = pick_idx_seg(["sinistros", "sinistro", "sinistro_ocorrido", "vl_sinistro", "sinistro_total", "vl_sinistros"])

            if seg_id_i is None or seg_ym_i is None or premio_i is None:
                raise RuntimeError(f"Ses_seguros encontrado ({seguros}), mas não consegui mapear id/competência/prêmio: {h_seg[:40]}")

            def iter_seguros_rows():
                _, rows = _read_csv_rows_from_zip(z, seguros)
                return rows

            # Passo 1: descobrir max_ym
            max_ym: Optional[int] = None
            for row in iter_seguros_rows():
                if not row or len(row) <= seg_ym_i:
                    continue
                ym = _parse_ym(row[seg_ym_i])
                if ym is None:
                    continue
                if (max_ym is None) or (ym > max_ym):
                    max_ym = ym

            if max_ym is None:
                raise RuntimeError(f"Não consegui identificar competência (AAAAMM) em {seguros}.")

            start_ym = _ym_add(max_ym, -11)

            # Passo 2: agrega rolling_12m
            agg: Dict[str, Dict[str, float]] = {}
            for row in iter_seguros_rows():
                if not row or len(row) <= max(seg_id_i, seg_ym_i, premio_i):
                    continue

                ym = _parse_ym(row[seg_ym_i])
                if ym is None or ym < start_ym or ym > max_ym:
                    continue

                ses_id = (row[seg_id_i] or "").strip()
                ses_id = _digits(ses_id) or ses_id.strip()
                if not ses_id:
                    continue
                ses_id = ses_id.zfill(6)

                premio = _parse_brl_number(row[premio_i])
                sin = 0.0
                if sin_i is not None and len(row) > sin_i:
                    sin = _parse_brl_number(row[sin_i])

                cur = agg.setdefault(ses_id, {"premiums": 0.0, "claims": 0.0})
                cur["premiums"] += premio
                cur["claims"] += sin

            period_from = _ym_to_iso_01(start_ym)
            period_to = _ym_to_iso_01(max_ym)

            # Merge: só quem tem prêmio > 0
            out: Dict[str, Dict[str, Any]] = {}
            for ses_id, fin in agg.items():
                if fin.get("premiums", 0.0) <= 0:
                    continue
                base = companies.get(ses_id)
                if not base:
                    base = {"name": f"SES_ENTIDADE_{ses_id}", "cnpj": None}

                out[ses_id] = {
                    "sesId": ses_id,
                    "name": base.get("name"),
                    "cnpj": base.get("cnpj"),
                    "premiums": round(float(fin.get("premiums", 0.0)), 2),
                    "claims": round(float(fin.get("claims", 0.0)), 2),
                }

            meta = SesExtractionMeta(
                zip_url=zip_url,
                cias_file=cias,
                seguros_file=seguros,
                period_from=period_from,
                period_to=period_to,
            )
            return meta, out
    finally:
        try:
            tmp_zip.unlink(missing_ok=True)
        except Exception:
            pass
