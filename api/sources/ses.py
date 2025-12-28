from __future__ import annotations

import csv
import io
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import requests

SES_HOME = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"
UA = {"User-Agent": "Mozilla/5.0 (compatible; SanidaBot/1.0)"}

_SESSION = requests.Session()
_SESSION.trust_env = False  # blindagem: ignora proxies do ambiente


@dataclass
class SesMeta:
    zip_url: str
    cias_file: str
    seguros_file: str
    period_from: str  # YYYY-MM
    period_to: str  # YYYY-MM


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9_ ]+", "", s)
    return s.strip()


def _digits(s: Any) -> str:
    return re.sub(r"\D+", "", str(s or ""))


def _fetch_text(url: str) -> str:
    r = _SESSION.get(url, headers=UA, timeout=60)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def _download_bytes(url: str) -> bytes:
    r = _SESSION.get(url, headers=UA, timeout=180)
    r.raise_for_status()
    b = r.content
    # Hardening: evita zipfile.BadZipFile quando veio HTML/erro no lugar de ZIP
    if not (
        b.startswith(b"PK\x03\x04")
        or b.startswith(b"PK\x05\x06")
        or b.startswith(b"PK\x07\x08")
    ):
        snippet = b[:200].decode("latin-1", errors="replace").strip()
        raise RuntimeError(f"SES: download não parece ZIP (head={snippet!r})")
    return b


def discover_ses_zip_url() -> str:
    html = _fetch_text(SES_HOME)
    # pega qualquer link .zip e escolhe o "mais promissor"
    hrefs = re.findall(r'href="([^"]+\.zip)"', html, flags=re.I)
    if not hrefs:
        raise RuntimeError("SES: não encontrei link .zip na página principal do SES.")
    # heurística simples: prefere links com 'Base'/'Completa'/'Download'
    scored: list[tuple[int, str]] = []
    for h in hrefs:
        txt = h.lower()
        score = 0
        if "base" in txt:
            score += 5
        if "complet" in txt:
            score += 5
        if "download" in txt:
            score += 3
        scored.append((score, h))
    scored.sort(reverse=True)
    return urljoin(SES_HOME, scored[0][1])


def _pick_member(z: zipfile.ZipFile, contains: tuple[str, ...]) -> str:
    members = z.namelist()
    for m in members:
        nm = _norm(m)
        if all(c in nm for c in contains):
            return m
    # fallback: tenta por “ses_cias”/“ses_seguros” aproximado
    for m in members:
        nm = _norm(m)
        if any(c in nm for c in contains):
            return m
    raise RuntimeError(f"SES: arquivo não encontrado no ZIP (contains={contains}, total={len(members)})")


def _read_csv_all(
    z: zipfile.ZipFile, member: str, delimiter: str = ";"
) -> tuple[list[str], list[list[str]]]:
    with z.open(member, "r") as fh:
        txt = io.TextIOWrapper(fh, encoding="latin-1", errors="replace", newline="")
        reader = csv.reader(txt, delimiter=delimiter)
        header = next(reader, [])
        rows = [row for row in reader if row]
    return header, rows


def _idx(header: list[str], keys: list[str]) -> int | None:
    norm_h = [_norm(h) for h in header]
    keyset = {_norm(k) for k in keys}
    for i, h in enumerate(norm_h):
        if h in keyset:
            return i
    # fallback por "contains"
    for i, h in enumerate(norm_h):
        for k in keyset:
            if k and k in h:
                return i
    return None


def _find_cnpj_idx(header: list[str]) -> int | None:
    norm_h = [_norm(h) for h in header]
    for i, h in enumerate(norm_h):
        if "cnpj" in h:
            return i
    return None


def _infer_cnpj_idx_by_sampling(
    header: list[str],
    rows: list[list[str]],
    exclude: set[int] | None = None,
) -> int | None:
    exclude = exclude or set()
    if not header or not rows:
        return None

    sample = rows[:200]
    best_i: int | None = None
    best_hits = 0

    for i in range(len(header)):
        if i in exclude:
            continue
        hits = 0
        for r in sample:
            if i >= len(r):
                continue
            d = _digits(r[i])
            if len(d) == 14:
                hits += 1
        if hits > best_hits:
            best_hits = hits
            best_i = i

    if best_i is None:
        return None

    min_hits = max(3, int(len(sample) * 0.2))
    return best_i if best_hits >= min_hits else None


def _parse_money_ptbr(s: Any) -> float:
    try:
        t = str(s or "").strip()
        t = t.replace(".", "").replace(",", ".")
        return float(t) if t else 0.0
    except Exception:
        return 0.0


def _ym_add(year: int, month: int, delta: int) -> tuple[int, int]:
    # delta pode ser negativo
    y = year
    m = month + delta
    while m <= 0:
        y -= 1
        m += 12
    while m > 12:
        y += 1
        m -= 12
    return y, m


def _ym_to_int(y: int, m: int) -> int:
    return y * 100 + m


def extract_ses_master_and_financials() -> tuple[SesMeta, dict[str, dict[str, Any]]]:
    zip_url = discover_ses_zip_url()
    b = _download_bytes(zip_url)

    with zipfile.ZipFile(io.BytesIO(b)) as z:
        # tenta localizar os 2 CSVs pelo nome
        cias_member = _pick_member(z, ("ses", "cias"))
        seg_member = _pick_member(z, ("ses", "seguros"))

        # --- Lê lista mestre (cias) ---
        h_cias, rows_cias = _read_csv_all(z, cias_member, delimiter=";")

        sid_i = _idx(h_cias, ["cd_cia", "codigo_cia", "cod_cia", "id_cia"])
        name_i = _idx(h_cias, ["nm_cia", "nome_cia", "denominacao", "razao_social", "nome"])
        if sid_i is None or name_i is None:
            raise RuntimeError("SES: não consegui localizar colunas de ID/NOME em Ses_cias.csv")

        cnpj_i = _find_cnpj_idx(h_cias)
        if cnpj_i is None:
            cnpj_i = _infer_cnpj_idx_by_sampling(h_cias, rows_cias, exclude={sid_i, name_i})

        companies: dict[str, dict[str, Any]] = {}
        for r in rows_cias:
            if sid_i >= len(r) or name_i >= len(r):
                continue
            sid = str(r[sid_i]).strip()
            if not sid:
                continue
            nm = str(r[name_i]).strip()
            cnpj = ""
            if cnpj_i is not None and cnpj_i < len(r):
                d = _digits(r[cnpj_i])
                if len(d) == 14:
                    cnpj = d
            companies[sid] = {
                "ses_id": sid,
                "name": nm,
                "cnpj": cnpj or None,
                "premiums": 0.0,
                "claims": 0.0,
            }

        # --- Lê financeiros (seguros) e calcula rolling_12m ---
        with z.open(seg_member, "r") as fh:
            txt = io.TextIOWrapper(fh, encoding="latin-1", errors="replace", newline="")
            reader = csv.reader(txt, delimiter=";")
            h_seg = next(reader, [])
            if not h_seg:
                raise RuntimeError("SES: Ses_seguros.csv sem header")

            cia_i = _idx(h_seg, ["cd_cia", "codigo_cia", "cod_cia", "id_cia"])
            ano_i = _idx(h_seg, ["ano", "nr_ano", "aa"])
            mes_i = _idx(h_seg, ["mes", "nr_mes", "mm"])

            prem_i = _idx(
                h_seg,
                [
                    "vl_premio",
                    "vl_premio_direto",
                    "premio_direto",
                    "premio",
                    "vlpremio",
                    "vl_prm",
                ],
            )
            clm_i = _idx(
                h_seg,
                [
                    "vl_sinistro",
                    "vl_sinistros",
                    "sinistro",
                    "soma_sinistro",
                    "vlsinistro",
                    "vl_sin",
                ],
            )

            if None in (cia_i, ano_i, mes_i, prem_i, clm_i):
                raise RuntimeError("SES: não consegui localizar colunas chave em Ses_seguros.csv")

            # cia -> yyyymm -> [prem, clm]
            by_cia_month: dict[str, dict[int, list[float]]] = {}
            max_ym: int | None = None

            for row in reader:
                if not row:
                    continue
                if max(cia_i, ano_i, mes_i, prem_i, clm_i) >= len(row):
                    continue

                cia = str(row[cia_i]).strip()
                if not cia:
                    continue

                try:
                    y = int(str(row[ano_i]).strip())
                    m = int(str(row[mes_i]).strip())
                except Exception:
                    continue

                ym = _ym_to_int(y, m)
                if max_ym is None or ym > max_ym:
                    max_ym = ym

                prem = _parse_money_ptbr(row[prem_i])
                clm = _parse_money_ptbr(row[clm_i])

                d = by_cia_month.setdefault(cia, {})
                acc = d.setdefault(ym, [0.0, 0.0])
                acc[0] += prem
                acc[1] += clm

        if max_ym is None:
            raise RuntimeError("SES: não consegui inferir mês máximo em Ses_seguros.csv")

        max_year = max_ym // 100
        max_month = max_ym % 100
        start_y, start_m = _ym_add(max_year, max_month, -11)
        start_ym = _ym_to_int(start_y, start_m)

        # agrega rolling_12m por cia
        for cia, months_map in by_cia_month.items():
            prem_sum = 0.0
            clm_sum = 0.0
            for ym, vals in months_map.items():
                if start_ym <= ym <= max_ym:
                    prem_sum += float(vals[0])
                    clm_sum += float(vals[1])

            # injeta no master (se existir); se não existir, cria
            it = companies.get(cia)
            if not it:
                companies[cia] = {
                    "ses_id": cia,
                    "name": f"SES_ENTIDADE_{cia}",
                    "cnpj": None,
                    "premiums": prem_sum,
                    "claims": clm_sum,
                }
            else:
                it["premiums"] = prem_sum
                it["claims"] = clm_sum

        meta = SesMeta(
            zip_url=zip_url,
            cias_file=cias_member,
            seguros_file=seg_member,
            period_from=f"{start_y:04d}-{start_m:02d}",
            period_to=f"{max_year:04d}-{max_month:02d}",
        )

    return meta, companies
