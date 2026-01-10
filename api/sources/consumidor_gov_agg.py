# api/sources/consumidor_gov_agg.py
from __future__ import annotations

import csv
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    # Preferir o normalizador do projeto (se existir)
    from api.utils.name_cleaner import normalize_name_key
except Exception:  # pragma: no cover
    def normalize_name_key(s: str) -> str:
        s = (s or "").lower()
        s = re.sub(r"[^a-z0-9]+", " ", s).strip()
        s = re.sub(r"\s+", " ", s)
        return s


RAW_DIR = Path(os.getenv("CG_RAW_DIR", "data/raw/consumidor_gov"))
DERIVED_DIR = Path(os.getenv("CG_DERIVED_DIR", "data/derived/consumidor_gov"))

TARGET_SEGMENT = os.getenv(
    "CG_TARGET_SEGMENT",
    "Seguros, Capitalização e Previdência",
)

# Campo padrão da Base Completa
FIELD_SEGMENT = os.getenv("CG_FIELD_SEGMENT", "Segmento de Mercado")
FIELD_NAME = os.getenv("CG_FIELD_NAME", "Nome Fantasia")
FIELD_RESPONDED = os.getenv("CG_FIELD_RESPONDED", "Respondida")
FIELD_STATUS = os.getenv("CG_FIELD_STATUS", "Situação")
FIELD_RESOLUTION = os.getenv("CG_FIELD_RESOLUTION", "Avaliação Reclamação")
FIELD_SCORE = os.getenv("CG_FIELD_SCORE", "Nota do Consumidor")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_float(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        s = str(v).strip()
        if s in ("", "NA", "N/A", "-", "nan"):
            return 0.0
        return float(s.replace(",", "."))
    except Exception:
        return 0.0


def _detect_delimiter(first_line: str) -> str:
    # Base Completa costuma vir com ';'
    return ";" if ";" in first_line else ","


def _iter_rows(csv_path: Path) -> Iterable[dict[str, str]]:
    encodings = ("utf-8", "utf-8-sig", "latin1")
    last_err: Exception | None = None

    for enc in encodings:
        try:
            with csv_path.open("r", encoding=enc, newline="") as f:
                first = f.readline()
                if not first:
                    return
                delim = _detect_delimiter(first)
                f.seek(0)
                reader = csv.DictReader(f, delimiter=delim)
                for row in reader:
                    # csv.DictReader pode devolver None em chaves ausentes
                    yield {str(k): ("" if v is None else str(v)) for k, v in row.items()}
            return
        except UnicodeDecodeError as e:
            last_err = e
            continue

    raise RuntimeError(f"Consumidor.gov: falha de encoding ao ler {csv_path.name}: {last_err}")


def _pick_latest_base_completa(raw_dir: Path, period: str | None) -> Path | None:
    """
    Procura basecompletaYYYY-MM*.csv.
    - Se period (YYYY-MM) for informado, tenta casar esse.
    - Senão, pega o mais recente (por mtime).
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    candidates = list(raw_dir.glob("basecompleta*.csv"))

    if period:
        # Aceita basecompleta2025-12.csv e variações com sufixo
        wanted = [p for p in candidates if period in p.stem]
        if wanted:
            return max(wanted, key=lambda p: p.stat().st_mtime)

    if candidates:
        return max(candidates, key=lambda p: p.stat().st_mtime)

    return None


def extract_consumidor_gov_aggregated() -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Lê a Base Completa (transacional), filtra apenas TARGET_SEGMENT e agrega por empresa.

    Retorna:
      cg_meta: dict com contadores/diagnóstico
      cg_payload: dict compatível com o matcher (by_name_key_raw, by_cnpj_key_raw, etc.)
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    DERIVED_DIR.mkdir(parents=True, exist_ok=True)

    period = os.getenv("CG_PERIOD")  # opcional: "2025-12"
    csv_path = _pick_latest_base_completa(RAW_DIR, period)

    if not csv_path:
        meta = {
            "status": "missing_raw_csv",
            "generated_at": _utc_now(),
            "target_segment": TARGET_SEGMENT,
            "raw_dir": str(RAW_DIR),
        }
        payload = {"by_name_key_raw": {}, "by_cnpj_key_raw": {}}
        return meta, payload

    # Cache derivado (pequeno): reprocessa se raw for mais novo
    derived_path = DERIVED_DIR / f"consumidor_gov_{csv_path.stem}.json"
    if derived_path.exists() and derived_path.stat().st_mtime >= csv_path.stat().st_mtime:
        try:
            obj = json.loads(derived_path.read_text(encoding="utf-8"))
            meta = obj.get("meta", {}) if isinstance(obj, dict) else {}
            payload = obj.get("payload", {}) if isinstance(obj, dict) else {}
            if isinstance(meta, dict) and isinstance(payload, dict):
                return meta, payload
        except Exception:
            # Se cache corromper, cai para reprocessar
            pass

    rows_total = 0
    rows_segment = 0
    rows_used = 0

    # key -> entry
    by_name_key_raw: dict[str, dict[str, Any]] = {}

    for row in _iter_rows(csv_path):
        rows_total += 1

        segment = (row.get(FIELD_SEGMENT) or "").strip()
        if segment != TARGET_SEGMENT:
            continue
        rows_segment += 1

        status = (row.get(FIELD_STATUS) or "").strip()
        # “Cancelada” normalmente é ruído para reputação
        if "Cancelada" in status:
            continue

        display_name = (row.get(FIELD_NAME) or "").strip()
        if not display_name:
            continue

        key = normalize_name_key(display_name)
        if not key:
            continue

        entry = by_name_key_raw.get(key)
        if not entry:
            entry = {
                "name": display_name,
                "display_name": display_name,
                "cnpj": "",  # Base Completa normalmente não traz CNPJ do fornecedor reclamado
                "statistics": {
                    # padrão “novo”
                    "complaintsCount": 0,
                    "respondedCount": 0,
                    "resolvedCount": 0,
                    "finalizedCount": 0,
                    "scoreSum": 0.0,
                    "scoreCount": 0,
                    "averageScore": None,
                    # compatibilidade “legada”
                    "total_claims": 0,
                    "responded_claims": 0,
                    "resolved_claims": 0,
                    "finalized_claims": 0,
                },
            }
            by_name_key_raw[key] = entry

        stats = entry["statistics"]

        stats["complaintsCount"] += 1
        stats["total_claims"] += 1

        if (row.get(FIELD_RESPONDED) or "").strip() == "S":
            stats["respondedCount"] += 1
            stats["responded_claims"] += 1

        if "Finalizada" in status or "Encerrada" in status:
            stats["finalizedCount"] += 1
            stats["finalized_claims"] += 1

        if (row.get(FIELD_RESOLUTION) or "").strip() == "Resolvida":
            stats["resolvedCount"] += 1
            stats["resolved_claims"] += 1

        score = _safe_float(row.get(FIELD_SCORE))
        if score > 0:
            stats["scoreSum"] += score
            stats["scoreCount"] += 1

        rows_used += 1

    # pós-processamento: averageScore
    for e in by_name_key_raw.values():
        st = e.get("statistics", {})
        try:
            sc = float(st.get("scoreCount", 0) or 0)
            ss = float(st.get("scoreSum", 0.0) or 0.0)
            st["averageScore"] = round(ss / sc, 4) if sc > 0 else None
        except Exception:
            st["averageScore"] = None

    meta = {
        "status": "processed_base_completa",
        "generated_at": _utc_now(),
        "target_segment": TARGET_SEGMENT,
        "source_file": str(csv_path),
        "rows_total": rows_total,
        "rows_segment": rows_segment,
        "rows_used": rows_used,
        "companies": len(by_name_key_raw),
    }

    payload = {
        # compatível com o matcher (ele tenta by_name_key_raw ou by_name)
        "by_name_key_raw": by_name_key_raw,
        "by_name": by_name_key_raw,
        # Base Completa não traz CNPJ do fornecedor (na prática, fica vazio)
        "by_cnpj_key_raw": {},
        "by_cnpj_key": {},
    }

    try:
        derived_path.write_text(
            json.dumps({"meta": meta, "payload": payload}, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        # cache não é crítico
        pass

    return meta, payload
