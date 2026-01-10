# api/sources/consumidor_gov_agg.py
from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any


def _read_json_any(path: Path) -> dict[str, Any]:
    """
    Lê JSON tanto em .json quanto em .json.gz.
    """
    if path.suffix == ".gz":
        with gzip.open(path, "rb") as f:
            return json.loads(f.read().decode("utf-8"))
    return json.loads(path.read_text(encoding="utf-8"))


def extract_consumidor_gov_aggregated(
    derived_dir: str | Path = "data/derived/consumidor_gov",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Lê o agregado do Consumidor.gov produzido por `python -m api.build_consumidor_gov`.

    Retorna:
      - meta: dict
      - payload: dict (by_name_key_raw, by_cnpj_key_raw, etc.)

    Compatível com nomes antigos e com a nova saída .json.gz.
    Também faz fallback para o "monthly" mais recente, se o agregado não existir.
    """
    d = Path(derived_dir)

    candidates = [
        # novo padrão (gzip)
        d / "consumidor_gov_agg.json.gz",
        d / "consumidor_gov_aggregated.json.gz",
        d / "consumidor_gov.json.gz",
        # legado (sem gzip)
        d / "consumidor_gov_agg.json",
        d / "consumidor_gov_aggregated.json",
        d / "consumidor_gov.json",
    ]

    path = next((p for p in candidates if p.exists()), None)

    # Fallback: tenta usar o monthly mais recente
    if not path:
        monthly_dir = d / "monthly"
        monthly_candidates = sorted(monthly_dir.glob("consumidor_gov_20??-??.json.gz"))
        if monthly_candidates:
            path = monthly_candidates[-1]  # último por ordem lexical = mês mais recente

    if not path:
        meta = {
            "status": "missing",
            "error": f"nenhum agregado encontrado em {d}",
            "candidates": [str(p) for p in candidates],
            "monthly_dir": str(d / "monthly"),
        }
        return meta, {}

    try:
        root = _read_json_any(path)
    except Exception as e:
        return {"status": "invalid", "error": f"falha ao ler {path}: {e}"}, {}

    if not isinstance(root, dict):
        return {"status": "invalid", "error": f"arquivo {path} não é dict"}, {}

    meta = root.get("meta") if isinstance(root.get("meta"), dict) else {"status": "ok"}
    payload = {k: v for k, v in root.items() if k != "meta"}

    # Back-compat: versões antigas podem ter apenas by_name/by_cnpj
    if "by_name_key_raw" not in payload and "by_name" in payload:
        payload["by_name_key_raw"] = payload.get("by_name")
    if "by_cnpj_key_raw" not in payload and "by_cnpj" in payload:
        payload["by_cnpj_key_raw"] = payload.get("by_cnpj")

    # Alguns produtores usam by_name_key em vez de by_name_key_raw
    if "by_name_key_raw" not in payload and "by_name_key" in payload:
        payload["by_name_key_raw"] = payload.get("by_name_key")
    if "by_cnpj_key_raw" not in payload and "by_cnpj_key" in payload:
        payload["by_cnpj_key_raw"] = payload.get("by_cnpj_key")

    meta = {**meta, "path": str(path)}
    return meta, payload
