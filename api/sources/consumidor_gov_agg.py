from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def extract_consumidor_gov_aggregated(
    derived_dir: str | Path = "data/derived/consumidor_gov",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Lê o agregado do Consumidor.gov produzido por `python -m api.build_consumidor_gov`.

    Retorna:
      - meta: dict
      - payload: dict (by_name_key_raw, by_cnpj_key_raw, etc.)

    Compatível com nomes antigos de arquivo.
    """
    d = Path(derived_dir)
    candidates = [
        d / "consumidor_gov_agg.json",
        d / "consumidor_gov_aggregated.json",
        d / "consumidor_gov.json",
    ]
    path = next((p for p in candidates if p.exists()), None)
    if not path:
        meta = {
            "status": "missing",
            "error": f"nenhum agregado encontrado em {d}",
            "candidates": [str(p) for p in candidates],
        }
        return meta, {}

    root = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(root, dict):
        return {"status": "invalid", "error": f"arquivo {path} não é dict"}, {}

    meta = root.get("meta") if isinstance(root.get("meta"), dict) else {"status": "ok"}
    payload = {k: v for k, v in root.items() if k != "meta"}

    # Back-compat: versões antigas podem ter apenas by_name/by_cnpj
    if "by_name_key_raw" not in payload and "by_name" in payload:
        payload["by_name_key_raw"] = payload.get("by_name")
    if "by_cnpj_key_raw" not in payload and "by_cnpj" in payload:
        payload["by_cnpj_key_raw"] = payload.get("by_cnpj")

    meta = {**meta, "path": str(path)}
    return meta, payload
