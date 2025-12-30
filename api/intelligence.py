# api/build_insurers.py
from __future__ import annotations

import gzip
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.matching.consumidor_gov_match import NameMatcher
from api.sources.ses import extract_ses_master_and_financials
from api.sources.opin_products import extract_open_insurance_products
# Import movido para a posição correta (após __future__)
from api.intelligence import run_scoring_pipeline

# --- Paths ---
ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_SNAPSHOTS = ROOT / "data" / "snapshots"
DATA_DERIVED = ROOT / "data" / "derived"
API_V1 = ROOT / "api" / "v1"

API_INSURERS = API_V1 / "insurers.json"
FULL_RAW_GZ = DATA_RAW / "insurers_full.json.gz"

CONSUMIDOR_GOV_LATEST = (
    DATA_DERIVED / "consumidor_gov" / "consumidor_gov_agg_latest.json"
)
CONSUMIDOR_GOV_MATCH_REPORT = (
    DATA_DERIVED / "consumidor_gov" / "match_report_insurers.json"
)

OPIN_PARTICIPANTS = API_V1 / "participants.json"

# --- Hardening Configs ---
MIN_COUNT = int(os.getenv("MIN_INSURERS_COUNT", "200"))
MAX_DROP_PCT = float(os.getenv("MAX_COUNT_DROP_PCT", "0.20"))

# Matching knobs (optional)
CONSUMIDOR_MATCH_THRESHOLD = float(os.getenv("CONSUMIDOR_MATCH_THRESHOLD", "0.85"))
CONSUMIDOR_MATCH_MIN_MARGIN = float(os.getenv("CONSUMIDOR_MATCH_MIN_MARGIN", "0.08"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _taxonomy() -> dict[str, Any]:
    return {
        "segments": {
            "S1": "Seguradoras de Grande Porte (comparáveis entre si)",
            "S2": "Seguradoras de Médio Porte",
            "S3": "Seguradoras de Pequeno Porte",
            "S4": "Insurtechs / supervisionadas especiais",
        },
        "products": {
            "auto": "Automóvel",
            "vida": "Pessoas e Vida",
            "patrimonial": "Residencial e Patrimonial",
            "rural": "Rural",
        },
    }


def _methodology_stub() -> dict[str, Any]:
    return {
        "score": {
            "range": [0, 100],
            "weights": {
                "solvency": 0.35,
                "reputation": 0.40,
                "product": 0.15,
                "friction": 0.10,
            },
            "formula": (
                "Score Composto Multidimensional: "
                "1. Solvência suavizada (Bühlmann-Straub). "
                "2. Reputação ajustada (Bayes + Lei de Potência). "
                "3. Densidade de Produto (OPIN). "
                "4. Atrito (LAE)."
            ),
            "notes": [
                "B1: Dados financeiros SUSEP suavizados para evitar volatilidade em pequenas seguradoras.",
                "B2: Reputação Consumidor.gov normalizada por volume (não-linear) e qualidade (bayesiana).",
            ],
        }
    }


def _infer_segment_fallback(val: float) -> str:
    if val >= 2e9:
        return "S1"
    if val >= 5e8:
        return "S2"
    if val >= 5e7:
        return "S3"
    return "S4"


def _read_existing_count(path: Path) -> int | None:
    try:
        if not path.exists() or path.stat().st_size < 10:
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        return int(data.get("meta", {}).get("count", 0)) or None
    except Exception:
        return None


def _guard_count_regression(new_count: int, old_count: int | None) -> None:
    if new_count <= 0:
        raise RuntimeError(
            "CRITICAL: meta.count=0. Abortando para preservar dados existentes."
        )

    if new_count < MIN_COUNT:
        raise RuntimeError(
            f"CRITICAL: meta.count={new_count} < MIN_COUNT={MIN_COUNT}. Abortando."
        )

    if old_count and old_count > 0:
        drop_pct = (old_count - new_count) / float(old_count)
        if drop_pct > MAX_DROP_PCT:
            raise RuntimeError(
                f"CRITICAL: queda abrupta em meta.count ({old_count} -> {new_count}, queda de {drop_pct:.1%}) "
                f"acima do limite permitido ({MAX_DROP_PCT:.0%}). Abortando."
            )
    print(f"Stats Check: Count {old_count or 'N/A'} -> {new_count} (OK)")


def _atomic_write_text(path: Path, content: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def _normalize_cnpj(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value)
    digits = re.sub(r"\D+", "", s)
    if len(digits) != 14:
        return None
    return digits


def _load_consumidor_gov() -> tuple[
    dict[str, Any] | None,
    dict[str, Any] | None,
    dict[str, Any] | None,
    str | None,
]:
    """
    Returns: (meta, by_name_key, by_cnpj_key, error_note)
    """
    if (
        not CONSUMIDOR_GOV_LATEST.exists()
        or CONSUMIDOR_GOV_LATEST.stat().st_size < 10
    ):
        return None, None, None, "consumidorGov: derived file missing"

    try:
        payload = json.loads(CONSUMIDOR_GOV_LATEST.read_text(encoding="utf-8"))
        meta = payload.get("meta") or {}

        by_name_key = payload.get("by_name_key") or {}
        by_cnpj_key = payload.get("by_cnpj_key") or {}

        if not isinstance(by_name_key, dict):
            by_name_key = {}
        if not isinstance(by_cnpj_key, dict):
            by_cnpj_key = {}

        if not by_name_key and not by_cnpj_key:
            return (
                meta,
                None,
                None,
                "consumidorGov: both by_name_key and by_cnpj_key empty/invalid",
            )

        return meta, (by_name_key or None), (by_cnpj_key or None), None

    except Exception as e:
        return None, None, None, f"consumidorGov: failed to load ({e})"


def _build_consumidor_matcher(by_name_key: dict[str, Any]) -> NameMatcher:
    # candidates: {consumer_key: display_name}
    candidates: dict[str, str] = {}
    for k, v in by_name_key.items():
        if isinstance(v, dict):
            candidates[str(k)] = str(v.get("display_name") or "")
    return NameMatcher(candidates)


def _load_opin_participants_cnpjs() -> tuple[set[str], str | None]:
    """
    Extract CNPJs from api/v1/participants.json (schema can vary).
    Returns: (set_cnpjs, error_note)
    """
    if not OPIN_PARTICIPANTS.exists() or OPIN_PARTICIPANTS.stat().st_size < 10:
        return set(), "opin: participants.json missing"

    try:
        payload = json.loads(OPIN_PARTICIPANTS.read_text(encoding="utf-8"))
    except Exception as e:
        return set(), f"opin: failed to parse participants.json ({e})"

    cnpjs: set[str] = set()

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                lk = str(k).lower()
                if any(
                    t in lk
                    for t in (
                        "cnpj",
                        "registrationnumber",
                        "document",
                        "documentnumber",
                    )
                ):
                    n = _normalize_cnpj(v)
                    if n:
                        cnpjs.add(n)
                walk(v)
        elif isinstance(obj, list):
            for it in obj:
                walk(it)
        elif isinstance(obj, str):
            for m in re.findall(r"\b\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\b", obj):
                n = _normalize_cnpj(m)
                if n:
                    cnpjs.add(n)

    walk(payload)

    if not cnpjs:
        blob = json.dumps(payload, ensure_ascii=False)
        for m in re.findall(r"\b\d{14}\b", blob):
            n = _normalize_cnpj(m)
            if n:
                cnpjs.add(n)

    if not cnpjs:
        return set(), "opin: no CNPJ extracted from participants.json"

    return cnpjs, None


def _write_consumidor_match_report(report: dict[str, Any]) -> None:
    CONSUMIDOR_GOV_MATCH_REPORT.parent.mkdir(parents=True, exist_ok=True)
    CONSUMIDOR_GOV_MATCH_REPORT.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_payload() -> dict[str, Any]:
    # --- 1. Load SES base ---
    print("\n--- INICIANDO COLETA SUSEP (FINANCEIRO) ---")
    meta_ses, companies = extract_ses_master_and_financials()

    # --- 2. Load Consumidor.gov derived ---
    cg_meta, cg_by_name, cg_by_cnpj, cg_err = _load_consumidor_gov()
    matcher: NameMatcher | None = (
        _build_consumidor_matcher(cg_by_name) if cg_by_name else None
    )

    # --- 3. Load OPIN participants CNPJs ---
    opin_cnpjs, opin_err = _load_opin_participants_cnpjs()

    # --- 4. Coleta Open Insurance (Produtos) - NOVO ---
    try:
        print("\n--- INICIANDO COLETA OPEN INSURANCE (PRODUTOS) ---")
        meta_opin_prod = extract_open_insurance_products()
    except Exception as e:
        print(f"OPIN WARNING: Falha ao baixar produtos: {e}")
        meta_opin_prod = None

    insurers: list[dict[str, Any]] = []

    # Match auditing
    matched: list[dict[str, Any]] = []
    low_conf: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []

    for ses_id, it in companies.items():
        premiums = float(it.get("premiums") or 0.0)
        claims = float(it.get("claims") or 0.0)

        loss_ratio = round((claims / premiums), 6) if premiums > 0 else 0.0
        segment = _infer_segment_fallback(premiums)

        name = (it.get("name") or f"SES_ENTIDADE_{ses_id}").strip()
        cnpj = _normalize_cnpj(it.get("cnpj"))

        # --- B3 (OPIN flag) ---
        is_opin = bool(cnpj and cnpj in opin_cnpjs)

        insurer_obj: dict[str, Any] = {
            "id": f"ses:{ses_id}",
            "name": name,
            "cnpj": cnpj,
            "segment": segment,
            "products": [],
            "flags": {"openInsuranceParticipant": is_opin},
            "data": {
                "premiums": round(premiums, 2),
                "claims": round(claims, 2),
                "lossRatio": loss_ratio,
                "complaints": None,
                "score": None,  # Será calculado abaixo
            },
        }

        # --- B2 (Consumidor.gov reputation) ---
        cg_matched = False

        if cnpj and cg_by_cnpj:
            metrics = cg_by_cnpj.get(cnpj)
            if isinstance(metrics, dict):
                block = {
                    "match": {
                        "consumer_key": f"cnpj:{cnpj}",
                        "matched_name": metrics.get("display_name"),
                        "score": 1.0,
                        "method": "cnpj",
                    },
                    "metrics": {
                        "complaints_total": metrics.get("complaints_total"),
                        "complaints_finalizadas": metrics.get("complaints_finalizadas"),
                        "responded_rate": metrics.get("responded_rate")
                        or metrics.get("response_rate"),
                        "resolution_rate": metrics.get("resolution_rate"),
                        "satisfaction_avg": metrics.get("satisfaction_avg"),
                        "avg_response_days": metrics.get("avg_response_days"),
                    },
                    "meta": {
                        "as_of": (cg_meta or {}).get("as_of"),
                        "window_months": (cg_meta or {}).get("window_months"),
                        "months": (cg_meta or {}).get("months"),
                    },
                }

                insurer_obj["data"].setdefault("reputation", {})
                insurer_obj["data"]["reputation"]["consumidorGov"] = block

                matched.append(
                    {
                        "insurer_id": insurer_obj["id"],
                        "insurer_name": name,
                        "consumer_key": f"cnpj:{cnpj}",
                        "consumer_name": metrics.get("display_name"),
                        "score": 1.0,
                    }
                )
                cg_matched = True

        if (not cg_matched) and matcher and cg_by_name:
            m = matcher.best(
                name,
                threshold=CONSUMIDOR_MATCH_THRESHOLD,
                min_margin=CONSUMIDOR_MATCH_MIN_MARGIN,
            )
            if m:
                metrics = cg_by_name.get(m.key)
                if isinstance(metrics, dict):
                    block = {
                        "match": {
                            "consumer_key": m.key,
                            "matched_name": metrics.get("display_name"),
                            "score": m.score,
                            "method": "token_jaccard_margin",
                        },
                        "metrics": {
                            "complaints_total": metrics.get("complaints_total"),
                            "complaints_finalizadas": metrics.get("complaints_finalizadas"),
                            "responded_rate": metrics.get("responded_rate")
                            or metrics.get("response_rate"),
                            "resolution_rate": metrics.get("resolution_rate"),
                            "satisfaction_avg": metrics.get("satisfaction_avg"),
                            "avg_response_days": metrics.get("avg_response_days"),
                        },
                        "meta": {
                            "as_of": (cg_meta or {}).get("as_of"),
                            "window_months": (cg_meta or {}).get("window_months"),
                            "months": (cg_meta or {}).get("months"),
                        },
                    }

                    insurer_obj["data"].setdefault("reputation", {})
                    insurer_obj["data"]["reputation"]["consumidorGov"] = block

                    rec = {
                        "insurer_id": insurer_obj["id"],
                        "insurer_name": name,
                        "consumer_key": m.key,
                        "consumer_name": metrics.get("display_name"),
                        "score": m.score,
                    }
                    matched.append(rec)

                    if float(m.score) < (CONSUMIDOR_MATCH_THRESHOLD + 0.03):
                        low_conf.append(rec)
                else:
                    unmatched.append(
                        {"insurer_id": insurer_obj["id"], "insurer_name": name}
                    )
            else:
                unmatched.append(
                    {"insurer_id": insurer_obj["id"], "insurer_name": name}
                )

        insurers.append(insurer_obj)

    # --- 5. INTELIGÊNCIA: Cálculo de Scores ---
    # Aplica Bühlmann-Straub, Bayes e Leis de Potência
    print("\n--- CALCULANDO SCORES (INTELIGÊNCIA) ---")
    try:
        insurers = run_scoring_pipeline(insurers)
        print("Scores calculados com sucesso.")
    except Exception as e:
        print(f"ERRO CRÍTICO no cálculo de scores: {e}")
        # Não aborta, mas os scores ficarão null

    # --- Match report (auditável) ---
    if cg_by_name or cg_by_cnpj:
        report = {
            "generatedAt": _now_iso(),
            "consumidorGov": {
                "as_of": (cg_meta or {}).get("as_of"),
                "window_months": (cg_meta or {}).get("window_months"),
                "months": (cg_meta or {}).get("months"),
                "threshold": CONSUMIDOR_MATCH_THRESHOLD,
                "min_margin": CONSUMIDOR_MATCH_MIN_MARGIN,
                "note": cg_err,
            },
            "stats": {
                "insurers_total": len(insurers),
                "matched": len(matched),
                "unmatched": len(unmatched),
                "low_confidence": len(low_conf),
            },
            "matched": matched[:500],
            "low_confidence": low_conf[:200],
            "unmatched": unmatched[:500],
        }
        _write_consumidor_match_report(report)

    # --- Sources section ---
    sources: dict[str, Any] = {
        "ses": {
            "dataset": "SUSEP SES Base Completa",
            "url": meta_ses.zip_url,
            "files": [meta_ses.cias_file, meta_ses.seguros_file],
        },
        "consumidorGov": {
            "dataset": "Consumidor.gov.br (Dados Abertos - Base Completa)",
            "url": "https://dados.mj.gov.br/",
            "as_of": (cg_meta or {}).get("as_of") if cg_meta else None,
            "window_months": (cg_meta or {}).get("window_months") if cg_meta else None,
            "note": cg_err or "B2 (reputação) — integrado via data/derived/consumidor_gov",
        },
        "opin": {
            "dataset": "OPIN Participants",
            "url": "https://data.directory.opinbrasil.com.br/participants",
            "note": opin_err or "B3 (Open Insurance participants) — flag por CNPJ",
        },
        "open_insurance_products": {
            "source": meta_opin_prod.source,
            "stats": meta_opin_prod.stats,
            "files": [
                meta_opin_prod.products_auto_file,
                meta_opin_prod.products_life_file,
                meta_opin_prod.products_home_file,
            ],
        }
        if meta_opin_prod
        else None,
    }

    return {
        "schemaVersion": "1.0.0",
        "generatedAt": _now_iso(),
        "period": {
            "type": "rolling_12m",
            "from": meta_ses.period_from,
            "to": meta_ses.period_to,
            "currency": "BRL",
        },
        "sources": sources,
        "taxonomy": _taxonomy(),
        "methodology": _methodology_stub(),
        "insurers": insurers,
        "meta": {
            "count": len(insurers),
            "disclaimer": "B1: métricas financeiras (SES). B2: reputação (Consumidor.gov). B3: status OPIN. B4: Produtos OPIN.",
        },
    }


def write_outputs() -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    API_V1.mkdir(parents=True, exist_ok=True)
    DATA_DERIVED.mkdir(parents=True, exist_ok=True)

    # 1) Gera payload
    payload = build_payload()

    # 2) Guardrails
    new_count = int(payload.get("meta", {}).get("count", 0))
    old_count = _read_existing_count(API_INSURERS)
    _guard_count_regression(new_count, old_count)

    # 3) FULL raw (gzip)
    with gzip.open(FULL_RAW_GZ, "wt", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))

    # 4) Snapshot histórico
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap_path = DATA_SNAPSHOTS / f"insurers_full_{day}.json.gz"
    with gzip.open(snap_path, "wt", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))

    # 5) API pública (SLIM) atômica
    slim_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    _atomic_write_text(API_INSURERS, slim_json)


if __name__ == "__main__":
    write_outputs()
    print("OK: generated api/v1/insurers.json (SLIM) and FULL archives")
