# api/build_insurers.py
from __future__ import annotations

import gzip
import json
import os
import sys
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Set

from api.matching.consumidor_gov_match import NameMatcher, format_cnpj
from api.utils.identifiers import normalize_cnpj
from api.utils.name_cleaner import normalize_name_key, get_name_tokens
from api.sources.opin_participants import (
    extract_opin_participants,
    load_opin_participant_cnpjs,
)
from api.sources.opin_products import extract_open_insurance_products
from api.sources.ses import extract_ses_master_and_financials
from api.sources.consumidor_gov_agg import extract_consumidor_gov_aggregated

# Intelligence layer
try:
    from api.intelligence import apply_intelligence_batch
except Exception:  # pragma: no cover
    from api.intelligence.apply_intelligence import apply_intelligence_batch  # type: ignore


OUTPUT_FILE = Path("api/v1/insurers.json")
SNAPSHOT_DIR = Path("data/snapshots")
SCHEMA_VERSION = "1.0.0"

# Sanity checks (evergreen)
MIN_INSURERS_COUNT = int(os.getenv("MIN_INSURERS_COUNT", "0") or "0")
MAX_INSURERS_COUNT = int(os.getenv("MAX_INSURERS_COUNT", "0") or "0")
MAX_COUNT_DROP_PCT = float(os.getenv("MAX_COUNT_DROP_PCT", "0.20"))

# Opinion participants sanity
MIN_OPIN_MATCH_FLOOR = int(os.getenv("MIN_OPIN_MATCH_FLOOR", "10"))

# Debug
DEBUG_MATCH = os.getenv("DEBUG_MATCH", "0") == "1"

# Exclusions
EXCLUDE_NAME_SUBSTRINGS = {
    "ibracor",
    "corretora",
    "corretor",
    "corretagem",
    "broker",
}

WRITE_SNAPSHOT = os.getenv("WRITE_SNAPSHOT", "1") == "1"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _to_jsonable(x: Any) -> Any:
    """
    Converte objetos de meta (ex.: SesMeta) em estruturas JSON-serializáveis.
    Mantém dict/list/str/int/float/bool/None como estão.
    """
    if x is None:
        return None
    if isinstance(x, (str, int, float, bool)):
        return x
    if isinstance(x, dict):
        return {str(k): _to_jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, set)):
        return [_to_jsonable(v) for v in x]

    # dataclasses
    try:
        if is_dataclass(x):
            return _to_jsonable(asdict(x))
    except Exception:
        pass

    # pydantic v2 / v1
    if hasattr(x, "model_dump"):
        try:
            return _to_jsonable(x.model_dump())
        except Exception:
            pass
    if hasattr(x, "dict"):
        try:
            return _to_jsonable(x.dict())
        except Exception:
            pass

    # namedtuple-like
    if hasattr(x, "_asdict"):
        try:
            return _to_jsonable(x._asdict())
        except Exception:
            pass

    # generic object
    if hasattr(x, "__dict__"):
        try:
            return {k: _to_jsonable(v) for k, v in vars(x).items() if not str(k).startswith("_")}
        except Exception:
            pass

    # fallback
    return str(x)


def _json_default(o: Any) -> Any:
    return _to_jsonable(o)


def _should_exclude(name: str) -> bool:
    k = normalize_name_key(name)
    return any(s in k for s in EXCLUDE_NAME_SUBSTRINGS)


def _load_latest_snapshot_count() -> Optional[int]:
    if not SNAPSHOT_DIR.exists():
        return None

    candidates = list(SNAPSHOT_DIR.glob("insurers_full_*.json.gz")) + list(
        SNAPSHOT_DIR.glob("insurers_full_*.json")
    )
    if not candidates:
        return None

    latest = max(candidates, key=lambda p: p.stat().st_mtime)

    try:
        if latest.name.endswith(".json.gz"):
            with gzip.open(latest, "rt", encoding="utf-8") as f:
                payload = json.load(f)
        else:
            payload = json.loads(latest.read_text(encoding="utf-8"))

        # Leitura defensiva do count
        meta = payload.get("meta") or {}
        c = meta.get("count")
        if isinstance(c, int) and c > 0:
            return c
        
        # Fallback: contar lista se meta.count falhar
        insurers = payload.get("insurers")
        if isinstance(insurers, list):
            return len(insurers)
            
    except Exception:
        return None

    return None


def _save_snapshot(payload: dict) -> None:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out = SNAPSHOT_DIR / f"insurers_full_{stamp}.json.gz"
    try:
        with gzip.open(out, "wt", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, default=_json_default)
    except Exception:
        pass


def _sanity_check_counts(count: int) -> None:
    prev_count = _load_latest_snapshot_count()
    if prev_count and prev_count > 0:
        min_allowed = int(prev_count * (1.0 - MAX_COUNT_DROP_PCT))
        if count < min_allowed:
            raise RuntimeError(
                f"SanityCheck: count caiu demais. Atual={count}, Prev={prev_count}, "
                f"MinAllowed={min_allowed}, MAX_COUNT_DROP_PCT={MAX_COUNT_DROP_PCT}"
            )
        return

    if MIN_INSURERS_COUNT and count < MIN_INSURERS_COUNT:
        raise RuntimeError(
            f"SanityCheck: count abaixo do mínimo. Atual={count}, MIN_INSURERS_COUNT={MIN_INSURERS_COUNT}"
        )
    if MAX_INSURERS_COUNT and count > MAX_INSURERS_COUNT:
        raise RuntimeError(
            f"SanityCheck: count acima do máximo. Atual={count}, MAX_INSURERS_COUNT={MAX_INSURERS_COUNT}"
        )


def _debug_near_matches(matcher: NameMatcher, name: str) -> None:
    try:
        entries_list = getattr(matcher, "entries", None) or getattr(matcher, "entries_list", None)
        if not entries_list:
            return

        q = get_name_tokens(name)
        if not q:
            return

        scored: list[tuple[float, str]] = []
        for item in entries_list:
            # Handle different matcher entry formats
            if len(item) == 3:
                db_tokens, _, entry = item
            else:
                db_tokens, entry = item

            inter = len(q.intersection(db_tokens))
            if inter == 0: continue
            denom = min(len(q), len(db_tokens))
            if denom <= 0: continue
            score = inter / denom
            if score <= 0: continue

            disp = (entry or {}).get("display_name") or (entry or {}).get("name") or ""
            if disp:
                scored.append((score, str(disp)))

        if not scored: return

        scored.sort(reverse=True)
        top = scored[:3]
        print(f"DEBUG: No Match -> {name}")
        for s, disp in top:
            print(f"  - near: {disp} (token_overlap={s:.2f})")
    except Exception:
        return


def main() -> None:
    # 1) Load sources
    
    # SES: Handle both 2-tuple and 3-tuple returns
    ses_out = extract_ses_master_and_financials()
    if isinstance(ses_out, tuple) and len(ses_out) == 3:
        ses_meta, ses_companies, financials = ses_out
    elif isinstance(ses_out, tuple) and len(ses_out) == 2:
        ses_meta, ses_companies = ses_out
        financials = {}
    else:
        raise RuntimeError(f"SES: retorno inesperado: {type(ses_out)}")

    # Convert SES Meta to JSON-safe dict
    ses_meta_json = _to_jsonable(ses_meta)

    # Handle list vs dict company structure
    ses_iter = ses_companies.values() if isinstance(ses_companies, dict) else ses_companies

    # OPIN
    opin_meta, opin_participants = extract_opin_participants()
    # Reuse for "OpenInsurance" meta to avoid duplication
    oi_participants = opin_participants 
    
    oi_prod_meta, _oi_products = extract_open_insurance_products()
    cg_meta, cg_payload = extract_consumidor_gov_aggregated()

    # Convert metas to JSON-safe dicts
    opin_meta_json = _to_jsonable(opin_meta)
    oi_prod_meta_json = _to_jsonable(oi_prod_meta)
    cg_meta_json = _to_jsonable(cg_meta)

    # 2) Prepare matchers
    opin_by_cnpj: Set[str] = load_opin_participant_cnpjs(opin_participants)
    
    oi_participant_keys: Set[str] = set()
    for p in oi_participants:
        k = normalize_cnpj(p.get("cnpj_key") or p.get("cnpj"))
        if k: oi_participant_keys.add(k)

    matcher = NameMatcher(cg_payload)

    # 3) Build insurers
    insurers: list[dict] = []
    matched_reputation = 0
    skipped_b2b = 0
    matched_opin = 0
    excluded = 0

    susep_cnpjs_seen: Set[str] = set()
    opin_matched_unique: Set[str] = set()

    for comp in ses_iter:
        name = (comp.get("name") or comp.get("razao_social") or "").strip()
        if not name: continue

        if _should_exclude(name):
            excluded += 1
            continue

        cnpj_key = normalize_cnpj(comp.get("cnpj") or comp.get("cnpj_key"))
        cnpj_fmt = format_cnpj(cnpj_key) if cnpj_key else None

        if cnpj_key:
            susep_cnpjs_seen.add(cnpj_key)

        is_opin = bool(cnpj_key and cnpj_key in opin_by_cnpj)
        if is_opin:
            matched_opin += 1
            opin_matched_unique.add(cnpj_key)

        is_open_insurance = bool(cnpj_key and cnpj_key in oi_participant_keys)

        rep_entry, rep_meta = matcher.get_entry(name, cnpj=cnpj_key)
        is_b2b = bool(rep_meta and getattr(rep_meta, "is_b2b", False))

        if is_b2b:
            skipped_b2b += 1
        elif rep_entry:
            matched_reputation += 1
        elif DEBUG_MATCH:
            _debug_near_matches(matcher, name)

        fin = financials.get(comp.get("susep_id")) if isinstance(financials, dict) else None

        insurers.append({
            "susepId": comp.get("susep_id"),
            "cnpj": cnpj_fmt,
            "cnpjKey": cnpj_key,
            "name": name,
            "tradeName": comp.get("trade_name") or comp.get("nome_fantasia"),
            "segment": comp.get("segment"),
            "components": {
                "ses": {"company": comp, "meta": ses_meta_json},
                "financials": fin,
                "openInsurance": {
                    "participant": is_open_insurance,
                    "meta": opin_meta_json,
                    "productsMeta": oi_prod_meta_json,
                },
                "reputation": rep_entry if rep_entry else None,
            },
            "flags": {
                "opinParticipant": is_opin,
                "openInsuranceParticipant": is_open_insurance,
                "isB2B": is_b2b,
            },
        })

    # 4) Sanity Checks
    expected_opin_intersection = len(opin_by_cnpj.intersection(susep_cnpjs_seen))
    observed_opin_intersection = len(opin_matched_unique)

    if expected_opin_intersection < MIN_OPIN_MATCH_FLOOR:
        raise RuntimeError(f"OPIN sanity: very low intersection {expected_opin_intersection} < {MIN_OPIN_MATCH_FLOOR}")

    if observed_opin_intersection != expected_opin_intersection:
        raise RuntimeError(f"OPIN sanity: mismatch observed={observed_opin_intersection} expected={expected_opin_intersection}")

    # 5) Intelligence
    insurers = apply_intelligence_batch(insurers)

    if isinstance(opin_meta_json, dict):
        opin_meta_json = {
            **opin_meta_json,
            "integrity": {
                "expectedIntersectionUnique": expected_opin_intersection,
                "observedIntersectionUnique": observed_opin_intersection,
            },
        }

    # 6) Output
    out = {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": utc_now(),
        "period": "2024",
        "meta": {
            "generatedAt": utc_now(),
            "count": len(insurers),
            "stats": {
                "totalInsurers": len(insurers),
                "reputationMatched": matched_reputation,
                "reputationSkippedB2B": skipped_b2b,
                "openInsuranceParticipants": matched_opin,
                "excludedNonInsurers": excluded,
            },
            "sources": {
                "ses": ses_meta_json,
                "opin": opin_meta_json,
                "openInsurance": opin_meta_json,
                "openInsuranceProducts": oi_prod_meta_json,
                "consumidorGov": cg_meta_json,
            },
            "reputation": {
                "matched": matched_reputation,
                "skippedB2B": skipped_b2b,
            },
            "filters": {"excludedNonInsurers": excluded},
        },
        "insurers": insurers,
    }

    _sanity_check_counts(len(insurers))

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(out, ensure_ascii=False, default=_json_default), encoding="utf-8")

    if WRITE_SNAPSHOT:
        _save_snapshot(out)

    print(f"insurers: {len(insurers)}")
    print(f"reputation.matched: {matched_reputation}")
    print(f"reputation.skipped_b2b: {skipped_b2b}")
    print(f"excluded.non_insurers: {excluded}")
    print(f"opin.intersection.unique: {observed_opin_intersection}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
