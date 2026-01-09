# api/build_insurers.py
from __future__ import annotations

import gzip
import json
import os
import re
import sys
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from api.matching.consumidor_gov_match import NameMatcher, format_cnpj
from api.sources.consumidor_gov_agg import extract_consumidor_gov_aggregated
from api.sources.ses import extract_ses_master_and_financials
from api.utils.identifiers import normalize_cnpj
from api.utils.name_cleaner import get_name_tokens, normalize_name_key

# Open Insurance Imports
try:
    from api.sources.open_insurance import (  # type: ignore
        extract_open_insurance_participants,
        extract_open_insurance_products,
    )
except Exception:  # pragma: no cover
    from api.sources.opin_participants import (  # type: ignore
        extract_opin_participants as extract_open_insurance_participants,
        load_opin_participant_cnpjs,
    )
    from api.sources.opin_products import extract_open_insurance_products  # type: ignore
else:
    try:
        from api.sources.open_insurance import load_open_insurance_participant_cnpjs as load_opin_participant_cnpjs  # type: ignore
    except Exception:  # pragma: no cover
        from api.sources.opin_participants import load_opin_participant_cnpjs  # type: ignore

# Intelligence
try:
    from api.intelligence import apply_intelligence_batch
except Exception:  # pragma: no cover
    from api.intelligence.apply_intelligence import apply_intelligence_batch  # type: ignore


OUTPUT_FILE = Path("api/v1/insurers.json")
SNAPSHOT_DIR = Path("data/snapshots")

SCHEMA_VERSION = "1.0.0"
DEFAULT_PERIOD = os.getenv("DATA_PERIOD", os.getenv("PERIOD", "2024"))

# Evergreen sanity checks
MIN_INSURERS_COUNT = int(os.getenv("MIN_INSURERS_COUNT", "0") or "0")
MAX_INSURERS_COUNT = int(os.getenv("MAX_INSURERS_COUNT", "0") or "0")
MAX_COUNT_DROP_PCT = float(os.getenv("MAX_COUNT_DROP_PCT", "0.20"))

# Opinion/OpenInsurance sanity (soft floor by default)
MIN_OPIN_MATCH_FLOOR = int(os.getenv("MIN_OPIN_MATCH_FLOOR", "10"))
# FIX: Flag para transformar WARN em ERROR
STRICT_OPIN_SANITY = os.getenv("STRICT_OPIN_SANITY", "0") == "1"

DEBUG_MATCH = os.getenv("DEBUG_MATCH", "0") == "1"

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
    if x is None:
        return None
    if isinstance(x, (str, int, float, bool)):
        return x
    if isinstance(x, dict):
        return {str(k): _to_jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, set)):
        return [_to_jsonable(v) for v in x]
    if is_dataclass(x):
        return _to_jsonable(asdict(x))
    if hasattr(x, "model_dump"):
        return _to_jsonable(x.model_dump())
    if hasattr(x, "dict"):
        return _to_jsonable(x.dict())
    if hasattr(x, "_asdict"):
        return _to_jsonable(x._asdict())
    if hasattr(x, "__dict__"):
        return {k: _to_jsonable(v) for k, v in vars(x).items() if not str(k).startswith("_")}
    return str(x)


def _json_default(o: Any) -> Any:
    return _to_jsonable(o)


def _should_exclude(name: str) -> bool:
    k = normalize_name_key(name)
    return any(s in k for s in EXCLUDE_NAME_SUBSTRINGS)


def _normalize_segment(value: Any) -> str:
    if value is None:
        return "S4"
    s = str(value).strip().upper()
    if s in {"S1", "S2", "S3", "S4"}:
        return s
    m = re.match(r"^(S[1-4])\b", s)
    if m: return m.group(1)
    if s in {"1", "2", "3", "4"}: return f"S{s}"
    return "S4"


def _insurer_id(comp: Dict[str, Any], cnpj_key: Optional[str], name: str) -> str:
    for k in ("id", "susepId", "susep_id", "susepID"):
        v = comp.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    if cnpj_key:
        return cnpj_key
    nk = normalize_name_key(name)
    return nk or f"unknown-{abs(hash(name))}"


def _as_iterable_companies(ses_companies: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(ses_companies, dict):
        return ses_companies.values()
    if isinstance(ses_companies, list):
        return ses_companies
    return []


def _parse_number_ptbr(s: str) -> Optional[float]:
    if not isinstance(s, str): return None
    t = s.strip()
    if not t: return None
    t = re.sub(r"[^\d,\.\-\+eE]", "", t)
    if not t: return None
    if "." in t and "," in t:
        t = t.replace(".", "").replace(",", ".")
    elif "," in t and "." not in t:
        t = t.replace(",", ".")
    try:
        return float(t)
    except Exception:
        return None


def _coerce_float(x: Any) -> float:
    if x is None: return 0.0
    if isinstance(x, bool): return 0.0
    if isinstance(x, (int, float)): return float(x)
    if isinstance(x, str):
        v = _parse_number_ptbr(x)
        return float(v) if v is not None else 0.0
    if isinstance(x, dict):
        total_keys = (
            "total", "valor_total", "value", "amount", "sum",
            "premiums_total", "premiumsTotal", "premio_total", "premios_total",
            "premios", "premio", "premio_emitido", "premioEmitido",
            "claims_total", "claimsTotal", "sinistro_total", "sinistros_total",
            "sinistros", "sinistro",
        )
        for k in total_keys:
            if k in x: return _coerce_float(x.get(k))
        acc = 0.0
        for v in x.values(): acc += _coerce_float(v)
        return acc
    if isinstance(x, (list, tuple, set)):
        return sum(_coerce_float(v) for v in x)
    return 0.0


def _extract_raw_premiums_claims(comp: Dict[str, Any], fin: Any) -> Tuple[Any, Any]:
    fin_d = fin if isinstance(fin, dict) else {}
    premiums_raw = fin_d.get("premiums") or comp.get("premiums")
    claims_raw = fin_d.get("claims") or comp.get("claims")
    if premiums_raw is None:
        premiums_raw = fin_d.get("premios") or comp.get("premios")
    if claims_raw is None:
        claims_raw = fin_d.get("sinistros") or comp.get("sinistros")
    return premiums_raw, claims_raw


def _load_latest_snapshot_count() -> Optional[int]:
    if not SNAPSHOT_DIR.exists(): return None
    candidates = list(SNAPSHOT_DIR.glob("insurers_full_*.json.gz")) + list(SNAPSHOT_DIR.glob("insurers_full_*.json"))
    if not candidates: return None
    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    try:
        if latest.name.endswith(".json.gz"):
            with gzip.open(latest, "rt", encoding="utf-8") as f:
                payload = json.load(f)
        else:
            payload = json.loads(latest.read_text(encoding="utf-8"))
        meta = payload.get("meta") or {}
        c = meta.get("count")
        if isinstance(c, int) and c > 0: return c
        insurers = payload.get("insurers")
        if isinstance(insurers, list) and insurers: return len(insurers)
    except Exception: return None
    return None


def _sanity_check_counts(count: int, universe_count: int | None = None) -> None:
    prev_count = _load_latest_snapshot_count()
    baseline = prev_count
    if baseline and universe_count and universe_count > 0:
        baseline = min(baseline, universe_count)
    if baseline and baseline > 0:
        min_allowed = int(baseline * (1.0 - MAX_COUNT_DROP_PCT))
        if count < min_allowed:
            raise RuntimeError(
                f"SanityCheck: count caiu demais. Atual={count}, Prev={prev_count}, "
                f"Universe={universe_count}, MinAllowed={min_allowed}"
            )
    if MIN_INSURERS_COUNT and count < MIN_INSURERS_COUNT:
        raise RuntimeError(f"SanityCheck: count abaixo do mínimo. Atual={count}, Min={MIN_INSURERS_COUNT}")


def _debug_near_matches(matcher: NameMatcher, name: str) -> None:
    # [FIX] Implementação completa restaurada para evitar erro de lint (unused import)
    try:
        entries_list = getattr(matcher, "entries", None) or getattr(matcher, "entries_list", None)
        if not entries_list:
            return

        q = get_name_tokens(name)
        if not q:
            return

        scored: List[Tuple[float, str]] = []
        for item in entries_list:
            if isinstance(item, (list, tuple)) and len(item) == 3:
                db_tokens, _, entry = item
            else:
                db_tokens, entry = item

            inter = len(q.intersection(db_tokens))
            if inter == 0:
                continue
            denom = min(len(q), len(db_tokens))
            if denom <= 0:
                continue
            score = inter / denom
            if score <= 0:
                continue

            disp = (entry or {}).get("display_name") or (entry or {}).get("name") or ""
            if disp:
                scored.append((score, str(disp)))

        if not scored:
            return

        scored.sort(reverse=True)
        for s, disp in scored[:3]:
            print(f"DEBUG: near {name} -> {disp} (token_overlap={s:.2f})")
    except Exception:
        return


def main() -> None:
    ses_out = extract_ses_master_and_financials()
    if isinstance(ses_out, tuple) and len(ses_out) == 3:
        ses_meta, ses_companies, financials = ses_out
    elif isinstance(ses_out, tuple) and len(ses_out) == 2:
        ses_meta, ses_companies = ses_out
        financials = {}
    else:
        raise RuntimeError(f"SES: retorno inesperado: {type(ses_out)}")

    ses_meta_json = _to_jsonable(ses_meta)
    ses_iter = _as_iterable_companies(ses_companies)
    financials_map: Dict[str, Any] = financials if isinstance(financials, dict) else {}

    oi_meta, oi_participants = extract_open_insurance_participants()
    oi_meta_json = _to_jsonable(oi_meta)

    oi_prod_out = extract_open_insurance_products()
    if isinstance(oi_prod_out, tuple) and len(oi_prod_out) >= 2:
        oi_prod_meta, oi_products_by_cnpj = oi_prod_out[0], oi_prod_out[1]
    else:
        oi_prod_meta, oi_products_by_cnpj = {"status": "invalid_return"}, {}
    oi_prod_meta_json = _to_jsonable(oi_prod_meta)
    if not isinstance(oi_products_by_cnpj, dict): oi_products_by_cnpj = {}

    cg_meta, cg_payload = extract_consumidor_gov_aggregated()
    cg_meta_json = _to_jsonable(cg_meta)

    opin_by_cnpj: Set[str] = load_opin_participant_cnpjs(oi_participants)
    oi_participant_keys: Set[str] = set()
    for p in oi_participants:
        k = normalize_cnpj(p.get("cnpj_key") or p.get("cnpj"))
        if k: oi_participant_keys.add(k)

    matcher = NameMatcher(cg_payload)

    insurers: List[Dict[str, Any]] = []
    matched_reputation = 0
    skipped_b2b = 0
    matched_open_insurance = 0
    excluded = 0

    susep_cnpjs_seen: Set[str] = set()
    opin_matched_unique: Set[str] = set()

    for comp in ses_iter:
        if not isinstance(comp, dict): continue
        name = (comp.get("name") or comp.get("razao_social") or "").strip()
        if not name: continue

        if _should_exclude(name):
            excluded += 1
            continue

        cnpj_key = normalize_cnpj(comp.get("cnpj") or comp.get("cnpj_key"))
        cnpj_fmt = format_cnpj(cnpj_key) if cnpj_key else None
        if cnpj_key: susep_cnpjs_seen.add(cnpj_key)

        segment = _normalize_segment(comp.get("segment") or comp.get("segmento") or comp.get("porte"))

        is_open_insurance = bool(cnpj_key and cnpj_key in oi_participant_keys)
        if is_open_insurance: matched_open_insurance += 1

        is_opin = bool(cnpj_key and cnpj_key in opin_by_cnpj)
        if is_opin and cnpj_key: opin_matched_unique.add(cnpj_key)

        # Lógica de Reputação e B2B (FIX: Prioriza exclusão B2B antes de stats)
        rep_entry, rep_meta = matcher.get_entry(name, cnpj=cnpj_key)
        
        # Tenta detectar se é B2B via metadados do Matcher
        is_b2b_flag = False
        if rep_meta:
            # Tolerância para diferentes retornos (dict ou objeto)
            if isinstance(rep_meta, dict):
                is_b2b_flag = bool(rep_meta.get("is_b2b"))
            else:
                is_b2b_flag = bool(getattr(rep_meta, "is_b2b", False))

        if rep_entry and is_b2b_flag:
            skipped_b2b += 1
            rep_entry = None # Anula match B2B
        elif rep_entry:
            # Só aceita se passou pelo filtro B2B e tem stats válidos
            stats = rep_entry.get("statistics") or {}
            has_signal = any(
                int(stats.get(k) or 0) > 0 
                for k in ("complaintsCount", "total_claims", "resolvedCount", "respondedCount")
            )
            if has_signal:
                matched_reputation += 1
            else:
                rep_entry = None # Descarta falso positivo sem dados
        elif DEBUG_MATCH:
            _debug_near_matches(matcher, name)

        # Montagem do objeto final
        susep_id = comp.get("susep_id") or comp.get("susepId") or comp.get("id")
        fin = None
        if susep_id is not None:
            fin = financials_map.get(str(susep_id)) or financials_map.get(susep_id)

        premiums_raw, claims_raw = _extract_raw_premiums_claims(comp, fin)
        premiums = _coerce_float(premiums_raw)
        claims = _coerce_float(claims_raw)
        net_worth_val = (fin or {}).get("net_worth") or comp.get("net_worth")
        net_worth = _coerce_float(net_worth_val)

        products: List[Any] = []
        if cnpj_key:
            raw_products = oi_products_by_cnpj.get(cnpj_key, [])
            if isinstance(raw_products, list): products = raw_products

        insurers.append({
            "id": _insurer_id(comp, cnpj_key, name),
            "name": name,
            "segment": segment,
            "products": products,
            "data": {
                "premiums": premiums,
                "claims": claims,
                "net_worth": net_worth,
                "premiumsRaw": _to_jsonable(premiums_raw),
                "claimsRaw": _to_jsonable(claims_raw),
            },
            "flags": {
                "openInsuranceParticipant": bool(is_open_insurance),
                "isB2B": bool(is_b2b_flag),
            },
            "cnpj": cnpj_fmt,
            "cnpjKey": cnpj_key,
            "tradeName": comp.get("trade_name") or comp.get("nome_fantasia"),
            "reputation": rep_entry,
            "components": {
                "ses": {"company": comp, "meta": ses_meta_json},
                "openInsurance": {
                    "participant": bool(is_open_insurance),
                    "meta": oi_meta_json,
                    "productsMeta": oi_prod_meta_json,
                },
                "reputation": rep_entry,
                "financials": _to_jsonable(fin),
            },
        })

    # 7) OPIN Sanity (FIX: Soft fail por padrão)
    expected_opin_intersection = len(opin_by_cnpj.intersection(susep_cnpjs_seen))
    observed_opin_intersection = len(opin_matched_unique)

    if expected_opin_intersection < MIN_OPIN_MATCH_FLOOR:
        msg = f"OPIN sanity: low intersection (expected={expected_opin_intersection} < floor={MIN_OPIN_MATCH_FLOOR})"
        if STRICT_OPIN_SANITY:
            raise RuntimeError(msg)
        print(f"WARN: {msg}")

    if observed_opin_intersection != expected_opin_intersection:
        msg = (f"OPIN sanity: mismatch. observed={observed_opin_intersection} "
               f"expected={expected_opin_intersection}.")
        if STRICT_OPIN_SANITY:
            raise RuntimeError(msg)
        print(f"WARN: {msg}")

    insurers = apply_intelligence_batch(insurers)

    generated_at = utc_now()
    out = {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": generated_at,
        "period": str(DEFAULT_PERIOD),
        "sources": {
            "ses": ses_meta_json,
            "openInsurance": oi_meta_json,
            "openInsuranceProducts": oi_prod_meta_json,
            "consumidorGov": cg_meta_json,
        },
        "meta": {"generatedAt": generated_at, "count": len(insurers)},
        "insurers": insurers,
    }

    universe_count = len(ses_companies) if isinstance(ses_companies, (dict, list)) else None
    _sanity_check_counts(len(insurers), universe_count=universe_count)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(out, ensure_ascii=False, default=_json_default), encoding="utf-8")

    if WRITE_SNAPSHOT:
        SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        snap = SNAPSHOT_DIR / f"insurers_full_{stamp}.json.gz"
        try:
            with gzip.open(snap, "wt", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, default=_json_default)
        except Exception: pass

    print(f"insurers: {len(insurers)}")
    print(f"reputation.matched: {matched_reputation}")
    print(f"reputation.skipped_b2b: {skipped_b2b}")
    print(f"excluded.non_insurers: {excluded}")
    print(f"openInsurance.intersection.unique: {observed_opin_intersection}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
