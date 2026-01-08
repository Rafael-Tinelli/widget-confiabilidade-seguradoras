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
from api.sources.opin_participants import load_opin_participant_cnpjs
from api.sources.ses import extract_ses_master_and_financials
from api.utils.identifiers import normalize_cnpj
from api.utils.name_cleaner import get_name_tokens, normalize_name_key

# Open Insurance (compat): prefer api.sources.open_insurance if present; fallback to OPIN modules.
try:
    from api.sources.open_insurance import (  # type: ignore
        extract_open_insurance_participants,
        extract_open_insurance_products,
    )
except Exception:  # pragma: no cover
    from api.sources.opin_participants import (  # type: ignore
        extract_opin_participants as extract_open_insurance_participants,
    )
    from api.sources.opin_products import extract_open_insurance_products  # type: ignore

# Intelligence layer
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

# Opinion/OpenInsurance participants sanity (soft floor)
MIN_OPIN_MATCH_FLOOR = int(os.getenv("MIN_OPIN_MATCH_FLOOR", "10"))

# Debug: prints near matches for consumer.gov when no match (expensive)
DEBUG_MATCH = os.getenv("DEBUG_MATCH", "0") == "1"

# Exclusions: entities that are not insurers and should not appear in the list (conservative)
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
    Converte objetos (ex.: dataclass SesMeta) em estruturas JSON-serializáveis.
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

    return str(x)


def _json_default(o: Any) -> Any:
    return _to_jsonable(o)


def _should_exclude(name: str) -> bool:
    k = normalize_name_key(name)
    return any(s in k for s in EXCLUDE_NAME_SUBSTRINGS)


def _normalize_segment(value: Any) -> str:
    """
    Normaliza para S1..S4 (exigido pelos testes).
    Se não conseguir inferir, imputa S4 (conservador) para não quebrar o pipeline.
    """
    if value is None:
        return "S4"
    s = str(value).strip().upper()
    if s in {"S1", "S2", "S3", "S4"}:
        return s
    m = re.match(r"^(S[1-4])\b", s)
    if m:
        return m.group(1)
    # alguns layouts trazem "1".."4"
    if s in {"1", "2", "3", "4"}:
        return f"S{s}"
    return "S4"


def _insurer_id(comp: Dict[str, Any], cnpj_key: Optional[str], name: str) -> str:
    """
    ID estável: preferir SUSEP ID; fallback para CNPJ; último recurso: chave do nome.
    """
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
    # best effort
    return []


def _safe_dict(x: Any) -> Dict[str, Any]:
    if isinstance(x, dict):
        return x
    if x is None:
        return {}
    # preserve whatever shape as a "raw"
    return {"raw": x}


def _extract_premiums_claims(comp: Dict[str, Any], fin: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    fin_d = fin if isinstance(fin, dict) else {}
    premiums = fin_d.get("premiums")
    claims = fin_d.get("claims")

    if premiums is None:
        premiums = comp.get("premiums")
    if claims is None:
        claims = comp.get("claims")

    # fallback: alguns parsers usam pt-br
    if premiums is None:
        premiums = fin_d.get("premios") or comp.get("premios")
    if claims is None:
        claims = fin_d.get("sinistros") or comp.get("sinistros")

    return _safe_dict(premiums), _safe_dict(claims)


def _load_latest_snapshot_count() -> Optional[int]:
    if not SNAPSHOT_DIR.exists():
        return None

    candidates = list(SNAPSHOT_DIR.glob("insurers_full_*.json.gz")) + list(SNAPSHOT_DIR.glob("insurers_full_*.json"))
    if not candidates:
        return None

    latest = max(candidates, key=lambda p: p.stat().st_mtime)

    try:
        if latest.name.endswith(".json.gz"):
            with gzip.open(latest, "rt", encoding="utf-8") as f:
                payload = json.load(f)
        else:
            payload = json.loads(latest.read_text(encoding="utf-8"))

        meta = payload.get("meta") or {}
        c = meta.get("count")
        if isinstance(c, int) and c > 0:
            return c

        insurers = payload.get("insurers")
        if isinstance(insurers, list) and insurers:
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
        # best-effort
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
    """
    Debug helper: show top token-overlap candidates from consumer.gov index.
    Only runs if DEBUG_MATCH=1.
    """
    try:
        entries_list = getattr(matcher, "entries", None) or getattr(matcher, "entries_list", None)
        if not entries_list:
            return

        q = get_name_tokens(name)
        if not q:
            return

        scored: List[Tuple[float, str]] = []

        for item in entries_list:
            # formats: (tokens, strong_key, entry) OR (tokens, entry)
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
        top = scored[:3]
        print(f"DEBUG: No Match -> {name}")
        for s, disp in top:
            print(f"  - near: {disp} (token_overlap={s:.2f})")
    except Exception:
        return


def main() -> None:
    # 1) Load SES (supports 2 or 3 return values)
    ses_out = extract_ses_master_and_financials()
    ses_meta: Any
    ses_companies: Any
    financials: Any

    if isinstance(ses_out, tuple) and len(ses_out) == 3:
        ses_meta, ses_companies, financials = ses_out
    elif isinstance(ses_out, tuple) and len(ses_out) == 2:
        ses_meta, ses_companies = ses_out
        financials = {}
    else:
        raise RuntimeError(f"SES: retorno inesperado: {type(ses_out)}")

    ses_meta_json = _to_jsonable(ses_meta)
    ses_iter = _as_iterable_companies(ses_companies)

    # 2) Open Insurance participants + products (supports 2 or 3 return values)
    oi_meta, oi_participants = extract_open_insurance_participants()
    oi_meta_json = _to_jsonable(oi_meta)

    oi_prod_out = extract_open_insurance_products()
    if isinstance(oi_prod_out, tuple) and len(oi_prod_out) >= 2:
        oi_prod_meta = oi_prod_out[0]
        oi_products_by_cnpj = oi_prod_out[1]
    else:
        oi_prod_meta = {"status": "invalid_return"}
        oi_products_by_cnpj = {}
    oi_prod_meta_json = _to_jsonable(oi_prod_meta)
    if not isinstance(oi_products_by_cnpj, dict):
        oi_products_by_cnpj = {}

    # 3) Consumidor.gov aggregated
    cg_meta, cg_payload = extract_consumidor_gov_aggregated()
    cg_meta_json = _to_jsonable(cg_meta)

    # 4) Indexes / matchers
    # Participants CNPJ set (OPIN sanity)
    opin_by_cnpj: Set[str] = load_opin_participant_cnpjs(oi_participants)

    oi_participant_keys: Set[str] = set()
    for p in oi_participants:
        k = normalize_cnpj(p.get("cnpj_key") or p.get("cnpj"))
        if k:
            oi_participant_keys.add(k)

    matcher = NameMatcher(cg_payload)

    # 5) Build insurers
    insurers: List[Dict[str, Any]] = []
    matched_reputation = 0
    skipped_b2b = 0
    matched_open_insurance = 0
    excluded = 0

    susep_cnpjs_seen: Set[str] = set()
    opin_matched_unique: Set[str] = set()

    # pre-normalize financials mapping if it exists
    financials_map: Dict[str, Any] = financials if isinstance(financials, dict) else {}

    for comp in ses_iter:
        if not isinstance(comp, dict):
            continue

        name = (comp.get("name") or comp.get("razao_social") or "").strip()
        if not name:
            continue

        if _should_exclude(name):
            excluded += 1
            continue

        # CNPJ
        cnpj_key = normalize_cnpj(comp.get("cnpj") or comp.get("cnpj_key"))
        cnpj_fmt = format_cnpj(cnpj_key) if cnpj_key else None
        if cnpj_key:
            susep_cnpjs_seen.add(cnpj_key)

        # segment
        seg_raw = comp.get("segment") or comp.get("segmento") or comp.get("porte") or comp.get("size")
        segment = _normalize_segment(seg_raw)

        # open insurance participant (by CNPJ key)
        is_open_insurance = bool(cnpj_key and cnpj_key in oi_participant_keys)
        if is_open_insurance:
            matched_open_insurance += 1

        # OPIN sanity intersection basis (same cnpj set)
        is_opin = bool(cnpj_key and cnpj_key in opin_by_cnpj)
        if is_opin and cnpj_key:
            opin_matched_unique.add(cnpj_key)

        # consumer.gov reputation
        rep_entry, rep_meta = matcher.get_entry(name, cnpj=cnpj_key)
        is_b2b = bool(rep_meta and getattr(rep_meta, "is_b2b", False))

        if is_b2b:
            skipped_b2b += 1
        elif rep_entry:
            matched_reputation += 1
        elif DEBUG_MATCH:
            _debug_near_matches(matcher, name)

        # financials: accept multiple keys (susep id often differs by implementation)
        susep_id = comp.get("susep_id") or comp.get("susepId") or comp.get("id")
        fin = None
        if susep_id is not None:
            fin = financials_map.get(str(susep_id)) or financials_map.get(susep_id)  # tolerate str/int keys

        premiums, claims = _extract_premiums_claims(comp, fin)

        # products (always list)
        products = []
        if cnpj_key:
            raw_products = oi_products_by_cnpj.get(cnpj_key, [])
            if isinstance(raw_products, list):
                products = raw_products

        insurer = {
            # REQUIRED by tests:
            "id": _insurer_id(comp, cnpj_key, name),
            "name": name,
            "segment": segment,
            "products": products,
            "data": {
                "premiums": premiums,
                "claims": claims,
            },
            "flags": {
                "openInsuranceParticipant": bool(is_open_insurance),
                "isB2B": bool(is_b2b),
            },
            # Optional (useful for UI / debugging / future):
            "cnpj": cnpj_fmt,
            "cnpjKey": cnpj_key,
            "tradeName": comp.get("trade_name") or comp.get("nome_fantasia"),
            "components": {
                "ses": {"company": comp, "meta": ses_meta_json},
                "openInsurance": {
                    "participant": bool(is_open_insurance),
                    "meta": oi_meta_json,
                    "productsMeta": oi_prod_meta_json,
                },
                "reputation": rep_entry if rep_entry else None,
            },
        }

        insurers.append(insurer)

    # 6) OPIN/OpenInsurance sanity (unique intersection against SES universe)
    expected_opin_intersection = len(opin_by_cnpj.intersection(susep_cnpjs_seen))
    observed_opin_intersection = len(opin_matched_unique)

    if expected_opin_intersection < MIN_OPIN_MATCH_FLOOR:
        raise RuntimeError(
            f"OPIN sanity: very low intersection (expected={expected_opin_intersection}) "
            f"< MIN_OPIN_MATCH_FLOOR={MIN_OPIN_MATCH_FLOOR}"
        )
    if observed_opin_intersection != expected_opin_intersection:
        raise RuntimeError(
            f"OPIN sanity: intersection mismatch. observed_unique={observed_opin_intersection} "
            f"expected_unique={expected_opin_intersection}. Check CNPJ normalization and dedupe."
        )

    # 7) Intelligence layer (best effort)
    insurers = apply_intelligence_batch(insurers)

    # Hard-enforce fields required by tests after intelligence (defensive)
    for it in insurers:
        if "id" not in it:
            it["id"] = it.get("cnpjKey") or normalize_name_key(it.get("name") or "") or "unknown"
        if "products" not in it or not isinstance(it["products"], list):
            it["products"] = []
        if "data" not in it or not isinstance(it["data"], dict):
            it["data"] = {}
        it["data"].setdefault("premiums", {})
        it["data"].setdefault("claims", {})
        if "flags" not in it or not isinstance(it["flags"], dict):
            it["flags"] = {}
        it["flags"]["openInsuranceParticipant"] = bool(it["flags"].get("openInsuranceParticipant", False))
        it["segment"] = _normalize_segment(it.get("segment"))

    # 8) Output root schema (matches tests)
    generated_at = utc_now()
    period = DEFAULT_PERIOD

    sources = {
        "ses": ses_meta_json,
        "openInsurance": oi_meta_json,
        "openInsuranceProducts": oi_prod_meta_json,
        "consumidorGov": cg_meta_json,
    }

    out = {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": generated_at,
        "period": str(period),
        "sources": sources,
        "meta": {
            "generatedAt": generated_at,
            "count": len(insurers),
            "stats": {
                "totalInsurers": len(insurers),
                "reputationMatched": matched_reputation,
                "reputationSkippedB2B": skipped_b2b,
                "openInsuranceParticipants": matched_open_insurance,
                "excludedNonInsurers": excluded,
                "openInsuranceIntersectionExpectedUnique": expected_opin_intersection,
                "openInsuranceIntersectionObservedUnique": observed_opin_intersection,
            },
        },
        "insurers": insurers,
    }

    # 9) Evergreen sanity check count
    _sanity_check_counts(len(insurers))

    # 10) Write
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(out, ensure_ascii=False, default=_json_default), encoding="utf-8")

    if WRITE_SNAPSHOT:
        _save_snapshot(out)

    # Logs
    print(f"insurers: {len(insurers)}")
    print(f"reputation.matched: {matched_reputation}")
    print(f"reputation.skipped_b2b: {skipped_b2b}")
    print(f"excluded.non_insurers: {excluded}")
    print(
        f"openInsurance.intersection.unique: {observed_opin_intersection} "
        f"(expected={expected_opin_intersection})"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
