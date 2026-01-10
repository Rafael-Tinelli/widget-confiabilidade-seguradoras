# api/build_insurers.py
from __future__ import annotations

import copy
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
        from api.sources.open_insurance import (  # type: ignore
            load_open_insurance_participant_cnpjs as load_opin_participant_cnpjs,
        )
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
MAX_COUNT_DROP_PCT = float(os.getenv("MAX_COUNT_DROP_PCT", "0.60"))

# Opinion/OpenInsurance sanity (soft floor by default)
MIN_OPIN_MATCH_FLOOR = int(os.getenv("MIN_OPIN_MATCH_FLOOR", "10"))
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
    if m:
        return m.group(1)
    if s in {"1", "2", "3", "4"}:
        return f"S{s}"
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
    if not isinstance(s, str):
        return None
    t = s.strip()
    if not t:
        return None
    t = re.sub(r"[^\d,\.\-\+eE]", "", t)
    if not t:
        return None
    if "." in t and "," in t:
        t = t.replace(".", "").replace(",", ".")
    elif "," in t and "." not in t:
        t = t.replace(",", ".")
    try:
        return float(t)
    except Exception:
        return None


def _coerce_float(x: Any) -> float:
    if x is None:
        return 0.0
    if isinstance(x, bool):
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
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
            "net_worth", "netWorth", "patrimonio_liquido", "patrimonioLiquido", "pl",
        )
        for k in total_keys:
            if k in x and x.get(k) is not None:
                return _coerce_float(x.get(k))
        acc = 0.0
        for v in x.values():
            acc += _coerce_float(v)
        return acc
    if isinstance(x, (list, tuple, set)):
        return sum(_coerce_float(v) for v in x)
    return 0.0


def _pick_first(d: Dict[str, Any], keys: Iterable[str]) -> Any:
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return None


def _extract_raw_premiums_claims(comp: Dict[str, Any], fin: Any) -> Tuple[Any, Any]:
    fin_d = fin if isinstance(fin, dict) else {}

    premiums_raw = _pick_first(fin_d, ("premiums", "premios", "premiums_total", "premios_total", "premio_emitido", "premioEmitido"))
    if premiums_raw is None:
        premiums_raw = _pick_first(comp, ("premiums", "premios", "premiums_total", "premios_total", "premio_emitido", "premioEmitido"))

    claims_raw = _pick_first(fin_d, ("claims", "sinistros", "claims_total", "sinistros_total", "sinistro_total"))
    if claims_raw is None:
        claims_raw = _pick_first(comp, ("claims", "sinistros", "claims_total", "sinistros_total", "sinistro_total"))

    return premiums_raw, claims_raw


def _extract_raw_net_worth(comp: Dict[str, Any], fin: Any) -> Any:
    fin_d = fin if isinstance(fin, dict) else {}
    nw = _pick_first(fin_d, ("net_worth", "netWorth", "patrimonio_liquido", "patrimonioLiquido", "pl"))
    if nw is None:
        nw = _pick_first(comp, ("net_worth", "netWorth", "patrimonio_liquido", "patrimonioLiquido", "pl"))
    return nw


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
    if MAX_INSURERS_COUNT and count > MAX_INSURERS_COUNT:
        raise RuntimeError(f"SanityCheck: count acima do máximo. Atual={count}, Max={MAX_INSURERS_COUNT}")


def _debug_near_matches(matcher: NameMatcher, name: str) -> None:
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


def _derive_trade_name_from_legal(name: str) -> Optional[str]:
    if not name or not str(name).strip():
        return None

    generic = {
        "cia", "companhia", "comp", "sociedade",
        "seguros", "seguro", "seguradora", "resseguros", "resseguradora",
        "capitalizacao", "previdencia", "vida", "saude",
        "brasil", "brasileira", "gerais",
        "sa", "s", "a", "ltda", "inc", "corp", "group", "holding",
        "do", "de", "da", "e", "participacoes",
    }

    toks = [t for t in get_name_tokens(str(name)) if t and t not in generic]
    if not toks:
        return None

    cand = " ".join(toks).strip()
    if not cand:
        return None

    # evita devolver o mesmo nome só “reformatado”
    if normalize_name_key(cand) == normalize_name_key(name):
        return None

    return cand


def _get_financials(financials_map: Dict[str, Any], comp: Dict[str, Any], ins_id: str, cnpj_key: Optional[str]) -> Any:
    candidates: List[Any] = []
    for k in ("susep_id", "susepId", "susepID", "id"):
        v = comp.get(k)
        if v is not None and str(v).strip():
            candidates.append(v)
            candidates.append(str(v))
    if cnpj_key:
        candidates.append(cnpj_key)
        cnpj_fmt = format_cnpj(cnpj_key)
        if cnpj_fmt:
            candidates.append(cnpj_fmt)
    candidates.append(ins_id)

    for key in candidates:
        if key is None:
            continue
        if key in financials_map:
            return financials_map[key]
        sk = str(key)
        if sk in financials_map:
            return financials_map[sk]
    return None


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
    if not isinstance(oi_products_by_cnpj, dict):
        oi_products_by_cnpj = {}

    cg_meta, cg_payload = extract_consumidor_gov_aggregated()
    print(
        "consumidor.gov meta:",
        {
            "status": cg_meta.get("status") if isinstance(cg_meta, dict) else None,
            "source_file": cg_meta.get("source_file") if isinstance(cg_meta, dict) else None,
            "companies": cg_meta.get("companies") if isinstance(cg_meta, dict) else None,
        },
    )
    cg_meta_json = _to_jsonable(cg_meta)

    opin_by_cnpj: Set[str] = load_opin_participant_cnpjs(oi_participants)

    oi_participant_keys: Set[str] = set()
    for p in oi_participants:
        k = normalize_cnpj(p.get("cnpj_key") or p.get("cnpj"))
        if k:
            oi_participant_keys.add(k)

    matcher = NameMatcher(cg_payload)
    reputation_audit_by_id: Dict[str, Dict[str, Any]] = {}
    unique_brands_matched: Set[str] = set()

    insurers: List[Dict[str, Any]] = []
    matched_reputation = 0
    skipped_b2b = 0
    matched_open_insurance = 0
    excluded = 0
    fin_found = 0

    susep_cnpjs_seen: Set[str] = set()
    opin_matched_unique: Set[str] = set()

    for comp in ses_iter:
        if not isinstance(comp, dict):
            continue

        name = (comp.get("name") or comp.get("razao_social") or "").strip()
        if not name:
            continue

        if _should_exclude(name):
            excluded += 1
            continue

        cnpj_key = normalize_cnpj(comp.get("cnpj") or comp.get("cnpj_key"))
        ins_id = _insurer_id(comp, cnpj_key, name)
        cnpj_fmt = format_cnpj(cnpj_key) if cnpj_key else None

        if cnpj_key:
            susep_cnpjs_seen.add(cnpj_key)

        segment = _normalize_segment(comp.get("segment") or comp.get("segmento") or comp.get("porte"))

        is_open_insurance = bool(cnpj_key and cnpj_key in oi_participant_keys)
        if is_open_insurance:
            matched_open_insurance += 1

        is_opin = bool(cnpj_key and cnpj_key in opin_by_cnpj)
        if is_opin and cnpj_key:
            opin_matched_unique.add(cnpj_key)

        trade_name = (comp.get("trade_name") or comp.get("nome_fantasia") or "").strip() or None
        derived_trade_name = None
        if not trade_name:
            derived_trade_name = _derive_trade_name_from_legal(name)
            if derived_trade_name:
                trade_name = derived_trade_name

        rep_entry, rep_meta = matcher.get_entry(
            name=name,
            trade_name=trade_name,
            cnpj=cnpj_key,
        )

        # FIX: B2B deve contar mesmo quando rep_entry=None (b2b_skip)
        is_b2b_flag = bool(getattr(rep_meta, "is_b2b", False))

        if is_b2b_flag:
            skipped_b2b += 1
            rep_entry = None
        elif rep_entry:
            stats = rep_entry.get("statistics") or {}
            has_signal = any(
                int(stats.get(k) or 0) > 0
                for k in ("complaintsCount", "total_claims", "resolvedCount", "respondedCount")
            )
            if has_signal:
                matched_reputation += 1
            else:
                rep_entry = None
        elif DEBUG_MATCH:
            _debug_near_matches(matcher, name)

        # Snapshot audit (não vai para o JSON público)
        if getattr(rep_meta, "method", None) != "no_match" or rep_entry or is_b2b_flag:
            reputation_audit_by_id[ins_id] = {
                "method": getattr(rep_meta, "method", None),
                "score": getattr(rep_meta, "score", None),
                "query": getattr(rep_meta, "query", None),
                "matchedName": getattr(rep_meta, "matched_name", None),
                "matchedCnpj": getattr(rep_meta, "matched_cnpj", None),
                "isB2B": bool(getattr(rep_meta, "is_b2b", False)),
                "derivedTradeName": derived_trade_name,
            }

        if rep_entry and (not is_b2b_flag) and getattr(rep_meta, "matched_name", None):
            unique_brands_matched.add(str(getattr(rep_meta, "matched_name")))

        fin = _get_financials(financials_map, comp, ins_id, cnpj_key)
        if fin is not None:
            fin_found += 1

        premiums_raw, claims_raw = _extract_raw_premiums_claims(comp, fin)
        premiums = _coerce_float(premiums_raw)
        claims = _coerce_float(claims_raw)

        net_worth_raw = _extract_raw_net_worth(comp, fin)
        net_worth = _coerce_float(net_worth_raw)

        products: List[Any] = []
        if cnpj_key:
            raw_products = oi_products_by_cnpj.get(cnpj_key, [])
            if isinstance(raw_products, list):
                products = raw_products

        insurers.append(
            {
                "id": ins_id,
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
                "tradeName": trade_name,
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
            }
        )

    # OPIN sanity (soft por padrão)
    expected_opin_intersection = len(opin_by_cnpj.intersection(susep_cnpjs_seen))
    observed_opin_intersection = len(opin_matched_unique)

    if expected_opin_intersection < MIN_OPIN_MATCH_FLOOR:
        msg = f"OPIN sanity: low intersection (expected={expected_opin_intersection} < floor={MIN_OPIN_MATCH_FLOOR})"
        if STRICT_OPIN_SANITY:
            raise RuntimeError(msg)
        print(f"WARN: {msg}")

    if observed_opin_intersection != expected_opin_intersection:
        msg = (
            f"OPIN sanity: mismatch. observed={observed_opin_intersection} "
            f"expected={expected_opin_intersection}."
        )
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

    # Fail-fast mínimo (antes de escrever)
    if MIN_INSURERS_COUNT > 0 and len(insurers) < MIN_INSURERS_COUNT:
        raise RuntimeError(f"Sanity Check Falhou: {len(insurers)} seguradoras < min {MIN_INSURERS_COUNT}")

    universe_count = len(ses_companies) if isinstance(ses_companies, (dict, list)) else None
    _sanity_check_counts(len(insurers), universe_count=universe_count)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(out, ensure_ascii=False, default=_json_default), encoding="utf-8")

    if WRITE_SNAPSHOT:
        try:
            SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            snap = SNAPSHOT_DIR / f"insurers_full_{stamp}.json.gz"

            out_snap = copy.deepcopy(out)
            for ins in out_snap.get("insurers", []):
                audit = reputation_audit_by_id.get(ins.get("id"))
                if audit:
                    ins["reputationMatch"] = audit

            with gzip.open(snap, "wt", encoding="utf-8") as f:
                json.dump(out_snap, f, ensure_ascii=False, default=_json_default)
        except Exception:
            pass

    print(f"insurers: {len(insurers)}")
    print(f"financials.found: {fin_found}")
    print(f"reputation.matched: {matched_reputation}")
    print(f"reputation.unique_brands: {len(unique_brands_matched)}")
    print(f"reputation.skipped_b2b: {skipped_b2b}")
    print(f"excluded.non_insurers: {excluded}")
    print(f"openInsurance.intersection.unique: {observed_opin_intersection}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
