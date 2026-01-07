# api/build_insurers.py
from __future__ import annotations

import gzip
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from api.matching.consumidor_gov_match import NameMatcher, format_cnpj
from api.utils.identifiers import normalize_cnpj
from api.utils.name_cleaner import normalize_name_key
from api.sources.opin_participants import (
    extract_opin_participants,
    load_opin_participant_cnpjs,
)
from api.sources.open_insurance import (
    extract_open_insurance_participants,
    extract_open_insurance_products,
)
from api.sources.ses import extract_ses_master_and_financials
from api.sources.consumidor_gov_agg import extract_consumidor_gov_aggregated

# Intelligence layer (import must work whether api/intelligence.py is a module
# or api/intelligence/ is a package)
try:
    from api.intelligence import apply_intelligence_batch
except Exception:  # pragma: no cover
    from api.intelligence.apply_intelligence import apply_intelligence_batch  # type: ignore


OUTPUT_FILE = Path("api/v1/insurers.json")
SNAPSHOT_DIR = Path("data/snapshots")

# Sanity checks (evergreen)
MIN_INSURERS_COUNT = int(os.getenv("MIN_INSURERS_COUNT", "0") or "0")
MAX_INSURERS_COUNT = int(os.getenv("MAX_INSURERS_COUNT", "0") or "0")
MAX_COUNT_DROP_PCT = float(os.getenv("MAX_COUNT_DROP_PCT", "0.20"))

# Opinion participants sanity
MIN_OPIN_MATCH_FLOOR = int(os.getenv("MIN_OPIN_MATCH_FLOOR", "10"))

# Exclusions: entities that are not insurers and should not appear in the list.
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

        meta = payload.get("meta") or {}
        c = meta.get("count")
        if isinstance(c, int) and c > 0:
            return c

        insurers = payload.get("insurers") or []
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
            json.dump(payload, f, ensure_ascii=False)
    except Exception:
        # Snapshot is best-effort; should not break the pipeline.
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


def main() -> None:
    # 1) Load sources
    ses_meta, ses_companies, financials = extract_ses_master_and_financials()
    opin_meta, opin_participants = extract_opin_participants()
    oi_meta, oi_participants = extract_open_insurance_participants()
    oi_prod_meta, oi_products = extract_open_insurance_products()
    cg_meta, cg_payload = extract_consumidor_gov_aggregated()

    # 2) Prepare matchers / indexes
    opin_by_cnpj = load_opin_participant_cnpjs(opin_participants)
    oi_participant_keys = set(
        (p.get("cnpj_key") or p.get("cnpj") or "").strip() for p in oi_participants
    )

    matcher = NameMatcher(cg_payload)

    # 3) Build insurers
    insurers: list[dict] = []
    matched_reputation = 0
    skipped_b2b = 0
    matched_opin = 0
    excluded = 0

    for comp in ses_companies:
        name = (comp.get("name") or comp.get("razao_social") or "").strip()
        if not name:
            continue

        if _should_exclude(name):
            excluded += 1
            continue

        cnpj_key = normalize_cnpj(comp.get("cnpj") or comp.get("cnpj_key"))
        cnpj_fmt = format_cnpj(cnpj_key) if cnpj_key else None

        # Opinion participants (by CNPJ)
        is_opin = bool(cnpj_key and cnpj_key in opin_by_cnpj)
        if is_opin:
            matched_opin += 1

        # Open Insurance participants (by CNPJ key)
        is_open_insurance = bool(cnpj_key and cnpj_key in oi_participant_keys)

        # Consumer.gov reputation match
        rep_entry, rep_meta = matcher.get_entry(name, cnpj=cnpj_key)
        is_b2b = bool(rep_meta and rep_meta.is_b2b)

        if is_b2b:
            skipped_b2b += 1
        elif rep_entry:
            matched_reputation += 1

        fin = financials.get(comp.get("susep_id")) if isinstance(financials, dict) else None

        insurers.append(
            {
                "susepId": comp.get("susep_id"),
                "cnpj": cnpj_fmt,
                "cnpjKey": cnpj_key,
                "name": name,
                "tradeName": comp.get("trade_name") or comp.get("nome_fantasia"),
                "segment": comp.get("segment"),
                "components": {
                    "ses": {"company": comp, "meta": ses_meta},
                    "financials": fin,
                    "openInsurance": {
                        "participant": is_open_insurance,
                        "meta": oi_meta,
                        "productsMeta": oi_prod_meta,
                    },
                    "reputation": rep_entry if rep_entry else None,
                },
                "flags": {
                    "opinParticipant": is_opin,
                    "openInsuranceParticipant": is_open_insurance,
                    "isB2B": is_b2b,
                },
            }
        )

    # 4) Opinion sanity checks (evergreen)
    opin_expected = len(opin_participants)
    opin_intersection = sum(1 for ins in insurers if ins["flags"]["opinParticipant"])
    if opin_expected != opin_intersection:
        # We compare to source-driven expectation; if you want this fully soft,
        # set MIN_OPIN_MATCH_FLOOR low.
        raise RuntimeError(
            f"OPIN sanity: participants={opin_expected} but intersection={opin_intersection}"
        )
    if opin_expected < MIN_OPIN_MATCH_FLOOR:
        raise RuntimeError(
            f"OPIN sanity: too few participants ({opin_expected}) < MIN_OPIN_MATCH_FLOOR={MIN_OPIN_MATCH_FLOOR}"
        )

    # 5) Apply intelligence (scores, labels, final fields)
    insurers = apply_intelligence_batch(insurers)

    out = {
        "meta": {
            "generatedAt": utc_now(),
            "count": len(insurers),
            "sources": {
                "ses": ses_meta,
                "opin": opin_meta,
                "openInsurance": oi_meta,
                "openInsuranceProducts": oi_prod_meta,
                "consumidorGov": cg_meta,
            },
            "reputation": {
                "matched": matched_reputation,
                "skippedB2B": skipped_b2b,
            },
            "filters": {"excludedNonInsurers": excluded},
        },
        "insurers": insurers,
    }

    # 6) Sanity check count (evergreen)
    _sanity_check_counts(len(insurers))

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")

    # Snapshot (for delta checks)
    if WRITE_SNAPSHOT:
        _save_snapshot(out)

    # Logs
    print(f"insurers: {len(insurers)}")
    print(f"reputation.matched: {matched_reputation}")
    print(f"reputation.skipped_b2b: {skipped_b2b}")
    print(f"excluded.non_insurers: {excluded}")
    print(f"opin.participants: {len(opin_participants)} (intersection ok)")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
