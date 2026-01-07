# api/build_insurers.py
from __future__ import annotations
from __future__ import annotations

import glob
import gzip
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from api.intelligence.apply_intelligence import apply_intelligence_batch
from api.matching.consumidor_gov_match import NameMatcher
from api.sources.opin_participants import load_opin_participant_cnpjs
from api.sources.ses import extract_ses_master_and_financials
from api.utils.identifiers import normalize_cnpj
from api.utils.name_cleaner import normalize_name_key

# ---------------------------------------------------------------------------
# Inputs / Outputs
# ---------------------------------------------------------------------------

OUTPUT_PATH = os.getenv("INSURERS_OUTPUT_PATH", "api/v1/insurers.json")
CONSUMIDOR_GOV_FILE = os.getenv("CONSUMIDOR_GOV_FILE", "data/processed/consumidor_gov_agg.json")
OPIN_PRODUCTS_FILE = os.getenv("OPIN_PRODUCTS_FILE", "data/processed/opin_products_2025-12-27.json")

# ---------------------------------------------------------------------------
# Evergreen sanity checks (DoD / Delta)
# ---------------------------------------------------------------------------

SNAPSHOT_GLOB = os.getenv("INSURERS_SNAPSHOT_GLOB", "data/snapshots/insurers_full_*.json.gz")
MAX_DELTA_PCT = float(os.getenv("INSURERS_MAX_DELTA_PCT", "25"))

# Usado apenas quando ainda não existe snapshot para comparar.
MIN_COUNT_FALLBACK = int(os.getenv("INSURERS_MIN_COUNT", "180"))
MAX_COUNT_FALLBACK = int(os.getenv("INSURERS_MAX_COUNT", "400"))

# Sanity de reputation matching (evitar alarme falso por universo B2B).
MIN_REPUTATION_MATCHED = int(os.getenv("INSURERS_MIN_REPUTATION_MATCHED", "60"))
MIN_REPUTATION_MATCH_RATIO = float(os.getenv("INSURERS_MIN_REPUTATION_MATCH_RATIO", "0.20"))

# Entidades que nunca devem entrar no universo (corretoras/associações etc.)
EXCLUDE_NAME_SUBSTRINGS = tuple(
    s.strip().lower()
    for s in os.getenv(
        "INSURERS_EXCLUDE_SUBSTRINGS",
        "ibracor,corretora,corretor,corretagem,broker,brokers,resseguro corretora,resseguradora corretora",
    ).split(",")
    if s.strip()
)

_SNAPSHOT_DATE_RE = re.compile(r"insurers_full_(\d{4}-\d{2}-\d{2})\.json\.gz$")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: str, obj: Dict[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _latest_snapshot_count() -> Optional[int]:
    """
    Lê meta.count do snapshot mais recente insurers_full_YYYY-MM-DD.json.gz, se existir.
    Permite sanity check evergreen (delta %) em vez de count rígido.
    """
    files = glob.glob(SNAPSHOT_GLOB)
    if not files:
        return None

    def sort_key(p: str) -> Tuple[int, str]:
        m = _SNAPSHOT_DATE_RE.search(p.replace("\\", "/"))
        if m:
            y, mo, d = m.group(1).split("-")
            return int(y + mo + d), p
        try:
            return int(Path(p).stat().st_mtime), p
        except Exception:
            return 0, p

    files_sorted = sorted(files, key=sort_key, reverse=True)
    for fp in files_sorted[:5]:
        try:
            with gzip.open(fp, "rt", encoding="utf-8") as f:
                data = json.load(f)
            meta = data.get("meta") or {}
            count = meta.get("count") or (meta.get("stats") or {}).get("totalInsurers")
            if isinstance(count, int) and count > 0:
                return count
        except Exception:
            continue
    return None


def _should_exclude_name(name: str) -> bool:
    nk = normalize_name_key(name)
    return any(sub in nk for sub in EXCLUDE_NAME_SUBSTRINGS)


def main() -> None:
    print("Building insurers.json...")

    # 1) SES (universo + financeiros)
    ses_master, companies = extract_ses_master_and_financials()

    # 2) Consumidor.gov (reputação)
    reputation_root = _load_json(CONSUMIDOR_GOV_FILE)
    matcher = NameMatcher(reputation_root)

    # 3) Open Insurance participants
    opin_participant_cnpjs = load_opin_participant_cnpjs(OPIN_PRODUCTS_FILE)

    insurers: list[dict] = []

    matched_reputation = 0
    matched_opin = 0
    skipped_b2b = 0
    excluded_entities = 0

    susep_cnpjs_seen: set[str] = set()

    # 4) Build
    for _id, comp in (companies or {}).items():
        name = (comp.get("name") or "").strip()
        if not name:
            continue

        # Exclusões (ibracor/corretora/etc)
        if _should_exclude_name(name):
            excluded_entities += 1
            continue

        cnpj = normalize_cnpj(comp.get("cnpj"))
        if cnpj:
            susep_cnpjs_seen.add(cnpj)

        rep_entry, match_meta = matcher.get_entry(name, cnpj=cnpj)

        is_b2b = bool(match_meta and getattr(match_meta, "is_b2b", False))
        if is_b2b:
            skipped_b2b += 1

        is_open_insurance = bool(cnpj and cnpj in opin_participant_cnpjs)
        if is_open_insurance:
            matched_opin += 1

        intelligence = apply_intelligence_batch(comp, ses_master, rep_entry, is_open_insurance)

        # Flags + isB2B
        flags = intelligence.get("flags")
        if not isinstance(flags, dict):
            flags = {}
            intelligence["flags"] = flags
        flags["isB2B"] = is_b2b

        # Match meta (útil p/ auditoria)
        if match_meta:
            intelligence["matchMeta"] = {
                "method": match_meta.method,
                "score": match_meta.score,
                "target": match_meta.target,
            }

        if rep_entry:
            matched_reputation += 1

        insurers.append(intelligence)

    total_count = len(insurers)

    # 5) Validação DoD dinâmica (evergreen)
    prev_count = _latest_snapshot_count()
    if prev_count is not None:
        delta_pct = (abs(total_count - prev_count) / prev_count) * 100.0
        if delta_pct > MAX_DELTA_PCT:
            raise RuntimeError(
                f"Sanity check failed: insurers count changed too much vs latest snapshot "
                f"({prev_count} -> {total_count}, Δ={delta_pct:.1f}%, max={MAX_DELTA_PCT:.1f}%)."
            )
    else:
        if not (MIN_COUNT_FALLBACK <= total_count <= MAX_COUNT_FALLBACK):
            raise RuntimeError(
                f"Sanity check failed: insurers count out of fallback range "
                f"[{MIN_COUNT_FALLBACK}, {MAX_COUNT_FALLBACK}] (got {total_count}). "
                f"Set INSURERS_MIN_COUNT/INSURERS_MAX_COUNT or provide snapshots."
            )

    eligible_for_rep = max(0, total_count - skipped_b2b)
    if eligible_for_rep > 0:
        rep_ratio = matched_reputation / eligible_for_rep
        if matched_reputation < MIN_REPUTATION_MATCHED and rep_ratio < MIN_REPUTATION_MATCH_RATIO:
            raise RuntimeError(
                "Sanity check failed: too few reputation matches from Consumidor.gov "
                f"(matched={matched_reputation}, eligible={eligible_for_rep}, ratio={rep_ratio:.2%}). "
                f"Thresholds: abs>={MIN_REPUTATION_MATCHED} OR ratio>={MIN_REPUTATION_MATCH_RATIO:.2%}."
            )

    # Open Insurance sanity: interseção de CNPJs (OPIN vs SES)
    expected_opin_matches = len([c for c in opin_participant_cnpjs if c in susep_cnpjs_seen])
    if matched_opin != expected_opin_matches:
        raise RuntimeError(
            "Open Insurance participants mismatch: "
            f"matched_opin={matched_opin}, expected={expected_opin_matches} "
            "(based on CNPJs intersection between OPIN snapshot and SES universe)."
        )

    # 6) Output
    out = {
        "schemaVersion": "1.0.0",
        "generatedAt": _utc_now_iso(),
        "period": "2024",
        "sources": ["SUSEP (SES)", "Open Insurance Brasil", "Consumidor.gov.br"],
        "meta": {
            "count": total_count,
            "stats": {
                "totalInsurers": total_count,
                "reputationMatched": matched_reputation,
                "openInsuranceParticipants": matched_opin,
                "b2bSkipped": skipped_b2b,
                "excludedEntities": excluded_entities,
            },
        },
        "insurers": insurers,
    }

    _save_json(OUTPUT_PATH, out)

    print(
        "Integrity Check Passed: "
        f"{total_count} insurers "
        f"(excluded={excluded_entities}, b2b={skipped_b2b}), "
        f"reputationMatched={matched_reputation}, "
        f"opinParticipants={matched_opin} (expected={expected_opin_matches})."
    )


if __name__ == "__main__":
    main()
