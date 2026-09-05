from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj_identity import SAFE_OUTSIDE_PRIMARY_CNAES
from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import normalize_name_key
from api.v2.consumer_gov_universe_resolution import (
    build_full_universe_provider_index,
    resolve_cnpj_against_full_universe,
)

DEFAULT_RECEITA_IDENTITY_SNAPSHOT = Path(
    "data/derived/v2/receita_consumer_gov_identity.json"
)
DEFAULT_RECEITA_PROVIDER_HINTS = Path(
    "data/reference/v2/consumer_gov_receita_verified_hints.json"
)

# Additional CNAEs that are intrinsically non-carrier activities. SUSEP still
# wins first: a CNPJ that is a current ordinary insurer is admitted before this
# exclusion layer is consulted.
SAFE_OUTSIDE_PRIMARY_CNAES_EXTENDED = {
    **SAFE_OUTSIDE_PRIMARY_CNAES,
    "6492100": "receita_securitization_activity",
    "6629100": "receita_auxiliary_insurance_pension_activity",
}

_INSURANCE_CNAES = {"6511101", "6512000"}
_PENSION_CNAES = {"6541300", "6542100"}
_LEGAL_NAME_TOKENS = {
    "sa",
    "s",
    "a",
    "ltda",
    "cia",
    "companhia",
    "sociedade",
    "de",
    "da",
    "do",
    "das",
    "dos",
    "e",
}
_WRAPPER_TOKENS = {"antiga", "antigo", "atual", "desativado", "desativada"}
_INDUSTRY_TOKENS = {
    "capitalizacao",
    "previdencia",
    "seguradora",
    "seguradoras",
    "seguro",
    "seguros",
    "corretora",
    "corretor",
    "vida",
    "brasil",
}


def load_receita_identity_snapshot(
    path: Path = DEFAULT_RECEITA_IDENTITY_SNAPSHOT,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("Receita identity snapshot must be a JSON object")
    if payload.get("artifact") != "v2_receita_cnpj_identity":
        raise ValueError("unexpected Receita identity artifact")
    return payload


def load_verified_receita_provider_hints(
    path: Path = DEFAULT_RECEITA_PROVIDER_HINTS,
) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("verified Receita provider hints must be a JSON object")
    rows = payload.get("rows") or []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        provider = str(row.get("provider_name") or "").strip()
        key = normalize_name_key(provider)
        if not key:
            raise ValueError("verified Receita hint requires provider_name")
        if key in out:
            raise ValueError(f"duplicate verified Receita provider hint: {provider}")
        state = str(row.get("resolution_state") or "")
        if state not in {"matched_current_insurer", "outside_157"}:
            raise ValueError(f"invalid verified Receita hint state for {provider}: {state}")
        if not normalize_cnpj_v2(row.get("target_cnpj")):
            raise ValueError(f"verified Receita hint requires valid target_cnpj: {provider}")
        evidence = row.get("evidence") or []
        if not evidence:
            raise ValueError(f"verified Receita hint requires evidence: {provider}")
        out[key] = row
    return out


def build_receita_provider_index(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not payload:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in payload.get("provider_matches") or []:
        if not isinstance(row, dict):
            continue
        key = normalize_name_key(str(row.get("provider") or row.get("provider_key") or ""))
        if not key:
            continue
        if key in out:
            raise ValueError(f"duplicate Receita provider identity row: {key}")
        out[key] = row
    return out


def _candidate_cnae_codes(candidate: dict[str, Any]) -> set[str]:
    out = {str(candidate.get("primary_cnae_code") or "").strip()}
    for row in candidate.get("secondary_cnaes") or []:
        if isinstance(row, dict):
            out.add(str(row.get("code") or "").strip())
    out.discard("")
    return out


def _safe_receita_outside_resolution(candidate: dict[str, Any]) -> dict[str, Any] | None:
    primary = str(candidate.get("primary_cnae_code") or "").strip()
    reason = SAFE_OUTSIDE_PRIMARY_CNAES_EXTENDED.get(primary)
    if not reason:
        return None
    return {
        "resolution_state": "outside_157",
        "entity_id": None,
        "matched_canonical_entity_id": None,
        "entity_type": "receita_non_insurer_activity",
        "legal_name": candidate.get("legal_name_receita"),
        "reason_code": reason,
        "match_method": "receita_name_candidate_safe_primary_cnae",
        "receita_candidate": candidate,
    }


def _candidate_resolution(
    candidate: dict[str, Any],
    full_universe_index: dict[str, Any],
) -> dict[str, Any] | None:
    canonical = resolve_cnpj_against_full_universe(
        candidate.get("cnpj"),
        full_universe_index,
    )
    if canonical is not None:
        result = dict(canonical)
        result["receita_candidate"] = candidate
        return result
    return _safe_receita_outside_resolution(candidate)


def _provider_qualifiers(provider_name: str) -> set[str]:
    tokens = set(normalize_name_key(provider_name).split())
    out: set[str] = set()
    if "capitalizacao" in tokens:
        out.add("capitalization")
    if "previdencia" in tokens:
        out.add("pension")
    if "corretora" in tokens or "corretor" in tokens:
        out.add("broker")
    # "seguros" alone is deliberately not treated as a strong qualifier:
    # brokers and sales channels may use it in a trade name. "seguradora" is
    # materially stronger and may discriminate an actual carrier candidate.
    if "seguradora" in tokens or "seguradoras" in tokens:
        out.add("insurer")
    return out


def _candidate_matches_qualifier(candidate: dict[str, Any], qualifier: str) -> bool:
    codes = _candidate_cnae_codes(candidate)
    legal = normalize_name_key(str(candidate.get("legal_name_receita") or ""))
    if qualifier == "capitalization":
        return "6450600" in codes
    if qualifier == "pension":
        return bool(codes & _PENSION_CNAES) or "previdencia" in legal
    if qualifier == "broker":
        return "6622300" in codes or "6629100" in codes
    if qualifier == "insurer":
        return bool(codes & _INSURANCE_CNAES)
    return False


def _distinctive_tokens(name: str) -> set[str]:
    return {
        token
        for token in normalize_name_key(name).split()
        if token not in _LEGAL_NAME_TOKENS
        and token not in _WRAPPER_TOKENS
        and token not in _INDUSTRY_TOKENS
        and len(token) >= 4
    }


def _candidate_is_sector_relevant(
    candidate: dict[str, Any],
    full_universe_index: dict[str, Any],
) -> bool:
    if _candidate_resolution(candidate, full_universe_index) is not None:
        return True
    return bool(_candidate_cnae_codes(candidate) & _INSURANCE_CNAES)


def _resolve_provider_candidate_row(
    provider_name: str,
    row: dict[str, Any] | None,
    full_universe_index: dict[str, Any],
) -> dict[str, Any] | None:
    if not row:
        return None
    candidates = [c for c in row.get("candidates") or [] if isinstance(c, dict)]
    state = str(row.get("candidate_state") or "")

    if state == "unique_candidate" and len(candidates) == 1:
        result = _candidate_resolution(candidates[0], full_universe_index)
        if result is None:
            return None
        result = dict(result)
        result["match_method"] = (
            "receita_unique_name_candidate_cnpj_or_activity_"
            + str(row.get("match_method") or "unknown")
        )
        return result

    if state != "ambiguous_candidates" or not candidates:
        return None

    resolvable: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for candidate in candidates:
        result = _candidate_resolution(candidate, full_universe_index)
        if result is not None:
            resolvable.append((candidate, result))

    qualifiers = _provider_qualifiers(provider_name)
    if qualifiers:
        qualified = [
            (candidate, result)
            for candidate, result in resolvable
            if any(
                _candidate_matches_qualifier(candidate, qualifier)
                for qualifier in qualifiers
            )
        ]
        if len(qualified) == 1:
            candidate, result = qualified[0]
            resolved = dict(result)
            resolved["match_method"] = "receita_ambiguous_candidates_unique_regulatory_qualifier"
            resolved["receita_candidate"] = candidate
            return resolved

    sector_relevant = [
        (candidate, result)
        for candidate, result in resolvable
        if _candidate_is_sector_relevant(candidate, full_universe_index)
    ]
    if len(sector_relevant) == 1 and _distinctive_tokens(provider_name):
        candidate, result = sector_relevant[0]
        resolved = dict(result)
        resolved["match_method"] = "receita_ambiguous_candidates_unique_insurance_sector_candidate"
        resolved["receita_candidate"] = candidate
        return resolved

    return None


def _primary_provider_variant(provider_name: str) -> str:
    # Consumer.gov labels frequently carry historical notes in parentheses.
    # The current/primary label before the note is the only part used for
    # deterministic canonical-name comparison.
    return re.split(r"\s*\(", str(provider_name or ""), maxsplit=1)[0].strip()


def _name_tokens(name: str) -> list[str]:
    return [
        token
        for token in normalize_name_key(name).split()
        if token not in _LEGAL_NAME_TOKENS and token not in _WRAPPER_TOKENS
    ]


def _canonical_record_match_strength(
    provider_name: str,
    candidate: dict[str, Any],
) -> int | None:
    provider = _primary_provider_variant(provider_name)
    provider_key = normalize_name_key(provider)
    provider_tokens = _name_tokens(provider)
    if not provider_key or not provider_tokens:
        return None

    best: int | None = None
    for raw_name in (
        candidate.get("trade_name"),
        candidate.get("legal_name_receita"),
        candidate.get("project_legal_name"),
    ):
        name = str(raw_name or "").strip()
        if not name:
            continue
        name_key = normalize_name_key(name)
        name_tokens = _name_tokens(name)
        if provider_key == name_key or provider_tokens == name_tokens:
            best = 1 if best is None else min(best, 1)
            continue
        if (
            set(provider_tokens).issubset(set(name_tokens))
            and _distinctive_tokens(provider)
        ):
            best = 2 if best is None else min(best, 2)

    qualifiers = _provider_qualifiers(provider)
    if qualifiers:
        candidate_tokens = set(
            _name_tokens(
                " ".join(
                    str(candidate.get(key) or "")
                    for key in ("trade_name", "legal_name_receita", "project_legal_name")
                )
            )
        )
        shared_distinctive = _distinctive_tokens(provider) & candidate_tokens
        if shared_distinctive and any(
            _candidate_matches_qualifier(candidate, qualifier)
            for qualifier in qualifiers
        ):
            best = 3 if best is None else min(best, 3)
    return best


def _resolve_against_canonical_receita_records(
    provider_name: str,
    payload: dict[str, Any],
    full_universe_index: dict[str, Any],
) -> dict[str, Any] | None:
    matches: list[tuple[int, dict[str, Any]]] = []
    for candidate in payload.get("canonical_records") or []:
        if not isinstance(candidate, dict):
            continue
        strength = _canonical_record_match_strength(provider_name, candidate)
        if strength is not None:
            matches.append((strength, candidate))
    if not matches:
        return None

    best_strength = min(strength for strength, _ in matches)
    best = [candidate for strength, candidate in matches if strength == best_strength]
    by_base = {
        str(candidate.get("cnpj_base") or candidate.get("cnpj") or ""): candidate
        for candidate in best
        if str(candidate.get("cnpj_base") or candidate.get("cnpj") or "")
    }
    # Name evidence itself must identify one legal CNPJ root. We do not select
    # the only SUSEP-resolvable row out of several equally matching companies.
    if len(by_base) != 1:
        return None
    candidate = next(iter(by_base.values()))
    result = _candidate_resolution(candidate, full_universe_index)
    if result is None:
        return None
    resolved = dict(result)
    resolved["match_method"] = f"receita_canonical_record_name_tier_{best_strength}"
    resolved["receita_candidate"] = candidate
    return resolved


def _resolve_verified_hint(
    provider_name: str,
    hint: dict[str, Any] | None,
    full_universe_index: dict[str, Any],
) -> dict[str, Any] | None:
    if not hint:
        return None
    target_cnpj = normalize_cnpj_v2(hint.get("target_cnpj"))
    if not target_cnpj:
        return None
    expected = str(hint.get("resolution_state") or "")

    canonical = resolve_cnpj_against_full_universe(target_cnpj, full_universe_index)
    if expected == "matched_current_insurer":
        if canonical is None or canonical.get("resolution_state") != "matched_current_insurer":
            return None
        result = dict(canonical)
        result["match_method"] = "verified_receita_provider_hint_cnpj_to_current_susep"
        result["receita_candidate"] = {
            "cnpj": target_cnpj,
            "legal_name_receita": hint.get("legal_name_receita"),
            "trade_name": hint.get("trade_name"),
            "primary_cnae_code": hint.get("primary_cnae_code"),
            "legal_nature_code": hint.get("legal_nature_code"),
            "evidence": hint.get("evidence"),
        }
        return result

    if expected != "outside_157":
        return None
    if canonical is not None:
        if canonical.get("resolution_state") != "outside_157":
            return None
        result = dict(canonical)
        result["match_method"] = "verified_receita_provider_hint_cnpj_to_canonical_outside"
        result["receita_candidate"] = {
            "cnpj": target_cnpj,
            "legal_name_receita": hint.get("legal_name_receita"),
            "trade_name": hint.get("trade_name"),
            "primary_cnae_code": hint.get("primary_cnae_code"),
            "legal_nature_code": hint.get("legal_nature_code"),
            "evidence": hint.get("evidence"),
        }
        return result

    candidate = {
        "cnpj": target_cnpj,
        "legal_name_receita": hint.get("legal_name_receita"),
        "trade_name": hint.get("trade_name"),
        "primary_cnae_code": hint.get("primary_cnae_code"),
        "secondary_cnaes": hint.get("secondary_cnaes") or [],
        "legal_nature_code": hint.get("legal_nature_code"),
        "evidence": hint.get("evidence"),
    }
    result = _safe_receita_outside_resolution(candidate)
    if result is None:
        return None
    result["match_method"] = "verified_receita_provider_hint_safe_primary_cnae"
    return result


def resolve_provider_via_receita(
    provider_name: str,
    receita_provider_index: dict[str, dict[str, Any]],
    full_universe_index: dict[str, Any],
) -> dict[str, Any] | None:
    """Resolve one provider through the already-discovered Receita candidates.

    This function intentionally uses only the provider-match rows. The payload
    wrapper below additionally consults canonical Receita records and the
    source-backed verified-hint registry.
    """
    row = receita_provider_index.get(normalize_name_key(provider_name))
    return _resolve_provider_candidate_row(provider_name, row, full_universe_index)


def resolve_provider_via_receita_payload(
    provider_name: str,
    payload: dict[str, Any] | None,
    entities: list[dict[str, Any]],
    *,
    verified_hints: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    if not payload:
        return None
    full_index = build_full_universe_provider_index(entities)
    provider_index = build_receita_provider_index(payload)
    row = provider_index.get(normalize_name_key(provider_name))

    result = _resolve_provider_candidate_row(provider_name, row, full_index)
    if result is not None:
        return result

    # Preserve genuine ambiguity. A canonical-record shortcut must never erase
    # the fact that the national CNPJ scan found multiple insurance-sector
    # companies using the same label (e.g. an insurer and an unrelated broker).
    if not row or row.get("candidate_state") != "ambiguous_candidates":
        result = _resolve_against_canonical_receita_records(
            provider_name,
            payload,
            full_index,
        )
        if result is not None:
            return result

    hints = verified_hints
    if hints is None:
        hints = load_verified_receita_provider_hints()
    hint = hints.get(normalize_name_key(provider_name)) if hints else None
    return _resolve_verified_hint(provider_name, hint, full_index)
