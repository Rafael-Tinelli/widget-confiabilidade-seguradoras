from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2

# Receita/CNAE is deliberately a hypothesis layer. It can corroborate identity
# and steer review, but it never proves a public market role or a risk carrier.
# SUSEP remains authoritative for insurer status.
CNAE_ROLE_SIGNALS: dict[str, dict[str, str]] = {
    "6511101": {
        "signal": "insurance_activity",
        "strength": "strong",
        "semantics": "corroborating_only_susep_authoritative",
    },
    "6512000": {
        "signal": "insurance_activity",
        "strength": "strong",
        "semantics": "corroborating_only_susep_authoritative",
    },
    "6622300": {
        "signal": "insurance_broker_or_agent_activity",
        "strength": "strong",
        "semantics": "role_hypothesis_only",
    },
    "6629100": {
        "signal": "auxiliary_insurance_or_pension_activity",
        "strength": "medium",
        "semantics": "role_hypothesis_only",
    },
    "7490104": {
        "signal": "general_business_intermediation",
        "strength": "weak",
        "semantics": "role_hypothesis_only",
    },
    "7911200": {
        "signal": "travel_agency_activity",
        "strength": "medium",
        "semantics": "role_hypothesis_only",
    },
    "7912100": {
        "signal": "tour_operator_activity",
        "strength": "medium",
        "semantics": "role_hypothesis_only",
    },
    "7990200": {
        "signal": "other_travel_services_activity",
        "strength": "weak",
        "semantics": "role_hypothesis_only",
    },
}


def _cnae_codes(candidate: dict[str, Any]) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    primary = str(candidate.get("primary_cnae_code") or "").strip()
    if primary:
        rows.append((primary, "primary"))
    for item in candidate.get("secondary_cnaes") or []:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code") or "").strip()
        if code:
            rows.append((code, "secondary"))
    return rows


def market_role_signals_from_receita(candidate: dict[str, Any]) -> list[dict[str, str]]:
    """Return non-authoritative role signals from a Receita establishment row.

    The result is intentionally incapable of asserting ``broker``,
    ``insurance_representative``, ``risk_carrier`` or any other public role.
    It only tells the review layer where stronger evidence should be sought.
    """

    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for code, position in _cnae_codes(candidate):
        policy = CNAE_ROLE_SIGNALS.get(code)
        if not policy:
            continue
        key = (code, position)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "source": "receita_cnae",
                "cnae_code": code,
                "position": position,
                "signal": policy["signal"],
                "strength": policy["strength"],
                "semantics": policy["semantics"],
                "assertion_effect": "none",
            }
        )
    return out


def resolve_receita_market_identity_signal(
    candidate: dict[str, Any],
    *,
    current_susep_insurer_cnpjs: Iterable[str] = (),
) -> dict[str, Any]:
    """Build an evidence state for one exact Receita candidate.

    A distinct legal entity may be verified from CNPJ/Receita while its market
    role remains unresolved. This separation is the core guardrail: existence
    of a company, its declared CNAEs, and its role in a particular insurance
    arrangement are different facts.
    """

    cnpj = normalize_cnpj_v2(candidate.get("cnpj"))
    legal_name = str(candidate.get("legal_name_receita") or "").strip() or None
    trade_name = str(candidate.get("trade_name") or "").strip() or None
    susep = {
        normalized
        for value in current_susep_insurer_cnpjs
        if (normalized := normalize_cnpj_v2(value))
    }
    signals = market_role_signals_from_receita(candidate)

    identity_verified = bool(cnpj and legal_name)
    insurer_verified = bool(cnpj and cnpj in susep)

    if insurer_verified:
        insurer_status = "authorized_insurer"
        classification_state = "verified_regulatory"
        review_reason = "current_susep_insurer_exact_cnpj"
    elif identity_verified:
        insurer_status = "not_verified_as_insurer"
        classification_state = "role_unresolved"
        review_reason = "legal_identity_verified_market_role_requires_source_backed_review"
    else:
        insurer_status = "unknown"
        classification_state = "identity_unresolved"
        review_reason = "receita_candidate_does_not_verify_distinct_legal_identity"

    return {
        "identity_state": "verified_legal_entity" if identity_verified else "unresolved",
        "classification_state": classification_state,
        "relationship_state": "unresolved",
        "publication_state": "needs_review",
        "insurer_status": insurer_status,
        "cnpj": cnpj,
        "legal_name": legal_name,
        "trade_name": trade_name,
        "role_signals": signals,
        "public_market_role": None,
        "risk_carrier": None,
        "assessment_inheritance": "forbidden",
        "complaint_transfer_effect": "none",
        "automatic_registry_mutation": "forbidden",
        "review_reason": review_reason,
    }


def resolve_unknown_query_with_receita_candidates(
    query: str,
    candidates: Iterable[dict[str, Any]],
    *,
    current_susep_insurer_cnpjs: Iterable[str] = (),
) -> dict[str, Any]:
    """Conservatively resolve a widget unknown-query against Receita candidates.

    Exactly one Receita legal root is required before the system may even call
    the legal identity verified. Zero or multiple candidates remain unresolved.
    This is also what keeps product/program names from becoming public market
    identities merely because users search for them.
    """

    rows = [dict(row) for row in candidates if isinstance(row, dict)]
    roots: dict[str, dict[str, Any]] = {}
    for row in rows:
        cnpj = normalize_cnpj_v2(row.get("cnpj"))
        if not cnpj:
            continue
        roots.setdefault(cnpj[:8], row)

    if len(roots) != 1:
        return {
            "query": str(query or "").strip(),
            "identity_state": "unresolved",
            "classification_state": "unresolved",
            "relationship_state": "unresolved",
            "publication_state": "private_candidate",
            "public_market_role": None,
            "risk_carrier": None,
            "automatic_registry_mutation": "forbidden",
            "review_reason": (
                "no_distinct_legal_entity_candidate"
                if not roots
                else "ambiguous_legal_entity_candidates"
            ),
            "candidate_count": len(roots),
        }

    row = next(iter(roots.values()))
    resolved = resolve_receita_market_identity_signal(
        row,
        current_susep_insurer_cnpjs=current_susep_insurer_cnpjs,
    )
    resolved["query"] = str(query or "").strip()
    resolved["candidate_count"] = 1
    return resolved
