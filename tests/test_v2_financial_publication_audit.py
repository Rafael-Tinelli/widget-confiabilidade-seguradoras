from __future__ import annotations

from copy import deepcopy

import pytest

from api.v2.audit_financial_publication_chain import (
    FinancialPublicationAuditError,
    audit_financial_publication_chain,
)
from api.v2.financial_evidence import FINANCIAL_EVIDENCE_VERSION
from api.v2.financial_periods import MATURITY_POLICY_VERSION


def _payloads() -> tuple[dict, dict, dict, dict, list[dict]]:
    financial = {
        "meta": {
            "financial_evidence_version": FINANCIAL_EVIDENCE_VERSION,
            "financial_period_maturity": {
                "policy_version": MATURITY_POLICY_VERSION,
                "capital_pla_source_field": "new_pla",
                "selected_period": 202606,
            },
        },
        "entities": [
            {
                "entity_id": "fip:000001",
                "eligibility": {"regulatory_universe_eligible": True},
                "financial_evidence": {
                    "capital": {
                        "reference_period": 202606,
                        "pla_cmr_numerator_field": "new_pla",
                        "pla_cmr_ratio_state": "derivable",
                        "pla_cmr_ratio": 1.1,
                        "latest": {
                            "new_pla": 110.0,
                            "pla_adjusted": 70.0,
                            "cmr": 100.0,
                        },
                    }
                },
            },
            {
                "entity_id": "fip:000002",
                "eligibility": {"regulatory_universe_eligible": True},
                "financial_evidence": {
                    "capital": {
                        "reference_period": 202606,
                        "pla_cmr_numerator_field": "new_pla",
                        "pla_cmr_ratio_state": "unavailable",
                        "pla_cmr_ratio": None,
                        "latest": {
                            "new_pla": 90.0,
                            "pla_adjusted": 120.0,
                            "cmr": 0.0,
                        },
                    }
                },
            },
        ],
    }
    closure = {
        "status": "financial_methodology_closed_for_signal_design",
        "source_contract": {"reference_period": 202606},
        "entities": [
            {
                "entity_id": "fip:000001",
                "reference_period": 202606,
                "capital": {
                    "state": "capital_meets_or_exceeds_cmr",
                    "pla_cmr_ratio": 1.1,
                },
            },
            {
                "entity_id": "fip:000002",
                "reference_period": 202606,
                "capital": {
                    "state": "capital_signal_unavailable",
                    "pla_cmr_ratio": None,
                },
            },
        ],
    }
    explorer = {
        "entities": [
            {
                "entity_id": "fip:000001",
                "legal_name": "ALFA SEGURADORA S.A.",
                "financial": {
                    "reference_period": 202606,
                    "capital": {
                        "state": "capital_meets_or_exceeds_cmr",
                        "pla_cmr_ratio": 1.1,
                    },
                },
            },
            {
                "entity_id": "fip:000002",
                "legal_name": "BETA SEGURADORA S.A.",
                "financial": {
                    "reference_period": 202606,
                    "capital": {
                        "state": "capital_signal_unavailable",
                        "pla_cmr_ratio": None,
                    },
                },
            },
        ]
    }
    leaderboard = {
        "id": "highest_pla_cmr_ratio",
        "metric": "pla_cmr_ratio",
        "direction": "descending",
        "top_positions": 10,
        "is_general_ranking": False,
        "entries": [
            {
                "entity_id": "fip:000001",
                "pla_cmr_ratio": 1.1,
                "leaderboard_rank": 1,
            }
        ],
    }
    profiles = [
        {
            "identity": {"entity_id": "fip:000001"},
            "assessment": {
                "financial": {
                    "reference_period": 202606,
                    "capital": {
                        "state": "capital_meets_or_exceeds_cmr",
                        "technical": {
                            "ratio": {
                                "value": 1.1,
                                "availability": "available",
                            }
                        },
                    },
                }
            },
        },
        {
            "identity": {"entity_id": "fip:000002"},
            "assessment": {
                "financial": {
                    "reference_period": 202606,
                    "capital": {
                        "state": "capital_signal_unavailable",
                        "technical": {
                            "ratio": {
                                "value": None,
                                "availability": "unavailable",
                            }
                        },
                    },
                }
            },
        },
    ]
    return financial, closure, explorer, leaderboard, profiles


def test_audit_verifies_new_pla_through_publication_chain() -> None:
    result = audit_financial_publication_chain(*_payloads())

    assert result["status"] == "financial_publication_chain_verified"
    assert result["reference_period"] == 202606
    assert result["contracts"]["capital_pla_source_field"] == "new_pla"
    assert result["population"] == {
        "regulatory_entities": 2,
        "capital_derivable": 1,
        "capital_unavailable": 1,
        "capital_below_cmr": 0,
        "leaderboard_entries": 1,
    }
    assert result["scoring"] == "forbidden_in_this_artifact"
    assert result["ranking"] == "forbidden_in_this_artifact"


def test_audit_rejects_old_or_tampered_financial_evidence_ratio() -> None:
    financial, closure, explorer, leaderboard, profiles = _payloads()
    financial = deepcopy(financial)
    financial["entities"][0]["financial_evidence"]["capital"]["pla_cmr_ratio"] = 0.7

    with pytest.raises(FinancialPublicationAuditError, match="new_pla/CMR"):
        audit_financial_publication_chain(
            financial,
            closure,
            explorer,
            leaderboard,
            profiles,
        )


def test_audit_rejects_financial_closure_drift() -> None:
    financial, closure, explorer, leaderboard, profiles = _payloads()
    closure = deepcopy(closure)
    closure["entities"][0]["capital"]["pla_cmr_ratio"] = 1.09

    with pytest.raises(FinancialPublicationAuditError, match="Financial Closure PLA/CMR"):
        audit_financial_publication_chain(
            financial,
            closure,
            explorer,
            leaderboard,
            profiles,
        )


def test_audit_rejects_public_profile_drift() -> None:
    financial, closure, explorer, leaderboard, profiles = _payloads()
    profiles = deepcopy(profiles)
    profiles[0]["assessment"]["financial"]["capital"]["technical"]["ratio"][
        "value"
    ] = 1.08

    with pytest.raises(FinancialPublicationAuditError, match="Profile PLA/CMR"):
        audit_financial_publication_chain(
            financial,
            closure,
            explorer,
            leaderboard,
            profiles,
        )


def test_audit_rejects_contract_that_does_not_declare_new_pla() -> None:
    financial, closure, explorer, leaderboard, profiles = _payloads()
    financial = deepcopy(financial)
    financial["meta"]["financial_period_maturity"][
        "capital_pla_source_field"
    ] = "pla_adjusted"

    with pytest.raises(FinancialPublicationAuditError, match="capital numerator mismatch"):
        audit_financial_publication_chain(
            financial,
            closure,
            explorer,
            leaderboard,
            profiles,
        )


def test_audit_rejects_leaderboard_not_derived_from_explorer() -> None:
    financial, closure, explorer, leaderboard, profiles = _payloads()
    leaderboard = deepcopy(leaderboard)
    leaderboard["entries"][0]["entity_id"] = "fip:000002"

    with pytest.raises(FinancialPublicationAuditError, match="leaderboard order"):
        audit_financial_publication_chain(
            financial,
            closure,
            explorer,
            leaderboard,
            profiles,
        )
