from __future__ import annotations

from copy import deepcopy

import pytest

from api.v2.audit_financial_publication_chain import (
    FinancialPublicationAuditError,
    audit_financial_publication_chain,
)
from api.v2.financial_evidence import FINANCIAL_EVIDENCE_VERSION
from api.v2.financial_periods import MATURITY_POLICY_VERSION
from api.v2.operating_states import OPERATING_STATE_VERSION


def _operating_context(
    signal: str,
    history_state: str,
    formula_state: str,
) -> dict:
    return {
        "signal": signal,
        "history_state": history_state,
        "formula_state": formula_state,
        "reference_metric": "ICA",
        "supporting_metric": "IC",
        "overrides_core_signal": False,
    }


def _payloads() -> tuple[
    dict,
    dict,
    dict,
    dict,
    dict,
    dict,
    dict,
    list[dict],
]:
    financial = {
        "meta": {
            "financial_evidence_version": FINANCIAL_EVIDENCE_VERSION,
            "financial_source": {
                "malformed_row_policy": "fail_closed_not_skipped",
                "key_parsing_policy": (
                    "strict_integer_keys_and_valid_aaaamm_periods"
                ),
                "numeric_parsing_policy": (
                    "strict_finite_decimal_or_scientific_notation"
                ),
                "balance_quadro_policy": (
                    "formula_cmpids_must_match_official_22A_22P_23"
                ),
            },
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
                        "duplicate_rows_current": 0,
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
                        "duplicate_rows_current": 0,
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
    liquidity = {
        "artifact": "v2_liquidity_experiment",
        "summary": {
            "reference_period": 202606,
            "period_maturity": {"policy_version": MATURITY_POLICY_VERSION},
        },
        "entities": [
            {
                "entity_id": "fip:000001",
                "reference_period": 202606,
                "metrics": {
                    "ILT": {
                        "current": {
                            "state": "derivable",
                            "value": 1.2,
                            "numerator": 120.0,
                            "denominator": 100.0,
                        }
                    }
                },
            },
            {
                "entity_id": "fip:000002",
                "reference_period": 202606,
                "metrics": {
                    "ILT": {
                        "current": {
                            "state": "non_positive_denominator",
                            "value": None,
                            "numerator": 50.0,
                            "denominator": 0.0,
                        }
                    }
                },
            },
        ],
    }
    operating = {
        "artifact": "v2_operating_experiment",
        "period_maturity": {"policy_version": MATURITY_POLICY_VERSION},
        "summary": {"reference_period": 202606},
        "entities": [
            {
                "entity_id": "fip:000001",
                "reference_period": 202606,
                "operating_state": {
                    "version": OPERATING_STATE_VERSION,
                    "operating_signal": "balanced_persistent",
                    "history_state": "established",
                    "formula_state": "derivable",
                },
            },
            {
                "entity_id": "fip:000002",
                "reference_period": 202606,
                "operating_state": {
                    "version": OPERATING_STATE_VERSION,
                    "operating_signal": "indeterminate",
                    "history_state": "limited",
                    "formula_state": "missing_formula_components",
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
                "liquidity": {
                    "state": "ilt_at_or_above_arithmetic_parity",
                    "metric": "ILT",
                    "value": 1.2,
                    "parity_is_regulatory_threshold": False,
                },
                "operating_context": _operating_context(
                    "balanced_persistent",
                    "established",
                    "derivable",
                ),
            },
            {
                "entity_id": "fip:000002",
                "reference_period": 202606,
                "capital": {
                    "state": "capital_signal_unavailable",
                    "pla_cmr_ratio": None,
                },
                "liquidity": {
                    "state": "ilt_signal_unavailable",
                    "metric": "ILT",
                    "value": None,
                    "parity_is_regulatory_threshold": False,
                },
                "operating_context": _operating_context(
                    "indeterminate",
                    "limited",
                    "missing_formula_components",
                ),
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
                    "liquidity": {
                        "state": "ilt_at_or_above_arithmetic_parity",
                        "metric": "ILT",
                        "value": 1.2,
                        "parity_is_regulatory_threshold": False,
                    },
                    "operating_context": _operating_context(
                        "balanced_persistent",
                        "established",
                        "derivable",
                    ),
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
                    "liquidity": {
                        "state": "ilt_signal_unavailable",
                        "metric": "ILT",
                        "value": None,
                        "parity_is_regulatory_threshold": False,
                    },
                    "operating_context": _operating_context(
                        "indeterminate",
                        "limited",
                        "missing_formula_components",
                    ),
                },
            },
        ]
    }
    capital_leaderboard = {
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
    ilt_leaderboard = {
        "id": "highest_ilt",
        "metric": "ilt",
        "direction": "descending",
        "top_positions": 10,
        "is_general_ranking": False,
        "entries": [
            {
                "entity_id": "fip:000001",
                "ilt": 1.2,
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
                    "liquidity": {
                        "state": "ilt_at_or_above_arithmetic_parity",
                        "technical": {
                            "ratio": {
                                "value": 1.2,
                                "availability": "available",
                            }
                        },
                    },
                    "operating_context": _operating_context(
                        "balanced_persistent",
                        "established",
                        "derivable",
                    ),
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
                    "liquidity": {
                        "state": "ilt_signal_unavailable",
                        "technical": {
                            "ratio": {
                                "value": None,
                                "availability": "unavailable",
                            }
                        },
                    },
                    "operating_context": _operating_context(
                        "indeterminate",
                        "limited",
                        "missing_formula_components",
                    ),
                }
            },
        },
    ]
    return (
        financial,
        liquidity,
        operating,
        closure,
        explorer,
        capital_leaderboard,
        ilt_leaderboard,
        profiles,
    )


def test_audit_verifies_financial_signals_through_publication_chain() -> None:
    result = audit_financial_publication_chain(*_payloads())

    assert result["status"] == "financial_publication_chain_verified"
    assert result["reference_period"] == 202606
    assert result["contracts"]["capital_pla_source_field"] == "new_pla"
    assert result["population"] == {
        "regulatory_entities": 2,
        "capital_derivable": 1,
        "capital_unavailable": 1,
        "capital_below_cmr": 0,
        "ilt_derivable": 1,
        "ilt_unavailable": 1,
        "ilt_below_arithmetic_parity": 0,
        "operating_signal_counts": {
            "balanced_persistent": 1,
            "indeterminate": 1,
        },
        "capital_leaderboard_entries": 1,
        "ilt_leaderboard_entries": 1,
    }
    assert result["scoring"] == "forbidden_in_this_artifact"
    assert result["ranking"] == "forbidden_in_this_artifact"


def test_audit_rejects_old_or_tampered_financial_evidence_ratio() -> None:
    payloads = list(_payloads())
    financial = deepcopy(payloads[0])
    financial["entities"][0]["financial_evidence"]["capital"]["pla_cmr_ratio"] = 0.7
    payloads[0] = financial

    with pytest.raises(FinancialPublicationAuditError, match="new_pla/CMR"):
        audit_financial_publication_chain(*payloads)


def test_audit_rejects_non_fail_closed_source_parser_contract() -> None:
    payloads = list(_payloads())
    financial = deepcopy(payloads[0])
    financial["meta"]["financial_source"]["malformed_row_policy"] = "skip"
    payloads[0] = financial

    with pytest.raises(
        FinancialPublicationAuditError,
        match="financial source parser contract mismatch",
    ):
        audit_financial_publication_chain(*payloads)


def test_audit_rejects_derivable_capital_with_duplicate_source_period() -> None:
    payloads = list(_payloads())
    financial = deepcopy(payloads[0])
    financial["entities"][0]["financial_evidence"]["capital"][
        "duplicate_rows_current"
    ] = 1
    payloads[0] = financial

    with pytest.raises(
        FinancialPublicationAuditError,
        match="derivable PLA/CMR has 1 duplicate source rows",
    ):
        audit_financial_publication_chain(*payloads)


def test_audit_rejects_financial_closure_capital_drift() -> None:
    payloads = list(_payloads())
    closure = deepcopy(payloads[3])
    closure["entities"][0]["capital"]["pla_cmr_ratio"] = 1.09
    payloads[3] = closure

    with pytest.raises(FinancialPublicationAuditError, match="Financial Closure PLA/CMR"):
        audit_financial_publication_chain(*payloads)


def test_audit_rejects_public_profile_capital_drift() -> None:
    payloads = list(_payloads())
    profiles = deepcopy(payloads[7])
    profiles[0]["assessment"]["financial"]["capital"]["technical"]["ratio"][
        "value"
    ] = 1.08
    payloads[7] = profiles

    with pytest.raises(FinancialPublicationAuditError, match="Profile PLA/CMR"):
        audit_financial_publication_chain(*payloads)


def test_audit_rejects_contract_that_does_not_declare_new_pla() -> None:
    payloads = list(_payloads())
    financial = deepcopy(payloads[0])
    financial["meta"]["financial_period_maturity"][
        "capital_pla_source_field"
    ] = "pla_adjusted"
    payloads[0] = financial

    with pytest.raises(FinancialPublicationAuditError, match="capital numerator mismatch"):
        audit_financial_publication_chain(*payloads)


def test_audit_rejects_capital_leaderboard_not_derived_from_explorer() -> None:
    payloads = list(_payloads())
    leaderboard = deepcopy(payloads[5])
    leaderboard["entries"][0]["entity_id"] = "fip:000002"
    payloads[5] = leaderboard

    with pytest.raises(FinancialPublicationAuditError, match="highest_pla_cmr_ratio: order"):
        audit_financial_publication_chain(*payloads)


def test_audit_rejects_explorer_ilt_drift() -> None:
    payloads = list(_payloads())
    explorer = deepcopy(payloads[4])
    explorer["entities"][0]["financial"]["liquidity"]["value"] = 1.19
    payloads[4] = explorer

    with pytest.raises(FinancialPublicationAuditError, match="Explorer ILT"):
        audit_financial_publication_chain(*payloads)


def test_audit_rejects_ilt_leaderboard_not_derived_from_explorer() -> None:
    payloads = list(_payloads())
    leaderboard = deepcopy(payloads[6])
    leaderboard["entries"][0]["ilt"] = 1.19
    payloads[6] = leaderboard

    with pytest.raises(FinancialPublicationAuditError, match="highest_ilt value"):
        audit_financial_publication_chain(*payloads)


def test_audit_rejects_operating_context_drift() -> None:
    payloads = list(_payloads())
    profiles = deepcopy(payloads[7])
    profiles[0]["assessment"]["financial"]["operating_context"][
        "signal"
    ] = "recent_pressure"
    payloads[7] = profiles

    with pytest.raises(FinancialPublicationAuditError, match="Profile operating context drift"):
        audit_financial_publication_chain(*payloads)
