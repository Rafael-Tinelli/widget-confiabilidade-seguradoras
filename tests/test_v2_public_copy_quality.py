import pytest

from api.v2.build_conduct_methodology_closure import _final_pressure_state
from api.v2.validate_public_search_profile_contract import (
    PublicProfileValidationError,
    _validate_public_copy,
)


def _pressure(state: str, *, months: int = 12) -> dict:
    return {
        "temporal_coverage": {"comparable_months": months},
        "annual": {"uncertainty": {"state": state}},
    }


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (
            "above_size_proportional_reference",
            "Há mais reclamações do que esperaríamos para o tamanho da operação nos meses comparáveis.",
        ),
        (
            "below_size_proportional_reference",
            "Há menos reclamações do que esperaríamos para o tamanho da operação nos meses comparáveis; isso não prova melhor atendimento.",
        ),
        (
            "not_distinguishable_from_size_proportional_reference",
            "Os dados não mostram diferença suficientemente clara em relação ao esperado para o tamanho da operação.",
        ),
    ],
)
def test_public_conduct_summaries_preserve_portuguese_accents(
    state: str, expected: str
) -> None:
    _, summary = _final_pressure_state(_pressure(state), {})
    assert summary == expected


@pytest.mark.parametrize("term", ["v2", "projeto", "widget", "snapshot"])
def test_public_profile_validator_rejects_internal_product_vocabulary(term: str) -> None:
    profiles = {
        "entity:test": {
            "profile_id": "entity:test",
            "public_summary": {"quick_answer": f"Texto interno: {term}."},
        }
    }

    with pytest.raises(PublicProfileValidationError, match="public profile copy"):
        _validate_public_copy(profiles)


def test_public_profile_validator_accepts_reader_facing_copy() -> None:
    _validate_public_copy(
        {
            "entity:test": {
                "profile_id": "entity:test",
                "public_summary": {
                    "quick_answer": "Seguradora identificada no cadastro regulatório consultado."
                },
            }
        }
    )
