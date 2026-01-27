# api/sources/open_insurance.py
"""
Compat shim.

O projeto tem os sources reais em:
- api/sources/opin_participants.py
- api/sources/opin_products.py

Mas build_insurers.py espera:
- api.sources.open_insurance.extract_open_insurance_participants
- api.sources.open_insurance.extract_open_insurance_products

Este arquivo apenas reexporta as funções com os nomes esperados,
sem alterar a lógica existente (baixo risco de regressão).
"""

from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

from api.sources.opin_participants import extract_opin_participants, load_opin_participant_cnpjs
from api.sources.opin_products import extract_open_insurance_products


def extract_open_insurance_participants() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    # Reusa o extrator existente (OPIN directory /participants)
    return extract_opin_participants()

def load_open_insurance_participant_cnpjs(oi_participants: Any) -> Set[str]:
    """
    Wrapper para padronizar import no build_insurers.
    A regra do selo é: se aparece no diretório de participantes, leva selo.
    """
    return load_opin_participant_cnpjs(oi_participants)

__all__ = [
    "extract_open_insurance_participants",
    "extract_open_insurance_products",
    "load_open_insurance_participant_cnpjs",
]
