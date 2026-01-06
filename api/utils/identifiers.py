# api/utils/identifiers.py
from __future__ import annotations

import numbers
import re
from typing import Any

_DIGITS_ONLY = re.compile(r"\D+")

def normalize_cnpj(val: Any) -> str | None:
    """
    Normaliza CNPJ para 'só dígitos' (14 chars).
    
    Robustez ETL (Blindado):
    - Rejeita bool (True != 1 neste contexto).
    - Aceita numbers.Integral (int, numpy.int64, etc).
    - Aceita numbers.Real (float, numpy.float64) se for inteiro.
    - Corrige perda de zero à esquerda.
    
    Retorna None se inválido.
    """
    if val is None:
        return None

    # Segurança: bool é subclasse de int em Python, mas semanticamente não é CNPJ
    if isinstance(val, bool):
        return None

    # Caso 1: Inteiro genérico (int, np.int64, etc.)
    if isinstance(val, numbers.Integral):
        digits = str(val).zfill(14)
        return digits if len(digits) == 14 else None

    # Caso 2: Float genérico (float, np.float64) - comum em pandas/excel
    if isinstance(val, numbers.Real):
        # Verifica se é "inteiro matematicamente" (ex: 123.0)
        if float(val).is_integer():
            digits = str(int(val)).zfill(14)
            return digits if len(digits) == 14 else None
        return None 

    # Caso 3: String
    s = str(val).strip()
    if not s:
        return None

    digits = _DIGITS_ONLY.sub("", s)

    # Recupera zero à esquerda perdido em conversão string
    if len(digits) == 13:
        digits = digits.zfill(14)

    return digits if len(digits) == 14 else None
