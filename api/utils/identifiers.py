# api/utils/identifiers.py
from __future__ import annotations

import numbers
import re
from typing import Any

_DIGITS_ONLY = re.compile(r"\D+")
_ALNUM_ONLY = re.compile(r"[^0-9A-Za-z]+")


def normalize_cnpj(val: Any) -> str | None:
    """
    Normaliza CNPJ legado para 'só dígitos' (14 chars).

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


def normalize_cnpj_v2(val: Any) -> str | None:
    """Normalize numeric or alphanumeric CNPJ for the v2 regulatory model.

    Since July 2026, new CNPJ registrations may use letters and numbers in the
    first 12 positions while the final two check-digit positions remain numeric.
    Existing numeric CNPJs remain valid. This helper is intentionally separate
    from ``normalize_cnpj`` so the production v1 pipeline keeps its legacy
    behavior during the v2 migration.
    """
    if val is None or isinstance(val, bool):
        return None

    # Preserve legacy ETL handling for numeric objects, including leading-zero
    # restoration after spreadsheet coercion.
    if isinstance(val, numbers.Integral):
        text = str(val).zfill(14)
    elif isinstance(val, numbers.Real):
        if not float(val).is_integer():
            return None
        text = str(int(val)).zfill(14)
    else:
        text = _ALNUM_ONLY.sub("", str(val).strip()).upper()
        if len(text) == 13 and text.isdigit():
            text = text.zfill(14)

    if len(text) != 14:
        return None
    if not text[:12].isalnum() or not text[-2:].isdigit():
        return None
    if text == "0" * 14:
        return None
    return text
