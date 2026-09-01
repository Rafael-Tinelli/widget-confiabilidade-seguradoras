from __future__ import annotations

import math
from typing import Any

from api.v2.financial_evidence import CAPITAL_PLA_SOURCE_FIELD


def capital_pla_cmr_ratio(record: dict[str, Any] | None) -> float | None:
    """Return the canonical prudential PLA/CMR ratio for diagnostic consumers.

    Financial Evidence defines NovoPla/new_pla as the prudential PLA numerator.
    plajustado/pla_adjusted is deliberately not a fallback because it is an
    intermediate source field with different semantics.
    """
    if not record:
        return None
    try:
        pla = float(record.get(CAPITAL_PLA_SOURCE_FIELD))
        cmr = float(record.get("cmr"))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(pla) or not math.isfinite(cmr) or cmr <= 0:
        return None
    ratio = pla / cmr
    return ratio if math.isfinite(ratio) else None
