# api/matching/consumidor_gov_match.py
from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Dict, Optional, Tuple

from api.utils.name_cleaner import (
    get_name_tokens,
    is_likely_b2b,
    normalize_name_key,
    normalize_strong,
)

# Manual aliases for brand-to-legal-name mismatches or known abbreviations.
# Keep it minimal; the token/strong matcher should cover most cases.
ALIASES: Dict[str, str] = {
    # Example patterns (extend only if you have evidence):
    # "sulamerica": "sul america",
    "sulacap": "sul america capitalizacao",
}


_CNPJ_RE = re.compile(r"\D+")


def format_cnpj(cnpj14: str) -> str:
    d = _CNPJ_RE.sub("", cnpj14 or "")
    if len(d) != 14:
        return ""
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"


def _cnpj_key(cnpj: Optional[str]) -> Optional[str]:
    if not cnpj:
        return None
    d = _CNPJ_RE.sub("", str(cnpj))
    return d if len(d) == 14 else None


def _token_sim(a: str, b: str) -> float:
    """
    Token similarity:
      - exact match: 1.0
      - prefix match (min len >= 3): 0.90
      - substring match (min len >= 4): 0.80
      - otherwise 0.0
    """
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    # normalize ordering for checks
    s, t = (a, b) if len(a) <= len(b) else (b, a)

    if len(s) >= 3 and t.startswith(s):
        return 0.90
    if len(s) >= 4 and s in t:
        return 0.80
    return 0.0


def _weighted_dice(q_tokens: set[str], t_tokens: set[str]) -> float:
    """
    Weighted Dice coefficient using greedy best matching between token sets.
    Returns score in [0.0, 1.0].
    """
    if not q_tokens or not t_tokens:
        return 0.0

    q = list(q_tokens)
    t = set(t_tokens)

    sum_best = 0.0
    # Greedy: for each query token, match to best remaining target token
    for qt in q:
        best_tt = None
        best_s = 0.0
        for tt in t:
            s = _token_sim(qt, tt)
            if s > best_s:
                best_s = s
                best_tt = tt
            # early exit on perfect
            if best_s == 1.0:
                break
        if best_tt is not None and best_s > 0.0:
            sum_best += best_s
            t.remove(best_tt)

    denom = (len(q_tokens) + len(t_tokens))
    if denom <= 0:
        return 0.0
    return (2.0 * sum_best) / float(denom)


@dataclass(frozen=True)
class MatchMeta:
    method: str
    score: float
    target_key: Optional[str] = None
    is_b2b: bool = False


class NameMatcher:
    """
    Matches an insurer (SES) to consumidor.gov aggregated metrics.

    Expected payload formats (we support both keys):
      - by_cnpj_key_raw / by_name_key_raw
      - by_cnpj_key     / by_name_key
    """

    def __init__(self, consumidor_payload: Dict[str, Any]) -> None:
        self._by_cnpj: Dict[str, Dict[str, Any]] = (
            (consumidor_payload.get("by_cnpj_key_raw") or {})
            or (consumidor_payload.get("by_cnpj_key") or {})
        )
        self._by_name: Dict[str, Dict[str, Any]] = (
            (consumidor_payload.get("by_name_key_raw") or {})
            or (consumidor_payload.get("by_name_key") or {})
        )

        # Precompute search structures
        self._entries: list[tuple[str, set[str], str, Dict[str, Any]]] = []
        for k, obj in self._by_name.items():
            display = (
                obj.get("display_name")
                or obj.get("name")
                or (obj.get("statistics") or {}).get("name")
                or k
            )
            tokens = get_name_tokens(str(display))
            strong = normalize_strong(str(display))
            self._entries.append((k, tokens, strong, obj))

    def get_entry(
        self, ses_name: str, cnpj: Optional[str] = None, threshold: float = 0.80
    ) -> Tuple[Optional[Dict[str, Any]], Optional[MatchMeta]]:
        # 0) B2B skip: do not attempt to match; treat as "not applicable"
        if is_likely_b2b(ses_name):
            return None, MatchMeta(method="b2b_skipped", score=1.0, is_b2b=True)

        # 1) CNPJ exact match (best)
        ckey = _cnpj_key(cnpj)
        if ckey and ckey in self._by_cnpj:
            return self._by_cnpj[ckey], MatchMeta(
                method="cnpj", score=1.0, target_key=ckey
            )

        # 2) Alias normalization
        key_norm = normalize_name_key(ses_name)
        for a, target in ALIASES.items():
            if a in key_norm:
                # attempt to find target by normalized key substring
                tgt_norm = normalize_name_key(target)
                if tgt_norm in self._by_name:
                    return self._by_name[tgt_norm], MatchMeta(
                        method="alias", score=0.99, target_key=tgt_norm
                    )

        # 3) Token fuzzy (weighted Dice + prefix/substring soft match)
        q_tokens = get_name_tokens(ses_name)
        if not q_tokens:
            return None, None

        best_key = None
        best_obj: Optional[Dict[str, Any]] = None
        best_score = 0.0

        for k, tokens, strong, obj in self._entries:
            s = _weighted_dice(q_tokens, tokens)
            if s > best_score:
                best_score = s
                best_key = k
                best_obj = obj
            if best_score >= 0.99:
                break

        if best_obj is not None and best_score >= threshold:
            return best_obj, MatchMeta(
                method="fuzzy_tokens", score=best_score, target_key=best_key
            )

        # 4) Strong full-string fallback with high threshold (>= 0.92)
        q_strong = normalize_strong(ses_name)
        if q_strong and len(q_strong) >= 6:
            best_key2 = None
            best_obj2: Optional[Dict[str, Any]] = None
            best_ratio = 0.0

            for k, tokens, strong, obj in self._entries:
                if not strong:
                    continue
                r = SequenceMatcher(None, q_strong, strong).ratio()
                if r > best_ratio:
                    best_ratio = r
                    best_key2 = k
                    best_obj2 = obj

            if best_obj2 is not None and best_ratio >= 0.92:
                return best_obj2, MatchMeta(
                    method="fuzzy_strong", score=best_ratio, target_key=best_key2
                )

        return None, None
