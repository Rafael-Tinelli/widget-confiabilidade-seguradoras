from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Optional, Set, Tuple


_STOP = {
    "s", "a", "sa", "s.a", "ltda", "me", "epp", "cia", "companhia",
    "de", "do", "da", "das", "dos", "e",
    "seguro", "seguros", "seguradora", "previdencia", "capitalizacao",
    "brasil", "gerais", "participacoes", "holding",
}


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return re.sub(r"\s+", " ", s)


def _tokens(s: str) -> Set[str]:
    t = set(_norm(s).split())
    return {x for x in t if len(x) > 2 and x not in _STOP}


def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _contains_bonus(q: str, c: str) -> float:
    qn = _norm(q).replace(" ", "")
    cn = _norm(c).replace(" ", "")
    if not qn or not cn:
        return 0.0
    if qn in cn or cn in qn:
        return 0.10
    return 0.0


@dataclass(frozen=True)
class Match:
    key: str
    score: float


class NameMatcher:
    def __init__(self, candidates: Dict[str, str]) -> None:
        self.candidates = candidates
        self._cand_tokens = {k: _tokens(v) for k, v in candidates.items()}

    def best(
        self,
        query: str,
        *,
        threshold: float = 0.85,
        min_margin: float = 0.08,
    ) -> Optional[Match]:
        qt = _tokens(query)
        if not qt:
            return None

        best_key = ""
        best_score = 0.0
        second = 0.0

        for k, cname in self.candidates.items():
            ct = self._cand_tokens.get(k, set())
            if not ct:
                continue

            s = _jaccard(qt, ct) + _contains_bonus(query, cname)
            if s > best_score:
                second = best_score
                best_score = s
                best_key = k
            elif s > second:
                second = s

        # Caso com 1 token: seja mais rígido (evita “PORTO” bater em qualquer coisa)
        if len(qt) == 1:
            threshold = max(threshold, 0.92)
            min_margin = max(min_margin, 0.12)

        if best_key and best_score >= threshold and (best_score - second) >= min_margin:
            return Match(key=best_key, score=round(best_score, 4))

        return None
