# api/matching/consumidor_gov_match.py
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _norm(s: str) -> str:
    s = _strip_accents((s or "").lower()).strip()
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


DEFAULT_STOPWORDS: Set[str] = {
    "s",
    "sa",
    "s.a",
    "cia",
    "companhia",
    "comp",
    "ltda",
    "me",
    "epp",
    "de",
    "da",
    "do",
    "das",
    "dos",
    "em",
    "para",
    "por",
    "e",
    "the",
    "of",
    # domínio seguro
    "seguro",
    "seguros",
    "seguradora",
    "seguradoras",
    "previdencia",
    "previdenciaria",
    "capitalizacao",
    "resseguro",
    "corretora",
    "corretagem",
    "servicos",
    "servico",
    "grupo",
    "holding",
}


def _tokens(s: str, stopwords: Set[str]) -> Set[str]:
    s = _norm(s)
    toks = set(s.split())
    return {t for t in toks if t and t not in stopwords and len(t) >= 2}


def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


@dataclass(frozen=True)
class Match:
    key: str
    score: float


class NameMatcher:
    def __init__(self, candidates: Dict[str, str], stopwords: Optional[Set[str]] = None) -> None:
        self.stopwords = stopwords or DEFAULT_STOPWORDS
        self.candidates = candidates
        self._cand_tokens: Dict[str, Set[str]] = {k: _tokens(v, self.stopwords) for k, v in candidates.items()}

    def best(self, query: str, threshold: float = 0.85, min_margin: float = 0.08) -> Optional[Match]:
        qtok = _tokens(query, self.stopwords)
        if not qtok:
            return None

        best_key = None
        best_score = 0.0
        second = 0.0

        for k, ctok in self._cand_tokens.items():
            sc = _jaccard(qtok, ctok)
            if sc > best_score:
                second = best_score
                best_score = sc
                best_key = k
            elif sc > second:
                second = sc

        if best_key is None:
            return None
        if best_score < threshold:
            return None
        if (best_score - second) < min_margin:
            return None

        return Match(key=best_key, score=best_score)
