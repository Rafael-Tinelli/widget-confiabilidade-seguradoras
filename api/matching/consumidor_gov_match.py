from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


_CORP_STOPWORDS = {
    "seguro", "seguros", "seguradora", "seguradoras", "resseguro", "resseguradora",
    "previdencia", "previdenciaria", "capitalizacao",
    "sa", "s", "a", "s/a", "s.a", "s.a.",
    "ltda", "me", "epp", "eireli",
    "cia", "cia.", "companhia",
    "de", "do", "da", "das", "dos", "e", "em", "para", "por", "no", "na",
    "brasil", "br", "brazil",
    "holding", "grupo",
    "servicos", "servico", "administradora", "administracao", "adm",
    "corretora", "corretagem",
    "banco",
}


def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("\ufeff", "")
    s = _strip_accents(s)
    s = s.replace("&", " e ")
    s = re.sub(r"[\./\\\-_,;:(){}\[\]<>|+*=!@#$%^~`]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _tokenize(name: str) -> List[str]:
    s = _norm(name)
    if not s:
        return []
    toks = [t for t in s.split(" ") if t and t not in _CORP_STOPWORDS and len(t) > 1]
    toks = [t for t in toks if not t.isdigit()]
    return toks


def _jaccard(a: Iterable[str], b: Iterable[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _containment(a: Iterable[str], b: Iterable[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa:
        return 0.0
    return len(sa & sb) / len(sa)


@dataclass(frozen=True)
class Match:
    key: str
    score: float


class NameMatcher:
    def __init__(self, candidates: Dict[str, str]):
        self._candidates = candidates
        self._cand_tokens: Dict[str, List[str]] = {k: _tokenize(v) for k, v in candidates.items()}

    def best(
        self,
        query_name: str,
        *,
        threshold: float = 0.85,
        min_margin: float = 0.08,
    ) -> Optional[Match]:
        q_tokens = _tokenize(query_name)
        if not q_tokens:
            return None

        best_key: Optional[str] = None
        best_score = 0.0
        second_score = 0.0

        q_join = " ".join(q_tokens)

        for key, c_tokens in self._cand_tokens.items():
            if not c_tokens:
                continue

            j = _jaccard(q_tokens, c_tokens)
            c = _containment(q_tokens, c_tokens)
            score = max(j, c)

            c_join = " ".join(c_tokens)
            if c_join.startswith(q_join) or q_join.startswith(c_join):
                score = min(1.0, score + 0.05)

            if score > best_score:
                second_score = best_score
                best_score = score
                best_key = key
            elif score > second_score:
                second_score = score

        if not best_key:
            return None
        if best_score < threshold:
            return None
        if (best_score - second_score) < min_margin:
            return None

        return Match(best_key, float(round(best_score, 6)))
