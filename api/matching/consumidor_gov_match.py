from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from math import log
from typing import Dict, Optional, Set


_STOPWORDS: Set[str] = {
    # conectivos e partículas
    "de", "da", "do", "das", "dos", "e", "em", "no", "na", "nos", "nas", "por",
    # sufixos societários / jurídicos comuns
    "sa", "s", "a", "s.a", "s/a", "ltda", "me", "epp", "eireli", "sistema",
    "cia", "companhia", "comp", "cooperativa", "assoc", "associacao",
    "instituicao", "fundacao",
    # termos corporativos genéricos
    "grupo", "holding", "participacoes", "participacao", "administradora",
    "brasil", "brasileira", "nacional",
}

# Tokens curtos relevantes para marcas (ex.: "bb") entram via regra abaixo.
_MIN_TOKEN_LEN = 2


def _strip_parentheticals(s: str) -> str:
    # Remove conteúdo entre parênteses e colchetes (ex.: "(INATIVA)")
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"\[[^]]*\]", " ", s)
    return s


def _norm_ascii_lower(s: str) -> str:
    s = (s or "").strip()
    s = _strip_parentheticals(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    return s


def normalize_tokens(name: str) -> Set[str]:
    """
    Normaliza o nome e devolve tokens "úteis" para matching.

    Observação:
    - NÃO removemos termos do ramo (ex.: "seguros", "vida", "previdencia") por stopword.
      Eles ajudam a separar entidades dentro de um mesmo grupo econômico.
    """
    s = _norm_ascii_lower(name)
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    if not s:
        return set()

    tokens = [t for t in s.split() if len(t) >= _MIN_TOKEN_LEN]
    return {t for t in tokens if t not in _STOPWORDS}


@dataclass(frozen=True)
class Match:
    key: str
    score: float


class NameMatcher:
    """
    Matcher conservador para cruzar nomes entre Consumidor.gov (nome fantasia)
    e SES (nome formal). Retorna a melhor chave do Consumidor.gov (key) e score.

    Similaridade: Weighted Jaccard (IDF-like) + bônus leve de contenção.
    """

    def __init__(self, candidates: Dict[str, str]) -> None:
        # candidates: {consumer_key: display_name}
        self.candidates = candidates

        token_freq: Dict[str, int] = {}
        processed: Dict[str, tuple[str, Set[str], str]] = {}

        for key, display in candidates.items():
            disp = (display or "").strip()
            toks = normalize_tokens(disp)
            core = re.sub(r"\s+", "", _norm_ascii_lower(disp))
            processed[str(key)] = (disp, toks, core)

            for t in toks:
                token_freq[t] = token_freq.get(t, 0) + 1

        self._processed = processed
        self._N = max(1, len(processed))

        # IDF-like weights: raros pesam mais; muito comuns pesam pouco (mas não zeram)
        self._w: Dict[str, float] = {}
        for t, f in token_freq.items():
            self._w[t] = log((self._N + 1) / (f + 1)) + 1.0

    def _weighted_jaccard(self, a: Set[str], b: Set[str]) -> float:
        if not a or not b:
            return 0.0
        inter = a.intersection(b)
        uni = a.union(b)

        w_inter = 0.0
        for t in inter:
            w_inter += self._w.get(t, 1.0)

        w_union = 0.0
        for t in uni:
            w_union += self._w.get(t, 1.0)

        return (w_inter / w_union) if w_union > 0 else 0.0

    def best(
        self,
        query_name: str,
        *,
        threshold: float = 0.85,
        min_margin: float = 0.08,
    ) -> Optional[Match]:
        q = (query_name or "").strip()
        q_tokens = normalize_tokens(q)
        if not q_tokens:
            return None

        q_core = re.sub(r"\s+", "", _norm_ascii_lower(q))

        best_key: Optional[str] = None
        best_score = 0.0
        second = 0.0

        for key, (_disp, toks, core) in self._processed.items():
            if not toks:
                continue

            score = self._weighted_jaccard(q_tokens, toks)

            # Bônus leve por contenção: ajuda a desempatar em nomes muito parecidos
            # Ex.: "porto seguro" vs "porto seguro cartoes"
            if core and (core in q_core or q_core in core):
                score = min(1.0, score + 0.03)

            if score > best_score:
                second = best_score
                best_score = score
                best_key = key
            elif score > second:
                second = score

        if best_key is None:
            return None
        if best_score < threshold:
            return None
        if (best_score - second) < min_margin:
            return None

        return Match(key=best_key, score=best_score)
