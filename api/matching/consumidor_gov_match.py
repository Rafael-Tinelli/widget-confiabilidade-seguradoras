# api/matching/consumidor_gov_match.py
from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, Optional, Tuple

from api.utils.name_cleaner import get_name_tokens, is_likely_b2b, normalize_name_key, normalize_strong

# Aliases mínimos (use com parcimônia).
ALIASES: dict[str, str] = {
    "brasilseg": "bb seguros",
    "brasilveiculos": "bb seguros",
    "banco do brasil": "bb seguros",
    "bancodobrasil": "bb seguros",
    "bb seguros": "bb seguros",
    "tokio": "tokio marine",
    "cardif": "bnpparibas cardif",
    "metropolitan": "metlife",
    "met life": "metlife",
    "itau": "itau seguros",
    "itaú": "itau seguros",
    "axa cs": "axa corporate solutions",
    "global corporate": "allianz global corporate & specialty",
}


@dataclass(frozen=True)
class MatchMeta:
    method: str  # 'cnpj', 'alias', 'fuzzy_token', 'fuzzy_seq', 'b2b_skipped'
    score: int   # 0-100
    target: str  # display/identifier
    is_b2b: bool = False


_CNPJ_RE = re.compile(r"\D+")


def _clean_cnpj(raw: Any) -> str:
    return _CNPJ_RE.sub("", str(raw or ""))


def _token_weight(t: str) -> float:
    # 1..~4 (favorece tokens maiores sem explodir)
    return 1.0 + min(3.0, (len(t) - 2) / 4.0)


def _soft_token_match(a: str, b: str) -> float:
    """
    Força de match em [0,1]:
      - 1.0 exact
      - 0.85 prefix forte (min len >= 3)
      - 0.80 substring (min len >= 3)
      - 0.0 caso contrário
    """
    if a == b:
        return 1.0
    if len(a) < 3 or len(b) < 3:
        return 0.0
    if a.startswith(b) or b.startswith(a):
        return 0.85
    if a in b or b in a:
        return 0.80
    return 0.0


def _weighted_dice(a_tokens: Iterable[str], b_tokens: Iterable[str]) -> float:
    """
    Dice ponderado com soft token match.
    Score em [0,1].
    """
    a = list(set(a_tokens))
    b = list(set(b_tokens))
    if not a or not b:
        return 0.0

    # Greedy: casa tokens mais pesados primeiro para evitar overcount.
    a_sorted = sorted(a, key=_token_weight, reverse=True)
    b_remaining = set(b)

    match_weight = 0.0
    for ta in a_sorted:
        best_tb = None
        best_strength = 0.0
        for tb in b_remaining:
            strength = _soft_token_match(ta, tb)
            if strength > best_strength:
                best_strength = strength
                best_tb = tb
                if best_strength >= 1.0:
                    break
        if best_tb is not None and best_strength > 0.0:
            match_weight += _token_weight(ta) * best_strength
            b_remaining.remove(best_tb)

    total_weight = sum(_token_weight(t) for t in a) + sum(_token_weight(t) for t in b)
    if total_weight <= 0:
        return 0.0
    return (2.0 * match_weight) / total_weight


class NameMatcher:
    """
    Match de seguradora (nome/CNPJ) contra agregado Consumidor.gov.
    Suporta:
      - by_cnpj_key_raw: {"123...": {...}}
      - by_name_key_raw/by_name: {"normalized name": {...}}
    """

    def __init__(self, reputation_root: Dict[str, Any]):
        self.raw = reputation_root or {}
        self.by_cnpj: dict[str, dict] = {}
        # entries: (tokens, strong_key, entry)
        self.entries: list[tuple[set[str], str, dict]] = []
        self._build_indexes()

    def _iter_entries(self) -> Iterable[tuple[str, dict]]:
        by_name = self.raw.get("by_name") or {}
        by_name_key = self.raw.get("by_name_key_raw") or self.raw.get("by_name_key") or {}
        if by_name:
            return by_name.items()
        return by_name_key.items()

    def _build_indexes(self) -> None:
        for key, entry in self._iter_entries():
            if not isinstance(entry, dict):
                continue

            cnpj = entry.get("cnpj")
            if cnpj:
                c = _clean_cnpj(cnpj)
                if len(c) == 14:
                    self.by_cnpj[c] = entry

            display = entry.get("display_name") or entry.get("name") or key or ""
            toks = get_name_tokens(display)
            strong = normalize_strong(display)
            if toks:
                self.entries.append((toks, strong, entry))

    def get_entry(
        self,
        name: str,
        *,
        cnpj: Optional[str] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[MatchMeta]]:
        if not name:
            return None, None

        # 0) B2B/resseguro => "N/A correto"
        if is_likely_b2b(name):
            return None, MatchMeta(method="b2b_skipped", score=0, target="", is_b2b=True)

        # 1) CNPJ exact
        if cnpj:
            c = _clean_cnpj(cnpj)
            if c and c in self.by_cnpj:
                return self.by_cnpj[c], MatchMeta(method="cnpj", score=100, target=c)

        q_tokens = get_name_tokens(name)
        if not q_tokens:
            return None, None

        # 2) Alias (silver)
        name_key = normalize_name_key(name)
        for src_sub, alias_target in ALIASES.items():
            if src_sub in name_key:
                alias_tokens = get_name_tokens(alias_target)
                best_entry, best_score = self._best_token_match(alias_tokens)
                if best_entry is not None and best_score >= 0.80:
                    return best_entry, MatchMeta(method="alias", score=95, target=alias_target)

        # 3) Fuzzy token (Dice ponderado) (bronze)
        best_entry, best_score = self._best_token_match(q_tokens)
        if best_entry is not None and best_score >= 0.80:
            target_name = best_entry.get("display_name") or best_entry.get("name") or "unknown"
            return best_entry, MatchMeta(method="fuzzy_token", score=int(best_score * 100), target=str(target_name))

        # 4) Fallback SequenceMatcher com limiar alto (>=0.92)
        q_strong = normalize_strong(name)
        if q_strong:
            best_entry, best_ratio = self._best_seq_match(q_strong)
            if best_entry is not None and best_ratio >= 0.92:
                target_name = best_entry.get("display_name") or best_entry.get("name") or "unknown"
                return best_entry, MatchMeta(method="fuzzy_seq", score=int(best_ratio * 100), target=str(target_name))

        return None, None

    def _best_token_match(self, query_tokens: set[str]) -> tuple[Optional[dict], float]:
        best_entry: Optional[dict] = None
        best_score = 0.0
        for db_tokens, _strong, entry in self.entries:
            sc = _weighted_dice(query_tokens, db_tokens)
            if sc > best_score:
                best_score = sc
                best_entry = entry
        return best_entry, best_score

    def _best_seq_match(self, query_strong: str) -> tuple[Optional[dict], float]:
        best_entry: Optional[dict] = None
        best_ratio = 0.0
        for _toks, strong, entry in self.entries:
            if not strong:
                continue
            ratio = SequenceMatcher(a=query_strong, b=strong).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_entry = entry
        return best_entry, best_ratio
