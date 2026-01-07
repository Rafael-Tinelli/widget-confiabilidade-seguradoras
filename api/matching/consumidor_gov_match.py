# api/matching/consumidor_gov_match.py
from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

from api.utils.name_cleaner import (
    get_name_tokens,
    is_likely_b2b,
    normalize_cnpj,
    normalize_name_key,
    normalize_strong,
)


def format_cnpj(cnpj_digits: str | None) -> str | None:
    d = normalize_cnpj(cnpj_digits)
    if not d:
        return None
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"


# Aliases: gatilhos (normalize_strong) -> forma canônica de busca
ALIASES: dict[str, str] = {
    # SulAmérica
    "sulamerica": "sul america",
    "sulacap": "sul america capitalizacao",
    # Allianz Trade (Euler Hermes)
    "eulerhermes": "euler hermes",
    "allianztrade": "euler hermes",
    # crédito/garantia
    "atradius": "atradius",
    "creditoycaucion": "credito y caucion",
    "creditoexportacao": "credito a exportacao",
    # pedidos explícitos
    "markel": "markel",
    "sbce": "seguradora brasileira de credito a exportacao",
    "abgf": "agencia brasileira gestora de fundos garantidores e garantias",
}


@dataclass(frozen=True)
class MatchMeta:
    method: str
    score: float
    query: str
    matched_name: str | None = None
    matched_cnpj: str | None = None
    is_b2b: bool = False


def _token_weights(tokens: set[str]) -> dict[str, float]:
    """
    Peso por token: privilegia tokens mais longos (mais discriminantes).
    """
    w: dict[str, float] = {}
    for t in tokens:
        w[t] = max(1.0, min(6.0, len(t) / 3.0))
    return w


def _soft_overlap_weight(q_tokens: set[str], t_tokens: set[str]) -> float:
    """
    Overlap "soft": além do match exato, aceita prefixo/substring com penalidade.
    """
    if not q_tokens or not t_tokens:
        return 0.0

    tw = _token_weights(t_tokens)
    used_q: set[str] = set()
    score = 0.0

    for qt in q_tokens:
        best = 0.0
        best_tt = None

        for tt in t_tokens:
            if qt == tt:
                best = tw.get(tt, 1.0)
                best_tt = tt
                break

            # prefixo (min 3) penaliza menos
            if len(qt) >= 3 and (tt.startswith(qt) or qt.startswith(tt)):
                cand = tw.get(tt, 1.0) * 0.65
                if cand > best:
                    best = cand
                    best_tt = tt
                continue

            # substring (min 4) penaliza mais
            if len(qt) >= 4 and (qt in tt or tt in qt):
                cand = tw.get(tt, 1.0) * 0.50
                if cand > best:
                    best = cand
                    best_tt = tt

        if best_tt and qt not in used_q:
            used_q.add(qt)
            score += best

    return score


def _weighted_dice(q_tokens: set[str], t_tokens: set[str]) -> float:
    """
    Dice ponderado (0..1), usando overlap soft.
    """
    if not q_tokens or not t_tokens:
        return 0.0

    qw = _token_weights(q_tokens)
    tw = _token_weights(t_tokens)

    w_q = sum(qw.values())
    w_t = sum(tw.values())
    w_i = _soft_overlap_weight(q_tokens, t_tokens)

    denom = w_q + w_t
    if denom <= 0:
        return 0.0
    return max(0.0, min(1.0, (2.0 * w_i) / denom))


class NameMatcher:
    """
    Matcher sobre dados agregados do Consumidor.gov.

    Espera um root com:
      - by_name_key_raw: {<name_key>: {...}}
      - by_cnpj_key_raw: {<cnpj14>: {...}}
    """

    def __init__(self, reputation_root: dict[str, Any]) -> None:
        self.by_name: dict[str, dict[str, Any]] = {}
        self.by_cnpj: dict[str, dict[str, Any]] = {}

        if not isinstance(reputation_root, dict):
            return

        bn = reputation_root.get("by_name_key_raw") or {}
        bc = reputation_root.get("by_cnpj_key_raw") or {}

        if isinstance(bn, dict):
            self.by_name = {str(k): v for k, v in bn.items() if isinstance(v, dict)}
        if isinstance(bc, dict):
            self.by_cnpj = {str(k): v for k, v in bc.items() if isinstance(v, dict)}

        # índice tokens -> name_keys candidatos
        self._token_index: dict[str, list[str]] = {}
        for nk, entry in self.by_name.items():
            disp = str(entry.get("display_name") or entry.get("name") or "")
            for t in get_name_tokens(disp):
                self._token_index.setdefault(t, []).append(nk)

    def _apply_alias(self, name: str) -> str:
        strong = normalize_strong(name)
        for trigger, canonical in ALIASES.items():
            if trigger and trigger in strong:
                return canonical
        return name

    def get_entry(
        self,
        name: str,
        cnpj: str | None = None,
        threshold: float = 0.80,
    ) -> tuple[dict[str, Any] | None, MatchMeta]:
        q_name = (name or "").strip()
        if not q_name:
            return None, MatchMeta(method="empty", score=0.0, query=q_name)

        # 0) B2B: não tentamos reputação
        if is_likely_b2b(q_name):
            return None, MatchMeta(method="b2b_skip", score=0.0, query=q_name, is_b2b=True)

        # 1) CNPJ (prioritário)
        c = normalize_cnpj(cnpj)
        if c and c in self.by_cnpj:
            e = self.by_cnpj[c]
            dn = str(e.get("display_name") or e.get("name") or "")
            return e, MatchMeta(method="cnpj", score=1.0, query=q_name, matched_name=dn, matched_cnpj=c)

        # 2) Alias/harmonização
        q2 = self._apply_alias(q_name)

        q_tokens = set(get_name_tokens(q2))
        if not q_tokens:
            return self._fallback_seq(q2, threshold=0.92)

        # 3) candidatos por índice
        candidate_keys: set[str] = set()
        for t in q_tokens:
            for nk in self._token_index.get(t, []):
                candidate_keys.add(nk)

        if not candidate_keys:
            candidate_keys = set(self.by_name.keys())

        best_key: str | None = None
        best_score = 0.0

        for nk in candidate_keys:
            entry = self.by_name.get(nk)
            if not entry:
                continue
            disp = str(entry.get("display_name") or entry.get("name") or "")
            t_tokens = set(get_name_tokens(disp))
            sc = _weighted_dice(q_tokens, t_tokens)
            if sc > best_score:
                best_score = sc
                best_key = nk

        if best_key and best_score >= threshold:
            e = self.by_name[best_key]
            dn = str(e.get("display_name") or e.get("name") or "")
            return e, MatchMeta(method="dice", score=round(best_score, 4), query=q_name, matched_name=dn)

        # 4) fallback SequenceMatcher (limiar alto)
        return self._fallback_seq(q2, threshold=0.92)

    def _fallback_seq(self, q: str, threshold: float = 0.92) -> tuple[dict[str, Any] | None, MatchMeta]:
        qs = normalize_name_key(q)
        qstrong = normalize_strong(q)

        best_key: str | None = None
        best_ratio = 0.0

        for nk, entry in self.by_name.items():
            disp = str(entry.get("display_name") or entry.get("name") or "")
            ds = normalize_name_key(disp)
            dstrong = normalize_strong(disp)

            r1 = SequenceMatcher(None, qs, ds).ratio() if qs and ds else 0.0
            r2 = SequenceMatcher(None, qstrong, dstrong).ratio() if qstrong and dstrong else 0.0
            r = max(r1, r2)

            if r > best_ratio:
                best_ratio = r
                best_key = nk

        if best_key and best_ratio >= threshold:
            e = self.by_name[best_key]
            dn = str(e.get("display_name") or e.get("name") or "")
            return e, MatchMeta(method="seq", score=round(best_ratio, 4), query=q, matched_name=dn)

        return None, MatchMeta(method="no_match", score=round(best_ratio, 4), query=q)
