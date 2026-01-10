# api/matching/consumidor_gov_match.py
from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Iterable

from api.utils.name_cleaner import (
    get_name_tokens,
    is_likely_b2b,
    normalize_cnpj,
    normalize_name_key,
    normalize_strong,
)

__all__ = ["NameMatcher", "MatchMeta", "format_cnpj"]

def format_cnpj(cnpj_digits: str | None) -> str | None:
    d = normalize_cnpj(cnpj_digits)
    if not d: return None
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"

ALIASES: dict[str, str] = {
    "sulamerica": "sul america",
    "sulacap": "sul america capitalizacao",
    "eulerhermes": "euler hermes",
    "allianztrade": "euler hermes",
    "creditoycaucion": "credito y caucion",
    "creditoexportacao": "credito a exportacao",
    "markel": "markel",
    "sbce": "seguradora brasileira de credito a exportacao",
    "abgf": "agencia brasileira gestora de fundos garantidores e garantias",
    "tokiomarine": "tokio marine",
    "hdi": "hdi seguros",
    "liberty": "liberty seguros",
    "zurich": "zurich seguros",
    "chubb": "chubb seguros",
}

GENERIC_TERMS = {
    "cia", "companhia", "comp", "sociedade",
    "seguros", "seguro", "seguradora", "resseguros", "resseguradora",
    "capitalizacao", "previdencia", "vida", "saude",
    "brasil", "brasileira", "gerais",
    "sa", "s", "a", "ltda", "inc", "corp", "group", "holding",
    "do", "de", "da", "e", "participacoes"
}

@dataclass(frozen=True)
class MatchMeta:
    method: str
    score: float
    query: str
    matched_name: str | None = None
    matched_cnpj: str | None = None
    is_b2b: bool = False

def _token_weights(tokens: Iterable[str]) -> dict[str, float]:
    w: dict[str, float] = {}
    for t in set(tokens):
        if t in GENERIC_TERMS or len(t) <= 2:
            w[t] = 0.1
        else:
            w[t] = max(1.0, min(5.0, len(t) / 2.0))
    return w

def _soft_overlap_weight(q_tokens: set[str], t_tokens: set[str]) -> float:
    if not q_tokens or not t_tokens: return 0.0

    tw = _token_weights(t_tokens)
    qw = _token_weights(q_tokens)
    
    matched_weight = 0.0
    t_map = {tt: tw[tt] for tt in t_tokens}
    
    for qt in q_tokens:
        q_weight = qw[qt]
        best_w = 0.0
        
        if qt in t_map:
            best_w = q_weight
        else:
            for tt in t_map: 
                if qt in GENERIC_TERMS or tt in GENERIC_TERMS: continue
                if len(qt) >= 4 and (qt.startswith(tt) or tt.startswith(qt)):
                    best_w = max(best_w, q_weight * 0.8)
                elif len(qt) >= 4 and (qt in tt or tt in qt):
                    best_w = max(best_w, q_weight * 0.6)
        
        matched_weight += best_w

    total_q = sum(qw.values())
    if total_q <= 0: return 0.0
    
    return min(1.0, matched_weight / total_q)

class NameMatcher:
    def __init__(self, reputation_root: dict[str, Any]) -> None:
        self.by_name = {}
        self.by_cnpj = {}
        
        if isinstance(reputation_root, dict):
            bn = reputation_root.get("by_name_key_raw") or reputation_root.get("by_name") or {}
            bc = reputation_root.get("by_cnpj_key_raw") or reputation_root.get("by_cnpj_key") or {}
            
            if isinstance(bn, dict): self.by_name = {str(k): v for k, v in bn.items() if isinstance(v, dict)}
            if isinstance(bc, dict): self.by_cnpj = {str(k): v for k, v in bc.items() if isinstance(v, dict)}

        self._token_index: dict[str, list[str]] = {}
        for nk, entry in self.by_name.items():
            disp = str(entry.get("display_name") or entry.get("name") or "")
            for t in get_name_tokens(disp):
                if t not in GENERIC_TERMS:
                    self._token_index.setdefault(t, []).append(nk)

    def _apply_alias(self, name: str) -> str:
        strong = normalize_strong(name)
        for trigger, canonical in ALIASES.items():
            if trigger and trigger in strong: return canonical
        return name

    def get_entry(
        self,
        name: str,
        trade_name: str | None = None,
        cnpj: str | None = None,
        threshold: float = 0.75,
        seq_threshold: float = 0.92
    ) -> tuple[dict[str, Any] | None, MatchMeta]:
        
        q_name = (name or "").strip()
        if not q_name: return None, MatchMeta("empty", 0.0, "")

        # 1. CNPJ (Prioridade Máxima - antes de B2B check)
        c = normalize_cnpj(cnpj)
        if c and c in self.by_cnpj:
            e = self.by_cnpj[c]
            dn = str(e.get("display_name") or e.get("name") or "")
            return e, MatchMeta("cnpj", 1.0, q_name, dn, c)

        # 2. B2B Check (Só se não casou por CNPJ)
        if is_likely_b2b(q_name):
            return None, MatchMeta("b2b_skip", 0.0, q_name, is_b2b=True)

        # 3. Match Razão Social
        e_legal, m_legal = self._match_text(q_name, threshold, seq_threshold)
        
        # 4. Match Trade Name
        if trade_name and trade_name.strip():
            t = trade_name.strip()
            if t.lower() != q_name.lower() and not is_likely_b2b(t):
                e_trade, m_trade = self._match_text(t, threshold, seq_threshold)
                
                score_l = m_legal.score if m_legal else 0.0
                score_t = m_trade.score if m_trade else 0.0
                
                if score_t > score_l:
                    if m_trade:
                        m_trade = MatchMeta(f"{m_trade.method}_trade", m_trade.score, t, m_trade.matched_name, m_trade.matched_cnpj, m_trade.is_b2b)
                    return e_trade, m_trade

        return e_legal, m_legal

    def _match_text(self, query: str, threshold: float, seq_threshold: float) -> tuple[dict[str, Any] | None, MatchMeta]:
        q2 = self._apply_alias(query)
        q_tokens = set(get_name_tokens(q2))
        
        relevant = [t for t in q_tokens if t not in GENERIC_TERMS]
        if not relevant: return self._fallback(q2, seq_threshold)

        candidates = set()
        for t in relevant:
            candidates.update(self._token_index.get(t, []))
            
        best_k, best_s = None, 0.0
        second_best_s = 0.0
        
        for k in candidates:
            entry = self.by_name[k]
            disp = str(entry.get("display_name") or entry.get("name") or "")
            t_tokens = set(get_name_tokens(disp))
            
            s = _soft_overlap_weight(q_tokens, t_tokens)
            
            if s > best_s:
                second_best_s = best_s
                best_s = s
                best_k = k
            elif s > second_best_s:
                second_best_s = s
        
        if best_k and best_s >= threshold:
            # Guard de Ambiguidade
            if (best_s - second_best_s) < 0.06 and best_s < 0.95:
                 return None, MatchMeta("ambiguous_rejected", round(best_s, 4), query)

            e = self.by_name[best_k]
            dn = str(e.get("display_name") or e.get("name") or "")
            return e, MatchMeta("smart_dice", round(best_s, 4), query, dn)
            
        return self._fallback(q2, seq_threshold)

    def _fallback(self, q: str, threshold: float) -> tuple[dict[str, Any] | None, MatchMeta]:
        qs = normalize_name_key(q)
        best_k, best_r = None, 0.0
        for k, e in self.by_name.items():
            # [AJUSTE] Prioriza display_name para consistência
            ds = normalize_name_key(str(e.get("display_name") or e.get("name") or ""))
            r = SequenceMatcher(None, qs, ds).ratio()
            if r > best_r:
                best_r = r
                best_k = k
        
        if best_k and best_r >= threshold:
            # [AJUSTE] Retorna display_name correto
            e = self.by_name[best_k]
            dn = str(e.get("display_name") or e.get("name") or "")
            return e, MatchMeta("seq", round(best_r, 4), q, dn)
        
        return None, MatchMeta("no_match", 0.0, q)
