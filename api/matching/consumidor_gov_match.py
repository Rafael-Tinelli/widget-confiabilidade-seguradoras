# api/matching/consumidor_gov_match.py
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Optional, Set

# --- DICIONÁRIO DE MATCH MANUAL (EXPANDIDO) ---
# Mapeia: Parte do nome SUSEP (normalizado) -> Nome Exato no Consumidor.gov
# Chaves devem ser minúsculas e sem acentos.
MANUAL_ALIASES = {
    # Grupo Bradesco
    "bradesco vida": "Bradesco Seguros",
    "bradesco auto": "Bradesco Seguros",
    "bradesco capitalizacao": "Bradesco Capitalização",
    "bradesco saude": "Bradesco Saúde",
    "bradesco seguros": "Bradesco Seguros",
    
    # Grupo SulAmérica
    "sul america": "SulAmérica Seguros",
    "sulamerica": "SulAmérica Seguros",
    
    # Grupo BB / Brasilprev / Mapfre
    "brasilprev": "Brasilprev",
    "brasilseg": "Brasilseg",
    "mapfre": "Mapfre Seguros",
    
    # Grupo Caixa
    "caixa vida": "Caixa Seguradora",
    "caixa seguradora": "Caixa Seguradora",
    "caixa residencial": "Caixa Seguradora",
    
    # Grupo Itaú
    "itau vida": "Itaú Seguros",
    "itau seguros": "Itaú Seguros",
    "itau auto": "Itaú Seguros",
    
    # Outros Grandes
    "porto seguro": "Porto Seguro",
    "azul seguros": "Azul Seguros",
    "tokio marine": "Tokio Marine Seguradora",
    "liberty": "Liberty Seguros",
    "allianz": "Allianz Seguros",
    "hdi": "HDI Seguros",
    "sompo": "Sompo Seguros",
    "chubb": "Chubb Seguros",
    "zurich": "Zurich Seguros",
    "zurich santander": "Zurich Santander",
    "prudential": "Prudential do Brasil",
    "generali": "Generali Brasil",
    "icatu": "Icatu Seguros",
    "unimed": "Seguros Unimed",
}

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _norm(s: str) -> str:
    s = _strip_accents((s or "").lower()).strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

DEFAULT_STOPWORDS: Set[str] = {
    "s", "sa", "s.a", "cia", "companhia", "comp", "ltda", "me", "epp", 
    "de", "da", "do", "das", "dos", "em", "para", "por", "e", "the", "of",
    "grupo", "holding", "participacoes"
}

def _tokens(s: str, stopwords: Set[str]) -> Set[str]:
    s = _norm(s)
    toks = set(s.split())
    return {t for t in toks if t and t not in stopwords and len(t) >= 2}

def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)

@dataclass(frozen=True)
class Match:
    key: str
    score: float

class NameMatcher:
    def __init__(self, candidates: Dict[str, str], stopwords: Optional[Set[str]] = None) -> None:
        self.stopwords = stopwords or DEFAULT_STOPWORDS
        # candidates = {NomeOficialConsumidorGov: Dados...}
        # Invertemos para facilitar a busca manual: {nome_lower: NomeOficial}
        self._real_names_map = {k.lower(): k for k in candidates.keys()}
        self._cand_tokens = {k: _tokens(k, self.stopwords) for k in candidates.keys()}

    def best(self, query: str, threshold: float = 0.60, min_margin: float = 0.05) -> Optional[Match]:
        q_norm = _norm(query)
        
        # 1. TENTATIVA MANUAL (ALIAS)
        for alias, target_name in MANUAL_ALIASES.items():
            if alias in q_norm:
                # Verifica se o target existe na base carregada
                # (Ex: se "Bradesco Seguros" está no JSON do Consumidor.gov)
                real_key = self._find_target_key(target_name)
                if real_key:
                    return Match(key=real_key, score=1.0)

        # 2. ALGORITMO AUTOMÁTICO
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

        if best_key and best_score >= threshold:
            if best_score == 1.0 or (best_score - second) >= min_margin:
                return Match(key=best_key, score=best_score)

        return None

    def _find_target_key(self, partial_name: str) -> Optional[str]:
        p_norm = partial_name.lower()
        # Busca exata
        if p_norm in self._real_names_map:
            return self._real_names_map[p_norm]
        # Busca parcial
        for k_lower, k_original in self._real_names_map.items():
            if p_norm in k_lower:
                return k_original
        return None
