# api/matching/consumidor_gov_match.py
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Optional, Set

# --- 1. REDE DE SEGURANÇA (ALIASES MANUAIS) ---
# Garante que os grandes players tenham match imediato, ignorando limpeza de tokens.
# Chave: Parte única do nome na SUSEP (normalizado) -> Valor: Nome exato no Consumidor.gov (ou parte dele)
MANUAL_ALIASES = {
    "porto seguro": "Porto Seguro",
    "itaú": "Itaú",
    "itau": "Itaú",
    "bradesco": "Bradesco",
    "sul america": "SulAmérica",
    "sulamerica": "SulAmérica",
    "caixa vida": "Caixa Seguradora",
    "caixa seguradora": "Caixa Seguradora",
    "brasilprev": "Brasilprev",
    "brasilseg": "Brasilseg",
    "mapfre": "Mapfre",
    "santander": "Santander",
    "azul": "Azul Seguros",
    "allianz": "Allianz",
    "liberty": "Liberty",
    "tokio": "Tokio Marine",
    "zurich": "Zurich",
    "chubb": "Chubb",
    "hdi": "HDI",
    "sompo": "Sompo",
    "generali": "Generali",
    "metlife": "MetLife",
    "prudential": "Prudential",
    "mag": "MAG Seguros",
    "mongeral": "MAG Seguros",
    "icatú": "Icatu",
    "icatu": "Icatu",
    "unimed": "Seguros Unimed",
    "cardif": "BNP Paribas Cardif",
    "bnp": "BNP Paribas Cardif",
    "suhai": "Suhai",
}

# --- 2. CONFIGURAÇÃO DO ALGORITMO ---
GENERIC_TERMS = {
    # Jurídico
    "s", "sa", "s/a", "s.a", "ltda", "ltd", "inc", "corp", "corporation", 
    "limitada", "me", "epp", "cia", "companhia", "comp", "sociedade",
    "grupo", "holding", "participacoes", "participacao", "do", "da", "de", "e",
    
    # Setor
    "seguros", "seguro", "seguradora", "seguridade",
    "previdencia", "vida", "saude", "capitalizacao", "consorcio",
    "resseguros", "resseguradora", "auto", "automovel", "patrimonial",
    "habitacional", "riscos", "especiais", "gerais", "corporativos",
    "brasil", "nacional", "internacional", "global", "servicos",
    "assistencia", "investimentos", "financeira", "banco", "bank"
}

def _normalize(s: str) -> str:
    s = str(s or "").lower()
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def _extract_brand_tokens(name: str) -> Set[str]:
    normalized = _normalize(name)
    tokens = set(normalized.split())
    # Mantém apenas tokens que NÃO são genéricos
    return {t for t in tokens if t not in GENERIC_TERMS and len(t) > 1}

@dataclass(frozen=True)
class Match:
    key: str
    score: float

class NameMatcher:
    def __init__(self, candidates: Dict[str, str]) -> None:
        # candidates = {NomeOficialConsumidorGov: Dados...}
        self.candidates = candidates
        # Mapa reverso para busca manual rápida
        self._candidates_norm = {_normalize(k): k for k in candidates.keys()}
        
        # Pré-processa tokens para busca algorítmica
        self._cand_brands = {
            k: _extract_brand_tokens(k) for k in candidates.keys()
        }

    def best(self, query: str, threshold: float = 0.0) -> Optional[Match]:
        q_norm = _normalize(query)
        
        # 1. BUSCA POR ALIAS (Prioridade Máxima)
        for alias, target_keyword in MANUAL_ALIASES.items():
            # Se o alias (ex: "porto seguro") está contido no nome da SUSEP
            if alias in q_norm:
                # Tenta encontrar o alvo na lista do Consumidor.gov
                target_match = self._find_target_by_keyword(target_keyword)
                if target_match:
                    return Match(key=target_match, score=1.0)

        # 2. BUSCA ALGORÍTMICA (Brand Stemming)
        q_brands = _extract_brand_tokens(query)
        if not q_brands:
            return None

        best_key = None
        best_overlap = 0
        min_len_diff = float('inf')

        for cand_name, cand_brands in self._cand_brands.items():
            if not cand_brands:
                continue

            intersection = q_brands.intersection(cand_brands)
            overlap = len(intersection)
            
            # Regra de Match: Todos os tokens de um lado devem estar no outro (Subset)
            is_strong_match = (
                overlap > 0 and (
                    overlap == len(q_brands) or 
                    overlap == len(cand_brands)
                )
            )

            if is_strong_match:
                # Desempate por tamanho do nome (preferência pelo nome mais próximo em comprimento)
                len_diff = abs(len(cand_name) - len(query))
                
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_key = cand_name
                    min_len_diff = len_diff
                elif overlap == best_overlap:
                    if len_diff < min_len_diff:
                        min_len_diff = len_diff
                        best_key = cand_name

        if best_key:
            return Match(key=best_key, score=1.0)
            
        return None

    def _find_target_by_keyword(self, keyword: str) -> Optional[str]:
        """Encontra o nome real no Consumidor.gov que contém a keyword."""
        kw_norm = _normalize(keyword)
        # 1. Tenta match exato primeiro
        for norm_name, real_name in self._candidates_norm.items():
            if norm_name == kw_norm:
                return real_name
        
        # 2. Tenta match parcial (keyword contida no nome)
        for norm_name, real_name in self._candidates_norm.items():
            if kw_norm in norm_name:
                return real_name
        return None
