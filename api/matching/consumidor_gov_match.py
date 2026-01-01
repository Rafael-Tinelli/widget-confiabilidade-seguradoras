# api/matching/consumidor_gov_match.py
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Optional, Set


def _strip_accents(s: str) -> str:
    """Remove acentuação (ex: 'São' -> 'Sao')."""
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _norm(s: str) -> str:
    """Normaliza string para lowercase, sem acentos e sem caracteres especiais."""
    s = _strip_accents((s or "").lower()).strip()
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


# LISTA DE STOPWORDS REVISADA (Menos agressiva)
# Removemos termos do "Core Business" (Seguro, Previdência) da exclusão.
# Agora o match considera "Porto Seguro" diferente de "Porto Serviços".
DEFAULT_STOPWORDS: Set[str] = {
    # Jurídico / Sufixos Genéricos
    "s", "sa", "s.a", "cia", "companhia", "comp", "ltda", "ltd", "inc",
    "me", "epp", "corp", "corporation", "limitada",
    
    # Conectivos e Preposições
    "de", "da", "do", "das", "dos", "em", "para", "por", "e", "ou",
    "the", "of", "and", "&",
    
    # Termos genéricos de estrutura corporativa (Ruído)
    "grupo", "holding", "participacoes", "participacao",
    "servicos", "servico", "assessoria", "consultoria", "negocios"
    
    # NOTA: Termos como "Seguros", "Vida", "Previdencia", "Capitalizacao" 
    # foram REMOVIDOS desta lista para serem considerados no cálculo de similaridade.
}


def _tokens(s: str, stopwords: Set[str]) -> Set[str]:
    """Quebra a string em tokens únicos e limpos."""
    s = _norm(s)
    toks = set(s.split())
    # Mantém tokens que não são stopwords e têm pelo menos 2 caracteres
    return {t for t in toks if t and t not in stopwords and len(t) >= 2}


def _jaccard(a: Set[str], b: Set[str]) -> float:
    """Calcula Índice de Jaccard (Interseção / União)."""
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
        # Pré-calcula tokens dos candidatos para performance
        self._cand_tokens: Dict[str, Set[str]] = {
            k: _tokens(v, self.stopwords) for k, v in candidates.items()
        }

    def best(self, query: str, threshold: float = 0.85, min_margin: float = 0.05) -> Optional[Match]:
        """
        Encontra o melhor match para a query.
        
        Args:
            query: Nome a ser buscado (ex: "Porto Seguro S.A.")
            threshold: Score mínimo (0 a 1) para aceitar o match.
            min_margin: Diferença mínima entre o 1º e o 2º colocado para evitar ambiguidade.
        """
        qtok = _tokens(query, self.stopwords)
        if not qtok:
            return None

        best_key = None
        best_score = 0.0
        second_score = 0.0

        for k, ctok in self._cand_tokens.items():
            sc = _jaccard(qtok, ctok)
            
            if sc > best_score:
                second_score = best_score
                best_score = sc
                best_key = k
            elif sc > second_score:
                second_score = sc

        # Critérios de Aceite
        if best_key is None:
            return None
        
        # 1. Deve atingir o score mínimo
        if best_score < threshold:
            return None
            
        # 2. Deve ser inequivocamente melhor que o segundo lugar (safety margin)
        # Exceção: Se o score for perfeito (1.0), aceitamos mesmo sem margem 
        # (caso de nomes duplicados na base mas que são idênticos)
        if best_score < 1.0 and (best_score - second_score) < min_margin:
            return None

        return Match(key=best_key, score=best_score)
