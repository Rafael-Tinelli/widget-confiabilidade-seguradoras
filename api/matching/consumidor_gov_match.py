# api/matching/consumidor_gov_match.py
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Optional, Set

# Palavras que NÃO definem a identidade da empresa (Ruído)
GENERIC_TERMS = {
    # Jurídico
    "s", "sa", "s/a", "s.a", "ltda", "ltd", "inc", "corp", "corporation", 
    "limitada", "me", "epp", "cia", "companhia", "comp", "sociedade",
    "grupo", "holding", "participacoes", "participacao", "do", "da", "de", "e",
    
    # Setor Seguros/Financeiro (Essenciais para remover!)
    "seguros", "seguro", "seguradora", "seguridade",
    "previdencia", "vida", "saude", "capitalizacao", "consocrio",
    "resseguros", "resseguradora", "auto", "automovel", "patrimonial",
    "habitacional", "riscos", "especiais", "gerais", "corporativos",
    "brasil", "nacional", "internacional", "global", "servicos",
    "assistencia", "investimentos", "financeira", "banco", "bank"
}

def _normalize(s: str) -> str:
    """Remove acentos, lowercase e caracteres especiais."""
    s = str(s or "").lower()
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def _extract_brand_tokens(name: str) -> Set[str]:
    """
    Extrai apenas os tokens que provavemente são a MARCA.
    Ex: 'Bradesco Vida e Previdência S.A.' -> {'bradesco'}
    Ex: 'Tokio Marine Seguradora' -> {'tokio', 'marine'}
    """
    normalized = _normalize(name)
    tokens = set(normalized.split())
    # Remove palavras genéricas e tokens muito curtos (1 letra)
    brand_tokens = {t for t in tokens if t not in GENERIC_TERMS and len(t) > 1}
    return brand_tokens

@dataclass(frozen=True)
class Match:
    key: str
    score: float

class NameMatcher:
    def __init__(self, candidates: Dict[str, str]) -> None:
        # candidates = {NomeOficial: Dados...}
        self.candidates = candidates
        # Pré-processa os tokens de marca de todos os candidatos (SUSEP)
        self._cand_brands = {
            k: _extract_brand_tokens(k) for k in candidates.keys()
        }

    def best(self, query: str, threshold: float = 0.0) -> Optional[Match]:
        """
        Encontra o melhor match baseando-se na intersecção de MARCAS.
        Threshold é ignorado aqui pois usamos lógica booleana de conjuntos.
        """
        q_brands = _extract_brand_tokens(query)
        
        # Se não sobrou nada (ex: o nome era só "Seguradora S.A."), aborta
        if not q_brands:
            return None

        best_key = None
        best_overlap = 0
        min_len_diff = float('inf') # Para desempatar pelo tamanho do nome

        for cand_name, cand_brands in self._cand_brands.items():
            if not cand_brands:
                continue

            # Intersecção: Quantas palavras da marca coincidem?
            # Ex: {tokio, marine} & {tokio, marine} = 2
            intersection = q_brands.intersection(cand_brands)
            overlap = len(intersection)
            
            # Regra de Ouro:
            # Para dar match, todos os tokens de marca extraídos de um lado 
            # devem estar presentes no outro, ou vice-versa (subset).
            # Isso evita que "Porto Seguro" dê match com "Azul Seguros" (zero overlap)
            
            is_strong_match = (
                overlap > 0 and (
                    overlap == len(q_brands) or   # Query está contida no Candidato
                    overlap == len(cand_brands)   # Candidato está contido na Query
                )
            )

            if is_strong_match:
                # Se temos um match forte, usamos heurística para escolher o "melhor"
                # (Geralmente o que tem o tamanho mais próximo ou maior overlap)
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_key = cand_name
                    min_len_diff = abs(len(cand_name) - len(query))
                elif overlap == best_overlap:
                    # Desempate: pega o que tem tamanho de nome mais parecido
                    len_diff = abs(len(cand_name) - len(query))
                    if len_diff < min_len_diff:
                        min_len_diff = len_diff
                        best_key = cand_name

        if best_key:
            # Score 1.0 pois é um match semântico, não estatístico
            return Match(key=best_key, score=1.0)
            
        return None
