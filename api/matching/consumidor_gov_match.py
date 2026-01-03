# api/matching/consumidor_gov_match.py
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Set

# -----------------------------
# CNPJ helpers (Mantidos para compatibilidade)
# -----------------------------

_CNPJ_DIGITS_RE = re.compile(r"\D+")

def normalize_cnpj(value: Optional[str]) -> Optional[str]:
    """Return 14-digit CNPJ or None."""
    if not value:
        return None
    digits = _CNPJ_DIGITS_RE.sub("", str(value))
    return digits if len(digits) == 14 else None

def format_cnpj(digits14: str) -> str:
    """Format 14-digit CNPJ to 00.000.000/0000-00."""
    d = _CNPJ_DIGITS_RE.sub("", str(digits14))
    if len(d) != 14:
        return str(digits14)
    return f"{d[0:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"

# -----------------------------
# Lógica "Antiga" Restaurada (Agressiva)
# -----------------------------

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _norm(s: str) -> str:
    s = _strip_accents((s or "").lower()).strip()
    # Remove caracteres especiais mantendo espaços
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

# STOPWORDS DO ANTIGO (CRÍTICO PARA O MATCH FUNCIONAR)
_STOPWORDS: Set[str] = {
    # Legal
    "s", "sa", "s.a", "s/a", "ltda", "me", "epp", "eireli", "ei", "sucursal", "filial", 
    "companhia", "cia", "comp", "co", "inc", "ltd", "corp",
    # Conectivos
    "de", "da", "do", "das", "dos", "em", "para", "por", "e", "the", "of", "a", "o", "ao", "aos",
    # Domínio Seguros/Financeiro (ESSENCIAL)
    "seguro", "seguros", "seguradora", "seguradoras", 
    "previdencia", "previdenciaria", "vida", 
    "capitalizacao", "resseguro", "resseguradora", "resseguros",
    "corretora", "corretagem", "administradora", 
    "servicos", "servico", "assistencia", "beneficios",
    "grupo", "holding", "participacoes", "participacao",
    "banco", "financeira", "cons", "consorcio", "investimentos"
}

def _tokens(s: str) -> Set[str]:
    """Quebra em tokens limpando as palavras inúteis."""
    s = _norm(s)
    toks = set(s.split())
    # Filtra stopwords e palavras muito curtas (exceto siglas conhecidas se houver)
    return {t for t in toks if t and t not in _STOPWORDS and len(t) >= 2}

def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0

# -----------------------------
# Public API (Interface Nova)
# -----------------------------

@dataclass(frozen=True)
class MatchResult:
    key: str
    score: float
    method: str  # "cnpj" | "fuzzy"
    candidate: str | None = None
    details: dict[str, float] | None = None

class NameMatcher:
    def __init__(self, aggregated_root: dict[str, Any]):
        self._root = aggregated_root or {}

        # Suporte híbrido a schemas (by_name ou raiz direta)
        if isinstance(self._root, dict) and ("by_name" in self._root or "by_cnpj_key" in self._root):
            self.by_name: dict[str, Any] = self._root.get("by_name") or {}
            self.by_cnpj: dict[str, Any] = self._root.get("by_cnpj_key") or {}
        else:
            # Assumindo formato simples {normalized_name: entry}
            self.by_name = dict(self._root) if isinstance(self._root, dict) else {}
            self.by_cnpj = {}

        # 1. Indexação por CNPJ (se houver, mesmo que raro no HTML scraper)
        self._cnpj_to_key: dict[str, str] = {}
        for k, v in self.by_name.items():
            # Tenta achar CNPJ dentro do objeto de dados, se existir
            if isinstance(v, dict):
                c = normalize_cnpj(v.get("cnpj"))
                if c:
                    self._cnpj_to_key[c] = k
        
        # 2. Pré-processamento dos Tokens dos Candidatos (Otimização)
        self._candidates_tokens: list[tuple[str, str, Set[str]]] = []
        for key, entry in self.by_name.items():
            # Nome oficial no JSON
            official_name = entry.get("name") or entry.get("display_name") or key
            toks = _tokens(official_name)
            if toks:
                self._candidates_tokens.append((key, official_name, toks))

    def best(self, susep_name: str, *, cnpj: Optional[str] = None, threshold: float = 0.65) -> Optional[MatchResult]:
        """
        Encontra a melhor empresa no Consumidor.gov.
        Threshold padrão reduzido para 0.65 pois Jaccard é punitivo.
        """
        # 1. Tentativa Exata por CNPJ
        cnpj_digits = normalize_cnpj(cnpj)
        if cnpj_digits and cnpj_digits in self._cnpj_to_key:
            return MatchResult(
                key=self._cnpj_to_key[cnpj_digits], 
                score=1.0, 
                method="cnpj",
                candidate="CNPJ Match"
            )

        # 2. Matching Fuzzy (Tokens + Jaccard)
        q_tokens = _tokens(susep_name)
        if not q_tokens:
            return None

        best_key = None
        best_score = 0.0
        best_cand_name = None

        for key, cand_name, c_tokens in self._candidates_tokens:
            # Jaccard puro é excelente para "Bradesco Seguros" (A) vs "Bradesco" (B)
            # Tokens A: {bradesco} (seguros removido)
            # Tokens B: {bradesco}
            # Score: 1.0
            score = _jaccard(q_tokens, c_tokens)
            
            if score > best_score:
                best_score = score
                best_key = key
                best_cand_name = cand_name
                
                # Otimização: Se achou match perfeito, para.
                if score >= 0.99:
                    break

        if best_key and best_score >= threshold:
            return MatchResult(
                key=best_key, 
                score=best_score, 
                method="fuzzy", 
                candidate=best_cand_name,
                details={"jaccard": best_score}
            )
        
        return None

    def get_entry(self, susep_name: str, *, cnpj: Optional[str] = None, threshold: float = 0.65) -> tuple[Optional[Any], Optional[MatchResult]]:
        mr = self.best(susep_name, cnpj=cnpj, threshold=threshold)
        if not mr:
            return None, None
        
        entry = self.by_name.get(mr.key)
        return entry, mr
