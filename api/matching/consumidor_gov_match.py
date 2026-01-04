# api/matching/consumidor_gov_match.py
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Optional, Set

# -----------------------------
# CNPJ helpers
# -----------------------------

_CNPJ_DIGITS_RE = re.compile(r"\D+")


def normalize_cnpj(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    digits = _CNPJ_DIGITS_RE.sub("", str(value))
    return digits if len(digits) == 14 else None


def format_cnpj(digits14: str) -> str:
    d = _CNPJ_DIGITS_RE.sub("", str(digits14))
    if len(d) != 14:
        return str(digits14)
    return f"{d[0:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"


# -----------------------------
# Lógica de Matching
# -----------------------------

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _norm(s: str) -> str:
    s = _strip_accents((s or "").lower()).strip()
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _norm_strong(s: str) -> str:
    """Remove espaços para comparar 'SulAmerica' com 'Sul America'."""
    return _norm(s).replace(" ", "")


# STOPWORDS CRÍTICAS
_STOPWORDS: Set[str] = {
    "s", "sa", "s.a", "s/a", "ltda", "me", "epp", "eireli", "ei", "sucursal", "filial",
    "companhia", "cia", "comp", "co", "inc", "ltd", "corp",
    "de", "da", "do", "das", "dos", "em", "para", "por", "e", "the", "of", "a", "o", "ao", "aos",
    "seguro", "seguros", "seguradora", "seguradoras",
    "previdencia", "previdenciaria", "vida", "capitalizacao", "resseguro", "resseguradora", "resseguros",
    "corretora", "corretagem", "administradora", "servicos", "servico", "assistencia", "beneficios",
    "grupo", "holding", "participacoes", "participacao", "banco", "financeira", "cons", "consorcio", "investimentos"
}

# ALIAS MAP (Manual Override para Top Players que falham)
_MANUAL_ALIASES = {
    "SUL AMERICA": ["SULAMERICA"],
    "SULAMERICA": ["SUL AMERICA"],
    "TOKIO MARINE": ["TOKIO MARINE SEGURADORA"],
    "BRADESCO": ["BRADESCO SEGUROS"],
    "MAPFRE": ["MAPFRE SEGUROS"],
    "PORTO SEGURO": ["PORTO SEGURO CIA"],
    "AZUL": ["AZUL SEGUROS"],
    "ITAÚ": ["ITAU SEGUROS", "ITAU VIDA E PREVIDENCIA"],
    "ITAU": ["ITAU SEGUROS", "ITAU VIDA E PREVIDENCIA"],
    "SANTANDER": ["SANTANDER SEGUROS"],
    "CAIXA": ["CAIXA SEGURADORA", "CAIXA VIDA E PREVIDENCIA"],
    "BB": ["BB SEGUROS", "BRASILSEG"],
}


def _tokens(s: str) -> Set[str]:
    s = _norm(s)
    toks = set(s.split())
    return {t for t in toks if t and t not in _STOPWORDS and len(t) >= 2}


def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _string_similarity(a: str, b: str) -> float:
    """Ratcliff/Obershelp similarity (built-in do Python)"""
    return SequenceMatcher(None, a, b).ratio()


@dataclass(frozen=True)
class MatchResult:
    key: str
    score: float
    method: str
    candidate: str | None = None
    details: dict[str, float] | None = None


class NameMatcher:
    def __init__(self, aggregated_root: dict[str, Any]):
        self._root = aggregated_root or {}
        if isinstance(self._root, dict) and ("by_name" in self._root or "by_cnpj_key" in self._root):
            self.by_name: dict[str, Any] = self._root.get("by_name") or {}
            self.by_cnpj: dict[str, Any] = self._root.get("by_cnpj_key") or {}
        else:
            self.by_name = dict(self._root) if isinstance(self._root, dict) else {}
            self.by_cnpj = {}

        self._cnpj_to_key: dict[str, str] = {}
        for k, v in self.by_name.items():
            if isinstance(v, dict):
                c = normalize_cnpj(v.get("cnpj"))
                if c:
                    self._cnpj_to_key[c] = k

        # Pré-processamento
        self._candidates: list[dict] = []
        for key, entry in self.by_name.items():
            official_name = entry.get("name") or entry.get("display_name") or key
            toks = _tokens(official_name)
            norm_strong = _norm_strong(official_name)
            
            self._candidates.append({
                "key": key,
                "name": official_name,
                "tokens": toks,
                "norm_strong": norm_strong
            })

    def get_entry(self, susep_name: str, *, cnpj: Optional[str] = None, threshold: float = 0.65) -> tuple[Optional[Any], Optional[MatchResult]]:
        # 1. Match CNPJ
        cnpj_digits = normalize_cnpj(cnpj)
        if cnpj_digits and cnpj_digits in self._cnpj_to_key:
            key = self._cnpj_to_key[cnpj_digits]
            return self.by_name.get(key), MatchResult(key, 1.0, "cnpj", "CNPJ Match")

        # 2. Preparação Query
        q_tokens = _tokens(susep_name)
        q_norm_strong = _norm_strong(susep_name)
        
        # 3. Alias Check (Manual Override)
        uname = susep_name.upper()
        potential_aliases = []
        for k_alias, v_list in _MANUAL_ALIASES.items():
            if k_alias in uname:  # ex: "SUL AMERICA" in "SUL AMERICA CIA..."
                potential_aliases.extend(v_list)
        
        if potential_aliases:
            for cand in self._candidates:
                c_upper = cand["name"].upper()
                if any(alias in c_upper for alias in potential_aliases):
                    # Bônus massivo para alias manual
                    return self.by_name.get(cand["key"]), MatchResult(cand["key"], 0.95, "alias_manual", cand["name"])

        # 4. Fuzzy Match (Jaccard + String Containment)
        best_key = None
        best_score = 0.0
        best_cand_name = None

        for cand in self._candidates:
            # Jaccard Score
            j_score = _jaccard(q_tokens, cand["tokens"])
            
            # String Similarity (Strong Norm)
            s_score = 0.0
            if len(q_norm_strong) > 4 and len(cand["norm_strong"]) > 4:
                # Se um contém o outro exatamente após remover espaços
                if q_norm_strong in cand["norm_strong"] or cand["norm_strong"] in q_norm_strong:
                    s_score = 0.9
            
            final_score = max(j_score, s_score)

            if final_score > best_score:
                best_score = final_score
                best_key = cand["key"]
                best_cand_name = cand["name"]
                if best_score >= 0.99:
                    break

        if best_key and best_score >= threshold:
            return self.by_name.get(best_key), MatchResult(best_key, best_score, "fuzzy", best_cand_name, {"score": best_score})

        return None, None
