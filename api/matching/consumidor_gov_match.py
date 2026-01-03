# api/matching/consumidor_gov_match.py
from __future__ import annotations

import difflib
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable, Optional

# -----------------------------
# CNPJ helpers
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
# Name normalization
# -----------------------------

_STOPWORDS = {
    # legal forms
    "sa", "s", "a", "s/a", "ltda", "mei", "epp", "eireli", "ei", "sucursal", "filial",
    # org words
    "companhia", "cia", "co", "grupo", "holding", "participacoes", "participações",
    "administradora", "administracao", "administração", "instituicao", "instituição",
    "banco", "bancos", "financeira",
    # insurance domain words
    "seguro", "seguros", "seguradora", "seguradoras", "resseguro", "resseguros",
    "previdencia", "previdência", "previdenciaria", "previdenciária", "vida",
    "capitalizacao", "capitalização", "assistencia", "assistência", "beneficios", "benefícios",
    # glue words
    "de", "da", "do", "das", "dos", "e", "em", "por", "para", "ao", "aos", "a", "o", "as", "os",
}


def _strip_accents(text: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch)
    )


_PUNCT_RE = re.compile(r"[\.\,\/\-\(\)\[\]\{\}:;!?\|\"']+")


def normalize_company_name(name: str) -> tuple[str, set[str]]:
    """
    Normalize a company name into:
      - a normalized string (tokens joined by space)
      - a set of normalized tokens
    """
    if not name:
        return "", set()

    s = _strip_accents(str(name).lower())
    s = s.replace("&", " e ")
    s = _PUNCT_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()

    raw_tokens = [t for t in s.split() if t]
    tokens = [t for t in raw_tokens if t not in _STOPWORDS]

    if not tokens:
        tokens = raw_tokens

    # Handle common "decompounding" (e.g., "sul america" vs "sulamerica")
    augmented: set[str] = set(tokens)
    for i in range(len(tokens) - 1):
        t = tokens[i] + tokens[i + 1]
        if len(t) >= 6:
            augmented.add(t)

    norm = " ".join(sorted(augmented))
    return norm, augmented


# -----------------------------
# Similarity metrics
# -----------------------------

def _levenshtein_ratio(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    matcher = difflib.SequenceMatcher(None, a, b)
    return matcher.ratio()


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _token_set_ratio(a_tokens: set[str], b_tokens: set[str]) -> float:
    if not a_tokens or not b_tokens:
        return 0.0

    inter = sorted(a_tokens & b_tokens)
    diff_a = sorted(a_tokens - b_tokens)
    diff_b = sorted(b_tokens - a_tokens)

    s_inter = " ".join(inter)
    s_a = " ".join(inter + diff_a)
    s_b = " ".join(inter + diff_b)

    return max(
        difflib.SequenceMatcher(None, s_inter, s_a).ratio(),
        difflib.SequenceMatcher(None, s_inter, s_b).ratio(),
        difflib.SequenceMatcher(None, s_a, s_b).ratio(),
    )


def _combined_score(a_norm: str, a_tokens: set[str], b_norm: str, b_tokens: set[str]) -> tuple[float, dict[str, float]]:
    token_set = _token_set_ratio(a_tokens, b_tokens)
    lev = _levenshtein_ratio(a_norm, b_norm)
    jac = _jaccard(a_tokens, b_tokens)

    # Weighted blend optimized for brand vs legal name
    score = 0.55 * token_set + 0.25 * lev + 0.20 * jac
    return score, {"token_set": token_set, "lev": lev, "jaccard": jac}


# -----------------------------
# Public API
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

        # Detect schema structure
        if isinstance(self._root, dict) and ("by_name" in self._root or "by_cnpj_key" in self._root):
            self.by_name: dict[str, Any] = self._root.get("by_name") or {}
            self.by_cnpj: dict[str, Any] = self._root.get("by_cnpj_key") or {}
        else:
            # Heuristic detection
            keys = list(self._root.keys()) if isinstance(self._root, dict) else []
            sample = keys[: min(20, len(keys))]
            cnpj_like = sum(1 for k in sample if normalize_cnpj(k)) if sample else 0
            if sample and (cnpj_like / len(sample)) >= 0.6:
                self.by_cnpj = dict(self._root)
                self.by_name = {}
            else:
                self.by_name = dict(self._root) if isinstance(self._root, dict) else {}
                self.by_cnpj = {k: v for k, v in self.by_name.items() if normalize_cnpj(k)}

        # Build CNPJ index
        self._cnpj_to_key: dict[str, str] = {}
        for k in self.by_cnpj.keys():
            d = normalize_cnpj(k)
            if d:
                self._cnpj_to_key.setdefault(d, k)

        # Build fuzzy candidates
        self._candidates: list[tuple[str, str, str, set[str]]] = []
        self._build_candidates()

    def _iter_candidate_names(self, key: str, entry: Any) -> Iterable[str]:
        if not normalize_cnpj(key):
            yield str(key)

        if isinstance(entry, dict):
            for field in ("name", "provider", "providerName", "nomeFantasia", "brand", "marca"):
                v = entry.get(field)
                if v:
                    yield str(v)

    def _build_candidates(self) -> None:
        for source_dict in [self.by_name, self.by_cnpj]:
            for key, entry in source_dict.items():
                if not isinstance(entry, dict) and source_dict is self.by_cnpj:
                    continue
                
                seen: set[str] = set()
                for nm in self._iter_candidate_names(key, entry):
                    if nm in seen:
                        continue
                    seen.add(nm)
                    norm, toks = normalize_company_name(nm)
                    if norm:
                        self._candidates.append((key, nm, norm, toks))

    def best(self, susep_name: str, *, cnpj: Optional[str] = None, threshold: float = 0.85) -> Optional[MatchResult]:
        cnpj_digits = normalize_cnpj(cnpj)
        if cnpj_digits and cnpj_digits in self._cnpj_to_key:
            return MatchResult(key=self._cnpj_to_key[cnpj_digits], score=1.0, method="cnpj")

        a_norm, a_tokens = normalize_company_name(susep_name)
        best_key, best_score, best_candidate, best_details = None, 0.0, None, None

        for key, cand, b_norm, b_tokens in self._candidates:
            score, details = _combined_score(a_norm, a_tokens, b_norm, b_tokens)
            if score > best_score:
                best_score, best_key, best_candidate, best_details = score, key, cand, details

        if best_key and best_score >= threshold:
            return MatchResult(key=best_key, score=best_score, method="fuzzy", candidate=best_candidate, details=best_details)
        
        return None

    def get_entry(self, susep_name: str, *, cnpj: Optional[str] = None, threshold: float = 0.85) -> tuple[Optional[Any], Optional[MatchResult]]:
        mr = self.best(susep_name, cnpj=cnpj, threshold=threshold)
        if not mr:
            return None, None
        
        entry = self.by_cnpj.get(mr.key)
        if entry is None:
            entry = self.by_name.get(mr.key)
        return entry, mr
