# api/matching/consumidor_gov_match.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from api.utils.name_cleaner import (
    get_name_tokens,
    is_likely_b2b,
    normalize_name_key,
    normalize_strong,
)

# Aliases manuais (mínimo necessário p/ marcas que não batem com razão social)
# Regras: chave = gatilho (string normalizada forte), valor = nome-alvo canônico.
ALIASES: Dict[str, str] = {
    "brasilseg": "bb seguros",
    "brasilveiculos": "bb seguros",
    "bancodobrasil": "bb seguros",
    "metropolitan": "metlife",
    "metlife": "metlife",
    "tokio": "tokio marine",
    "tokiomarine": "tokio marine",
    "cardif": "cardif",
    "itau": "itau seguros",
    "itauseguros": "itau seguros",
}

_CNPJ_DIGITS_RE = re.compile(r"\D+")


def format_cnpj(raw: Any) -> str:
    s = _CNPJ_DIGITS_RE.sub("", str(raw or ""))
    if len(s) != 14:
        return s
    return f"{s[:2]}.{s[2:5]}.{s[5:8]}/{s[8:12]}-{s[12:]}"


def _clean_cnpj(raw: Any) -> str:
    s = _CNPJ_DIGITS_RE.sub("", str(raw or ""))
    return s if len(s) == 14 else ""


def _token_weight(token: str) -> float:
    # tokens longos tendem a ser mais distintivos (ex.: "sulamerica", "bradesco", "alicorp")
    return 2.0 if len(token) >= 6 else 1.0


def _token_weight_sum(tokens: Iterable[str]) -> float:
    return sum(_token_weight(t) for t in tokens)


def _token_overlap_score(query_tokens: set[str], target_tokens: set[str]) -> float:
    """Score 0..1 baseado em sobreposição ponderada (robusto p/ ruído societário)."""
    if not query_tokens or not target_tokens:
        return 0.0

    inter = query_tokens.intersection(target_tokens)
    if not inter:
        return 0.0

    w_inter = _token_weight_sum(inter)
    w_query = _token_weight_sum(query_tokens)
    w_target = _token_weight_sum(target_tokens)

    w_min = min(w_query, w_target)
    w_max = max(w_query, w_target)

    # match perfeito (mesmos tokens)
    if len(inter) == len(query_tokens) == len(target_tokens):
        return 1.0

    # Cobertura sobre o menor conjunto (subset-match)
    ratio = w_inter / w_min if w_min else 0.0

    # Penaliza tokens extras no maior conjunto
    penalty = max(0.0, (w_max - w_inter) * 0.05)

    score = ratio - penalty

    # Bônus leve quando uma lista de tokens está contida na outra
    if len(inter) == min(len(query_tokens), len(target_tokens)):
        score += 0.05

    # Clamp
    return max(0.0, min(1.0, score))


@dataclass(frozen=True)
class MatchResult:
    key: str
    score: float  # 0..1
    method: str   # 'cnpj' | 'name_key_exact' | 'alias' | 'fuzzy_token' | 'b2b_skipped'
    candidate: str | None = None
    details: dict[str, float] | None = None
    is_b2b: bool = False
    note: str | None = None


class NameMatcher:
    def __init__(self, reputation_data: Dict[str, Any]):
        self.raw_data = reputation_data or {}

        # Índices principais
        self.by_cnpj: Dict[str, Dict[str, Any]] = {}
        self.by_name_key: Dict[str, Dict[str, Any]] = {}
        self.candidates: List[Tuple[set[str], str, str, Dict[str, Any]]] = []
        # (tokens, name_key, display_name, entry)

        self._build_indexes()

    def _iter_entries(self) -> Iterable[Tuple[str, Dict[str, Any]]]:
        # v3: {"by_name": {name_key: entry}}
        by_name = self.raw_data.get("by_name")
        if isinstance(by_name, dict):
            for k, v in by_name.items():
                if isinstance(v, dict):
                    yield str(k), v
            return

        # fallback: {"by_name_key_raw": {name_key_raw: entry}}
        by_name_raw = self.raw_data.get("by_name_key_raw")
        if isinstance(by_name_raw, dict):
            for k, v in by_name_raw.items():
                if isinstance(v, dict):
                    yield str(k), v
            return

        # fallback: lista
        providers = self.raw_data.get("providers") or self.raw_data.get("entries") or self.raw_data.get("data")
        if isinstance(providers, list):
            for item in providers:
                if not isinstance(item, dict):
                    continue
                disp = item.get("display_name") or item.get("name") or ""
                key = normalize_name_key(str(disp))
                if key:
                    yield key, item

    def _build_indexes(self) -> None:
        for key, entry in self._iter_entries():
            # 1) Índice por CNPJ (ouro)
            cnpj = _clean_cnpj(entry.get("cnpj"))
            if cnpj:
                self.by_cnpj[cnpj] = entry

            # 2) Índice por chave de nome (exato)
            nk = normalize_name_key(entry.get("display_name") or entry.get("name") or key)
            if nk:
                self.by_name_key[nk] = entry

            # 3) Lista de candidatos para fuzzy por tokens
            display_name = str(entry.get("display_name") or entry.get("name") or key)
            tokens = get_name_tokens(display_name)
            if tokens:
                self.candidates.append((tokens, nk or key, display_name, entry))

    def get_entry(self, name: str, cnpj: Optional[str] = None) -> Tuple[Optional[Dict[str, Any]], Optional[MatchResult]]:
        if not name:
            return None, None

        # 0) N/A correto: resseguro/B2B (fail-fast sem penalizar)
        if is_likely_b2b(name):
            return None, MatchResult(
                key="",
                score=0.0,
                method="b2b_skipped",
                candidate=None,
                is_b2b=True,
                note="Entidade provavelmente B2B/Resseguro (Consumidor.gov não é fonte aplicável).",
            )

        # 1) Match por CNPJ
        if cnpj:
            clean = _clean_cnpj(cnpj)
            if clean and clean in self.by_cnpj:
                return self.by_cnpj[clean], MatchResult(key=clean, score=1.0, method="cnpj")

        # 2) Match exato por chave de nome
        name_key = normalize_name_key(name)
        if name_key and name_key in self.by_name_key:
            return self.by_name_key[name_key], MatchResult(key=name_key, score=1.0, method="name_key_exact")

        # Preparação tokens da query
        query_tokens = get_name_tokens(name)
        if not query_tokens:
            return None, None

        query_strong = normalize_strong(name)

        # 3) Alias manual -> busca pelo melhor candidato (prata)
        for trigger_strong, canonical_target in ALIASES.items():
            if trigger_strong and trigger_strong in query_strong:
                target_tokens = get_name_tokens(canonical_target)
                best_entry: Optional[Dict[str, Any]] = None
                best_key = ""
                best_candidate = ""
                best_score = 0.0

                for cand_tokens, cand_key, cand_name, entry in self.candidates:
                    score = _token_overlap_score(target_tokens, cand_tokens)
                    if score > best_score:
                        best_score = score
                        best_entry = entry
                        best_key = cand_key
                        best_candidate = cand_name

                if best_entry is not None and best_score >= 0.85:
                    return best_entry, MatchResult(
                        key=best_key,
                        score=min(0.95, best_score + 0.05),
                        method="alias",
                        candidate=best_candidate,
                        details={"alias_score": best_score},
                    )

        # 4) Fuzzy por tokens (bronze)
        best_entry = None
        best_key = ""
        best_candidate = ""
        best_score = 0.0

        for cand_tokens, cand_key, cand_name, entry in self.candidates:
            score = _token_overlap_score(query_tokens, cand_tokens)
            if score > best_score:
                best_score = score
                best_entry = entry
                best_key = cand_key
                best_candidate = cand_name

        if best_entry is not None and best_score >= 0.80:
            return best_entry, MatchResult(
                key=best_key,
                score=best_score,
                method="fuzzy_token",
                candidate=best_candidate,
            )

        return None, None
