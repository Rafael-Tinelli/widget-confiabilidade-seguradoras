# api/utils/name_cleaner.py
from __future__ import annotations

import re
import unicodedata
from functools import lru_cache

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+", re.I)
_MULTI_SPACE_RE = re.compile(r"\s+")
_CNPJ_RE = re.compile(r"\D+")

# Tokens ignorados para matching por nome (muito comuns e pouco discriminantes)
_STOPWORDS: set[str] = {
    # forma societária / jurídico
    "sa",
    "s",
    "a",
    "s a",
    "s a.",
    "s.a",
    "s.a.",
    "ltda",
    "me",
    "epp",
    "sucursal",
    "companhia",
    "cia",
    "comp",
    "grupo",
    "holding",
    # termos setoriais genéricos
    "seguro",
    "seguros",
    "seguradora",
    "seguradoras",
    "previdencia",
    "previdenciaria",
    "capitalizacao",
    "capital",
    "assistencia",
    "instituto",
    "fundos",
    "fundo",
    "garantidor",
    "garantidores",
    "garantias",
    # idiomas / corporativo genérico
    "insurance",
    "insurances",
    "company",
    "co",
    "corp",
    "corporation",
    "limited",
    "ltd",
    "plc",
    "se",
    "ag",
    # conectivos
    "de",
    "da",
    "do",
    "das",
    "dos",
    "e",
    "em",
    "para",
    "por",
    "y",
    "and",
    "the",
    "of",
}

# Substrings de alta precisão para classificar entidade como B2B / resseguro / specialist
# (usada para pular reputação Consumidor.gov sem "poluir" o matching)
_B2B_SUBSTRINGS: tuple[str, ...] = (
    # resseguro
    "resseguro",
    "resseguradora",
    "reinsurance",
    "reinsur",
    "rueck",
    "ruck",
    "lloyd",
    "syndicate",
    # corporate/specialty (alta precisão)
    "corporatesolutions",
    "globalcorporate",
    "corporateandspecialty",
    "corporate&specialty",
    "specialtyinsurance",
    "specialtyinsur",
    "marineandenergy",
    "marine&energy",
    # crédito/garantia (normalmente B2B)
    "tradecredit",
    "creditoycaucion",
    "creditoexportacao",
    "creditoaexportacao",
    "exportcredit",
    "surety",
    "bond",
    "caucion",
    # marcas/players comuns em dumps B2B
    "swissre",
    "scor",
    "hannover",
    "muenchener",
    "munichre",
    "catlinre",
    "axisre",
    "argore",
    "starr",
    "factorymutual",
    "federalinsurancecompany",
    "westport",
    "royalsunalliance",
    "gardmarine",
    # pedidos explícitos
    "markel",
    "eulerhermes",
    "atradius",
    "sbce",
    "abgf",
    "torusspecialty",
)

# Exclusões de "não-seguradoras" que por vezes entram no LISTAEMPRESAS
_EXCLUDE_PROVIDER_SUBSTRINGS: tuple[str, ...] = (
    "ibracor",
    "corretora",
    "corretor",
    "corretagem",
    "broker",
    "corretora de resseguros",
    "corretora de resseguro",
    "corretor de resseguros",
    "corretor de resseguro",
)


def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def normalize_name_key(name: str) -> str:
    """
    Normalização "leve" (chave): sem acentos, lower, alfanum + espaços.
    Ideal para indexação/lookup.
    """
    if not name:
        return ""
    s = _strip_accents(str(name)).lower()
    s = _NON_ALNUM_RE.sub(" ", s)
    s = _MULTI_SPACE_RE.sub(" ", s).strip()
    return s


def normalize_strong(name: str) -> str:
    """
    Normalização "forte": sem acentos, lower, apenas alfanum (sem espaços).
    Ideal para detecção de substrings (aliases/b2b).
    """
    if not name:
        return ""
    s = _strip_accents(str(name)).lower()
    s = _NON_ALNUM_RE.sub("", s)
    return s


@lru_cache(maxsize=50_000)
def get_name_tokens(name: str) -> frozenset[str]:
    """
    Tokens para matching (stopword-aware). Retorna frozenset para cache/uso como chave.
    Inclui bigramas "colados" para casos como "sul america" vs "sulamerica".
    """
    s = normalize_name_key(name)
    if not s:
        return frozenset()

    toks: list[str] = []
    for t in s.split():
        if len(t) < 2:
            continue
        if t in _STOPWORDS:
            continue
        if t.isdigit():
            continue
        toks.append(t)

    glued: list[str] = []
    for i in range(len(toks) - 1):
        glued.append(toks[i] + toks[i + 1])

    return frozenset(toks + glued)


def normalize_cnpj(v: str | None) -> str | None:
    if not v:
        return None
    d = _CNPJ_RE.sub("", str(v))
    return d if len(d) == 14 else None


def is_excluded_provider(name: str) -> bool:
    """
    Filtra entidades que não devem aparecer no universo de seguradoras (ex.: IBRACOR, corretoras).
    """
    s = normalize_strong(name)
    if not s:
        return False
    for sub in _EXCLUDE_PROVIDER_SUBSTRINGS:
        if normalize_strong(sub) in s:
            return True
    return False


def is_likely_b2b(name: str) -> bool:
    """
    Heurística para classificar entidades B2B / resseguro / specialist.
    Mantém alta precisão para evitar 'false positive' em varejo.
    """
    s = normalize_strong(name)
    if not s:
        return False

    for sub in _B2B_SUBSTRINGS:
        if sub.replace(" ", "") in s:
            return True

    # fallback mínimo
    if re.search(r"\b(reinsur|reinsurance)\b", normalize_name_key(name)):
        return True

    return False
