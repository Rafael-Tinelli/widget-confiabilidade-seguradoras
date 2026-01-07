# api/utils/name_cleaner.py
from __future__ import annotations

import re
import unicodedata
from typing import Iterable, Set

# ---------------------------------------------------------------------------
# Single Source of Truth (SSoT) for name normalization and matching helpers.
# Reutilize este módulo entre SES / Consumidor.gov / Open Insurance.
# ---------------------------------------------------------------------------

# Tokens que normalmente são ruído jurídico/societário para matching de identidade.
# Mantenha conservador: remover demais pode colapsar marcas distintas.
STOPWORDS: Set[str] = {
    # Societário / jurídico
    "s", "a", "sa", "s.a", "s/a", "s.a.", "s.a", "ltda", "limitada", "me", "epp",
    "cia", "cia.", "companhia", "companhia.", "comp", "sociedade", "soc", "anonima",
    "anonima.", "mutual", "cooperativa", "participacoes", "participacoes.", "part",
    "holding", "group", "grupo", "corp", "corporation", "inc", "llc", "plc", "ltd",
    "branch", "filial", "sucursal", "se",

    # Preposições e conectivos
    "do", "da", "de", "dos", "das", "e", "y", "and", "of", "the",

    # Geografia / escopo (cuidado para não matar marcas – evite termos “brand-like”)
    "brasil", "brazil", "nacional", "international", "internacional", "global",
    "latina", "latin",

    # Setor (ruído para focar na marca)
    "seguros", "seguro", "seguradora", "seguridade", "previdencia", "previdência",
    "capitalizacao", "capitalização", "vida", "saude", "saúde", "dental",
    "assistencia", "assistência", "garantias", "garantia", "credito", "crédito",
    "beneficios", "benefícios", "gestora", "fundos", "fundo",
}

# Sinais fortes de B2B / Resseguro.
# IMPORTANTE: usado para classificar "N/A correto" em reputação de varejo (Consumidor.gov),
# não para esconder a entidade.
B2B_SUBSTRINGS: tuple[str, ...] = (
    # Resseguro core
    "resseguro", "resseguradora", "resseguros",
    "reinsurance", "reinsur", "reinsurer", "retrocession",
    "ruck", "rueck", "rück", "ruckversicher", "rueckversicher", "rückversicher",

    # B2B / corporate-specialty wording
    "corporate solutions", "global corporate", "corporate & specialty",
    "global corporate & specialty", "specialty", "speciality",
    "marine", "energy", "aviation", "cargo", "surety", "bond", "bonds",

    # Crédito / export credit (muito frequente em players B2B)
    "credito a exportacao", "credito a exportação",
    "credito y caucion", "crédito y caución",
    "credito", "crédito",  # NOTE: isolado é amplo; não decide sozinho (ver heurística).
    "exportacao", "exportação",

    # Instituições que podem aparecer no dump SES e não são varejo
    "abgf", "gestora de fundos garantidores", "fundos garantidores",
    "sbce", "seguradora brasileira de credito a exportacao",
    "seguradora brasileira de crédito à exportação",
)

# Hints por marca (substrings normalizadas) para casos de dump BR.
B2B_BRAND_HINTS: tuple[str, ...] = (
    "swiss re", "munich re", "munchener", "hannover ruck", "hannover rueck", "scor",
    "lloyd", "argo re", "axis re", "catlin re", "starr insurance & reinsurance",
    "financial assurance company", "royal & sun alliance", "factory mutual",
    # Casos citados por você (dump)
    "markel", "euler hermes", "atradius", "sbce", "abgf",
    "credito a exportacao", "credito a exportação", "credito y caucion", "crédito y caución",
    # Outros comuns em corporate/specialty
    "xl insurance", "torus", "virginia surety", "westport", "federal insurance company",
    "allianz global corporate",
)


def _ascii_fold(s: str) -> str:
    return (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
    )


def normalize_name_key(name: str) -> str:
    """
    Normalização canônica para chaves/matching base:
    - NFKD + ASCII fold (remove acentos)
    - lowercase
    - troca não-alfanumérico por espaço
    - colapsa espaços
    """
    if not name:
        return ""
    s = _ascii_fold(str(name)).lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_strong(name: str) -> str:
    """
    Normalização forte para string similarity (limiares altos).
    Remove stopwords e retorna string compacta (sem espaços).
    """
    base = normalize_name_key(name)
    if not base:
        return ""
    toks = [t for t in base.split() if t and t not in STOPWORDS and len(t) > 1]
    return "".join(toks)


def _add_fused_ngrams(tokens: list[str]) -> Set[str]:
    """
    Adiciona bigramas/trigramas fundidos para ajudar casos como:
      'sul america'  <->  'sulamerica'
      'porto seguro' <->  'portoseguro'
    """
    out: Set[str] = set()
    for n in (2, 3):
        for i in range(0, len(tokens) - n + 1):
            out.add("".join(tokens[i : i + n]))
    return out


def get_name_tokens(name: str) -> Set[str]:
    """
    Tokens úteis para fuzzy matching:
    - remove stopwords
    - remove tokens com len <= 1
    - adiciona n-gramas fundidos
    """
    base = normalize_name_key(name)
    if not base:
        return set()

    toks = [t for t in base.split() if t and t not in STOPWORDS and len(t) > 1]
    out: Set[str] = set(toks)
    out |= _add_fused_ngrams(toks)
    return out


def _has_any_substring(key: str, needles: Iterable[str]) -> bool:
    return any(n in key for n in needles)


def is_likely_b2b(name: str) -> bool:
    """
    Heurística para classificar entidades que não devem ser avaliadas por reputação varejo
    (Consumidor.gov): resseguro, corporate-specialty, crédito à exportação etc.
    Retorna True quando a entidade é provavelmente B2B-only / resseguro.

    Observação: falsos positivos são piores que falsos negativos.
    """
    key = normalize_name_key(name)

    # Hard blocks: varejo conhecido (evitar falsos positivos)
    if "sul america" in key or "sulamerica" in key:
        return False
    if "porto" in key and "seguro" in key:
        return False
    if "bradesco" in key or "itau" in key or "ita" in key:
        return False

    if _has_any_substring(key, B2B_SUBSTRINGS):
        # "credito" isolado é amplo; exige um cue forte adicional.
        if "credito" in key or "crédito" in key:
            strong_credit_cue = any(
                c in key
                for c in (
                    "exportacao",
                    "exportação",
                    "credito y",
                    "caucion",
                    "caución",
                    "sbce",
                    "atradius",
                    "euler hermes",
                )
            )
            if strong_credit_cue:
                return True
        else:
            return True

    if _has_any_substring(key, B2B_BRAND_HINTS):
        return True

    toks = set(key.split())
    if "resseguro" in toks or "resseguradora" in toks or "resseguros" in toks:
        return True

    return False
