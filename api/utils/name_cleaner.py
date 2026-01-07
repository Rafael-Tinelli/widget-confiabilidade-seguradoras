# api/utils/name_cleaner.py
from __future__ import annotations

import re
import unicodedata
from typing import Set

# Single Source of Truth for name normalization and B2B/Resseguro detection.

# NOTE: This list is intentionally broad. A false positive (classifying a company
# as B2B) only prevents consumidor.gov matching/penalty, which is safer than a
# false negative that would incorrectly penalize reputation.
B2B_KEYWORDS: Set[str] = {
    # Resseguro / Reinsurance
    "resseguro",
    "resseguradora",
    "reinsurance",
    "reinsur",
    "retrocession",
    "retroces",
    "run off",

    # Market / Lloyd's / syndicates
    "lloyd",
    "syndicate",

    # Corporate / Specialty (very often B2B-only in practice)
    "corporate solutions",
    "global corporate",
    "global corporate & specialty",
    "global corporate and specialty",
    "specialty insurance",
    "specialty",
    "commercial lines",

    # Trade credit / export credit (common no-show in consumidor.gov)
    "credito a exportacao",
    "credito a exportação",
    "credito y caucion",
    "credito y caución",
    "caucion",
    "caução",
    "atradius",
    "euler hermes",
    "allianz trade",
    "sbce",
    "seguradora brasileira de credito a exportacao",
    "seguradora brasileira de crédito à exportação",
    "abgf",

    # Common global reinsurers / B2B carriers (brand substrings)
    "swiss re",
    "scor",
    "hannover ruck",
    "hannover rück",
    "munich re",
    "munchener ruck",
    "münchener ruck",
    "axis re",
    "argo re",
    "catlin re",
    "factory mutual",
    "fm insurance",
    "starr insurance",
    "starr reinsurance",
    "financial assurance",
    "westport insurance",
    "federal insurance",
    "royal sun alliance",
    "royal & sun alliance",
    "mitsui sumitomo",
    "torus",
    "virginia surety",

    # Your dump-specific signals
    "markel",
}

# Generic words to drop when tokenizing names (to improve matching quality).
STOPWORDS: Set[str] = {
    # Legal suffixes / forms
    "s",
    "a",
    "sa",
    "s a",
    "s.a",
    "s a.",
    "ltda",
    "limitada",
    "me",
    "epp",
    "eireli",
    "ss",
    "se",

    # Corporate nouns
    "companhia",
    "cia",
    "sociedade",
    "grupo",
    "group",
    "holding",
    "participacoes",
    "participações",
    "inc",
    "corp",
    "corporation",
    "company",
    "co",

    # Prepositions / glue
    "de",
    "da",
    "do",
    "das",
    "dos",
    "e",
    "em",
    "para",
    "por",
    "na",
    "no",
    "nas",
    "nos",
    "ao",
    "aos",
    "à",
    "as",
    "os",

    # Country / locale
    "brasil",
    "brazil",

    # Insurance domain generics (remove to keep brand tokens)
    "seguro",
    "seguros",
    "seguradora",
    "seguridade",
    "previdencia",
    "previdência",
    "capitalizacao",
    "capitalização",
    "assistencia",
    "assistência",
    "beneficios",
    "benefícios",
    "vida",
    "saude",
    "saúde",
    "pessoas",
    "gerais",
}


def _ascii_lower(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.lower()


def normalize_name_key(name: str) -> str:
    """
    Normalizes a name to a stable key:
    - strips accents
    - lowercases
    - converts non-alphanum to spaces
    - collapses spaces
    """
    if not name:
        return ""
    s = _ascii_lower(str(name))
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def normalize_strong(name: str) -> str:
    """
    Strong normalization for fuzzy full-string comparisons:
    - same as normalize_name_key
    - then removes ALL spaces
    """
    s = normalize_name_key(name)
    return s.replace(" ", "")


def get_name_tokens(name: str) -> Set[str]:
    """
    Tokenizes a name for matching purposes (drops stopwords and 1-char noise).
    """
    key = normalize_name_key(name)
    if not key:
        return set()
    toks = [t for t in key.split() if len(t) > 1 and t not in STOPWORDS]
    return set(toks)


def is_likely_b2b(name: str) -> bool:
    """
    Heuristic: returns True if name is likely B2B-only/resseguro/corporate-only.
    """
    key = normalize_name_key(name)
    if not key:
        return False
    return any(k in key for k in B2B_KEYWORDS)
