# api/utils/name_cleaner.py
from __future__ import annotations

import re
import unicodedata
from typing import Iterable, Set


# -------------------------
# Normalização
# -------------------------

def normalize_name_key(name: str) -> str:
    """Normalização canônica (humana) para chaves e comparações.

    - remove acentos
    - lowercase
    - substitui tudo que não é [a-z0-9] por espaço
    - colapsa espaços
    """
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", " ", s.lower())
    return s.strip()


def normalize_strong(name: str) -> str:
    """Normalização forte (compacta): remove acentos e todos separadores.

    Útil para detectar variações de espaçamento e pontuação.
    Ex.: "Sul América" -> "sulamerica".
    """
    key = normalize_name_key(name)
    return key.replace(" ", "")


# -------------------------
# Stopwords (matching de identidade)
# -------------------------

# Observação: esta lista precisa ser **conservadora**.
# Se removermos tokens que fazem parte da marca (ex.: "america"), o matcher perde sinal.
STOPWORDS: Set[str] = {
    # conectivos / artigos
    "a", "o", "as", "os", "ao", "aos", "da", "das", "de", "do", "dos", "e", "y", "del", "la", "el",

    # sufixos societários BR
    "sa", "s a", "s a.", "s.a", "s.a.", "s/a",
    "ltda", "limitada", "me", "epp", "eireli",

    # sufixos societários/legais internacionais (comuns em resseguro / operações admitidas)
    "inc", "corp", "corporation", "co", "company", "llc", "lp",
    "ltd", "limited", "plc", "ag", "nv", "bv", "gmbh", "pte", "pty",

    # termos societários genéricos
    "cia", "cia.", "companhia", "sociedade", "grupo", "group", "holding", "participacoes", "participacao",
    "filial", "sucursal", "branch",

    # geografia (genérico, normalmente não é parte da marca)
    "brasil", "brazil", "nacional", "internacional", "latina",

    # setor (genérico)
    "seguro", "seguros", "seguradora", "seguridade",
    "previdencia", "previdenciaria",
    "capitalizacao", "capitalizadora",
    "vida", "saude", "dental", "odontologica",
    "assistencia", "beneficios", "garantias", "garantia",
    "gestora", "gestao", "fundos",

    # inglês do setor
    "insurance", "assurance",
}


def get_name_tokens(name: str) -> Set[str]:
    """Extrai tokens relevantes para matching fuzzy (marca-identidade)."""
    key = normalize_name_key(name)
    if not key:
        return set()
    tokens = set(key.split())
    # Remove stopwords e tokens muito curtos
    return {t for t in tokens if t not in STOPWORDS and len(t) > 1}


# -------------------------
# Heurísticas de B2B / Resseguro / Out-of-scope
# -------------------------

# Substrings (já normalizadas por normalize_name_key) que caracterizam entidades B2B
B2B_SUBSTRINGS: Set[str] = {
    # PT
    "resseguro",
    "resseguradora",
    "corretora de resseguros",

    # EN/ES
    "reinsurance",
    "reinsur",
    "retrocession",

    # mercados corporativos
    "corporate solutions",
    "global corporate",
    "specialty",
    "marine energy",
    "trade credit",
    "credito a exportacao",
    "credito y caucion",

    # mercados/lócus típicos
    "lloyd",
    "syndicate",
}

# Tokens isolados úteis (com checagens adicionais)
B2B_TOKENS: Set[str] = {
    "re",          # Argo RE, Axis Re, etc. (usado com heurística de tamanho)
    "reinsurer",
    "reinsurers",
}


def any_keyword_in(key: str, keywords: Iterable[str]) -> bool:
    return any(kw in key for kw in keywords)


def is_likely_b2b(name: str) -> bool:
    """Heurística para decidir se reputação de Consumidor.gov é **não aplicável**.

    Retorna True para:
    - resseguradoras e operações típicas B2B/corporativas
    - corretoras de resseguros e entidades fora do escopo do Consumidor.gov
    """
    key = normalize_name_key(name)
    if not key:
        return False

    # 1) Substrings fortes
    if any_keyword_in(key, B2B_SUBSTRINGS):
        return True

    tokens = key.split()
    token_set = set(tokens)

    # 2.1) Entidades estrangeiras (normalmente B2B) com "Insurance/Assurance" e sem "Brasil/Brazil" no nome
    if (("insurance" in token_set) or ("assurance" in token_set)) and ("brasil" not in token_set) and ("brazil" not in token_set):
        return True

    # 2) "RE" como token isolado (com limite de tamanho para evitar falsos positivos)
    if "re" in token_set and len(tokens) <= 6:
        return True

    # 3) Padrões de "Rück" / "Rueck" (normaliza para ascii, então vira ruck/...)
    if "ruck" in key or "rueck" in key:
        return True

    # 4) Casos curtos e conhecidos (muito comuns no universo de resseguro)
    if key in {"scor se", "hannover ruck se", "lloyd s"}:
        return True

    return False
