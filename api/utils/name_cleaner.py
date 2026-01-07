# api/utils/name_cleaner.py
from __future__ import annotations

import re
import unicodedata
from typing import Iterable, Set

# Termos que sugerem operação B2B/Resseguro (não varejo).
# Objetivo: evitar penalização por "ausência" no Consumidor.gov quando a entidade
# não é um player de varejo/consumidor final.
B2B_KEYWORDS: Set[str] = {
    "resseguradora",
    "resseguro",
    "resseguros",
    "reinsurance",
    "retrocession",
    "corretora de resseguros",
    "corretagem de resseguros",
    "specialty",
    "corporate",
    "global corporate",
    "global risks",
}

# Stopwords para "identidade de marca" (remove ruído societário e genérico).
# Nota: estas stopwords são intencionalmente agressivas para reduzir falsos negativos
# em matching por tokens.
STOPWORDS: Set[str] = {
    # Societário / forma jurídica
    "s.a",
    "s/a",
    "sa",
    "ltda",
    "limitada",
    "cia",
    "cia.",
    "companhia",
    "sociedade",
    "mutual",
    "cooperativa",
    "participacoes",
    "participacao",
    "holding",
    "group",
    "grupo",
    "corp",
    "corporation",
    "inc",
    "branch",
    "filial",
    "sucursal",
    # Conectores / comuns PT/EN
    "do",
    "da",
    "de",
    "das",
    "dos",
    "e",
    "and",
    "the",
    "of",
    # Geografia / alcance
    "brasil",
    "brazil",
    "nacional",
    "international",
    "internacional",
    "global",
    "america",
    "latina",
    "latin",
    # Setor (geralmente não distingue marca)
    "seguros",
    "seguro",
    "seguradora",
    "seguridade",
    "previdencia",
    "capitalizacao",
    "vida",
    "saude",
    "dental",
    "assistencia",
    "microseguros",
    "garantias",
    "credito",
    "beneficios",
    "gestora",
    "fundos",
}

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+", re.IGNORECASE)


def _to_ascii_lower(s: str) -> str:
    s_norm = unicodedata.normalize("NFKD", str(s))
    s_ascii = s_norm.encode("ascii", "ignore").decode("ascii")
    return s_ascii.lower()


def normalize_name_key(name: str) -> str:
    """Normaliza para chave canônica (espaços simples, sem acentos, lower)."""
    if not name:
        return ""
    s = _to_ascii_lower(name)
    s = _NON_ALNUM_RE.sub(" ", s)
    return " ".join(s.split()).strip()


def normalize_strong(name: str) -> str:
    """Normaliza removendo *todos* os separadores (útil p/ detecção por substring)."""
    return normalize_name_key(name).replace(" ", "")


def get_name_tokens(name: str) -> Set[str]:
    """Extrai tokens relevantes para matching por identidade de marca."""
    key = normalize_name_key(name)
    if not key:
        return set()
    tokens = set(key.split())
    return {t for t in tokens if t not in STOPWORDS and len(t) > 1}


def any_keyword_in(key: str, keywords: Iterable[str]) -> bool:
    return any(kw in key for kw in keywords)


def is_likely_b2b(name: str) -> bool:
    """Heurística: detecta operação provavelmente B2B/Resseguro pelo nome."""
    key = normalize_name_key(name)
    if not key:
        return False
    # Normaliza keywords também (para consistência)
    return any_keyword_in(key, (normalize_name_key(k) for k in B2B_KEYWORDS))
