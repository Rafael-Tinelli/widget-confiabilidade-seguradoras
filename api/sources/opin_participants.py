# api/sources/opin_participants.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Set

# Importa do utilitário que acabamos de criar acima
from api.utils.identifiers import normalize_cnpj

# Caminho relativo à raiz do projeto
PARTICIPANTS_FILE = Path("api/v1/participants.json")

def load_opin_participant_cnpjs(path: Path = PARTICIPANTS_FILE) -> Set[str]:
    """
    Lê o participants.json e retorna um conjunto (Set) de CNPJs normalizados.
    Garante integridade referencial com a base SUSEP.
    """
    if not path.exists():
        print(f"OPIN: Arquivo {path} não encontrado. Join ignorado.")
        return set()

    try:
        content = json.loads(path.read_text(encoding="utf-8"))
        participants = content.get("participants", []) or []
        
        out: Set[str] = set()
        for p in participants:
            # O campo registrationNumber é a chave de ligação
            cnpj = normalize_cnpj(p.get("registrationNumber"))
            if cnpj:
                out.add(cnpj)
        
        print(f"OPIN: {len(out)} participantes carregados para cross-check.")
        return out
        
    except Exception as e:
        print(f"OPIN: Erro crítico ao ler participantes: {e}")
        return set()
