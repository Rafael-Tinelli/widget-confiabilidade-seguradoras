# api/matching/consumidor_gov_match.py
import re
import unicodedata
from typing import Dict, Any, Optional

class NameMatcher:
    def __init__(self, reputation_data: Dict[str, Any]):
        self.reputation_map = {}
        self.normalized_keys = {}
        
        # Pré-processa a base do Consumidor.gov
        for key, data in reputation_data.items():
            clean_name = self._simplify_name(key)
            self.reputation_map[key] = data
            # Mapa reverso: nome limpo -> chave original
            self.normalized_keys[clean_name] = key

    def _simplify_name(self, name: str) -> str:
        """Remove sufixos, acentos e caracteres especiais para facilitar o match."""
        if not name:
            return ""
        
        # 1. Normaliza Unicode (remove acentos)
        s = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("ASCII")
        s = s.lower().strip()
        
        # 2. Remove pontuação
        s = re.sub(r"[^a-z0-9\s]", " ", s)
        
        # 3. Remove sufixos corporativos comuns que atrapalham o match
        suffixes = [
            " s a", " sa", " s/a", " ltda", " limitda", " companhia", " cia", 
            " seguros", " previdencia", " vida", " capitalizacao", " do brasil", 
            " brasil", " group", " grupo", " holding", " participacoes"
        ]
        
        for suffix in suffixes:
            if s.endswith(suffix):
                s = s[:-len(suffix)].strip()
                
        # 4. Remove espaços extras
        s = re.sub(r"\s+", " ", s)
        return s.strip()

    class MatchResult:
        def __init__(self, key, score):
            self.key = key
            self.score = score

    def best(self, susep_name: str) -> Optional[MatchResult]:
        """Tenta encontrar o melhor match no Consumidor.gov."""
        clean_susep = self._simplify_name(susep_name)
        
        # Tentativa 1: Match Exato (após limpeza)
        if clean_susep in self.normalized_keys:
            real_key = self.normalized_keys[clean_susep]
            return self.MatchResult(real_key, 1.0)
            
        # Tentativa 2: Contém (Substring)
        for clean_cons, real_key in self.normalized_keys.items():
            # Proteção: ignora matches muito curtos
            if len(clean_cons) < 4:
                continue
                
            if clean_cons in clean_susep:
                return self.MatchResult(real_key, 0.9)
                
            if clean_susep in clean_cons:
                return self.MatchResult(real_key, 0.9)

        # Tentativa 3: Tokenização (Palavras em comum)
        susep_tokens = set(clean_susep.split())
        best_key = None
        max_overlap = 0
        
        for clean_cons, real_key in self.normalized_keys.items():
            if len(clean_cons) < 4:
                continue
            
            cons_tokens = set(clean_cons.split())
            overlap = len(susep_tokens & cons_tokens)
            
            if overlap > max_overlap and overlap >= 1:
                max_overlap = overlap
                best_key = real_key
        
        if best_key and max_overlap >= 1:
             return self.MatchResult(best_key, 0.8)

        return None
