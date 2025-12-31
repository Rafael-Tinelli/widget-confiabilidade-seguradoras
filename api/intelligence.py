# api/intelligence.py
import math
import numpy as np

def calculate_solvency_score(data: dict) -> float:
    """
    Calcula o score de solidez financeira (0-100).
    Baseado em: Patrimônio Líquido (Peso Alto) + Tamanho (Prêmios) + Sinistralidade.
    """
    premiums = data.get("premiums", 0.0)
    claims = data.get("claims", 0.0)
    net_worth = data.get("net_worth", 0.0)

    # 1. Score de Patrimônio (Capacidade de pagar)
    # Escala Logarítmica:
    # 10 Milhões = nota ~20
    # 100 Milhões = nota ~50
    # 1 Bilhão = nota ~80
    # 10 Bilhões = nota 100
    if net_worth <= 0:
        net_worth_score = 0.0
    else:
        # log10(10 Bi) = 10. log10(10 Mi) = 7.
        # Ajuste para base 100: (log - 6) * 25
        log_nw = math.log10(net_worth)
        net_worth_score = max(0, min(100, (log_nw - 6) * 25))

    # 2. Score de Sinistralidade (Saúde Operacional)
    # Ideal: 50-65%. Muito baixo (<20%) ou muito alto (>80%) penaliza.
    if premiums > 0:
        lr = claims / premiums
    else:
        lr = 0.0
    
    # Curva de Gauss simplificada centrada em 0.60
    # Se LR = 60%, score = 100. Se LR = 100%, score = 20.
    if lr <= 0:
        lr_score = 50.0 # Sem dados de sinistro
    else:
        dist = abs(lr - 0.60)
        # Penalidade exponencial
        lr_score = max(0, 100 - (dist * 200))

    # 3. Score Final de Solvência
    # O Patrimônio é o rei (70% do peso).
    final_score = (net_worth_score * 0.7) + (lr_score * 0.3)
    
    return round(final_score, 1)

def calculate_reputation_score(reputation_data: dict) -> float:
    """Calcula score de reputação baseado no Consumidor.gov"""
    if not reputation_data:
        return 50.0 # Neutro se não tiver dados
        
    metrics = reputation_data.get("metrics", {})
    
    # 1. Índice de Solução (Peso 60%)
    resolucao = metrics.get("resolution_rate") or 0.0
    if resolucao > 1.0: resolucao /= 100.0 # Normaliza se vier 80.0
    
    # 2. Satisfação (Peso 40%)
    satisfacao = metrics.get("satisfaction_avg") or 0.0
    # Escala 1-5 para 0-100
    satisfacao_norm = (satisfacao - 1) * 25
    
    score = (resolucao * 100 * 0.6) + (satisfacao_norm * 0.4)
    return max(0, min(100, score))

def calculate_opin_score(products: list) -> float:
    """Calcula score de inovação/abertura (Open Insurance)."""
    if not products:
        return 0.0
    
    # Quantidade de produtos conta pontos
    count = len(products)
    
    # Score simples: tem produto = 50 pontos + 1 ponto por produto (max 100)
    score = 50 + count
    return min(100.0, score)

def determine_segment(data: dict) -> str:
    """Classifica em S1, S2, S3, S4 baseado em Prêmios."""
    prem = data.get("premiums", 0.0)
    
    if prem > 1_000_000_000: # > 1 Bilhão
        return "S1"
    elif prem > 100_000_000: # > 100 Milhões
        return "S2"
    elif prem > 0:
        return "S3"
    else:
        return "S4"

def calculate_score(insurer_obj: dict) -> dict:
    """
    Função principal chamada pelo build_insurers.py.
    Recebe o objeto bruto e retorna com scores calculados.
    """
    data = insurer_obj.get("data", {})
    reputation = insurer_obj.get("reputation", {})
    products = insurer_obj.get("products", [])
    
    # 1. Calcula Sub-scores
    solvency_score = calculate_solvency_score(data)
    reputation_score = calculate_reputation_score(reputation)
    opin_score = calculate_opin_score(products)
    
    # 2. Score Geral Ponderado
    # Solvência (Financeiro) tem peso maior para "Confiabilidade"
    # Se for banco grande (S1), solvência importa mais.
    # Se for Insurtech, reputação importa mais.
    
    # Peso Padrão
    w_solv = 0.5
    w_rep = 0.4
    w_opin = 0.1
    
    final_score = (solvency_score * w_solv) + \
                  (reputation_score * w_rep) + \
                  (opin_score * w_opin)

    # 3. Atualiza o objeto
    insurer_obj["segment"] = determine_segment(data)
    
    # Salva dados calculados
    insurer_obj["data"]["score"] = round(final_score, 1)
    
    # Componentes visuais para o Frontend
    insurer_obj["components"] = {
        "solvency": solvency_score,
        "reputation": round(reputation_score, 1),
        "innovation": opin_score
    }
    
    # Loss Ratio para exibição
    if data.get("premiums", 0) > 0:
        insurer_obj["data"]["lossRatio"] = data["claims"] / data["premiums"]
    else:
        insurer_obj["data"]["lossRatio"] = 0.0

    return insurer_obj
