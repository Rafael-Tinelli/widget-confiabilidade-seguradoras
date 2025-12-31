# api/intelligence.py
import math

def calculate_solvency_score(data: dict) -> float:
    """Calcula o score de solidez financeira (0-100)."""
    premiums = data.get("premiums", 0.0)
    claims = data.get("claims", 0.0)
    net_worth = data.get("net_worth", 0.0)

    # 1. Score de Patrimônio
    if net_worth <= 0:
        net_worth_score = 0.0
    else:
        log_nw = math.log10(net_worth)
        net_worth_score = max(0, min(100, (log_nw - 6) * 25))

    # 2. Score de Sinistralidade
    if premiums > 0:
        lr = claims / premiums
    else:
        lr = 0.0
    
    if lr <= 0:
        lr_score = 50.0 
    else:
        dist = abs(lr - 0.60)
        lr_score = max(0, 100 - (dist * 200))

    final_score = (net_worth_score * 0.7) + (lr_score * 0.3)
    return round(final_score, 1)

def calculate_reputation_score(reputation_data: dict) -> float:
    """Calcula score de reputação baseado no Consumidor.gov"""
    if not reputation_data:
        return 50.0 
    metrics = reputation_data.get("metrics", {})
    resolucao = metrics.get("resolution_rate") or 0.0
    if resolucao > 1.0: resolucao /= 100.0
    
    satisfacao = metrics.get("satisfaction_avg") or 0.0
    satisfacao_norm = (satisfacao - 1) * 25
    
    score = (resolucao * 100 * 0.6) + (satisfacao_norm * 0.4)
    return max(0, min(100, score))

def calculate_opin_score(products: list) -> float:
    """Calcula score de inovação."""
    if not products:
        return 0.0
    count = len(products)
    score = 50 + count
    return min(100.0, score)

def determine_segment(data: dict) -> str:
    prem = data.get("premiums", 0.0)
    if prem > 1_000_000_000: return "S1"
    elif prem > 100_000_000: return "S2"
    elif prem > 0: return "S3"
    else: return "S4"

def calculate_score(insurer_obj: dict) -> dict:
    data = insurer_obj.get("data", {})
    reputation = insurer_obj.get("reputation", {})
    products = insurer_obj.get("products", [])
    
    solvency_score = calculate_solvency_score(data)
    reputation_score = calculate_reputation_score(reputation)
    opin_score = calculate_opin_score(products)
    
    # Pesos
    w_solv = 0.5
    w_rep = 0.4
    w_opin = 0.1
    
    final_score = (solvency_score * w_solv) + \
                  (reputation_score * w_rep) + \
                  (opin_score * w_opin)

    # Atualiza o objeto raiz
    insurer_obj["segment"] = determine_segment(data)
    
    # --- CORREÇÃO PARA O WIDGET-V2.JS ---
    # O JS espera: e.data.components.solvency
    
    components = {
        "solvency": solvency_score,
        "reputation": round(reputation_score, 1),
        "innovation": opin_score
    }
    
    # Injeta 'components' DENTRO de 'data'
    insurer_obj["data"]["components"] = components
    
    # Mantém score e lossRatio em data
    insurer_obj["data"]["score"] = round(final_score, 1)
    
    if data.get("premiums", 0) > 0:
        insurer_obj["data"]["lossRatio"] = data["claims"] / data["premiums"]
    else:
        insurer_obj["data"]["lossRatio"] = 0.0

    # (Opcional) Mantém components na raiz também para compatibilidade futura
    insurer_obj["components"] = components

    return insurer_obj
