# api/intelligence.py
import math

def calculate_solvency_score(data: dict) -> float:
    """Calcula o score de solidez financeira (0-100) com curva suavizada."""
    premiums = data.get("premiums", 0.0)
    claims = data.get("claims", 0.0)
    net_worth = data.get("net_worth", 0.0)

    # 1. Score de Patrimônio (Logarítmico Suavizado)
    # Piso de 100k para não zerar pequenas corretoras viáveis.
    if net_worth <= 100_000:
        net_worth_score = 0.0
    else:
        # Log10(1bi) = 9. Log10(1mi) = 6.
        # Ajuste para que empresas médias (~50mi) tenham score decente.
        log_nw = math.log10(net_worth)
        # (Log - 5) * 20 -> 100k=0, 10mi=40, 1bi=80, 10bi=100
        net_worth_score = max(0, min(100, (log_nw - 5) * 20))

    # 2. Score de Sinistralidade (Loss Ratio)
    if premiums > 0:
        lr = claims / premiums
    else:
        lr = 0.0
    
    # LR ideal entre 40% e 70%.
    if lr <= 0:
        lr_score = 50.0 
    else:
        # Penaliza desvios do ideal (0.60)
        dist = abs(lr - 0.60)
        lr_score = max(0, 100 - (dist * 200))

    final_score = (net_worth_score * 0.6) + (lr_score * 0.4)
    return round(final_score, 1)

def calculate_reputation_score(reputation_data: dict) -> float | None:
    """
    Calcula score de reputação. Retorna None se não houver dados,
    evitando o 'Phantom Score' de 50.0.
    """
    if not reputation_data:
        return None
        
    metrics = reputation_data.get("metrics", {})
    
    # Verifica se existem métricas reais
    if not metrics:
        return None

    resolucao = metrics.get("resolution_rate") or 0.0
    
    # Correção do linter: quebra de linha
    if resolucao > 1.0:
        resolucao /= 100.0
    
    satisfacao = metrics.get("satisfaction_avg") or 0.0
    # Satisfação (1 a 5) -> Normaliza para 0-100
    # 1=0, 3=50, 5=100
    satisfacao_norm = (satisfacao - 1) * 25
    
    score = (resolucao * 100 * 0.6) + (satisfacao_norm * 0.4)
    return max(0, min(100, score))

def calculate_opin_score(products: list, is_participant: bool) -> float:
    """Inovação: Baseado na participação e produtos."""
    score = 0.0
    if is_participant:
        score += 50.0 # Base por ser participante
    
    # Bônus por portfólio digital exposto
    if products:
        count = len(products)
        score += min(50.0, count * 2) # Cap de +50 pts
        
    return min(100.0, score)

def determine_segment(data: dict) -> str:
    prem = data.get("premiums", 0.0)
    
    # Correção do linter: quebra de linhas nos returns
    if prem > 1_000_000_000:
        return "S1"
    elif prem > 100_000_000:
        return "S2"
    elif prem > 0:
        return "S3"
    else:
        return "S4"

def calculate_score(insurer_obj: dict) -> dict:
    data = insurer_obj.get("data", {})
    reputation_raw = insurer_obj.get("reputation") # Pode ser None ou dict vazio
    products = insurer_obj.get("products", [])
    flags = insurer_obj.get("flags", {})
    
    solvency_score = calculate_solvency_score(data)
    reputation_score = calculate_reputation_score(reputation_raw)
    opin_score = calculate_opin_score(products, flags.get("openInsuranceParticipant", False))
    
    # Lógica de Pesos Dinâmicos
    if reputation_score is not None:
        # Cenário Padrão
        w_solv = 0.50
        w_rep  = 0.40
        w_opin = 0.10
        final_score = (solvency_score * w_solv) + \
                      (reputation_score * w_rep) + \
                      (opin_score * w_opin)
    else:
        # Cenário "Sem Match" (Redistribuição Proporcional)
        # Ignoramos Reputação e normalizamos os outros pesos para somar 1.0
        # Ex: Solvency assume o protagonismo (~83%)
        w_solv = 0.80
        w_opin = 0.20
        final_score = (solvency_score * w_solv) + (opin_score * w_opin)

    insurer_obj["segment"] = determine_segment(data)
    
    components = {
        "solvency": solvency_score,
        "reputation": round(reputation_score, 1) if reputation_score is not None else None,
        "innovation": opin_score
    }
    
    insurer_obj["data"]["components"] = components
    insurer_obj["data"]["score"] = round(final_score, 1)
    
    if data.get("premiums", 0) > 0:
        insurer_obj["data"]["lossRatio"] = data["claims"] / data["premiums"]
    else:
        insurer_obj["data"]["lossRatio"] = 0.0

    return insurer_obj
