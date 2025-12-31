# api/intelligence.py
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats

# Constantes sugeridas pela pesquisa
CONFIDENCE_WILSON = 0.95  # Nível de confiança para reputação
PRIOR_C = 50              # "Votos virtuais" para correção Bayesiana
DEFAULT_BETA = 0.92       # Expoente inicial para Lei de Potência

# Pesos do Score Final
WEIGHTS = {
    "solvency": 0.35,
    "reputation": 0.40,
    "product": 0.15,
    "friction": 0.10
}


def _calculate_buhlmann_credibility(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica Bühlmann-Straub para suavizar a Sinistralidade (LR).
    Z = P / (P + K)
    """
    # Evita divisão por zero
    df['premiums_safe'] = df['premiums'].replace(0, 1)

    # 1. Estatísticas por Cluster (S1..S4)
    # K estimado simplificado: Mediana dos prêmios do cluster (proxy de volatilidade)
    cluster_stats = df.groupby('segment')['premiums_safe'].median().to_dict()
    cluster_lr_mean = df.groupby('segment').apply(
        lambda x: np.average(x['loss_ratio'], weights=x['premiums_safe'])
        if x['premiums_safe'].sum() > 0 else 0
    ).to_dict()

    def get_adj_lr(row):
        P = row['premiums']
        if P <= 0:
            return row['loss_ratio']  # Sem prêmio, mantém observado

        K = cluster_stats.get(row['segment'], 1e9)
        Mu = cluster_lr_mean.get(row['segment'], 0.60)

        Z = P / (P + K)  # Fator de Credibilidade

        # LR Ajustado = Z * Observado + (1-Z) * Média Mercado
        return (Z * row['loss_ratio']) + ((1 - Z) * Mu)

    df['lr_credibility'] = df.apply(get_adj_lr, axis=1)

    # Transforma LR em Score (0-100).
    # Curva Gaussiana: O ideal é LR=0.60. Muito abaixo é atrito, muito acima é risco.
    df['score_solvency'] = 100 * np.exp(-((df['lr_credibility'] - 0.60)**2) / (2 * 0.15**2))

    return df


def _calculate_power_law_complaints(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza reclamações usando Lei de Potência para remover efeito de escala.
    Y = alpha * X^beta
    """
    # Filtra dados válidos para regressão (log > 0)
    valid = df[(df['premiums'] > 1000) & (df['complaints_total'] > 0)]

    if len(valid) > 10:
        # Calcula Beta empírico do mercado atual
        slope, intercept, _, _, _ = stats.linregress(
            np.log(valid['premiums']),
            np.log(valid['complaints_total'])
        )
        beta = slope
    else:
        beta = DEFAULT_BETA

    # Aplica normalização: Complaints / (Premiums ^ beta)
    df['complaints_normalized'] = df['complaints_total'] / (np.maximum(df['premiums'], 1) ** beta)

    # Inverte para Score (Menor é melhor) usando Rank Percentile
    df['score_reputation_vol'] = 100 * (1 - df['complaints_normalized'].rank(pct=True))

    return df


def _calculate_bayes_reputation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Média Bayesiana para notas (Star Rating).
    Score = (C*m + Sum(R)) / (C + N)
    """
    # Média global do mercado (m)
    global_mean = df['satisfaction_avg'].mean()
    if pd.isna(global_mean):
        global_mean = 3.0

    C = PRIOR_C

    # Nota Bayesiana
    df['bayes_rating'] = (
        (C * global_mean) +
        (df['complaints_finalizadas'] * df['satisfaction_avg'].fillna(global_mean))
    ) / (C + df['complaints_finalizadas'])

    # Normaliza 1-5 para 0-100
    df['score_reputation_qual'] = (df['bayes_rating'] / 5.0) * 100

    return df


def _calculate_product_density(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula score de produto (placeholder para lógica OPIN).
    """
    # Se a seguradora tem flag 'openInsuranceParticipant', ganha bônus
    df['score_product'] = np.where(df['is_opin'], 80, 40)
    return df


def _calculate_friction(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula índice de atrito (LAE/Claims). Placeholder.
    """
    df['score_friction'] = 80.0
    return df


def run_scoring_pipeline(insurers_list: list[dict]) -> list[dict]:
    """
    Orquestra o cálculo de todos os scores.
    """
    if not insurers_list:
        return []

    # 1. Converte Lista de Dicts para DataFrame Pandas
    rows = []
    for ins in insurers_list:
        data = ins.get('data', {})
        rep = data.get('reputation', {}).get('consumidorGov', {}).get('metrics', {})

        row = {
            'id': ins.get('id'),
            'segment': ins.get('segment', 'S4'),
            'premiums': data.get('premiums', 0.0),
            'loss_ratio': data.get('lossRatio', 0.0),
            'claims': data.get('claims', 0.0),
            'complaints_total': rep.get('complaints_total', 0),
            'complaints_finalizadas': rep.get('complaints_finalizadas', 0),
            'satisfaction_avg': rep.get('satisfaction_avg', 3.0),
            'is_opin': ins.get('flags', {}).get('openInsuranceParticipant', False)
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # 2. Executa Motores Matemáticos
    df = _calculate_buhlmann_credibility(df)
    df = _calculate_power_law_complaints(df)
    df = _calculate_bayes_reputation(df)
    df = _calculate_product_density(df)
    df = _calculate_friction(df)

    # 3. Agregação Final
    df['final_reputation'] = (0.6 * df['score_reputation_vol']) + (0.4 * df['score_reputation_qual'])

    df['final_score'] = (
        (df['score_solvency'] * WEIGHTS['solvency']) +
        (df['final_reputation'] * WEIGHTS['reputation']) +
        (df['score_product'] * WEIGHTS['product']) +
        (df['score_friction'] * WEIGHTS['friction'])
    )

    # Clip 0-100 e Arredondamento
    df['final_score'] = df['final_score'].clip(0, 100).round(1)

    # 4. Reconstrói a Lista Original
    score_map = df.set_index('id')[['final_score', 'score_solvency', 'final_reputation']].to_dict('index')

    for ins in insurers_list:
        sid = ins.get('id')
        if sid in score_map:
            scores = score_map[sid]
            ins['data']['score'] = scores['final_score']
            ins['data']['components'] = {
                'solvency': round(scores['score_solvency'], 1),
                'reputation': round(scores['final_reputation'], 1)
            }

    return insurers_list
