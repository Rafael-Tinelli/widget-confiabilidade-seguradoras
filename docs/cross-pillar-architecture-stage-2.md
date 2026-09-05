# Arquitetura entre pilares — Stage 2

Status: **experimento arquitetural fechado; score e ranking continuam proibidos**.

O Stage 2 recebe o diagnóstico do Stage 1 e representa os contratos fechados de Financeiro e Conduta sem esconder alertas, missingness ou escolhas normativas.

## Decisão estrutural

A arquitetura selecionada é uma **matriz de estados não compensatória**.

Propriedades obrigatórias:

- preserva o domínio exato do sinal adverso;
- missingness permanece separado de desempenho;
- não exige pesos entre pilares;
- não finge produzir ordem total;
- insuficiência de capital permanece alerta material;
- nenhum outro pilar apaga esse alerta.

Continuam não selecionados:

```text
continuous_weighted_score_selected                    false
lexicographic_total_order_selected                    false
pareto_front_number_selected_as_public_tier           false
capital_gate_total_order_selected                     false
```

## Estados da matriz

```text
no_current_core_adverse_signal
conduct_pressure_only
liquidity_pressure_only
liquidity_and_conduct_pressure
capital_shortfall_without_conduct_pressure
capital_shortfall_and_conduct_pressure
evidence_incomplete_for_joint_assessment
```

O conjunto de estados é contrato; a distribuição entre eles é snapshot.

## Trade-offs normativos

Pares em coordenadas diferentes podem ser incomparáveis sob dominância direta. Preferir um lado exige escolher prioridade entre Financeiro e Conduta; o Stage 2 preserva o trade-off em vez de escondê-lo numa média.

## Cobertura

```text
joint_conclusive_entities + joint_incomplete_entities = regulatory_universe
```

Shares de prêmio/reclamações são diagnósticos, não thresholds. `full_market_ranking_supported` permanece falso.

## Snapshot integrado corrente

Na geração integrada de 01/09/2026 usada na auditoria §19.1:

```text
regulatory_universe      156
joint_core_conclusive     82
joint_core_incomplete     74
```

Distribuição observada:

```text
no_current_core_adverse_signal                 45
conduct_pressure_only                          14
liquidity_pressure_only                        11
liquidity_and_conduct_pressure                 10
capital_shortfall_without_conduct_pressure      0
capital_shortfall_and_conduct_pressure          2
evidence_incomplete_for_joint_assessment       74
```

Trade-offs observados:

```text
tradeoff_entity_pairs 154
```

Cobertura das 82 conclusivas:

```text
entidades                                  52,56%
prêmio direto positivo                     69,69%
reclamações                                54,27%
```

Esses valores são fotografia da execução.

## Guardrails executáveis

- população reconciliada com Stage 1;
- mesmo conjunto de `entity_id`;
- matriz não compensatória;
- score e ranking proibidos;
- nenhuma arquitetura de ordem total selecionada;
- cobertura não autoriza ranking integral.

## Implementação

```text
api/v2/build_cross_pillar_architecture_experiment.py
tests/test_v2_cross_pillar_architecture_experiment.py
```

Artifact:

```text
data/derived/v2/cross_pillar_architecture_experiment.json
```

## Encadeamento já concluído

A linguagem pública é definida pelo contrato semântico seguinte na cadeia, que também já está fechado sem abrir score ou ranking.
