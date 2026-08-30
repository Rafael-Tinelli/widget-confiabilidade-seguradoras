# Arquitetura entre pilares — Stage 2

Status: **experimento arquitetural fechado; score e ranking continuam proibidos**.

O Stage 2 recebe o diagnóstico do Stage 1 e pergunta qual arquitetura preserva melhor os contratos fechados de Financeiro e Conduta sem esconder alertas, missingness ou escolhas normativas.

## Decisão estrutural

A candidata principal é uma **matriz de estados não compensatória**.

Propriedades obrigatórias:

- preserva o domínio exato do sinal adverso;
- missingness permanece separado de desempenho;
- não exige pesos entre pilares;
- não finge produzir ordem total;
- insuficiência de capital permanece alerta material;
- nenhum outro pilar apaga esse alerta;
- a simples existência de insuficiência de capital não autoriza, por si só, uma regra total de ordenação entre todas as combinações.

O artifact mantém explicitamente como não selecionados:

```text
continuous_weighted_score_selected                    false
lexicographic_total_order_selected                    false
pareto_front_number_selected_as_public_tier           false
capital_gate_total_order_selected                     false
```

## Estados da matriz

O vocabulário estrutural é:

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

Pares localizados em coordenadas diferentes podem ser incomparáveis sob dominância direta. O Stage 2 publica esses pares em vez de escondê-los numa média.

Exemplo estrutural:

```text
F0|C1  versus  F1|C0
```

Preferir um lado exige dizer se Financeiro ou Conduta recebe prioridade. Isso é uma escolha normativa adicional.

Portanto:

```text
tradeoff_entity_pairs = diagnóstico da execução
```

Não existe valor esperado fixo para esse campo.

## Cobertura

A arquitetura carrega a cobertura do Stage 1 para impedir que uma matriz válida em um subconjunto seja apresentada como ranking integral do mercado.

Regras:

- `joint_conclusive_entities + joint_incomplete_entities = regulatory_universe`;
- shares de prêmio e reclamações são diagnósticos, não thresholds arbitrários;
- `full_market_ranking_supported` permanece falso enquanto o contrato de ranking não sustentar escopo e ordenação.

## Snapshot validado — 30/08/2026

Run:

```text
V2 Cross-Pillar Architecture Stage 2
run 33323343779
head 35e509d31de68a9311ede57ac245de6b7d3c0e11
artifact 9735521230
SHA256 ZIP b5ffc4111f75c397d1ad8b9675b38d5f3408710e501af519e0bb5d2c452058c8
```

População:

```text
regulatory_universe      156
joint_core_conclusive     85
joint_core_incomplete     71
```

Distribuição observada:

```text
no_current_core_adverse_signal                 48
conduct_pressure_only                          15
liquidity_pressure_only                        11
liquidity_and_conduct_pressure                 10
capital_shortfall_without_conduct_pressure      0
capital_shortfall_and_conduct_pressure          1
evidence_incomplete_for_joint_assessment       71
```

Trade-offs observados:

```text
tradeoff_entity_pairs 165
```

Na fotografia atual, os 165 pares estão em `F0|C1` versus `F1|C0`. As outras combinações normativas possíveis têm zero ocorrência; isso não as remove do modelo.

Cobertura observada das 85 conclusivas:

```text
prêmio direto positivo  69,94%
reclamações              54,40%
```

## Guardrails executáveis

O workflow valida estrutura, não fotografia:

- população não vazia e reconciliada com Stage 1;
- entidades Stage 1 e Stage 2 têm o mesmo conjunto de IDs;
- matriz não compensatória;
- score e ranking proibidos;
- nenhuma arquitetura de ordem total marcada como selecionada;
- trade-offs não precisam ter quantidade fixa;
- cobertura não pode autorizar silenciosamente ranking integral.

## Implementação

```text
api/v2/build_cross_pillar_architecture_experiment.py
tests/test_v2_cross_pillar_architecture_experiment.py
.github/workflows/v2-cross-pillar-architecture-stage-2.yml
```

Artifact:

```text
data/derived/v2/cross_pillar_architecture_experiment.json
```

## Limite deste estágio

O Stage 2 escolhe uma arquitetura adequada para **representar evidência**, não uma linguagem pública final. A semântica de avaliação é fechada pelo contrato seguinte.
