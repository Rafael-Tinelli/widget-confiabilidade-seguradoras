# Preflight de elegibilidade de ranking — v2

Status: **fechado com o gate de ranking bloqueado**.

## Pergunta

> **Existe hoje uma coorte e uma regra de ordenação defensáveis para chamar o resultado de ranking sem esconder exclusões, empates ou escolhas normativas?**

Avaliação individual e ranking são contratos diferentes. Uma seguradora poder receber avaliação conjunta não implica que seja possível ordenar o mercado de 1 a N.

## Contrato de claims

```text
full_market_total_ranking_supported                 false
assessment_eligible_subset_total_ranking_supported  false
assessment_eligible_subset_semantic_comparison_supported true
semantic_comparison_may_be_called_total_ranking     false
public_assessment_classes_may_be_used_as_rank_order false
pareto_partial_order_may_be_called_total_ranking    false
excluded_entities_may_be_assigned_bottom_rank       false
missingness_may_be_used_as_tiebreaker               false
arbitrary_coverage_threshold_selected               false
explicit_subset_scope_disclosure_required           true
```

## Escopo e ordenação

O subset de avaliação não cobre todo o universo e não existe threshold arbitrário do tipo “X% já basta”. Mesmo dentro do subset elegível, não há regra de ordem total aprovada.

O preflight reconcilia:

```text
strictly_comparable_pairs
tied_pairs
incomparable_pairs
normative_tradeoff_pairs
```

A soma das três primeiras categorias precisa ser `n*(n-1)/2` para `n = assessment_eligible`.

## Snapshot integrado corrente

Na Full Generation #44 (`run_id = 33562392945`, `head = c95e8675f8a2363b325623b2f310886e81f1c027`):

```text
regulatory_universe             156
assessment_eligible              82
assessment_not_eligible          74
ranking_preflight_candidates     82
ranking_eligible                  0
```

Cobertura do subset elegível:

```text
entidades                         52,56%
prêmio direto positivo            69,69%
reclamações                       54,27%
```

As 74 não elegíveis concentram aproximadamente:

```text
prêmio direto positivo            30,31%
reclamações                       45,73%
```

As dez maiores não elegíveis por prêmio representam cerca de 25,91% do prêmio positivo do universo.

Diagnóstico de ordenação entre as 82 candidatas:

```text
unique_semantic_groups             5
largest_semantic_group            45
entities_in_tied_semantic_groups  82
pair_count                      3.321
strictly_comparable_pairs       1.985
tied_pairs                      1.182
incomparable_pairs                154
normative_tradeoff_pairs          154
```

Classes das candidatas:

```text
favorable_reading     45
attention              35
prudential_warning      2
```

A fotografia demonstra simultaneamente que existe comparação semântica útil para 82 entidades e que não existe base para convertê-la em ranking total.

## Guardrails executáveis

- população dinâmica e reconciliada;
- candidatos = assessment elegíveis;
- `ranking_eligible = 0`;
- shares válidos sem threshold de aceitação inventado;
- contabilidade de pares = `nC2`;
- nenhuma regra de ordem total/tiebreaker selecionada;
- nenhuma posição de ranking geral criada;
- nenhum score geral, financeiro ou de Conduta criado.

## Implementação

```text
api/v2/build_ranking_eligibility_preflight.py
tests/test_v2_ranking_eligibility_preflight.py
```

Artifact:

```text
data/derived/v2/ranking_eligibility_preflight.json
```

## Decisão de produto

Enquanto o gate permanecer fechado, o produto defensável é:

> **avaliação semântica + comparação lado a lado + explorações unidimensionais explicitamente rotuladas**.

Isso não deve ser chamado de ranking geral das seguradoras.
