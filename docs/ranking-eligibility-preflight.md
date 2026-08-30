# Preflight de elegibilidade de ranking — v2

Status: **fechado com o gate de ranking bloqueado**.

## Pergunta

> **Existe hoje uma coorte e uma regra de ordenação defensáveis para chamar o resultado de ranking sem esconder exclusões, empates ou escolhas normativas?**

Avaliação individual e ranking são contratos diferentes. Uma seguradora poder receber uma avaliação conjunta não implica que seja possível ordenar o mercado de 1 a N.

## Contrato de claims

No estado atual da metodologia:

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

## Duas perguntas independentes

### Escopo

O subset de avaliação cobre todo o universo regulatório? Quanto do prêmio e das reclamações representa? Há materialidade relevante entre os excluídos?

Esses valores são diagnósticos. Não existe threshold arbitrário codificado do tipo “70% já basta”.

### Ordenação

Mesmo dentro do subset elegível, existe uma regra de ordem total aprovada?

O preflight reconcilia:

```text
strictly_comparable_pairs
tied_pairs
incomparable_pairs
normative_tradeoff_pairs
```

A soma das três primeiras categorias deve ser igual a `n*(n-1)/2` para `n = assessment_eligible`.

Enquanto não houver regra normativa aprovada para resolver trade-offs e empates, não existe ranking total.

## Blockers estruturais

Para ranking de mercado, podem aparecer:

- avaliação não cobre todo o universo regulatório;
- representatividade integral não estabelecida;
- nenhuma regra de ordem total aprovada;
- trade-offs normativos não resolvidos;
- ordenação dentro de estados não suportada.

Para ranking apenas do subset elegível, o problema de cobertura integral deixa de ser blocker de escopo, mas permanecem os problemas de ordenação.

## Snapshot validado — 30/08/2026

Run:

```text
V2 Ranking Eligibility Preflight
run 33323343747
head 35e509d31de68a9311ede57ac245de6b7d3c0e11
artifact 9735520055
SHA256 ZIP 2affbf1702e7f440a2d53c742aa26054845e1f695dd4196b0a85387ccab8b8e4
```

População:

```text
regulatory_universe             156
assessment_eligible              85
assessment_not_eligible          71
ranking_preflight_candidates     85
ranking_eligible                  0
```

Cobertura do subset elegível:

```text
entidades                         54,49%
prêmio direto positivo            69,94%
reclamações                       54,40%
```

As 71 não elegíveis concentram aproximadamente:

```text
prêmio direto positivo            30,06%
reclamações                       45,60%
```

As dez maiores não elegíveis por prêmio representam, no snapshot, cerca de 25,92% do prêmio positivo do universo.

Diagnóstico de ordenação entre as 85 candidatas:

```text
unique_semantic_groups             5
largest_semantic_group            48
entities_in_tied_semantic_groups  84
pair_count                      3.570
strictly_comparable_pairs       2.072
tied_pairs                      1.333
incomparable_pairs                165
normative_tradeoff_pairs          165
```

A fotografia atual demonstra duas coisas simultaneamente:

1. existe informação suficiente para comparação semântica útil de 85 entidades;
2. não existe base para convertê-la em ranking total.

A quantidade de grupos e pares **não é constante metodológica**. No snapshot anterior havia 222 trade-offs; a queda para 165 ocorreu sem mudança do contrato, apenas pela evolução da população/evidência.

## Guardrails executáveis

O workflow valida:

- população dinâmica e reconciliada;
- candidatos de preflight = assessment elegíveis;
- `ranking_eligible = 0`;
- shares dentro de [0,1], sem exigir valores específicos;
- contabilidade de pares igual a `nC2`;
- categorias de pares reconciliadas;
- nenhuma regra de ordem total/tiebreaker selecionada;
- nenhuma posição de ranking criada;
- nenhum cohort de ranking selecionado;
- nenhum score geral, financeiro ou de Conduta criado.

## Implementação

```text
api/v2/build_ranking_eligibility_preflight.py
tests/test_v2_ranking_eligibility_preflight.py
.github/workflows/v2-ranking-eligibility-preflight.yml
```

Artifact:

```text
data/derived/v2/ranking_eligibility_preflight.json
```

## Decisão de produto

Enquanto o gate permanecer fechado, o produto defensável é:

> **avaliação semântica + comparação lado a lado + explorações unidimensionais explicitamente rotuladas**.

Isso não deve ser chamado de ranking geral das seguradoras.
