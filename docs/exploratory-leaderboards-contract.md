# Contrato de leaderboards exploratórios — v2

Status: **fechado para exploração pública; ranking geral continua bloqueado**.

Este contrato define o que pode ser publicado quando a pergunta é estritamente unidimensional ou quando o usuário quer explorar coleção semântica. Ele não cria score composto nem “melhor seguradora”.

## Princípio

Uma lista só pode ser ordenada quando a pergunta e a própria métrica definem a direção, sem compensar domínios diferentes.

Leaderboards públicos:

```text
largest_by_direct_premium
highest_pla_cmr_ratio
highest_ilt
lowest_conduct_pressure_ratio
highest_conduct_pressure_ratio
```

Regras:

- máximo de dez posições públicas;
- empate por valor recebe a mesma posição (`competition rank`);
- nenhum tiebreaker secundário de mérito;
- missingness exclui daquela métrica, não envia para o fim;
- Conduta baixa exige `below_expected_with_sufficient_evidence`;
- Conduta alta exige `above_expected_with_sufficient_evidence`;
- `is_general_ranking = false` em todos os boards.

## Coleções semânticas

Coleções são não ordenadas:

```text
financial_core_without_current_adverse_signal
favorable_joint_assessment
favorable_with_below_expected_conduct
conduct_improving_but_still_adverse
conduct_persistent_above_expected
```

`ordered = false` e `is_general_ranking = false` são invariantes.

## Conceitos não suportados

Continuam bloqueados, entre outros:

```text
mais_popular
emergente_promissora
consagrada_exemplar
ranking_geral
crescimento_de_premio
```

A interface não pode transformar um leaderboard factual em “melhor”, “mais confiável” ou “mais recomendada”.

## Snapshot integrado corrente

Na Full Generation #44 (`run_id = 33562392945`, `head = c95e8675f8a2363b325623b2f310886e81f1c027`):

```text
regulatory_universe       156
assessment_eligible        82
assessment_not_eligible    74
ranking_eligible             0
```

Candidates observados:

```text
largest_by_direct_premium          131
highest_pla_cmr_ratio              151
highest_ilt                        153
lowest_conduct_pressure_ratio       41
highest_conduct_pressure_ratio      26
```

Coleções observadas:

```text
financial_core_without_current_adverse_signal 120
favorable_joint_assessment                     45
favorable_with_below_expected_conduct          33
conduct_improving_but_still_adverse             4
conduct_persistent_above_expected               20
```

Cada leaderboard gerou dez linhas públicas. Counts e líderes são fotografia, não contrato.

## Public outputs

```text
data/derived/v2/public/insurer_explorer.json
data/derived/v2/public/explore_index.json
data/derived/v2/public/leaderboards/*.json
data/derived/v2/public/collections/*.json
```

O explorer é dataset de comparação das seguradoras ordinárias, não catálogo completo de todas as identidades pesquisáveis.

## Guardrails

O builder valida:

- população reconciliada;
- `ranking_eligible = 0`;
- ordem definida exclusivamente pela métrica do board;
- filtros semânticos de Conduta;
- coleções não ordenadas;
- ausência de score/ranking geral;
- nenhum missing recebe posição inferior artificial.

## Implementação

```text
api/v2/build_exploratory_leaderboards_contract.py
tests/test_v2_exploratory_leaderboards_contract.py
```

## Limites públicos obrigatórios

- prêmio mede volume econômico, não qualidade;
- PLA/CMR e ILT respondem perguntas financeiras específicas, não qualidade global;
- razão de Conduta depende de comparabilidade e não prova causalidade de atendimento;
- ausência de uma empresa de um board por missingness não é posição inferior;
- nenhum leaderboard unidimensional é ranking geral de confiabilidade.
