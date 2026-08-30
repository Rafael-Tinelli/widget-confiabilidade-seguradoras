# Contrato de leaderboards exploratórios — v2

Status: **fechado para exploração pública; ranking geral continua bloqueado**.

Este contrato define o que pode ser publicado quando uma pergunta é estritamente unidimensional ou quando o usuário quer explorar uma coleção semântica. Ele não cria score composto nem “melhor seguradora”.

## Princípio

Uma lista só pode ser ordenada quando a pergunta e a métrica já definem a direção sem necessidade de compensar domínios diferentes.

Exemplos permitidos:

```text
maior prêmio direto
maior PLA/CMR disponível
maior ILT disponível
menor razão de pressão de Conduta entre conclusões abaixo do esperado
maior razão de pressão de Conduta entre conclusões acima do esperado
```

Esses leaderboards são fatos métricos em escopo explícito. Não são proxies de qualidade geral.

## Leaderboards públicos fechados

IDs estruturais:

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
- nenhum tiebreaker secundário de mérito é inventado;
- missingness exclui a entidade daquela métrica, não a envia para o fim;
- Conduta baixa só admite conclusão `below_expected_with_sufficient_evidence`;
- Conduta alta só admite `above_expected_with_sufficient_evidence`;
- `is_general_ranking = false` em todos os boards.

## Coleções semânticas

Coleções são **não ordenadas**. IDs estruturais atuais:

```text
financial_core_without_current_adverse_signal
favorable_joint_assessment
favorable_with_below_expected_conduct
conduct_improving_but_still_adverse
conduct_persistent_above_expected
```

A quantidade de membros varia com os dados. `ordered = false` e `is_general_ranking = false` são invariantes.

## Conceitos explicitamente não suportados

O registry mantém bloqueados conceitos que exigiriam julgamento amplo sem contrato próprio, incluindo:

```text
mais_popular
emergente_promissora
consagrada_exemplar
ranking_geral
```

A interface não deve transformar um leaderboard factual em “melhor”, “mais confiável” ou “mais recomendada”.

## População dinâmica

Candidate counts, collection counts e nomes dos líderes são **diagnósticos de uma execução**.

O workflow valida apenas:

- IDs previstos pelo contrato;
- counts entre zero e o universo;
- entradas não excedem a população candidata;
- posições públicas entre 1 e 10;
- filtros semânticos de Conduta;
- coleções não ordenadas;
- explorer com o mesmo universo regulatório;
- ausência de score geral;
- ranking geral bloqueado.

Nenhum nome de empresa é um teste de integridade.

## Snapshot validado — 30/08/2026

Run:

```text
V2 Exploratory Leaderboards Contract
run 33323343770
head 35e509d31de68a9311ede57ac245de6b7d3c0e11
artifact 9735524497
SHA256 ZIP 5c9ade70aa2b51aecf581f0983c81a0471ce65d566af30051ed6285b046d665b
```

População:

```text
regulatory_universe       156
assessment_eligible        85
assessment_not_eligible    71
ranking_eligible             0
```

Candidates observados:

```text
largest_by_direct_premium          131
highest_pla_cmr_ratio              153
highest_ilt                        154
lowest_conduct_pressure_ratio       41
highest_conduct_pressure_ratio      26
```

Coleções observadas:

```text
financial_core_without_current_adverse_signal 123
favorable_joint_assessment                     48
favorable_with_below_expected_conduct          35
conduct_improving_but_still_adverse             4
conduct_persistent_above_expected               20
```

Cada leaderboard gerou dez linhas públicas na execução atual.

Os líderes atuais podem ser exibidos pelo próprio artifact, mas **não são contrato**. Por exemplo, uma mudança legítima de dados pode trocar o primeiro lugar sem que nenhum teste de metodologia deva falhar.

Essa distinção corrige o padrão anterior, no qual nomes de líderes e counts como 132/155/156 eram tratados como boundaries. Após a exclusão regulatória das SSPEs e atualização dos artifacts, os candidates passaram naturalmente para 131/153/154 em três métricas sem mudança da regra de publicação.

## Public outputs

```text
data/derived/v2/public/insurer_explorer.json
data/derived/v2/public/explore_index.json
data/derived/v2/public/leaderboards/*.json
data/derived/v2/public/collections/*.json
```

O explorer é dataset de comparação das seguradoras ordinárias, não catálogo completo de todas as identidades pesquisáveis.

## Implementação

```text
api/v2/build_exploratory_leaderboards_contract.py
tests/test_v2_exploratory_leaderboards_contract.py
.github/workflows/v2-exploratory-leaderboards-contract.yml
```

## Limites públicos obrigatórios

- prêmio mede volume econômico, não qualidade;
- PLA/CMR e ILT respondem perguntas financeiras específicas, não qualidade global;
- razão de Conduta depende de comparabilidade e não prova causalidade de atendimento;
- uma empresa ausente de um board por missingness não ocupa automaticamente posição inferior;
- nenhum leaderboard unidimensional deve ser apresentado como ranking geral de confiabilidade.
