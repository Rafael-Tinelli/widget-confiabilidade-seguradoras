# Ranking Eligibility Preflight — v2

Status: **preflight fechado e validado; o gate formal de ranking permanece bloqueado.**

Este preflight sucede `docs/assessment-eligibility-contract.md`.

A pergunta é:

> **Existe hoje uma coorte e uma regra de ordenação defensáveis para chamar o resultado de ranking, sem esconder exclusões, empates ou escolhas normativas?**

A resposta atual é **não**.

Isso não revoga as 85 avaliações já abertas. A conclusão é mais específica:

```text
assessment_eligible = 85
ranking_eligible    = 0
```

Avaliar uma seguradora e colocá-la em uma posição ordinal são operações metodologicamente diferentes.

## 1. O preflight separa duas alegações

### A. Ranking integral do mercado

Alegação implícita:

> “este ranking representa o universo atual de seguradoras ordinárias”.

### B. Ranking somente das seguradoras avaliáveis

Alegação explícita:

> “este ranking ordena apenas as seguradoras para as quais a metodologia possui avaliação conjunta completa”.

A segunda alegação evita fingir cobertura integral do mercado, mas **não resolve automaticamente o problema de ordenação**.

O preflight testa as duas separadamente.

## 2. Cobertura: por que o ranking integral do mercado não passa

Snapshot:

```text
universo regulatório             157
assessment_eligible               85
assessment_not_eligible           72
```

As 85 representam:

```text
54,14% das entidades
69,94% do prêmio direto positivo
54,40% das reclamações mapeadas
```

As 72 fora da avaliação conjunta representam:

```text
45,86% das entidades
30,06% do prêmio direto positivo
45,60% das reclamações mapeadas
```

Além disso, as dez maiores entidades excluídas por prêmio concentram aproximadamente:

```text
25,92% de todo o prêmio direto positivo do universo
```

A exclusão não é um ruído aleatório pequeno. Ela está concentrada principalmente em problemas estruturais de comparabilidade de Conduta — produto, carrier, atividade híbrida, subject compartilhado e transferências de carteira.

Portanto:

```text
full_market_scope_complete = false
full_market_representativeness_established = false
```

O projeto **não seleciona um threshold arbitrário** como “70% do prêmio basta” ou “80% das empresas basta”. Não há base metodológica já aprovada para transformar uma porcentagem escolhida em selo de representatividade.

Para uma alegação de ranking **integral** do mercado, a incompletude do universo é um bloqueio direto. Para uma alegação de subconjunto, ela vira disclosure obrigatório de escopo, não o principal bloqueio.

## 3. Ordenação: por que nem o ranking das 85 passa hoje

Entre as 85 avaliáveis existem seis estados centrais:

```text
F0|C0   46
F0|C1   14
F1|C0    8
F1|C1    8
F2|C0    5
F2|C1    4
```

Todos os seis grupos contêm mais de uma seguradora.

Logo:

```text
85/85 estão empatadas semanticamente com ao menos outra entidade
```

A análise par a par produz:

```text
pares totais                         3.570
estritamente comparáveis             2.150   60,22%
empatados                             1.198   33,56%
incomparáveis                           222    6,22%
```

Os 222 pares incomparáveis são os mesmos trade-offs normativos já identificados no Stage 2.

### Trade-offs centrais

```text
Conduta-only × Liquidez-only                    112 pares
Conduta-only × insuficiência de capital          70 pares
Liquidez + Conduta × insuficiência de capital    40 pares
```

Escolher Financeiro primeiro ou Conduta primeiro muda exatamente esses 222 pares.

Portanto:

```text
approved_total_order_selected = false
within_state_tiebreaker_selected = false
total_1_to_n_order_supported = false
```

## 4. Por que as classes públicas não podem ser usadas como ranking

Hoje o contrato semântico produz:

```text
Leitura central favorável   46
Atenção                     30
Alerta prudencial            9
```

Essas classes são linguagem de avaliação, não posições.

Em especial:

- `Alerta prudencial` obriga destaque de insuficiência de capital, mas o Stage 2 **não** autorizou concluir que toda insuficiência de capital deve ficar abaixo de qualquer combinação sem insuficiência;
- `Atenção` reúne domínios adversos distintos, como pressão de liquidez e pressão de Conduta;
- `Leitura central favorável` reúne 46 empresas que os indicadores centrais atuais não conseguem ordenar entre si sem inventar mérito adicional.

Logo:

```text
public_assessment_classes_may_be_used_as_rank_order = false
```

## 5. Pareto continua sendo diagnóstico, não ranking público

A ordem parcial de Pareto é válida para testar dominância:

```text
fronteira 1   46
fronteira 2   22
fronteira 3   13
fronteira 4    4
```

Mas transformar o número da fronteira em “posição” ou “nota” continuaria criando uma semântica de qualidade que os contratos não estabeleceram.

Portanto:

```text
pareto_partial_order_may_be_called_total_ranking = false
```

## 6. O que está autorizado hoje

Está autorizado:

```text
comparar semanticamente as 85 seguradoras avaliáveis
mostrar estados e alertas
mostrar diferenças de Financeiro e Conduta
mostrar qualificadores de persistência/tendência quando aplicáveis
explicar limites e cobertura
```

Não está autorizado:

```text
posição 1–85
posição 1–157
score composto
50/50 ou outro peso entre pilares
Financeiro-first silencioso
Conduta-first silencioso
usar missingness como pior desempenho
jogar as 72 não avaliáveis no fim da lista
usar prêmio/porte como desempate de mérito
usar histórico como bônus de desempenho
usar Pareto como tier público de qualidade
```

Em resumo:

> **há hoje um comparador semântico defensável para 85 seguradoras; ainda não há um ranking ordinal defensável.**

## 7. Estados do preflight por entidade

Para as 85 avaliáveis:

```text
ranking_preflight_candidate = true
ranking_preflight_state = candidate_but_ranking_contract_not_supported
ranking_eligible = false
comparison_cohort = null
```

Para as demais 72:

```text
ranking_preflight_candidate = false
ranking_preflight_state = blocked_by_assessment_ineligibility
ranking_eligible = false
comparison_cohort = null
```

`ranking_preflight_candidate` não é autorização para ranking. Significa apenas que a entidade já venceu o gate anterior de avaliação e poderia participar de uma futura decisão de coorte, caso escopo e ordenação venham a ser fechados.

Resultado favorável, atenção ou alerta prudencial **não altera essa candidatura**.

## 8. Blockers formais

### Ranking integral do mercado

Bloqueios atuais:

```text
assessment_does_not_cover_full_regulatory_universe
full_market_representativeness_not_established
no_approved_total_order_rule
normative_cross_pillar_tradeoffs_unresolved
within_state_order_not_supported
```

### Ranking somente das 85 avaliáveis

Bloqueios atuais:

```text
no_approved_total_order_rule
normative_cross_pillar_tradeoffs_unresolved
within_state_order_not_supported
all_current_candidates_share_semantic_ties
```

A diferença é intencional: **a cobertura integral não é usada para bloquear uma comparação explicitamente limitada às 85**. O que continua impedindo chamá-la de ranking é a falta de uma regra de ordenação defensável.

## 9. Guardrails

O builder e o workflow impedem:

- `ranking_eligible = true`;
- criação de score Financeiro, Conduta ou composto;
- criação de posição efetiva de ranking;
- seleção silenciosa de coorte;
- transformar avaliação adversa em inelegibilidade;
- transformar ausência de avaliação em posição inferior;
- renomear comparação semântica como ranking total;
- usar classe pública como ordem total;
- usar Pareto como ranking total;
- escolher threshold arbitrário de representatividade;
- abrir automaticamente o ranking caso, no futuro, os inputs passem a sustentar uma ordem total.

Esse último guardrail é importante: se uma futura versão resolver cobertura e ordenação, o preflight **falha** e exige um contrato formal de ranking. Ele não abre o gate silenciosamente.

## 10. Implementação

Arquivos:

```text
api/v2/build_ranking_eligibility_preflight.py
tests/test_v2_ranking_eligibility_preflight.py
.github/workflows/v2-ranking-eligibility-preflight.yml
```

Artifact:

```text
data/derived/v2/ranking_eligibility_preflight.json
```

Workflow:

```text
V2 Ranking Eligibility Preflight
```

## 11. Fechamento validado

Execução real:

```text
V2 Ranking Eligibility Preflight
run                     33028938405
job                     98376698450
Ruff                    verde
testes                  7/7
build real              verde
boundaries              verdes
artifact                9629478349
SHA256 ZIP              06f010b5bf4a5b0ea2b4f5ab98bed8e685d9f6f4c1023e0e4cb43702cd8d19c1
```

O artifact fechou como:

```text
status = ranking_eligibility_preflight_closed_gate_remains_blocked
ranking_eligibility_preflight_closed = true
ranking_eligibility_gate_opened = false
ranking_eligible = 0

full_market_ranking_supported = false
assessment_eligible_subset_total_ranking_supported = false
semantic_comparison_of_assessment_eligible_subset_supported = true
comparison_is_not_ranking = true
```

A validação real também confirmou:

```text
ranking_preflight_candidates = 85
ranking_position efetiva     = 0
comparison_cohort selecionada = 0
```

Ou seja: o preflight mede quem chegou ao ponto de poder ser considerado por um futuro contrato de ranking, mas não produz posição nem coorte por conta própria.

## 12. Próxima decisão

O projeto chegou a uma bifurcação metodológica real, que não deve ser escondida por código:

### Caminho A — manter a arquitetura semântica

Publicar a ferramenta como comparador/avaliação de confiabilidade, preservando estados, alertas e explicações sem posições artificiais.

### Caminho B — perseguir ranking ordinal

Exige um contrato posterior que declare explicitamente:

1. **escopo da alegação** — mercado integral ou subconjunto claramente definido;
2. **regra de prioridade entre Financeiro e Conduta** para os 222 trade-offs;
3. **regra de desempate dentro dos estados**, se posições individuais forem desejadas;
4. limites para impedir que qualificadores contextuais virem bônus silenciosos;
5. tratamento público das entidades não avaliáveis;
6. condições de reabertura quando a cobertura de Conduta melhorar.

Esse contrato não pode ser inferido dos dados atuais. Ele exigirá uma escolha normativa explícita e auditável.
