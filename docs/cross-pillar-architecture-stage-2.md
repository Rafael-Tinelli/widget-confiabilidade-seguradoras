# Calibração entre pilares — Stage 2: arquitetura de avaliação

Status: **experimento de arquitetura concluído; matriz não compensatória é o candidato líder; contrato formal de avaliação ainda não fechado**.

Este documento sucede `docs/cross-pillar-calibration-stage-1.md` e preserva integralmente os contratos fechados de:

- `docs/financial-methodology-closure.md`;
- `docs/conduct-methodology-closure.md`.

O Stage 2 não tenta produzir nota. Ele pergunta qual arquitetura consegue traduzir os dois pilares juntos sem esconder sinais materiais, inventar bônus ou transformar uma preferência normativa em suposta consequência matemática.

## 1. Pergunta humana

> **Qual arquitetura traduz melhor os sinais conjuntos sem inventar compensação, premiar ausência de problema ou esconder falta de evidência?**

Regra desta etapa:

> **se dois estados exigem uma prioridade normativa nova para serem ordenados, o experimento deve expor essa escolha em vez de disfarçá-la como matemática.**

## 2. Inputs fechados

O Stage 2 consome somente artifacts já validados:

```text
Cross-Pillar Calibration Stage 1
Cross-Pillar Coverage Audit
```

Portanto, não reabre:

- escolha de PLA/CMR;
- escolha de ILT;
- papel de ICA/IC;
- papel de ILPL;
- denominador de Conduta;
- small-sample credibility;
- persistência/tendência;
- política de `coramo`;
- regra de não comparabilidade.

## 3. Matriz candidata

O candidato líder é:

```text
noncompensatory_state_matrix_with_adverse_qualifiers
```

Ela preserva exatamente os dois eixos centrais sem criar taxa de câmbio entre eles.

Estados atuais entre as 85 seguradoras com conclusão conjunta:

```text
no_current_core_adverse_signal                 46
conduct_pressure_only                          14
liquidity_pressure_only                         8
liquidity_and_conduct_pressure                  8
capital_shortfall_without_conduct_pressure      5
capital_shortfall_and_conduct_pressure          4
```

Fora dessa matriz:

```text
evidence_incomplete_for_joint_assessment       72
```

Essas 72 permanecem materializadas e podem mostrar os pilares disponíveis separadamente. Ausência de conclusão conjunta não vira neutralidade.

## 4. Propriedades da matriz

O experimento confirmou:

```text
noncompensatory                                  true
preserves_exact_adverse_domain                   true
missingness_separate_from_performance            true
requires_cross_pillar_weights                    false
pretends_total_order                             false
capital_shortfall_is_material_flag               true
capital_shortfall_is_automatically_worse_than_every_noncapital_combination false
```

A última linha é importante.

PLA abaixo do CMR é um sinal material de natureza prudencial e deve aparecer obrigatoriamente. Isso não autoriza concluir automaticamente que toda empresa com insuficiência de capital deve ocupar posição inferior a toda empresa sem insuficiência de capital, independentemente dos demais sinais.

Essa passagem — de **alerta material** para **ordem total** — exigiria uma escolha normativa adicional.

## 5. Linguagem pública candidata

A matriz permite frases descritivas, sem converter cada estado em “boa”, “ruim” ou em nota.

### `no_current_core_adverse_signal`

> Nos indicadores centrais avaliados, não observamos insuficiência de capital, pressão de liquidez pelo ILT nem pressão de reclamações acima do esperado. Isso não é garantia de solvência, qualidade ou superioridade.

### `conduct_pressure_only`

> Os indicadores financeiros centrais não mostram insuficiência atual, mas a pressão de reclamações está acima do esperado para o tamanho da operação.

### `liquidity_pressure_only`

> O requisito de capital está atendido, mas o ILT está abaixo da paridade aritmética. A Conduta não mostra pressão de reclamações acima do esperado.

### `liquidity_and_conduct_pressure`

> O requisito de capital está atendido, mas há pressão de liquidez pelo ILT e pressão de reclamações acima do esperado.

### `capital_shortfall_without_conduct_pressure`

> O patrimônio ajustado está abaixo do capital mínimo requerido na competência de referência. A Conduta não mostra pressão de reclamações acima do esperado.

### `capital_shortfall_and_conduct_pressure`

> O patrimônio ajustado está abaixo do capital mínimo requerido na competência de referência e a pressão de reclamações está acima do esperado.

Essas frases são **candidatas de preflight**, não contrato público final.

## 6. Os 222 trade-offs normativos

O Stage 1 havia identificado 222 pares incomparáveis por dominância direta. O Stage 2 decompôs exatamente esses pares.

### Conduta-only × Liquidez-only

```text
F0|C1 × F1|C0
112 pares
```

- Financeiro-first prefere F0|C1;
- Conduta-first prefere F1|C0;
- gate de capital empata;
- camada de Pareto empata.

Não existe informação nos contratos fechados que determine que pressão de Conduta “vale mais” ou “vale menos” que pressão de liquidez.

### Conduta-only × insuficiência de capital

```text
F0|C1 × F2|C0
70 pares
```

- Financeiro-first prefere F0|C1;
- Conduta-first prefere F2|C0;
- gate de capital prefere F0|C1;
- camada de Pareto prefere F0|C1.

A preferência decorre de uma regra adicionada sobre prioridade de capital, não de uma escala comum já existente.

### Liquidez + Conduta × insuficiência de capital

```text
F1|C1 × F2|C0
40 pares
```

- Financeiro-first prefere F1|C1;
- Conduta-first prefere F2|C0;
- gate de capital prefere F1|C1;
- camada de Pareto empata.

Esse é um exemplo claro de por que “qual é pior?” não tem resposta automática: de um lado há duas pressões não prudenciais centrais; do outro, uma insuficiência prudencial material.

## 7. O teste lexicográfico

Foram simuladas duas ordens totais apenas para diagnosticar a arbitrariedade:

```text
financial_lexicographic
conduct_lexicographic
```

Elas divergem em **exatamente 222 pares**.

Isso é a prova operacional de que escolher “Financeiro primeiro” ou “Conduta primeiro” não é uma solução neutra. É uma preferência normativa capaz de mudar posições reais.

Decisão:

```text
lexicographic_total_order_selected = false
```

## 8. Gate de capital

Também foi testada uma arquitetura em que a insuficiência de capital funciona como gate de ordenação.

Buckets produzidos:

```text
46
22
8
5
4
```

O teste é útil porque preserva a materialidade de capital, mas ainda resolve algumas comparações por uma regra adicional de prioridade.

Decisão:

```text
capital_gate_total_order_selected = false
```

O que permanece aprovado conceitualmente é mais limitado:

> **insuficiência de capital deve ser um alerta material obrigatório e não pode ser compensada ou escondida.**

Ainda não:

> “qualquer insuficiência de capital determina automaticamente a pior posição relativa possível”.

## 9. Pareto

Transformar o número da fronteira de Pareto em faixa pública também foi rejeitado.

Buckets:

```text
46
22
13
4
```

Pareto continua útil para auditar dominância, mas o número da fronteira não tem semântica pública suficiente de qualidade.

Decisão:

```text
pareto_front_number_selected_as_public_tier = false
```

## 10. Score contínuo ponderado

O Stage 2 reforçou a conclusão do Stage 1:

```text
continuous_weighted_score_selected = false
```

A razão não é falta de capacidade matemática.

Os contratos fechados deliberadamente não definem:

- mérito crescente por excesso de PLA/CMR;
- mérito crescente por ILT extremo;
- bônus de qualidade por estar abaixo do esperado em reclamações;
- taxa de câmbio entre pressão financeira e pressão de Conduta.

Criar um score agora exigiria inventar justamente essas escalas ou alguma função equivalente.

## 11. Persistência como qualificador adverso

Entre as 26 seguradoras com Conduta atualmente acima do esperado:

```text
persistent_above_expected          20
episodic_or_sparse_above_expected   6
```

Isso permite qualificar o mesmo estado adverso sem criar bônus positivo no lado oposto.

Exemplo:

```text
conduct_pressure_only
+ conduct_adverse_persistent
```

versus:

```text
conduct_pressure_only
+ conduct_adverse_episodic_or_sparse
```

A persistência descreve quão recorrente é o problema observado; não transforma ausência de persistência em mérito acumulável.

Decisão experimental:

```text
persistence_can_qualify_adverse_state_without_positive_bonus = true
```

## 12. Tendência como qualificador adverso

Nas mesmas 26 seguradoras:

```text
conduct_pressure_deteriorating                              6
conduct_pressure_improving_but_current_level_remains_adverse 4
conduct_pressure_no_clear_change                           16
```

Uma melhora recente não apaga a conclusão anual corrente de pressão acima do esperado.

Logo:

```text
trend_can_qualify_adverse_state_without_erasing_current_adverse_level = true
```

Isso permite linguagem como:

> A pressão continua acima do esperado na janela anual, embora haja sinal recente de melhora.

Sem converter essa melhora em crédito capaz de compensar o estado atual.

## 13. Contexto operacional

ICA/IC continua disponível como contexto longitudinal do Financeiro.

O Stage 2 não o promove a novo eixo da matriz nem a desempate automático.

```text
operating_context = contextual qualifier
core_override = false
```

Assim, pressão operacional pode aparecer na explicação, mas não altera silenciosamente o estado formado por capital + liquidez + Conduta.

## 14. Cobertura continua sendo constraint

O Stage 2 preserva a auditoria do Stage 1:

```text
joint_conclusive_entities                   85
joint_conclusive_positive_premium_share     69,9357%
joint_conclusive_complaint_share            54,3950%
joint_incomplete_entities                   72
```

Portanto:

```text
full_market_ranking_supported = false
```

Mesmo que a matriz seja formalmente fechada para avaliação individual de uma subamostra, isso não abre automaticamente um ranking integral do mercado.

## 15. Arquitetura líder

Ao final do experimento:

```text
leading_public_assessment_candidate
= noncompensatory_state_matrix_with_adverse_qualifiers
```

Não selecionados:

```text
continuous_weighted_score_selected          false
lexicographic_total_order_selected           false
pareto_front_number_selected_as_public_tier  false
capital_gate_total_order_selected            false
```

A matriz é candidata líder porque:

1. preserva exatamente o domínio em que cada sinal adverso ocorreu;
2. não exige pesos;
3. não deixa um pilar apagar o outro;
4. não trata dado ausente como neutro;
5. não precisa criar mérito positivo inexistente;
6. aceita persistência/tendência como qualificadores adversos;
7. é explicável ao consumidor sem fingir uma precisão ordinal que os dados não sustentam.

## 16. O que ainda impede o fechamento formal da avaliação

O Stage 2 **não** fecha o contrato público de avaliação.

Permanecem três perguntas de fechamento:

### 16.1. Semântica pública

As seis frases candidatas precisam passar por preflight para garantir que:

- não equivalham a “boa/ruim” sem suporte;
- não prometam solvência;
- não tratem abaixo do esperado em Conduta como selo de atendimento;
- expliquem claramente estados mistos.

### 16.2. Materialidade de capital

Precisamos formalizar:

```text
capital shortfall = mandatory material warning
```

sem transformar essa regra, por acidente, em uma ordem total que o experimento não selecionou.

### 16.3. Assessment completeness e cobertura

Precisamos separar:

```text
entidade com avaliação conjunta completa
```

de:

```text
mercado suficientemente coberto para um ranking público
```

É possível que a primeira condição venha a ser satisfeita para uma subamostra antes da segunda.

## 17. Próximo teste

Nome conceitual:

```text
cross_pillar_assessment_contract_preflight
```

Perguntas:

1. As seis matrizes podem ser explicadas ao consumidor sem overclaiming?
2. A insuficiência de capital pode ser um warning material obrigatório sem impor ranking total?
3. Persistência e tendência funcionam consistentemente apenas como qualificadores de adversidade?
4. Qual evidência mínima define uma avaliação conjunta completa?
5. Qual disclosure de cobertura é obrigatório para publicar comparações entre avaliadas?
6. `ranking_eligible` deve permanecer fechado mesmo se `assessment_eligible` puder ser aberto para uma subamostra?

## 18. Validação real

Workflow:

`V2 Cross-Pillar Architecture Stage 2`

Primeiro run:

```text
33015144138
```

falhou apenas em Ruff por formatação de um import de teste. Nenhum build ou resultado metodológico foi aceito desse run.

Após a correção:

```text
run                  33015231566
Ruff                 verde
testes               3/3
build real           verde
157 entidades        preservadas
boundaries           verdes
artifact upload      verde
artifact id          9624261105
SHA256 artifact ZIP  b20d3ac48e6208bffdec98ed28423703c83bec96659f631206b6a6b10cbbb34d
```

## 19. Estado ao final do Stage 2

```text
Financeiro                         contrato fechado
Conduta                            contrato fechado
Cross-Pillar Stage 1               concluído
Coverage Audit                     concluído
Cross-Pillar Architecture Stage 2  concluído
matriz não compensatória           candidata líder
score contínuo                     não selecionado
pesos                              não selecionados
ordem lexicográfica                não selecionada
Pareto como tier público           não selecionado
gate de capital como ordem total   não selecionado
contrato formal de avaliação       ainda aberto
assessment_eligible                0
ranking_eligible                   0
ranking público                    bloqueado
```

A conclusão metodológica do Stage 2 é:

> **os dados já sustentam uma avaliação descritiva conjunta por estados com alertas e qualificadores; ainda não sustentam transformar essa avaliação em nota ou ordem total.**
