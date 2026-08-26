# Calibração entre pilares — Stage 1

Status: **investigação estrutural concluída; score, pesos e ranking continuam proibidos**.

Este documento registra a primeira etapa da calibração conjunta dos contratos fechados de **Financeiro** e **Conduta**. O objetivo desta etapa não é produzir uma nota. É descobrir quanto de uma avaliação geral já decorre dos sinais metodologicamente aprovados e quanto exigiria novas escolhas normativas da Sanida.

Documentos anteriores obrigatórios:

- `docs/financial-methodology-closure.md`;
- `docs/conduct-methodology-closure.md`.

## 1. Pergunta humana

A pergunta central do Stage 1 é:

> **O que os dois pilares conseguem afirmar juntos sem permitir que um sinal forte esconda um problema material ou que ausência de evidência vire neutralidade?**

Subperguntas:

1. Quantas seguradoras possuem conclusão central utilizável nos dois pilares?
2. Quais combinações de sinais adversos realmente aparecem?
3. Os contratos fechados determinam uma ordem entre seguradoras sem pesos adicionais?
4. Quanto da ordenação seria empate ou incomparabilidade sem escolhas normativas novas?
5. Quais casos ficam fora por evidência insuficiente, e não por desempenho?
6. Se usarmos apenas a população conjunta conclusiva, quanto do mercado econômico e das reclamações fica de fora?

Regra desta etapa:

> **medir poder de ordenação antes de escolher transformações ou pesos.**

## 2. Por que não começamos com 50/50

Os contratos fechados são deliberadamente assimétricos.

### Financeiro

O contrato identifica materialmente:

```text
capital_requirement_shortfall_observed
capital_requirement_met_with_liquidity_pressure
core_indicators_without_current_shortfall
```

Mas não transforma:

```text
PLA/CMR muito acima de 1
ILT muito acima de 1
```

em mérito crescente.

### Conduta

O contrato distingue:

```text
above_expected_with_sufficient_evidence
below_expected_with_sufficient_evidence
not_distinguishable_from_expected
```

Mas registra explicitamente que:

```text
below_expected != prova de melhor atendimento
```

Portanto, uma soma como:

```text
50% Financeiro + 50% Conduta
```

não é consequência matemática dos contratos. Antes dela seria necessário inventar uma escala cardinal de mérito dentro de cada pilar e decidir quanto um domínio pode compensar o outro.

Isso é uma nova decisão metodológica, não uma simples operação aritmética.

## 3. Coordenadas ordinais usadas somente como diagnóstico

Para medir ordenabilidade sem criar score, o Stage 1 atribui coordenadas de **adversidade**, não pontos de qualidade.

Financeiro:

```text
F0 = core_indicators_without_current_shortfall
F1 = capital_requirement_met_with_liquidity_pressure
F2 = capital_requirement_shortfall_observed
```

Conduta:

```text
C0 = below_expected_with_sufficient_evidence
     OU not_distinguishable_from_expected

C1 = above_expected_with_sufficient_evidence
```

A junção de `below_expected` com `not_distinguishable` em `C0` é intencional. Ela preserva a regra de que menos reclamações que o esperado, isoladamente, não prova qualidade superior de atendimento.

Essas coordenadas existem **somente para o diagnóstico de ordenabilidade**.

```text
score = proibido
ranking = proibido
```

## 4. População conjunta

Universo regulatório:

```text
157 seguradoras ordinárias
```

Resultado:

```text
conclusão central utilizável nos dois pilares     85
conclusão conjunta ainda indisponível              72
```

Motivos dos 72:

```text
Conduct não comparável estruturalmente             53
Conduct com cobertura temporal insuficiente        12
Conduct sensível ao denominador                     5
núcleo Financeiro incompleto                        2
```

Os estados são mutuamente exclusivos nesta classificação de prontidão conjunta. Uma das 54 entidades não comparáveis de Conduta também possui núcleo financeiro incompleto e, por isso, aparece na categoria financeira nesta visão conjunta.

## 5. Matriz real Financeiro × Conduta

### F0 — indicadores financeiros centrais sem insuficiência atual

```text
Conduta acima do esperado com evidência        14
Conduta abaixo do esperado                     33
Conduta sem diferença clara                    13
denominador sensível                            3
cobertura temporal insuficiente                10
não comparável                                 47
```

### F1 — capital atendido + pressão de liquidez

```text
Conduta acima do esperado com evidência         8
Conduta abaixo do esperado                      4
Conduta sem diferença clara                     4
denominador sensível                            1
cobertura temporal insuficiente                 1
não comparável                                  3
```

### F2 — insuficiência de capital observada

```text
Conduta acima do esperado com evidência         4
Conduta abaixo do esperado                      4
Conduta sem diferença clara                     1
denominador sensível                            1
cobertura temporal insuficiente                 1
não comparável                                  3
```

### Núcleo Financeiro indisponível

```text
denominador de Conduta sensível                 1
Conduta não comparável                          1
```

## 6. O que aparece entre as 85 conclusivas

Padrões centrais:

```text
nenhum sinal adverso central                    46
somente Conduta adversa                         14
somente Financeiro adverso                      13
ambos os pilares adversos                       12
```

Coordenadas:

```text
F0 | C0   46
F0 | C1   14
F1 | C0    8
F1 | C1    8
F2 | C0    5
F2 | C1    4
```

Todos os seis grupos contêm empates. Portanto, os contratos fechados não produzem uma ordem total 1–85.

## 7. Pareto como teste de ordenabilidade, não como ranking

O Stage 1 também calcula dominância parcial de Pareto usando as coordenadas de adversidade.

Uma seguradora domina outra somente quando não é pior em nenhum dos dois eixos e é estritamente melhor em pelo menos um.

Resultado:

```text
fronteira 1   46
fronteira 2   22
fronteira 3   13
fronteira 4    4
```

A primeira fronteira contém **46 de 85**, ou aproximadamente **54,1%** da população conjunta conclusiva.

Dentro dela:

```text
Conduta abaixo do esperado                     33
Conduta sem diferença estatisticamente clara   13
```

Essas 46 empresas não podem ser ordenadas entre si apenas pelos dois estados centrais sem adicionar uma nova regra de mérito.

### Comparação par a par

Entre 85 empresas há 3.570 pares possíveis:

```text
estritamente ordenáveis por dominância     2.150   60,22%
empatados                                   1.198   33,56%
incomparáveis                                 222    6,22%
```

A baixa incomparabilidade não resolve o problema de ranking total porque os empates são numerosos e concentrados justamente no grupo sem adversidade central.

## 8. Por que os contextos existentes não resolvem automaticamente os 46

Entre as 46 empresas da primeira fronteira, o projeto também possui contexto longitudinal.

Financeiro:

```text
established_core_history   46/46
```

Operação:

```text
balanced_persistent   33
improved               5
persistent_pressure    4
recent_pressure        4
```

Conduta — persistência:

```text
persistent_below_expected          22
episodic_or_sparse_below_expected  11
not_distinguishable_from_expected  13
```

Conduta — tendência:

```text
no_clear_change       31
insufficient_events    9
deteriorating          3
improving              3
```

Satisfação:

```text
insufficient_sample   33
stable                12
worsening               1
```

Essas informações são úteis para explicar a trajetória, mas os contratos atuais não autorizam transformá-las silenciosamente em bônus de qualidade:

- ICA/IC é contexto longitudinal, não terceiro score financeiro;
- histórico é confiança, não desempenho;
- satisfação é contexto sample-aware, não correção da incidência;
- pressão abaixo do esperado não é certificação de atendimento superior.

## 9. Auditoria de representatividade de mercado

A calibração não pode olhar apenas o número de empresas. Um conjunto metodologicamente comparável pode continuar sendo economicamente pouco representativo.

Foi criado o workflow separado `V2 Cross-Pillar Coverage Audit`.

Universo de exposição observado em 12 meses:

```text
157 entidades
prêmio direto positivo        R$ 210,502 bilhões
reclamações mapeadas                82.423
```

Para footprint econômico, prêmio direto negativo não reduz o tamanho de outra empresa:

```text
market_footprint = max(premio_direto, 0)
```

Isso é uma regra de auditoria de cobertura, não uma alteração do denominador de Conduta.

### 85 com conclusão conjunta

```text
prêmio direto positivo        R$ 147,216 bilhões
parcela do prêmio                    69,94%
reclamações                           44.834
parcela das reclamações               54,40%
```

### 72 sem conclusão conjunta

```text
prêmio direto positivo         R$ 63,286 bilhões
parcela do prêmio                    30,06%
reclamações                           37.589
parcela das reclamações               45,60%
```

Conclusão:

> **a população atual de 85 não pode ser apresentada como ranking integral do mercado.**

Ela é uma subamostra comparável relevante, mas deixa de fora parcela material do prêmio e quase metade das reclamações observadas.

## 10. Onde está o problema de cobertura

A maior perda não vem dos guardrails estatísticos.

### 103 candidatas à pressão

```text
prêmio positivo coberto      71,63%
reclamações cobertas         55,63%
```

### 54 não comparáveis estruturalmente

```text
prêmio positivo fora         28,37%
reclamações fora             44,37%
```

As 12 empresas bloqueadas apenas por cobertura temporal representam cerca de 0,04% do prêmio positivo; as cinco sensíveis ao denominador representam cerca de 1,65%.

Portanto, o principal gargalo de representatividade é **subject / carrier / produto / atividade híbrida**, não a exigência estatística de robustez.

## 11. Rotas com maior materialidade

### Híbridas seguros + previdência

```text
26 entidades
16,14% do prêmio positivo do universo
28,55% das reclamações observadas
```

Precisam de numerador de reclamações separado por produto. A solução continua não sendo somar previdência ao denominador de seguros.

### Shared consumer subject / product split

```text
2 entidades
6,92% do prêmio positivo
10,09% das reclamações
```

Exige separação verificável de reclamações por produto/carrier.

### Transferência Zurich

A contraparte de transferência concentra cerca de 3,92% do prêmio positivo e exige reconciliação temporal da carteira.

### Shared exposure com subject externo

O caso correspondente concentra cerca de 1,40% do prêmio positivo e exige exposição específica do subject/brand.

## 12. Concentração dos excluídos

Somente as dez maiores entidades sem conclusão conjunta por prêmio positivo representam aproximadamente:

```text
25,92% de todo o prêmio positivo do universo
```

Entre os maiores casos aparecem, entre outros:

- Bradesco Auto/RE;
- Itaú Seguros;
- Zurich Minas Brasil;
- Zurich Santander Brasil Seguros e Previdência;
- Icatu Seguros;
- Bradesco Vida e Previdência;
- Caixa Vida e Previdência;
- Cardif do Brasil Vida e Previdência;
- Metropolitan Life;
- Caixa Seguradora.

Esses nomes aparecem aqui somente para priorização de recuperação metodológica. A ausência de conclusão conjunta não é avaliação negativa.

## 13. Arquiteturas avaliadas no Stage 1

### Score contínuo ponderado

```text
status = não sustentado apenas pelos contratos fechados
```

Motivo: exigiria criar méritos cardinais ainda inexistentes e decidir compensações entre domínios.

### Matriz de estados não compensatória

```text
status = compatível com os contratos fechados
```

Preserva sinais materiais, missingness e a regra de que um eixo não apaga outro.

### Ordem parcial de Pareto

```text
status = compatível como diagnóstico
```

Útil para medir o quanto de ordem emerge sem pesos. Não deve ser convertido automaticamente em faixas públicas de qualidade.

### Ordem lexicográfica

```text
status = exige nova prioridade normativa
```

Para produzir ordem total seria necessário declarar, por exemplo, Financeiro sempre anterior a Conduta ou vice-versa. Os dados não determinam essa preferência.

### Gate + score interno

```text
status = possível investigação futura; não selecionado
```

Pode preservar travas materiais, mas o score dentro de cada gate ainda dependeria de uma calibração positiva defensável.

## 14. O que o Stage 1 permite concluir

O Stage 1 sustenta quatro conclusões.

### 14.1. Não escolher pesos agora é uma conclusão, não uma omissão

Os contratos fechados não fornecem escala cardinal suficiente para justificar `50/50`, `60/40`, `70/30` ou qualquer outra combinação.

### 14.2. A arquitetura mais compatível hoje é não compensatória

Uma matriz de estados preserva melhor o significado dos dois pilares do que uma média que permita compensação silenciosa.

### 14.3. Pareto revela ordem parcial, mas não resolve o topo

Mais da metade das 85 seguradoras conclusivas está na primeira fronteira. Portanto, Pareto é uma boa auditoria de dominância e uma solução ruim para fabricar posições únicas.

### 14.4. Cobertura precisa avançar em paralelo

Uma arquitetura perfeita sobre 85 empresas não autoriza chamar o resultado de ranking do mercado enquanto 30,06% do prêmio e 45,60% das reclamações permanecerem fora da conclusão conjunta.

## 15. Próximo Stage

O próximo experimento deve comparar **arquiteturas de avaliação**, não pesos arbitrários.

Prioridade sugerida:

```text
A. matriz de estados não compensatória
B. gates materiais + diferenciação apenas onde houver evidência positiva defensável
C. Pareto como auditoria de consistência
```

O Stage 2 deve responder:

1. insuficiência de capital deve criar um gate material próprio?
2. pressão de liquidez e pressão de Conduta devem ser tratadas como adversidades distintas ou podem formar uma mesma faixa?
3. persistência/tendência podem qualificar a severidade de um sinal adverso sem virar bônus positivo?
4. quais estados podem ser chamados publicamente de avaliação, e quais devem permanecer “evidência insuficiente”?;
5. é possível criar faixas úteis ao consumidor sem fingir uma precisão 1–N que os dados não sustentam?;
6. que critério mínimo de representatividade será exigido antes de abrir `ranking_eligible`?

Em paralelo, a recuperação dos casos não comparáveis deve ser priorizada por materialidade econômica e de reclamações.

## 16. Guardrails preservados

O Stage 1 não altera nenhum contrato anterior.

```text
excesso de PLA/CMR = não vira bônus crescente
ILT extremo = não vira bônus crescente
below_expected = não vira qualidade certificada
ausência de pilar = não vira neutralidade
confiança = não vira desempenho
ICA/IC = não sobrescreve núcleo financeiro
sem redistribuição silenciosa de peso
sem score
sem ranking
assessment_eligible permanece fechado
ranking_eligible permanece fechado
```

## 17. Validação

### V2 Cross-Pillar Calibration Stage 1

```text
run                  33013794644
Ruff                 verde
testes               3/3
build real           verde
boundaries           verdes
artifact upload      verde
artifact id          9623644844
SHA256 artifact ZIP  eaccbbc2636f708a04e4efe94120146fd5fd36acafbe59e56b8dd0571056dac3
```

### V2 Cross-Pillar Coverage Audit

```text
run                  33014199915
Ruff                 verde
testes               3/3
build real           verde
boundaries           verdes
artifact upload      verde
artifact id          9623805832
SHA256 artifact ZIP  e8baed1493f26193cec8e6ed8b0e99dd2163c8bd7982a993c565ac1be451da75
```

## 18. Estado ao final do Stage 1

```text
Financeiro               contrato fechado
Conduta                   contrato fechado
Cross-Pillar Stage 1      concluído
pesos                     não selecionados
score geral               não definido
assessment_eligible       0
ranking_eligible          0
ranking público           bloqueado
```

A conclusão metodológica desta etapa é simples:

> **primeiro precisamos escolher uma arquitetura de avaliação que respeite os sinais e a cobertura; só depois saberemos se existe alguma razão legítima para criar uma nota numérica.**
