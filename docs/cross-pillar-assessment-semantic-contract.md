# Contrato semântico de avaliação entre pilares — v2

Status: **fechado e validado nas 157 seguradoras; score e ranking continuam proibidos**.

Este documento sucede `docs/cross-pillar-assessment-contract-preflight.md` e transforma o desenho de linguagem pública em um contrato executável.

A pergunta central é:

> **O que os sinais disponíveis permitem dizer ao consumidor sobre esta seguradora, em linguagem útil, sem transformar evidência parcial em garantia?**

## 1. Ordem obrigatória de comunicação

A interface pública deve seguir a hierarquia:

```text
1. leitura geral
2. sinais encontrados
3. por que isso importa
4. qualificadores
5. números e metodologia
6. limites da conclusão
7. confiança e cobertura
```

Regra:

> resultado favorável pode ser reconhecido dentro do escopo avaliado; alerta material não pode ser compensado; ausência de evidência nunca vira desempenho.

## 2. Classes públicas

O contrato separa quatro classes sem transformá-las em score:

```text
favorable_reading
attention
prudential_warning
evidence_incomplete
```

No snapshot atual:

```text
Leitura central favorável     46
Atenção                       30
Alerta prudencial              9
Avaliação conjunta incompleta 72
TOTAL                         157
```

Essas classes não formam uma ordem total entre todas as seguradoras.

## 3. Estados públicos completos

### `no_current_core_adverse_signal`

Título:

```text
Leitura central favorável
```

Significado:

> Nos indicadores centrais analisados, não identificamos insuficiência de capital, pressão de liquidez pelo ILT nem pressão de reclamações acima do esperado para o tamanho da operação.

Limite obrigatório:

> Isso não garante solvência futura, qualidade de cobertura, preço, atendimento individual ou superioridade sobre outra seguradora.

### `conduct_pressure_only`

Título:

```text
Atenção à Conduta
```

A pressão de reclamações está acima do esperado para o tamanho da operação, enquanto o núcleo financeiro não mostra insuficiência atual.

### `liquidity_pressure_only`

Título:

```text
Atenção à liquidez
```

O requisito de capital está atendido, mas o ILT está abaixo da paridade aritmética usada como referência.

### `liquidity_and_conduct_pressure`

Título:

```text
Atenção em liquidez e Conduta
```

Os dois sinais de atenção permanecem visíveis simultaneamente e não se compensam.

### `capital_shortfall_without_conduct_pressure`

Título:

```text
Alerta prudencial de capital
```

PLA abaixo do CMR é alerta material obrigatório. Conduta sem pressão acima do esperado não pode apagá-lo.

### `capital_shortfall_and_conduct_pressure`

Título:

```text
Alerta de capital e Conduta
```

O alerta prudencial de capital e a pressão de Conduta aparecem conjuntamente e nenhum reduz o outro.

## 4. Avaliação conjunta incompleta

Quando não há evidência comparável suficiente nos dois pilares:

```text
Avaliação conjunta incompleta
```

Isso é uma limitação da evidência, não desempenho ruim nem neutralidade.

O contrato diferencia a causa:

```text
conduta não comparável com segurança
cobertura temporal insuficiente
conclusão sensível ao denominador
financeiro central incompleto
conduta inconclusiva/indisponível
```

## 5. Regra nova e importante: incomplete não esconde sinal disponível

O teste nas 157 seguradoras revelou que, entre as 72 avaliações conjuntas incompletas, existem sinais financeiros materiais já utilizáveis:

```text
5 com alerta prudencial de capital
5 com atenção à liquidez
```

Portanto, o contrato proíbe uma tela neutra de “dados insuficientes” que esconda esses fatos.

Exemplo:

```text
Avaliação conjunta incompleta

Alerta disponível:
O patrimônio líquido ajustado está abaixo do capital mínimo requerido.

Conduta:
Ainda não há base comparável suficiente para fechar a leitura conjunta.
```

Assim:

```text
joint assessment incomplete != suppress available pillar evidence
```

## 6. Conduta: detalhe preservado sem bônus

A matriz mantém `below_expected` e `not_distinguishable` no mesmo nível não adverso, mas a interface deve preservar a diferença.

No universo atual:

```text
acima do esperado                         26
abaixo do esperado                        41
sem diferença clara                       18
não comparável com segurança              54
cobertura temporal insuficiente           12
conclusão sensível ao denominador          6
```

`abaixo do esperado` pode ser descrito como resultado favorável para esse indicador, mas não se transforma em bônus de qualidade nem score.

## 7. Persistência e tendência: regra executável

Durante o preflight foi identificado um risco no artifact experimental do Stage 2: qualificadores de tendência eram carregados para alguns casos cuja conclusão final de Conduta não era adversa.

Isso não alterava a matriz, mas poderia gerar linguagem pública contraditória.

O contrato semântico não confia nesses qualificadores experimentais. Ele os deriva novamente da conclusão final de Conduta e impõe:

```text
conduct pressure != above_expected
→ persistence qualifier = null
→ trend qualifier       = null
```

Somente quando a conclusão anual é `above_expected_with_sufficient_evidence` podem aparecer:

```text
Pressão recorrente
Pressão episódica ou esparsa
Sinal recente de piora
Sinal recente de melhora, mas pressão anual ainda adversa
Sem mudança recente clara
```

No snapshot atual das 26 pressões adversas:

```text
persistente                         20
episódica/esparsa                    6

deteriorando                         6
melhorando, ainda adversa             4
sem mudança clara                    16
```

## 8. Filme operacional e confiança

O contrato preserva como qualificadores explicativos:

```text
trajetória operacional equilibrada
trajetória operacional em melhora
pressão operacional recente
pressão operacional persistente
trajetória operacional inconclusiva
```

E:

```text
histórico estabelecido
histórico limitado
evidência central insuficiente
```

Esses qualificadores não alteram silenciosamente o estado central.

## 9. Assessment completeness

Cada uma das 157 entidades recebe um estado explícito:

```text
joint_core_complete
joint_core_incomplete
```

No snapshot atual:

```text
joint_core_complete    85
joint_core_incomplete  72
```

Para as 85 completas, o contrato considera que existe suporte **semântico** para uma avaliação pública individual conjunta.

Isso ainda não altera o gate formal:

```text
formal_assessment_eligibility_gate_opened = false
```

A abertura de `assessment_eligible` pertence ao próximo contrato.

## 10. Ranking continua separado

O contrato confirma:

```text
score                         proibido
peso Financeiro/Conduta       não selecionado
ordem total                   não selecionada
ranking_eligible              não aberto
full_market_ranking_supported false
```

Uma entidade poder receber avaliação individual completa não significa que o mercado esteja coberto de maneira suficiente para um ranking integral.

## 11. Guardrails executáveis

O builder e os testes verificam que:

- 157 entidades são preservadas;
- Stage 1 e Stage 2 possuem a mesma população;
- o estado da matriz é coerente com a assinatura `F|C`;
- avaliação completa nunca existe sem estado de matriz;
- avaliação incompleta nunca recebe estado conjunto como se fosse completo;
- todo alerta de capital disponível aparece publicamente;
- avaliação incompleta não esconde alerta financeiro disponível;
- qualificadores adversos de Conduta só existem quando a conclusão anual está acima do esperado;
- `below_expected` e `not_distinguishable` continuam distinguíveis no cartão de Conduta;
- nenhum score é criado;
- nenhum ranking é criado;
- `assessment_eligible` e `ranking_eligible` não são abertos por esta camada.

## 12. Implementação

Arquivos:

```text
api/v2/build_cross_pillar_assessment_semantic_contract.py
tests/test_v2_cross_pillar_assessment_semantic_contract.py
.github/workflows/v2-cross-pillar-assessment-semantic-contract.yml
```

Artifact:

```text
data/derived/v2/cross_pillar_assessment_semantic_contract.json
```

Workflow:

```text
V2 Cross-Pillar Assessment Semantic Contract
```

## 13. Validação real

Execução validada em `2026-08-26`:

```text
run                     33021494915
job                     98352686525
Ruff                    verde
testes direcionados     4/4
build real              verde
universo                157/157
semanticamente completas 85
conjuntas incompletas    72
boundaries              verdes
artifact upload         verde
artifact id             9626706193
SHA256 artifact ZIP     1504ea96f132eff338bdcea3619884b397353cfa1cea43c42a594b347df6f514
```

A execução real confirmou:

```text
favorable_reading       46
attention                30
prudential_warning        9
evidence_incomplete      72
```

Também confirmou que, entre as 72 avaliações conjuntas incompletas, permanecem obrigatoriamente visíveis:

```text
prudential_capital_warning  5
liquidity_attention          5
```

O artifact preserva `scoring = forbidden`, `ranking = forbidden` e não abre os campos formais de elegibilidade.

## 14. Estado metodológico após este contrato

O estágio está fechado como:

```text
cross_pillar_assessment_semantic_contract_closed
```

Isso significa:

> a metodologia sabe não apenas quais sinais existem, mas como descrevê-los publicamente, quais limites devem acompanhar cada frase e como preservar alertas mesmo diante de evidência conjunta incompleta.

O próximo gate é:

```text
assessment_eligibility_contract
```

Ele decidirá se e em quais condições `assessment_eligible` pode finalmente ser aberto, ainda de forma independente de `ranking_eligible`.
