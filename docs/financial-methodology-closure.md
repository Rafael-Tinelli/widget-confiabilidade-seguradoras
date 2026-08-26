# Fechamento metodológico do sinal Financeiro — v2

Status: **fechado para desenho do sinal Financeiro; score numérico ainda proibido**.

Este documento complementa o README e registra o fechamento da arquitetura financeira já investigada na v2. Ele é o equivalente funcional do fechamento de Conduta, mas respeita uma diferença importante: os indicadores financeiros possuem referências de natureza distinta e não devem ser forçados para uma escala comum antes da calibração entre pilares.

## 1. Pergunta humana

A camada Financeira tenta responder, nesta ordem:

1. **O patrimônio ajustado cobre o capital mínimo requerido?**
2. **A liquidez total mostra pressão na competência observada?**
3. **A operação está equilibrada, melhorando ou sob pressão ao longo do tempo?**
4. **Temos histórico suficiente para confiar na leitura longitudinal?**

Regra principal:

> um indicador forte não apaga silenciosamente um problema material em outro eixo.

E uma segunda regra:

> excesso de magnitude não vira mérito ilimitado.

## 2. Arquitetura financeira encerrada

A busca por novos eixos independentes está encerrada.

```text
Capital regulatório   → PLA/CMR → eixo principal
Liquidez total        → ILT     → eixo principal
Liquidez corrente     → ILC     → diagnóstico complementar
Operação              → ICA     → filme longitudinal principal
                         IC      → explicação/componente de apoio
Rentabilidade         → ILPL    → diagnóstico apenas
```

ILPL não retorna como componente independente. ILC não disputa o papel de ILT. ICA/IC não formam um terceiro score financeiro bruto.

## 3. Competência financeira

Toda leitura central usa a política de competência financeira madura já aprovada:

```text
última competência observada ≠ última competência madura
```

No snapshot usado para o fechamento:

```text
última observada   2026-06
competência madura 2026-05
```

A seleção continua baseada na cobertura de PLA/CMR derivável na janela comum das fontes. A ferramenta não usa automaticamente um mês mais recente quando a cobertura prudencial ainda está materialmente incompleta.

## 4. Capital — PLA/CMR

PLA/CMR continua sendo o eixo principal de capital.

A referência `1,0` possui interpretação material: o Patrimônio Líquido Ajustado deve ser igual ou superior ao Capital Mínimo Requerido. A Resolução CNSP nº 432/2021 e alterações posteriores permanece a referência regulatória central de capital para sociedades seguradoras.

Estados do contrato:

```text
capital_below_cmr
capital_meets_or_exceeds_cmr
capital_signal_unavailable
```

Decisão importante:

```text
continuous_transform_selected = false
positive_tiers_above_reference_selected = false
```

Um PLA/CMR de 3 não recebe três vezes o mérito de um PLA/CMR de 1. O valor bruto continua visível e poderá ser estudado na futura calibração entre pilares, mas a camada de sinal não cria faixas arbitrárias de “bom”, “ótimo” e “excelente”.

Também não se afirma que PLA/CMR abaixo de 1 signifique insolvência ou incapacidade automática de pagar sinistros. A conclusão é mais precisa: **há insuficiência de capital observada em relação ao CMR na competência de referência**.

## 5. Liquidez — ILT

ILT permanece o indicador principal de liquidez. ILC continua como diagnóstico complementar de curto prazo.

A referência `1,0` do ILT é **paridade aritmética do indicador**, não um limite prudencial oficial da SUSEP.

Estados:

```text
ilt_below_arithmetic_parity
ilt_at_or_above_arithmetic_parity
ilt_signal_unavailable
```

Decisão:

```text
continuous_transform_selected = false
positive_tiers_above_parity_selected = false
```

A investigação mostrou distribuição fortemente assimétrica e valores extremos. Por isso, ILT de 10, 100 ou 600 não recebe recompensa linear ou crescente no contrato do sinal.

A linguagem correta é:

- abaixo de 1 → há **pressão de liquidez segundo a paridade aritmética do ILT**;
- igual ou acima de 1 → **não há pressão segundo essa referência aritmética**;
- nunca → “aprovada pela SUSEP em liquidez”.

## 6. Combinação de capital e liquidez

Não foi selecionada média ponderada interna.

```text
weighted_average_selected = false
numeric_internal_score_selected = false
gate_logic_selected = true
```

O contrato central fica:

### `capital_requirement_shortfall_observed`

PLA/CMR < 1.

Mesmo que o ILT seja alto, a insuficiência de capital observada não é apagada.

### `capital_requirement_met_with_liquidity_pressure`

PLA/CMR >= 1 e ILT < 1.

Capital excedente não apaga a pressão de liquidez.

### `core_indicators_without_current_shortfall`

PLA/CMR >= 1 e ILT >= 1.

Essa é uma formulação deliberadamente limitada. Ela significa que os dois indicadores centrais não mostram insuficiência na competência observada segundo suas respectivas referências. **Não significa selo de saúde financeira, garantia de solvência ou superioridade sobre outras seguradoras.**

### `capital_requirement_met_liquidity_unavailable`

Capital utilizável e aderente ao requisito, mas ILT indisponível.

A ferramenta não completa a conclusão por imputação.

### `core_financial_signal_unavailable`

PLA/CMR indisponível ou não utilizável.

Ausência de dado não vale zero e também não vale neutralidade.

## 7. Filme operacional — ICA/IC

O filme operacional continua separado do sinal central de capital + liquidez.

Estados já definidos:

```text
balanced_persistent
improved
recent_pressure
persistent_pressure
indeterminate
```

ICA é a referência principal; IC e seus componentes explicam a formação do resultado.

O ponto `1,0` em ICA/IC é paridade aritmética entre custos e base operacional segundo a fórmula, não threshold prudencial SUSEP.

Decisão:

```text
operating_role = longitudinal_context_only
operating_context_can_override_core_signal = false
```

Assim:

- pressão operacional não transforma automaticamente uma seguradora capitalizada em “insolvente”;
- operação equilibrada não apaga insuficiência de capital ou pressão de liquidez;
- melhora operacional qualifica a trajetória, não concede bônus oculto.

## 8. Confiança da evidência

Histórico afeta confiança, não desempenho.

Estados:

```text
complete_core_history → established_core_history
limited_core_history  → limited_core_history
outros                 → insufficient_core_evidence
```

Uma seguradora nova com bom indicador atual e pouco histórico não é punida por ser nova. A ferramenta apenas deixa explícito que possui menos base longitudinal para interpretar estabilidade.

## 9. Rentabilidade — ILPL

A investigação de ILPL já foi encerrada.

Decisão preservada:

```text
independent_component_selected = false
role = diagnostic_only
```

Rentabilidade pode ajudar a explicar geração de resultado, mas não entra como novo eixo independente do sinal financeiro atual.

## 10. Resultado real do fechamento

No snapshot de 157 seguradoras ordinárias e competência madura `2026-05`:

Capital:

```text
capital_meets_or_exceeds_cmr  141
capital_below_cmr              14
capital_signal_unavailable      2
```

Liquidez ILT:

```text
ilt_at_or_above_arithmetic_parity 127
ilt_below_arithmetic_parity        29
ilt_signal_unavailable              1
```

Combinação central:

```text
core_indicators_without_current_shortfall              120
capital_requirement_met_with_liquidity_pressure         21
capital_requirement_shortfall_observed                  14
core_financial_signal_unavailable                        2
capital_requirement_met_liquidity_unavailable            0
```

Confiança do núcleo financeiro:

```text
established_core_history 143
limited_core_history      12
insufficient_core_evidence 2
```

Filme operacional:

```text
balanced_persistent 90
improved            13
recent_pressure      7
persistent_pressure 14
indeterminate       33
```

Esses estados são contratos de evidência e linguagem, não ranking.

## 11. Guardrails finais

O fechamento proíbe:

```text
dado ausente = zero
porte absoluto = mérito
recompensa linear por excesso de capital
recompensa linear por ILT extremo
percentil de mercado = threshold prudencial
média ponderada que esconda insuficiência de um eixo
filme operacional sobrescrevendo capital/liquidez
histórico curto = desempenho ruim
financial_score antes da calibração entre pilares
```

## 12. Linguagem pública permitida

Exemplos de formulações permitidas:

- “O patrimônio ajustado ficou abaixo do capital mínimo requerido na competência de referência.”
- “O requisito de capital foi atendido, mas o ILT ficou abaixo da paridade aritmética e merece cautela.”
- “Os indicadores financeiros centrais não mostram insuficiência atual na competência de referência.”
- “O histórico disponível é limitado; a leitura da estabilidade deve ser feita com cautela.”
- “A operação mostra pressão persistente no filme longitudinal, sem substituir a leitura de capital e liquidez.”

Formulações não permitidas apenas com esta camada:

- “A seguradora é solvente.”
- “A seguradora é financeiramente segura.”
- “A SUSEP aprovou a liquidez da empresa.”
- “Quanto maior o PLA/CMR ou ILT, melhor a seguradora sem limite.”
- “Uma operação equilibrada compensa insuficiência de capital.”

## 13. Validação

Workflow:

`V2 Financial Methodology Closure`

Execução real validada em `2026-08-26`:

```text
run                    33003042769
Ruff                   verde
testes direcionados    3/3
build real             verde
universo               157/157
fronteiras score/rank  verdes
artifact upload        verde
artifact id            9619421688
```

O artifact valida o contrato de sinal e proíbe explicitamente score e ranking nesta camada.

## 14. O que significa “Financeiro fechado”

A partir deste contrato estão encerrados:

- escolha dos eixos financeiros;
- papel de cada indicador;
- competência de referência;
- semântica mínima de capital;
- semântica mínima de liquidez;
- combinação não compensatória entre capital e liquidez;
- papel contextual do filme operacional;
- papel da confiança da evidência;
- exclusão de ILPL como eixo independente;
- linguagem pública permitida e proibida.

Continuam deliberadamente fora desta camada:

```text
financial_numeric_score
peso do Financeiro no composto
peso de Conduta no composto
score composto
assessment confidence final
ranking final
frontend v2
```

Esses pontos pertencem à **calibração entre pilares**, agora que Financeiro e Conduta possuem contratos próprios fechados.
