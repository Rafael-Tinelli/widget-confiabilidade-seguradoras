# Fechamento metodológico do sinal Financeiro — v2

Status: **fechado para desenho do sinal Financeiro; score numérico ainda proibido**.

Este documento registra a arquitetura financeira consolidada da v2 e o snapshot integrado corrente. Os indicadores possuem referências de natureza distinta e não são convertidos prematuramente para uma escala comum.

## 1. Pergunta humana

A camada Financeira tenta responder, nesta ordem:

1. **O Novo PLA utilizado pelo contrato cobre o Capital Mínimo Requerido?**
2. **A liquidez total mostra pressão na competência observada?**
3. **A operação está equilibrada, melhorando ou sob pressão ao longo do tempo?**
4. **Temos histórico suficiente para interpretar a trajetória?**

Regras principais:

> um indicador forte não apaga silenciosamente um problema material em outro eixo.

> excesso de magnitude não vira mérito ilimitado.

## 2. Arquitetura financeira encerrada

```text
Capital regulatório   → PLA/CMR → eixo principal
Liquidez total        → ILT     → eixo principal
Liquidez corrente     → ILC     → diagnóstico complementar
Operação              → ICA     → filme longitudinal principal
                         IC      → explicação/componente de apoio
Rentabilidade         → ILPL    → diagnóstico apenas
```

Não há score financeiro numérico.

## 3. Competência financeira

A política em `financial_periods.py` distingue:

```text
última competência observada ≠ necessariamente última competência madura
```

No snapshot integrado corrente:

```text
reference_period = 202606
```

A seleção é determinada pelo contrato de maturidade e cobertura, não por data fixa em documentação.

## 4. Capital — PLA/CMR

### 4.1 Numerador canônico

Contrato fechado:

```text
campo SES NovoPla    → new_pla      → numerador final
campo SES plajustado → pla_adjusted → intermediário bruto
CMR                  → cmr

capital_ratio = new_pla / cmr
```

Não existe fallback de `new_pla` para `pla_adjusted`.

Se `new_pla` estiver ausente ou não utilizável, o sinal de capital fica indisponível mesmo que `pla_adjusted` exista. CMR zero não vira desempenho zero nem razão artificial.

### 4.2 Semântica

Estados:

```text
capital_below_cmr
capital_meets_or_exceeds_cmr
capital_signal_unavailable
```

A referência 1,0 representa a relação entre o numerador prudencial usado pelo contrato e o CMR. PLA/CMR abaixo de 1 não autoriza afirmar insolvência; PLA/CMR muito acima de 1 não gera mérito ilimitado.

## 5. Integridade da ingestão SES financeira

A fonte financeira falha fechado diante de:

- linha CSV estruturalmente malformada;
- FIP, `damesano` ou `CMPID` malformado/fracionário;
- competência que não seja `AAAAMM` válida;
- token numérico malformado ou não finito;
- `CMPID` de fórmula encontrado em quadro SES incompatível.

Números decimais e notação científica finita legítima são preservados. Caracteres arbitrários não são removidos para fabricar valores aparentemente válidos. Valores negativos e zeros válidos são preservados sem `abs()` ou clamp.

Duplicatas de capital são contabilizadas por entidade e competência. Uma competência duplicada não pode derivar `new_pla / CMR`, pois selecionar uma linha pela ordem física do arquivo seria arbitrário.

Os componentes de balanço usados nas fórmulas são aceitos somente nos quadros oficiais correspondentes (`22A`, `22P` ou `23`).

## 6. Liquidez — ILT

ILT permanece o indicador principal de liquidez; ILC é diagnóstico complementar.

A referência 1,0 do ILT é **paridade aritmética do indicador**, não limite prudencial oficial da SUSEP.

Estados:

```text
ilt_below_arithmetic_parity
ilt_at_or_above_arithmetic_parity
ilt_signal_unavailable
```

Capital e liquidez são não compensatórios.

## 7. Combinação central

Não foi selecionada média ponderada interna:

```text
weighted_average_selected = false
numeric_internal_score_selected = false
gate_logic_selected = true
```

Estados centrais:

```text
capital_requirement_shortfall_observed
capital_requirement_met_with_liquidity_pressure
core_indicators_without_current_shortfall
capital_requirement_met_liquidity_unavailable
core_financial_signal_unavailable
```

Missingness permanece separado de desempenho.

## 8. Filme operacional — ICA/IC

O filme operacional permanece contexto longitudinal e não sobrescreve capital/liquidez.

Estados:

```text
balanced_persistent
improved
recent_pressure
persistent_pressure
indeterminate
```

ICA é a referência principal; IC e componentes explicam a formação do resultado. O ponto 1,0 é paridade aritmética da fórmula, não threshold prudencial SUSEP.

## 9. Confiança da evidência

Histórico afeta confiança, não desempenho:

```text
complete_core_history → established_core_history
limited_core_history  → limited_core_history
outros                 → insufficient_core_evidence
```

Histórico curto não recebe punição automática.

## 10. Rentabilidade — ILPL

A investigação de ILPL permanece encerrada:

```text
independent_component_selected = false
role = diagnostic_only
```

Qualquer diagnóstico de redundância com capital deve usar o mesmo `new_pla/CMR` canônico.

## 11. Snapshot integrado corrente

Na Full Generation #44 (`run_id = 33562392945`, `head = c95e8675f8a2363b325623b2f310886e81f1c027`), com universo de **156 seguradoras ordinárias** e `reference_period = 202606`:

Capital:

```text
capital_meets_or_exceeds_cmr  148
capital_below_cmr               3
capital_signal_unavailable      5
```

Liquidez ILT:

```text
ilt_at_or_above_arithmetic_parity 124
ilt_below_arithmetic_parity        29
ilt_signal_unavailable              3
```

Combinação central:

```text
core_indicators_without_current_shortfall              120
capital_requirement_met_with_liquidity_pressure         26
capital_requirement_shortfall_observed                   3
core_financial_signal_unavailable                        5
capital_requirement_met_liquidity_unavailable            2
```

Confiança:

```text
established_core_history       141
limited_core_history            10
insufficient_core_evidence       5
```

Filme operacional:

```text
balanced_persistent  90
improved             12
recent_pressure      11
persistent_pressure  11
indeterminate        32
```

As contagens são fotografia da geração, não constantes metodológicas.

## 12. Auditoria ponta a ponta da publicação

`api/v2/audit_financial_publication_chain.py` bloqueia a materialização do `distribution_manifest.json` se a cadeia financeira não reconciliar.

No snapshot integrado corrente:

```text
status = financial_publication_chain_verified
capital_pla_source_field = new_pla
reference_period = 202606
regulatory_entities = 156
capital_derivable = 151
capital_unavailable = 5
capital_below_cmr = 3
ilt_derivable = 153
ilt_unavailable = 3
ilt_below_arithmetic_parity = 29
scoring = forbidden_in_this_artifact
ranking = forbidden_in_this_artifact
```

Fronteiras verificadas incluem:

- parser SES financeiro fail-closed;
- `new_pla / CMR` da fonte até o perfil público;
- competência duplicada de capital não pode produzir razão derivável;
- CMPIDs das fórmulas pertencem aos quadros SES esperados;
- Financial Closure, Explorer e perfil preservam valor/estado de capital e ILT;
- leaderboards financeiros derivam a própria métrica sem tiebreaker de mérito;
- estado operacional é preservado até o perfil público.

## 13. Guardrails finais

O fechamento proíbe:

```text
dado ausente = zero
pla_adjusted como fallback de new_pla
porte absoluto = mérito
recompensa linear por excesso de capital
recompensa linear por ILT extremo
percentil de mercado = threshold prudencial
média ponderada que esconda insuficiência de um eixo
filme operacional sobrescrevendo capital/liquidez
histórico curto = desempenho ruim
financial_score sem novo contrato explícito
```

## 14. Linguagem pública permitida

Permitido:

- “O indicador de capital ficou abaixo do CMR na competência de referência.”
- “O requisito de capital foi atendido, mas o ILT ficou abaixo da paridade aritmética e merece cautela.”
- “Os indicadores financeiros centrais não mostram insuficiência atual segundo suas referências.”
- “O histórico disponível é limitado; a leitura da estabilidade deve ser feita com cautela.”

Não permitido apenas com esta camada:

- “A seguradora é solvente.”
- “A seguradora é financeiramente segura.”
- “A SUSEP aprovou a liquidez da empresa.”
- “Quanto maior o PLA/CMR ou ILT, melhor a seguradora sem limite.”

**Score e ranking permanecem proibidos nesta camada.**
