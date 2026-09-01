# Fechamento metodológico do sinal Financeiro — v2

Status: **fechado para desenho do sinal Financeiro; score numérico ainda proibido**.

Este documento registra a arquitetura financeira consolidada da v2. Os indicadores possuem referências de natureza distinta e não são convertidos prematuramente para uma escala comum.

## 1. Pergunta humana

A camada Financeira tenta responder, nesta ordem:

1. **O Novo PLA utilizado pelo contrato cobre o Capital Mínimo Requerido?**
2. **A liquidez total mostra pressão na competência observada?**
3. **A operação está equilibrada, melhorando ou sob pressão ao longo do tempo?**
4. **Temos histórico suficiente para interpretar a trajetória?**

Regra principal:

> um indicador forte não apaga silenciosamente um problema material em outro eixo.

E uma segunda regra:

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

ILPL não retorna como componente independente. ILC não disputa o papel de ILT. ICA/IC não formam um terceiro score financeiro bruto.

## 3. Competência financeira

A leitura usa a política de competência madura definida em `financial_periods.py`:

```text
última competência observada ≠ necessariamente última competência madura
```

Na Full Generation canônica de 01/09/2026, a competência selecionada foi:

```text
reference_period = 202606
```

A seleção é determinada pelo contrato de maturidade e cobertura, não por uma data fixa em documentação.

## 4. Capital — PLA/CMR

PLA/CMR continua sendo o eixo principal de capital.

### 4.1 Numerador canônico

A correção crítica consolidada em 29/08/2026 é:

```text
campo SES NovoPla    → new_pla      → numerador final do PLA/CMR
campo SES plajustado → pla_adjusted → intermediário bruto, não numerador final
CMR                  → cmr

capital_ratio = new_pla / cmr
```

Não existe fallback de `new_pla` para `pla_adjusted`.

Se `new_pla` estiver ausente ou não utilizável, o sinal de capital fica indisponível mesmo que `pla_adjusted` exista.

As antigas reconstruções diagnósticas independentes de `pla_adjusted/CMR` em Liquidez, Operação e ILPL foram eliminadas. Os consumidores diagnósticos convergem para `financial_capital_semantics.capital_pla_cmr_ratio()`.

### 4.2 Semântica

A referência `1,0` possui interpretação material: o numerador de capital utilizado pelo contrato deve ser igual ou superior ao CMR.

Estados:

```text
capital_below_cmr
capital_meets_or_exceeds_cmr
capital_signal_unavailable
```

Decisão:

```text
continuous_transform_selected = false
positive_tiers_above_reference_selected = false
```

Um PLA/CMR de 3 não recebe três vezes o mérito de um PLA/CMR de 1. O valor bruto permanece visível, mas não gera faixas arbitrárias de “bom”, “ótimo” e “excelente”.

PLA/CMR abaixo de 1 também não autoriza afirmar insolvência ou incapacidade automática de pagar sinistros. A conclusão permitida é limitada ao requisito observado na competência de referência.

## 5. Liquidez — ILT

ILT permanece o indicador principal de liquidez. ILC continua como diagnóstico complementar de curto prazo.

A referência `1,0` do ILT é **paridade aritmética do indicador**, não limite prudencial oficial da SUSEP.

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

A linguagem correta é:

- abaixo de 1 → há pressão de liquidez segundo a paridade aritmética do ILT;
- igual ou acima de 1 → não há pressão segundo essa referência aritmética;
- nunca → “aprovada pela SUSEP em liquidez”.

## 6. Combinação de capital e liquidez

Não foi selecionada média ponderada interna.

```text
weighted_average_selected = false
numeric_internal_score_selected = false
gate_logic_selected = true
```

Estados centrais:

### `capital_requirement_shortfall_observed`

PLA/CMR < 1. ILT alto não apaga a insuficiência de capital observada.

### `capital_requirement_met_with_liquidity_pressure`

PLA/CMR >= 1 e ILT < 1. Capital excedente não apaga pressão de liquidez.

### `core_indicators_without_current_shortfall`

PLA/CMR >= 1 e ILT >= 1. Significa apenas que os dois indicadores centrais não mostram insuficiência segundo suas respectivas referências na competência observada. Não é selo de saúde financeira, garantia de solvência ou superioridade.

### `capital_requirement_met_liquidity_unavailable`

Capital utilizável e aderente ao requisito, mas ILT indisponível. Não há imputação.

### `core_financial_signal_unavailable`

PLA/CMR indisponível ou não utilizável. Ausência de dado não vale zero nem neutralidade.

## 7. Filme operacional — ICA/IC

O filme operacional permanece separado do sinal central de capital + liquidez.

Estados:

```text
balanced_persistent
improved
recent_pressure
persistent_pressure
indeterminate
```

ICA é a referência principal; IC e seus componentes explicam a formação do resultado.

O ponto `1,0` em ICA/IC é paridade aritmética entre custos e base operacional segundo a fórmula, não threshold prudencial SUSEP.

```text
operating_role = longitudinal_context_only
operating_context_can_override_core_signal = false
```

Pressão operacional não transforma automaticamente uma seguradora capitalizada em “insolvente”; operação equilibrada não apaga insuficiência de capital ou pressão de liquidez.

## 8. Confiança da evidência

Histórico afeta confiança, não desempenho.

```text
complete_core_history → established_core_history
limited_core_history  → limited_core_history
outros                 → insufficient_core_evidence
```

Uma seguradora com pouco histórico não recebe punição automática; a limitação longitudinal é explicitada.

## 9. Rentabilidade — ILPL

A investigação de ILPL permanece encerrada:

```text
independent_component_selected = false
role = diagnostic_only
```

A correlação/redundância de ILPL com capital passa a usar o mesmo `new_pla/CMR` canônico. Resultados de redundância anteriores à correção do numerador não são evidência válida para nova recalibração.

## 10. Snapshot canônico atual

Na Full Generation `33471617517`, com universo regulatório de **156 seguradoras ordinárias** e `reference_period = 202606`:

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

Confiança do núcleo:

```text
established_core_history       141
limited_core_history            10
insufficient_core_evidence       5
```

Filme operacional:

```text
balanced_persistent  89
improved             12
recent_pressure      11
persistent_pressure  11
indeterminate        33
```

Essas contagens são fotografia da geração, não constantes metodológicas.

## 11. Auditoria ponta a ponta da publicação

`api/v2/audit_financial_publication_chain.py` bloqueia a materialização do `distribution_manifest.json` se a cadeia financeira não reconciliar.

Na geração canônica, o auditor verificou:

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

Fronteiras verificadas:

- Financial Evidence: `new_pla / CMR`;
- Financial Closure preserva valor e estado de capital;
- Explorer preserva valor e estado de capital;
- perfil público preserva valor e estado de capital;
- `highest_pla_cmr_ratio` é derivado do Explorer sem segundo desempate de mérito;
- ILT é numerador/denominador positivo segundo o experimento canônico;
- Closure, Explorer e perfil preservam valor/estado do ILT;
- `highest_ilt` é derivado do Explorer;
- estado operacional é preservado de Operating Experiment → Closure → Explorer → perfil.

## 12. Guardrails finais

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
financial_score antes de decisão metodológica posterior explícita
```

## 13. Linguagem pública permitida

Permitido:

- “O indicador de capital ficou abaixo do CMR na competência de referência.”
- “O requisito de capital foi atendido, mas o ILT ficou abaixo da paridade aritmética e merece cautela.”
- “Os indicadores financeiros centrais não mostram insuficiência atual segundo suas referências.”
- “O histórico disponível é limitado; a leitura da estabilidade deve ser feita com cautela.”
- “A operação mostra pressão persistente no filme longitudinal, sem substituir capital e liquidez.”

Não permitido apenas com esta camada:

- “A seguradora é solvente.”
- “A seguradora é financeiramente segura.”
- “A SUSEP aprovou a liquidez da empresa.”
- “Quanto maior o PLA/CMR ou ILT, melhor a seguradora sem limite.”
- “Uma operação equilibrada compensa insuficiência de capital.”

## 14. Validação canônica

```text
workflow: V2 Gate 4 Full Generation Proof
run: 33471617517
head_sha: 4f0b8805c999d40fa820c5a030b49d1b687c1db9
build_id: v2-gate4-full-33471617517-a1
conclusion: success
artifact_id: 9787285465
artifact_digest: sha256:a8419e5285386b2ab26fd33bc52cbe71c5152507e4e9286dbfb8b9f9656c7ad1
package_sha256: c5a68480556a325a041553d56a1a040e1ab205d646243aa98c7b16cdea2f2aad
```

A prova ocorreu na mesma geração que produziu o pacote público e o auditor financeiro; não depende de composição de artifacts de workflows componentes.

## 15. O que significa “Financeiro fechado”

Estão encerrados neste contrato:

- escolha dos eixos financeiros;
- papel de cada indicador;
- política de competência;
- numerador canônico de PLA/CMR (`new_pla`);
- semântica de capital;
- semântica de liquidez;
- combinação não compensatória capital + liquidez;
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
ranking geral
frontend v2
```

Qualquer decisão futura sobre esses pontos exige etapa metodológica própria; não pode ser inferida deste fechamento.
