# Fechamento metodológico de Conduta — v2

Status: **fechado para desenho do sinal de Conduta; score numérico ainda proibido**.

Este documento complementa o README e registra o contrato corrente da camada de Conduta. As contagens abaixo são fotografia da geração integrada de 01/09/2026; não são constantes metodológicas.

## 1. Pergunta humana

A camada de Conduta tenta responder, nesta ordem:

1. **Reclama muito para o tamanho da operação?**
2. **Temos dados suficientes para confiar nessa diferença?**
3. **O sinal aparece de forma repetida ou parece episódico?**
4. **A pressão está melhorando, piorando ou sem mudança clara?**
5. **A carteira é diferente o bastante para exigir cautela?**
6. **Entre quem avaliou, como foi a experiência e qual o tamanho da amostra?**

Regra principal:

> quando a evidência não sustenta a frase, a ferramenta não afirma.

## 2. Denominador aprovado — escopo restrito

Para o universo `direct_one_to_one_candidate`, o denominador operacional de pressão de Conduta é:

```text
insurance_premium_direct
Ses_seguros.csv → premio_direto
```

A aprovação é restrita ao experimento de pressão `insurance_only` e não transforma prêmio em número de clientes, apólices ou contratos.

Continuam excluídos do denominador:

- previdência complementar aberta;
- capitalização.

`insurance_premium_earned` permanece como diagnóstico de sensibilidade. Se a conclusão estatística muda de estado entre prêmio direto e prêmio ganho, a ferramenta não publica conclusão direcional de pressão.

## 3. Alinhamento temporal obrigatório

A pressão anual é mensalmente alinhada:

```text
para cada mês comparável:
    expected_m = reclamações_do_mercado_m × prêmio_da_entidade_m / prêmio_do_mercado_m

expected_12m = soma(expected_m)
observed_12m = soma(reclamações da entidade apenas nos meses comparáveis)
pressure_12m = observed_12m / expected_12m
```

A população econômica e a população de reclamações precisam ser comparáveis no mesmo mês. Se o prêmio da entidade é não positivo, o mês não entra no denominador normalizado, embora suas reclamações permaneçam preservadas como evidência factual.

Se o mercado possui prêmio positivo, mas `market_complaints = 0`, não existe baseline de pressão naquele mês. O estado é explicitamente não comparável; `0/0` não vira neutralidade nem sinal favorável.

## 4. Missingness, zero e integridade da fonte

O contrato distingue:

```text
missing ≠ zero
zero factual ≠ sinal favorável
malformed ≠ missing
```

A ingestão SES de exposição:

- preserva linha onde `premio_direto` e/ou `premio_ganho` estão ausentes;
- falha fechado em token numérico malformado ou não finito;
- falha fechado em período/ramo malformado;
- não usa `abs()`, clamp ou fallback de sinal;
- rejeita contadores fracionários onde o contrato exige inteiros.

Unidade declarada:

```text
currency = BRL
source_unit_label = R$
scale_factor_applied = 1.0
```

## 5. Pequenas amostras e credibilidade

Razão bruta não é conclusão.

A pressão `observed / expected` usa intervalo exato de Poisson para razão padronizada. O tamanho de amostra estatístico usa somente `pressure_12m.observed_complaints`, isto é, reclamações dos meses realmente comparáveis; reclamações brutas fora desses meses permanecem preservadas como evidência, mas não podem inflar o sample bucket.

Decisão:

```text
shrinkage = não selecionado
Empirical Bayes = não selecionado
```

## 6. Cobertura temporal

Uma conclusão anual de pressão exige ao menos:

```text
9 meses comparáveis em 12
```

As janelas são validadas quanto a duplicidade, ordem cronológica e consecutividade. Histórico menor não é desempenho ruim; é evidência temporal insuficiente.

## 7. Persistência e tendência

Persistência é separada da pressão anual e exige a mesma direção com evidência suficiente em pelo menos metade dos meses comparáveis, além da cobertura mínima.

Estados de persistência incluem:

```text
persistent_above_expected
episodic_or_sparse_above_expected
persistent_below_expected
episodic_or_sparse_below_expected
not_distinguishable_from_expected
insufficient_temporal_coverage
```

A tendência compara os 6 meses recentes com os 6 meses iniciais da janela, usando pressão normalizada e incerteza explícita.

Estados:

```text
improving_pressure
deteriorating_pressure
no_clear_change
insufficient_events
insufficient_temporal_coverage
```

Tendência não substitui a conclusão anual.

## 8. Mix de carteira (`coramo`)

A investigação de mix foi encerrada sem seleção de coortes obrigatórias:

```text
portfolio_adjustment = false
peer_groups_selected = false
distance_threshold_selected = false
```

A pressão local entre peers, quando usada como diagnóstico, também é reconstruída mês a mês com a mesma população econômica. `coramo` permanece contexto e diagnóstico de sensibilidade. Não encontrar peer adequado nunca significa pressão neutra.

## 9. Satisfação

Satisfação é dimensão contextual separada da incidência de reclamações. Ela carrega média, tamanho da amostra e direção somente quando existe amostra suficiente.

Política atual:

```text
mínimo 10 avaliações em cada metade
```

Satisfação não recebe peso e não corrige a pressão de reclamações.

## 10. Remediação

A remediação não é inferida com o P3 atual:

```text
remediation = not_established_from_current_p3
```

Responder ou finalizar uma reclamação não prova solução do problema. Uma fonte futura BDR/SusepCon pode reabrir essa investigação sem alterar o contrato corrente.

## 11. Casos sem pressão comparável

Eles não desaparecem da ferramenta.

Para cada um:

- evidência de Conduta continua pesquisável;
- `pressure = null`;
- o motivo de não comparabilidade é explícito;
- a rota de recuperação é preservada;
- nenhum peso é redistribuído silenciosamente.

## 12. Snapshot integrado corrente

Na Full Generation #44 (`run_id = 33562392945`, `head = c95e8675f8a2363b325623b2f310886e81f1c027`):

```text
seguradoras ordinárias                         156
candidatas ao cálculo de pressão               101
pressão indisponível por não comparabilidade    55
```

Conclusão anual entre as 101 candidatas:

```text
acima do esperado com evidência suficiente       26
abaixo do esperado com evidência suficiente       41
sem diferença suficientemente clara               18
inconclusivas por sensibilidade do denominador      5
sem cobertura temporal suficiente                  11
```

Persistência:

```text
persistente acima do esperado                     20
episódica/esparsa acima                             7
persistente abaixo do esperado                     29
episódica/esparsa abaixo                           15
sem diferença clara                                19
cobertura temporal insuficiente                    11
```

Tendência:

```text
deteriorating_pressure                            11
improving_pressure                                 8
no_clear_change                                   60
insufficient_events                               12
insufficient_temporal_coverage                    10
```

Satisfação entre as 101 candidatas:

```text
stable                                             32
worsening                                           5
improving                                           1
insufficient_sample                                63
```

Esses números são diagnósticos de uma geração, não ranking nem thresholds metodológicos.

## 13. Linguagem pública autorizada

### Acima do esperado

> Há mais reclamações do que esperaríamos para o tamanho da operação nos meses comparáveis, e a diferença é sustentada pela evidência disponível.

### Abaixo do esperado

> Há menos reclamações do que esperaríamos para o tamanho da operação nos meses comparáveis. Isso, isoladamente, não prova melhor atendimento ou maior qualidade da seguradora.

### Sem diferença clara

> Os dados não mostram diferença suficientemente clara em relação ao esperado para o tamanho da operação.

### Evidência temporal insuficiente

> Ainda não há meses comparáveis suficientes para uma conclusão anual de pressão de reclamações.

### Sensibilidade de denominador

> A conclusão muda conforme a medida econômica usada para representar o tamanho da operação; por isso, não apresentamos uma conclusão direcional.

### Não comparável

> Há dados de Conduta, mas não há numerador e denominador comparáveis suficientes para calcular pressão sem inventar atribuições.

## 14. Estado do contrato

Fechado:

```text
identidade de Conduta
cobertura Consumer.gov P3
comparabilidade subject/carrier
exposição insurance-only
alinhamento mensal
pressão observed/expected
incerteza de pequenas amostras
persistência
tendência
sensibilidade premio_direto × premio_ganho
papel do coramo
papel da satisfação
limite atual de remediação
tratamento explícito dos não comparáveis
linguagem pública do sinal
```

Não autorizado por esta camada:

```text
conduct_numeric_score
peso de Conduta no resultado geral
score composto
ranking final
```

## 15. Critério de fechamento

A camada de Conduta é considerada metodologicamente fechada porque sabe quando pode comparar, quando não pode, o que está sendo normalizado, quanto confiar na diferença, como tratar persistência/tendência/satisfação e como preservar missingness sem convertê-la em desempenho.

Esse critério é executado pelo artifact `v2_conduct_methodology_closure`.

**Score e ranking permanecem proibidos neste artifact.**
