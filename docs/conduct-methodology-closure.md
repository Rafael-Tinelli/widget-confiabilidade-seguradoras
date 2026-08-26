# Fechamento metodológico de Conduta — v2

Status: **fechado para desenho do sinal de Conduta; score numérico ainda proibido**.

Este documento complementa o README e registra as decisões tomadas depois da Conduct Comparative Calibration v2, do diagnóstico de credibilidade e do diagnóstico de mix de carteira.

## 1. Pergunta humana

A camada de Conduta não existe para exibir uma razão bruta. Ela tenta responder, nesta ordem:

1. **Reclama muito para o tamanho da operação?**
2. **Temos dados suficientes para confiar nessa diferença?**
3. **O sinal aparece de forma repetida ou parece episódico?**
4. **A pressão está melhorando, piorando ou sem mudança clara?**
5. **A carteira é diferente o bastante para exigir cautela?**
6. **Entre quem avaliou, como foi a experiência e qual o tamanho da amostra?**

Regra principal:

> quando a evidência não sustenta a frase, a ferramenta não afirma.

## 2. Denominador aprovado — escopo restrito

Para o universo `direct_one_to_one_candidate`, o denominador operacional de pressão de Conduta passa a ser:

```text
insurance_premium_direct
Ses_seguros.csv → premio_direto
```

A aprovação é **restrita** ao experimento de pressão `insurance_only` e não transforma prêmio em número de clientes, apólices ou contratos.

Continuam excluídos do denominador:

- previdência complementar aberta;
- capitalização.

`insurance_premium_earned` permanece como diagnóstico de sensibilidade. Se a conclusão estatística muda de estado entre prêmio direto e prêmio ganho, a ferramenta não publica uma conclusão direcional de pressão.

O precedente oficial é conceitual: o SusepCon utiliza reclamações ponderadas pela arrecadação. Isso sustenta normalização pela escala econômica, mas não significa que a Sanida esteja reproduzindo literalmente o denominador interno do SusepCon.

## 3. Alinhamento temporal obrigatório

A pressão anual não é calculada pela divisão de dois agregados anuais independentes quando a população comparável varia ao longo dos meses.

A regra final é:

```text
para cada mês comparável:
    expected_m = reclamações_do_mercado_m × prêmio_da_entidade_m / prêmio_do_mercado_m

expected_12m = soma(expected_m)
observed_12m = soma(reclamações da entidade apenas nos meses comparáveis)
pressure_12m = observed_12m / expected_12m
```

Se o prêmio comparável da entidade é não positivo no mês, as reclamações daquele mês continuam preservadas como evidência de Conduta, mas **não entram na pressão normalizada daquele mês**.

Essa correção remove falsos extremos gerados por incompatibilidade temporal. Casos como FAIRWAY e ZENPLA deixam de produzir uma conclusão direcional anual quando a exposição comparável é insuficiente.

## 4. Pequenas amostras e credibilidade

Razão bruta não é conclusão.

A pressão `observed / expected` usa intervalo exato de Poisson para razão padronizada. A comparação anual controla o erro familiar entre as 103 entidades atualmente comparáveis.

A série mensal também usa incerteza explícita, com controle dentro da janela comum de 12 meses.

Decisão:

```text
shrinkage = não selecionado
Empirical Bayes = não selecionado
```

Motivo: nesta etapa a necessidade é impedir afirmações frágeis, não produzir uma magnitude suavizada pronta para score.

## 5. Cobertura temporal

Uma conclusão anual de pressão exige ao menos:

```text
9 meses comparáveis em 12
```

Histórico menor não é desempenho ruim. É simplesmente evidência temporal insuficiente.

## 6. Persistência

Persistência é separada da pressão anual.

Uma direção anual só é tratada como persistente quando:

- há pelo menos 9 meses comparáveis; e
- a mesma direção aparece com evidência suficiente em pelo menos metade dos meses comparáveis.

Estados possíveis incluem:

```text
persistent_above_expected
episodic_or_sparse_above_expected
persistent_below_expected
episodic_or_sparse_below_expected
not_distinguishable_from_expected
insufficient_temporal_coverage
```

O objetivo não é premiar ou punir; é distinguir sinal repetido de pico ou evidência esparsa.

## 7. Tendência

A tendência compara:

```text
6 meses recentes
versus
6 meses iniciais
```

com pressão normalizada em cada metade e intervalo exato condicional para a razão entre as duas pressões.

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

A investigação de mix foi encerrada sem seleção de coortes obrigatórias.

Resultados reais:

- Spearman entre distância de carteira e diferença de pressão em todos os pares: aproximadamente **0,184**;
- entre pares de entidades com 100+ reclamações: aproximadamente **0,137**;
- peers muito próximos são escassos; aumentar o raio melhora cobertura, mas torna a noção de peer progressivamente menos específica.

Decisão:

```text
portfolio_adjustment = false
peer_groups_selected = false
distance_threshold_selected = false
```

`coramo` permanece contexto e diagnóstico de sensibilidade. Não encontrar peer adequado nunca significa pressão neutra.

## 9. Satisfação

Satisfação é uma dimensão contextual separada da incidência de reclamações.

Ela deve sempre carregar:

- média observada;
- tamanho total da amostra;
- amostra da primeira metade;
- amostra da segunda metade;
- direção apenas quando há amostra suficiente.

Política atual para direção:

```text
mínimo 10 avaliações em cada metade
```

Satisfação não recebe peso nesta etapa e não corrige a pressão de reclamações.

## 10. Remediação

A remediação **não é inferida** com o P3 atual.

Responder ou finalizar uma reclamação não prova que o problema tenha sido solucionado. O core preservado também não mantém de forma suficiente o denominador avaliado necessário para transformar `Resolvida / Não Resolvida` em série robusta.

Portanto:

```text
remediation = not_established_from_current_p3
```

Uma futura fonte BDR/SusepCon pode reabrir essa investigação sem alterar o contrato atual.

## 11. Os 54 casos sem pressão comparável

Eles não desaparecem da ferramenta.

Para cada um:

- evidência de Conduta continua pesquisável;
- `pressure = null`;
- o motivo de não comparabilidade é explícito;
- a rota de recuperação é preservada;
- nenhum peso é redistribuído silenciosamente.

Exemplos de rotas:

- seguros + previdência → recuperar numerador de reclamações por produto;
- Zurich → reconciliação temporal da transferência de carteira;
- Bradesco → separar produto/carrier;
- Youse/Caixa → exposição específica da marca/subject;
- prêmio direto negativo → revisão contábil;
- run-off → contexto próprio, sem pressão corrente.

## 12. Resultado da execução real de fechamento

Universo:

```text
seguradoras ordinárias                         157
candidatas ao cálculo de pressão               103
pressão indisponível por não comparabilidade    54
```

Entre as 103 candidatas, depois de alinhamento temporal, incerteza e sensibilidade de denominador:

```text
acima do esperado com evidência suficiente       26
abaixo do esperado com evidência suficiente       41
sem diferença suficientemente clara               18
inconclusivas por sensibilidade do denominador      6
sem cobertura temporal suficiente                  12
```

Persistência:

```text
persistente acima do esperado                     20
episódica/esparsa acima                             7
persistente abaixo do esperado                     29
episódica/esparsa abaixo                           15
sem diferença clara                                20
cobertura temporal insuficiente                    12
```

Tendência:

```text
deteriorating_pressure                            11
improving_pressure                                 8
no_clear_change                                   60
insufficient_events                               13
insufficient_temporal_coverage                    11
```

Satisfação entre as 103:

```text
stable                                             32
worsening                                           5
improving                                           1
insufficient_sample                                65
```

Esses números são diagnósticos metodológicos, não ranking.

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

## 14. O que está fechado e o que não está

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
tratamento explícito dos 54 não comparáveis
linguagem pública do sinal
```

Ainda não definido:

```text
conduct_numeric_score
peso de Conduta no resultado geral
score composto
ranking final
```

Esses itens pertencem à calibração posterior entre pilares. Não devem ser resolvidos dentro da camada de Conduta.

## 15. Critério de fechamento

A camada de Conduta é considerada metodologicamente fechada quando sabe:

- quando pode comparar;
- quando não pode comparar;
- o que está sendo normalizado;
- quanto confiar na diferença;
- se o sinal é persistente ou episódico;
- se há mudança temporal clara;
- como tratar satisfação sem confundi-la com incidência;
- como preservar casos sem pressão comparável;
- e como explicar tudo isso sem dizer mais do que os dados sustentam.

Esse critério está atendido pelo artifact `v2_conduct_methodology_closure`.

**Score e ranking permanecem proibidos neste artifact.**
