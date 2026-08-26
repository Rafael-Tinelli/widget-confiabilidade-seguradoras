# Conduct v2 — modelo humano de credibilidade

Este documento traduz a matemática experimental de Conduta em perguntas que um consumidor consegue compreender.

Ele **não cria score, ranking, faixas de qualidade ou aprovação de seguradoras**. Seu papel é impedir que uma conta correta seja apresentada como conclusão quando a evidência ainda permite explicações alternativas relevantes.

## Princípio

A pergunta-base continua simples:

> **Esta seguradora reclama muito para o tamanho da operação?**

Mas uma razão observada/esperada, sozinha, não responde isso com segurança. Antes de transformar um número em informação para o consumidor, a v2 deve passar por pequenos dispositivos independentes de proteção contra erro.

A sequência humana é:

1. **Reclama muito para o tamanho da operação?**
2. **Temos dados suficientes para confiar nessa diferença?**
3. **A conclusão muda muito conforme a forma de medir o tamanho da operação?**
4. **Reclamações e exposição estão contando uma história temporal coerente?**
5. **Estamos comparando negócios com carteiras parecidas?**
6. **Foi um pico isolado ou um comportamento persistente?**
7. **Só então existe base para discutir uma leitura pública de Conduta.**

A arquitetura desejada é, portanto:

```text
observado
→ comparável
→ estatisticamente crível
→ robusto ao denominador
→ coerente no tempo
→ comparável por mix
→ persistente/remediado
→ interpretação pública
```

Nenhuma etapa posterior pode consertar silenciosamente uma falha anterior.

---

## 1. Pressão relativa

### Pergunta humana

> **Reclama muito para o tamanho da operação?**

### Conta experimental

```text
participação nas reclamações
÷
participação no prêmio
```

Equivalente a:

```text
reclamações observadas
÷
reclamações esperadas para aquela participação econômica
```

A referência `1,0` significa apenas proporcionalidade entre reclamações e a medida de exposição usada.

Não significa:

- 1% dos clientes reclamando;
- qualidade neutra;
- limite regulatório;
- nota SUSEP.

---

## 2. Credibilidade da diferença

### Pergunta humana

> **Temos evidência suficiente para acreditar que a diferença não apareceu apenas por pouca amostra?**

O artifact `v2_conduct_credibility_diagnostic` acrescenta duas travas diagnósticas:

1. intervalo binomial exato de Clopper-Pearson para a participação observada nas reclamações;
2. proteção Bonferroni para o fato de estarmos examinando muitas seguradoras simultaneamente.

A segunda trava é deliberadamente conservadora.

Ela não prova causalidade nem valida o denominador. Apenas reduz o risco de chamar de excepcional um resultado que surgiu porque observamos 103 empresas ao mesmo tempo.

### Resultado real — prêmio direto

Universo experimental:

```text
103 entidades
```

Depois da proteção simultânea:

```text
acima da referência proporcional        28
abaixo da referência proporcional       44
não distinguível da referência          31
```

`abaixo` **não é nota positiva de Conduta**. Significa somente que, neste teste e neste denominador, a participação observada de reclamações ficou abaixo da participação econômica com diferença estatisticamente distinguível.

### Exemplo que justifica a trava

**ZENPLA Seguros**:

```text
reclamações observadas       1
razão bruta                 ~112,31×
```

O número é visualmente extremo, mas, depois da proteção simultânea, a diferença passa para:

```text
not_distinguishable_from_size_proportional_reference
```

Em linguagem humana:

> **O número bruto parece enorme, mas há evidência insuficiente para tratá-lo como uma diferença confiável quando consideramos a pequena amostra e o fato de estarmos olhando muitas seguradoras.**

---

## 3. Robustez ao denominador

### Pergunta humana

> **A conclusão muda muito conforme a forma de medir o tamanho da operação?**

O contrato atual mantém:

```text
premio_direto  = candidato principal
premio_ganho   = diagnóstico de sensibilidade
```

Nenhum dos dois foi promovido a denominador final.

A SUSEP trata essas grandezas como conceitos contábeis diferentes. Por isso, divergência relevante entre elas deve virar alerta, não ajuste silencioso.

### Resultado real

```text
prêmio direto disponível                  103
prêmio ganho positivo para diagnóstico    102
prêmio ganho indisponível                    1
```

Comparando as duas leituras:

```text
mudanças de lado da referência bruta       4
mudanças de estado após a trava estatística 6
```

Para a maior parte da população, a leitura é relativamente estável:

```text
multiplicador pressão(ganho)/pressão(direto)
P10      ~0,868
P50      ~1,005
P90      ~1,549
```

Mas a cauda contém casos materialmente sensíveis; por isso a estabilidade média não autoriza ignorar exceções.

### Exemplo de bloqueio por sensibilidade

**Cardif do Brasil Vida e Previdência**:

```text
prêmio direto  → razão ~0,887
prêmio ganho   → razão ~1,526
```

Além de cruzar a referência `1,0`, o estado conservador muda de `abaixo` para `acima`.

Em linguagem humana:

> **A conclusão depende da forma como medimos o tamanho da operação. Logo, ainda não há uma verdade comparativa robusta para apresentar ao consumidor.**

---

## 4. Coerência temporal

### Pergunta humana

> **Reclamações e medida de exposição estão falando do mesmo fenômeno no tempo?**

O diagnóstico encontrou:

```text
entidades com reclamações em meses de prêmio direto não positivo   6
reclamações nesses meses                                           71
```

Casos:

- OXXY Seguradora;
- Zenpla Seguros;
- AKAD Seguros;
- Verbin Seguros;
- Fairway Seguros;
- 88I Seguradora Digital.

Isso **não significa ausência de clientes ou ausência de apólices**. `premio_direto` é fluxo contábil e uma reclamação pode se referir a relação contratual originada em período anterior.

Portanto o dispositivo apenas levanta:

```text
temporal_alignment_review = required
```

Ele não exclui automaticamente a empresa nem transfere a reclamação para outro mês.

### Exemplo que mostra por que uma única trava não basta

**Fairway Seguros**:

```text
reclamações                         5
razão com prêmio direto          ~168,77×
razão com prêmio ganho        ~32.590,30×
```

Mesmo com apenas 5 reclamações, o sinal permanece acima da referência no teste estatístico conservador porque a exposição medida é extremamente pequena.

Porém:

- as 5 reclamações estão em meses sem prêmio direto positivo;
- prêmio direto e prêmio ganho divergem fortemente;
- `premio_ganho / premio_direto ≈ 0,0049`.

Em linguagem humana:

> **A estatística sozinha não resolve o caso. O resultado sobrevive ao teste de pequena amostra, mas falha em testes de coerência da própria medida de tamanho. Portanto não deve virar conclusão pública.**

Este é um exemplo central da filosofia da v2: **uma trava não substitui as demais**.

---

## 5. O que a investigação já permite concluir

O problema não deve ser resolvido com um corte arbitrário do tipo:

```text
menos de N reclamações = ignorar
```

Isso apagaria casos pequenos que podem carregar sinal e, ao mesmo tempo, não resolveria casos grandes com problemas de comparabilidade.

Também não há justificativa, neste momento, para aplicar automaticamente:

- shrinkage;
- Empirical Bayes;
- score de confiança;
- threshold de pressão;
- bônus por ficar abaixo de `1,0`.

A conclusão mais defensável é usar **gates independentes** e preservar o motivo de cada bloqueio.

Exemplos:

```text
statistical_credibility_review
denominator_sensitivity_review
temporal_alignment_review
portfolio_mix_review
persistence_review
```

A ausência de um gate não deve ser convertida em punição nem em recompensa.

---

## 6. Próxima investigação

A próxima possibilidade relevante de erro é o **mix de carteira**.

### Pergunta humana

> **Estamos comparando esta seguradora com empresas que vendem coisas suficientemente parecidas?**

O artifact de calibração já preserva `coramo`, composição positiva, HHI, participação do ramo dominante, distância da composição de mercado e vizinhos por similaridade de carteira.

A próxima investigação deve testar, sem formar cohorts finais:

1. se seguradoras próximas no vetor de `coramo` exibem pressões estruturalmente semelhantes;
2. se os extremos continuam extremos quando comparados a carteiras próximas;
3. quantas entidades realmente possuem peers suficientemente próximos;
4. quando a distância é grande demais para uma comparação por pares ser defensável;
5. se a correção por mix reduz dispersão de forma consistente ou apenas desloca outliers;
6. se o resultado é estável no tempo;
7. se um eventual peer baseline mantém a população de reclamações e exposição rigorosamente alinhada.

Dois exemplos já justificam a investigação:

- Fairway possui carteira `coramo` muito concentrada e um peer de composição praticamente idêntica, mas o caso continua contaminado pela qualidade do denominador;
- Azul e BP aparecem próximas por composição e ambas apresentam pressão bruta elevada, sinal de que parte da diferença pode ser estrutural ao negócio e não apenas efeito individual.

Nenhum desses exemplos autoriza ajuste antes do teste sistemático.

---

## 7. Linguagem pública futura

Se a metodologia sobreviver aos gates, preferir frases que expressem exatamente o que foi medido.

Exemplos candidatos:

```text
"Tem mais reclamações do que esperaríamos para o tamanho medido da operação."
"Os dados ainda são insuficientes para dizer se reclama mais ou menos do que o esperado."
"A comparação é sensível à forma de medir o tamanho da operação; por isso não mostramos uma conclusão."
"Há uma inconsistência temporal que precisa ser resolvida antes da comparação."
"Ainda não encontramos empresas suficientemente parecidas para uma comparação justa por carteira."
"O sinal aparece de forma persistente, não apenas em um mês isolado."
```

Evitar:

```text
"X% dos clientes reclamam"
"é boa porque ficou abaixo de 1"
"é ruim porque ficou acima de 1"
"é a pior"
"é a melhor"
```

antes que cada uma dessas afirmações possa ser sustentada por evidência específica.

---

## 8. Estado do gate

Implementação experimental:

- `api/v2/build_conduct_credibility_diagnostic.py`;
- `tests/test_v2_conduct_credibility_diagnostic.py`;
- workflow `V2 Conduct Credibility Diagnostic`;
- artifact `data/derived/v2/conduct_credibility_diagnostic.json`.

Validação real:

```text
Ruff                         verde
testes direcionados          2/2
build sobre os 103 casos     verde
boundary validation          verde
artifact upload              verde
```

Guardrails do artifact:

```text
scoring                 forbidden
ranking                 forbidden
shrinkage_applied       false
empirical_bayes_applied false
denominator_selected    false
portfolio_mix_adjusted  false
```

Este gate reduz falsos extremos e identifica comparações frágeis, mas **não encerra a calibração de Conduta**.
