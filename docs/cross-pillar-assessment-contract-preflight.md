# Cross-Pillar Assessment Contract Preflight — linguagem pública e utilidade

Status: **preflight semântico concluído como proposta de contrato público; score, ranking e gates formais continuam fechados**.

Este documento sucede `docs/cross-pillar-architecture-stage-2.md` e responde à pergunta:

> **como transformar a matriz não compensatória em uma avaliação pública útil, compreensível e semanticamente segura?**

A resposta não é esconder os números. É organizá-los em uma linguagem em camadas, na qual o público primeiro entende **o que encontramos**, depois **por que isso importa**, depois **quais números sustentam a leitura** e, por fim, **o que a evidência não permite concluir**.

## 1. Princípio de comunicação

A ferramenta não deve começar pelo código interno (`F0|C1`), pela fórmula ou por uma nota.

A ordem pública deve ser:

```text
1. leitura geral
2. sinais encontrados
3. por que isso importa
4. qualificadores de persistência/tendência
5. números e metodologia
6. limites da conclusão
7. confiança/cobertura da evidência
```

Regra:

> **o algoritmo identifica sinais; a interface explica o significado desses sinais sem transformá-los em garantias.**

Outra regra:

> **resultado favorável pode ser reconhecido como favorável dentro do escopo avaliado, desde que a linguagem diga claramente o que não foi provado.**

Isso evita dois extremos ruins:

- chamar uma seguradora de `boa`, `segura` ou `confiável` a partir de poucos indicadores;
- usar linguagem tão defensiva que o consumidor não consiga distinguir uma situação favorável de uma situação problemática.

## 2. Vocabulário público

A natureza do sinal define a força da palavra usada.

### `leitura favorável`

Usada apenas quando os três núcleos avaliados não apresentam sinal adverso atual:

- capital em relação ao CMR;
- liquidez pelo ILT;
- pressão de reclamações relativa ao tamanho da operação.

Não significa `excelente`, `melhor`, `garantida`, `solvente para sempre` ou `sem risco`.

### `atenção`

Usada para sinais materiais de cautela que não são, neste contrato, exigências prudenciais oficiais equivalentes ao CMR:

- pressão de liquidez segundo a paridade aritmética do ILT;
- pressão de reclamações acima do esperado.

### `alerta prudencial`

Reservado ao caso em que o Patrimônio Líquido Ajustado está abaixo do Capital Mínimo Requerido na competência de referência.

O alerta é obrigatório e não pode ser apagado por outro indicador favorável.

O alerta **não** equivale automaticamente a insolvência, incapacidade de pagar sinistros ou ordem absoluta de pior qualidade.

### `avaliação conjunta incompleta`

Usada quando a ferramenta não pode formar uma leitura conjunta sem imputar, misturar populações ou inventar uma atribuição.

Ausência de evidência não é resultado ruim nem neutro.

## 3. Sete estados públicos

Os seis estados da matriz e o estado de evidência incompleta recebem títulos públicos próprios.

### 3.1. `no_current_core_adverse_signal`

**Título:** `Leitura central favorável`

**Resumo público:**

> Nos indicadores centrais analisados, não identificamos insuficiência de capital, pressão de liquidez pelo ILT nem pressão de reclamações acima do esperado para o tamanho da operação.

**Por que isso importa:**

> É um resultado favorável dentro do escopo da metodologia: os três sinais centrais não apontam fragilidade atual na competência e na janela analisadas.

**Limite obrigatório:**

> Isso não garante solvência futura, qualidade de cobertura, preço, atendimento individual ou superioridade sobre outra seguradora.

### 3.2. `conduct_pressure_only`

**Título:** `Atenção à Conduta`

**Resumo público:**

> Os indicadores financeiros centrais não mostram insuficiência atual, mas há mais reclamações do que esperaríamos para o tamanho da operação nos meses comparáveis.

**Por que isso importa:**

> A diferença é sustentada pela evidência disponível e indica pressão de problemas reportados pelos consumidores acima do padrão proporcional observado no mercado comparável.

**Limite obrigatório:**

> Isso não significa que todo cliente terá problema, não identifica sozinho a causa das reclamações e não prova má qualidade em todos os produtos ou canais da empresa.

### 3.3. `liquidity_pressure_only`

**Título:** `Atenção à liquidez`

**Resumo público:**

> O patrimônio ajustado cobre o capital mínimo requerido, mas o ILT está abaixo da paridade aritmética usada como referência. A Conduta não mostra pressão de reclamações acima do esperado.

**Por que isso importa:**

> A leitura pede cautela no eixo de liquidez, mesmo sem insuficiência de capital identificada na competência de referência.

**Limite obrigatório:**

> ILT abaixo de 1 não é, por si só, uma reprovação prudencial oficial da SUSEP e não permite concluir insolvência.

### 3.4. `liquidity_and_conduct_pressure`

**Título:** `Atenção em liquidez e Conduta`

**Resumo público:**

> O capital mínimo requerido está atendido, mas há pressão de liquidez pelo ILT e pressão de reclamações acima do esperado para o tamanho da operação.

**Por que isso importa:**

> Dois eixos centrais independentes apresentam cautela ao mesmo tempo. A metodologia preserva ambos em vez de permitir que um apague o outro.

**Limite obrigatório:**

> A combinação não equivale automaticamente a insolvência nem prova que todos os produtos ou atendimentos da seguradora sejam inadequados.

### 3.5. `capital_shortfall_without_conduct_pressure`

**Título:** `Alerta prudencial de capital`

**Resumo público:**

> O patrimônio líquido ajustado está abaixo do capital mínimo requerido na competência de referência. A Conduta não mostra pressão de reclamações acima do esperado.

**Por que isso importa:**

> O CMR é uma exigência prudencial de capital. Por isso, a insuficiência observada é um alerta material e não pode ser compensada por uma leitura favorável de reclamações.

**Limite obrigatório:**

> O dado não autoriza afirmar que a seguradora esteja insolvente ou que não consiga pagar sinistros. Ele registra uma insuficiência de capital em relação ao CMR na competência analisada.

### 3.6. `capital_shortfall_and_conduct_pressure`

**Título:** `Alerta de capital e Conduta`

**Resumo público:**

> O patrimônio líquido ajustado está abaixo do capital mínimo requerido e também há pressão de reclamações acima do esperado para o tamanho da operação.

**Por que isso importa:**

> Há simultaneamente um alerta prudencial material de capital e um sinal adverso de Conduta. Nenhum dos dois é reduzido ou compensado pelo outro.

**Limite obrigatório:**

> A combinação não permite afirmar insolvência automática nem generalizar a experiência de reclamação para todos os clientes ou produtos.

### 3.7. `evidence_incomplete_for_joint_assessment`

**Título:** `Avaliação conjunta incompleta`

**Resumo público:**

> Ainda não há evidência comparável suficiente para formar uma conclusão conjunta segura entre Financeiro e Conduta.

**Por que isso importa:**

> A ferramenta preserva os dados disponíveis separadamente e explica o que falta. Ela não transforma ausência de evidência em nota, neutralidade ou desempenho ruim.

**Limite obrigatório:**

> Este estado não é uma avaliação negativa da seguradora. É uma limitação da evidência disponível para a comparação proposta.

## 4. A matriz resume; os cartões dos pilares preservam os detalhes

A matriz trabalha com presença ou ausência de sinais adversos centrais. A interface não deve perder diferenças importantes dentro de um mesmo estado.

### Conduta sem pressão adversa

`C0` pode esconder duas situações diferentes e elas devem continuar visíveis no cartão de Conduta:

#### abaixo do esperado com evidência suficiente

> Foram observadas menos reclamações do que esperaríamos para o tamanho da operação nos meses comparáveis. É um resultado favorável para este indicador, mas não prova melhor atendimento ou maior qualidade geral.

#### sem diferença suficientemente clara

> Os dados não mostram diferença suficientemente clara entre as reclamações observadas e o esperado para o tamanho da operação.

Esses dois casos continuam empatados no eixo adverso da matriz porque o contrato não transforma `abaixo do esperado` em bônus de qualidade.

### Financeiro sem insuficiência central

A interface deve mostrar separadamente:

- PLA/CMR e se o requisito de capital foi atendido;
- ILT e se há pressão segundo a paridade aritmética;
- contexto operacional ICA/IC;
- confiança/histórico do núcleo financeiro.

`F0` não significa esconder os números nem chamar todas as empresas de igualmente fortes.

## 5. Cartão Financeiro — linguagem recomendada

A interface deve responder quatro perguntas humanas.

### Capital

Pergunta:

> **O patrimônio ajustado cobre o capital mínimo requerido?**

Estados públicos:

```text
Requisito atendido
Alerta: patrimônio abaixo do CMR
Dado indisponível
```

Mostrar o valor bruto `PLA/CMR`, a competência de referência e uma explicação curta.

### Liquidez

Pergunta:

> **O indicador de liquidez mostra pressão na competência analisada?**

Estados públicos:

```text
Sem pressão pela referência do ILT
Atenção: ILT abaixo da paridade aritmética
Dado indisponível
```

Nunca usar `aprovada pela SUSEP em liquidez`.

### Trajetória operacional

Pergunta:

> **A operação parece equilibrada, melhorando ou sob pressão ao longo do tempo?**

Estados públicos:

```text
Trajetória operacional equilibrada
Trajetória em melhora
Pressão operacional recente
Pressão operacional persistente
Trajetória inconclusiva
```

A trajetória é contexto e nunca sobrescreve capital ou liquidez.

### Confiança

Pergunta:

> **Temos histórico suficiente para interpretar a estabilidade?**

Estados públicos:

```text
Histórico estabelecido
Histórico limitado
Evidência central insuficiente
```

Histórico limitado reduz confiança; não reduz desempenho.

## 6. Cartão Conduta — linguagem recomendada

A interface deve responder cinco perguntas humanas.

### Pressão

Pergunta:

> **Há reclamações demais para o tamanho da operação?**

Estados públicos:

```text
Acima do esperado
Abaixo do esperado
Sem diferença clara
Conclusão sensível ao denominador
Cobertura temporal insuficiente
Não comparável com segurança
```

Mostrar, quando possível:

- reclamações observadas;
- reclamações esperadas;
- razão observadas/esperadas;
- meses comparáveis;
- intervalo/incerteza em linguagem expandível.

Explicação obrigatória:

> `esperadas` não significa número ideal de reclamações e não estima clientes insatisfeitos. É a referência proporcional calculada a partir do tamanho econômico da operação nos meses comparáveis.

### Persistência

Pergunta:

> **O sinal aparece repetidamente ou parece episódico?**

Quando a pressão anual está acima do esperado:

```text
Pressão recorrente
Pressão episódica ou esparsa
```

Persistência qualifica adversidade; ausência de persistência não gera bônus.

### Tendência

Pergunta:

> **A pressão está melhorando ou piorando?**

Quando o nível anual continua adverso:

```text
Sinal recente de piora
Sinal recente de melhora, mas pressão anual ainda acima do esperado
Sem mudança clara
```

Uma melhora recente não apaga a conclusão anual.

### Satisfação

Deve permanecer separada da incidência de reclamações.

Mostrar nota, amostra e direção apenas quando a amostra sustentar a leitura.

### Remediação

Enquanto o P3 atual não sustentar a inferência:

> **Não avaliamos a capacidade de resolver reclamações como um eixo próprio nesta versão.**

## 7. Qualificadores nunca alteram silenciosamente o estado central

A interface pode enriquecer o resumo com chips ou frases secundárias.

Exemplos:

```text
Pressão de Conduta recorrente
Pressão de Conduta episódica
Pressão recente piorando
Sinal recente de melhora
Pressão operacional persistente
Trajetória operacional em melhora
Histórico limitado
```

Regras:

- qualificadores explicam;
- não somam pontos;
- não apagam alertas;
- não criam ordem total;
- não transformam melhora recente em absolvição do nível anual adverso.

## 8. Modelo recomendado de tela pública

### Camada 1 — Leitura geral

Exemplo:

```text
ATENÇÃO À CONDUTA

Os indicadores financeiros centrais não mostram insuficiência atual,
mas há mais reclamações do que esperaríamos para o tamanho da operação.

[Pressão recorrente] [Sem mudança recente clara]
```

### Camada 2 — Por que chegamos a essa leitura

```text
Financeiro
✓ Capital mínimo requerido atendido
✓ ILT sem pressão pela referência aritmética
○ Operação equilibrada no histórico

Conduta
! Reclamações acima do esperado para o tamanho da operação
! Sinal persistente
○ Sem mudança recente suficientemente clara
```

Os símbolos são meramente ilustrativos; o frontend final pode definir outra linguagem visual.

### Camada 3 — Os números

```text
PLA/CMR       valor + competência
ILT           valor + competência
Reclamações   observadas / esperadas
Pressão       razão observadas/esperadas
Cobertura     meses comparáveis
Histórico     período utilizado
```

### Camada 4 — O que isso não significa

Uma caixa curta e específica ao estado, não um disclaimer genérico repetido.

### Camada 5 — Metodologia

Expansível para quem quiser auditar:

- fonte;
- competência/janela;
- fórmula;
- política de comparabilidade;
- incerteza;
- motivos de dado indisponível.

## 9. Resultado positivo sem overclaiming

A ferramenta deve ser capaz de reconhecer qualidades observadas.

São formulações aceitáveis:

- `O requisito de capital está atendido na competência analisada.`
- `O ILT não mostra pressão segundo a referência aritmética usada pela metodologia.`
- `Foram observadas menos reclamações do que o esperado para o tamanho da operação.`
- `A trajetória operacional aparece equilibrada no histórico disponível.`
- `Os três sinais centrais não apresentam alerta atual; a leitura é favorável dentro do escopo analisado.`

Não são aceitáveis apenas com esta metodologia:

- `A seguradora é segura.`
- `A seguradora é solvente.`
- `É uma boa seguradora.`
- `É a melhor seguradora.`
- `Tem ótimo atendimento.`
- `A SUSEP aprovou a liquidez.`
- `Não há risco para o cliente.`

A diferença é fundamental: a ferramenta pode afirmar **qualidades observadas dos indicadores**, mas não converter essas qualidades em uma garantia universal sobre a empresa.

## 10. Avaliação individual completa ≠ ranking de mercado

O preflight recomenda separar dois gates.

### `complete_joint_assessment_candidate`

Uma entidade pode receber avaliação conjunta completa quando:

- o núcleo Financeiro possui conclusão utilizável;
- a pressão de Conduta possui conclusão anual utilizável;
- a combinação corresponde a um dos seis estados da matriz;
- limitações de confiança e contexto continuam visíveis.

No snapshot atual isso corresponde às 85 entidades já identificadas pelo Stage 1.

Isso **não abre ainda `assessment_eligible`**; apenas define a semântica candidata para o gate futuro.

### `full_market_ranking_supported`

Continua `false`.

A população conjunta atual cobre aproximadamente:

```text
85 / 157 entidades
69,94% do prêmio direto positivo
54,40% das reclamações observadas
```

A exclusão é material e não aleatória. Portanto, comparar descritivamente as empresas avaliáveis pode ser útil, mas chamar essa subamostra de `ranking das seguradoras do mercado` seria enganoso.

## 11. Comparação pública permitida antes de ranking

A ferramenta pode permitir comparação lado a lado entre empresas com avaliação completa, desde que:

- mostre os estados, não uma posição ordinal inventada;
- preserve empates e incomparabilidades;
- exponha os números de cada pilar;
- informe a cobertura da população avaliável;
- não use `melhores`, `piores`, `top 10` ou posição `1º, 2º, 3º` enquanto o contrato de ranking não existir.

Exemplo de comparação legítima:

```text
Empresa A: leitura central favorável
Empresa B: atenção à Conduta
Empresa C: atenção à liquidez
```

A ferramenta pode explicar **em que elas diferem** sem afirmar uma ordem total quando os próprios contratos não a sustentam.

## 12. Disclosure de cobertura

Em toda superfície comparativa, mostrar perto do resultado:

> **Cobertura atual:** a avaliação conjunta está disponível para 85 das 157 seguradoras ordinárias do universo regulatório. Essa subamostra representa cerca de 69,9% do prêmio direto positivo e 54,4% das reclamações mapeadas na janela analisada. Empresas sem comparabilidade suficiente não recebem nota nem são tratadas como neutras.

Na página individual, uma versão curta é suficiente:

```text
Avaliação conjunta completa disponível
```

ou

```text
Avaliação conjunta incompleta — veja o que falta
```

com link para a metodologia/cobertura geral.

## 13. Guardrails semânticos

O contrato público deve impedir:

```text
sem alerta = garantia
abaixo do esperado = prova de ótimo atendimento
capital alto = mérito ilimitado
ILT alto = selo de liquidez
melhora recente = problema anual apagado
histórico curto = desempenho ruim
dado faltante = neutralidade
warning de capital = insolvência automática
matriz = ranking ordinal
comparação parcial = ranking de mercado
```

## 14. Conclusão do preflight

A matriz pode ser transformada em uma avaliação pública útil sem score.

A arquitetura pública recomendada é:

```text
matriz não compensatória
+ título semântico por natureza do sinal
+ explicação do significado
+ números auditáveis
+ qualificadores de persistência/tendência
+ limites específicos da conclusão
+ confiança/cobertura explícitas
```

O ganho para o consumidor é que a ferramenta deixa de responder apenas `quem está em primeiro` e passa a responder perguntas mais úteis:

- **há algum alerta central nesta seguradora?**
- **qual é a natureza do alerta?**
- **o problema é financeiro, de Conduta ou ambos?**
- **é recorrente ou episódico?**
- **está melhorando ou piorando?**
- **quais números sustentam essa leitura?**
- **o que ainda não sabemos?**

Essa é a função pública que os contratos atuais sustentam com maior fidelidade.

## 15. Gates que permanecem fechados

```text
financial_score
conduct_score
overall_score
pesos entre pilares
assessment_eligible
ranking_eligible
ranking_position
ranking público integral
```

Próximo passo técnico sugerido:

> transformar este preflight em um artifact de contrato semântico testável, validar as sete apresentações em 157 entidades e então decidir se o gate `assessment_eligible` pode ser formalmente aberto para a população com avaliação conjunta completa sem abrir `ranking_eligible`.
