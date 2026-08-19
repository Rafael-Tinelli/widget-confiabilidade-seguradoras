#!/usr/bin/env bash
set -euo pipefail

python - <<'PY'
from pathlib import Path
import re

path = Path('README.md')
text = path.read_text(encoding='utf-8')

old_status = '''> **Status do projeto:** refatoração metodológica e arquitetural em andamento.  
> Este README funciona, nesta fase, como **contrato de projeto, guia de implementação e registro das decisões já tomadas**.  
> Regras marcadas como **EM VALIDAÇÃO** não devem ser tratadas como metodologia definitiva nem incorporadas ao scoring sem os testes previstos neste documento.'''
new_status = '''> **Status do projeto:** refatoração metodológica e arquitetural em andamento.  
> **Marco atual (2026-08-19):** a arquitetura conceitual do pilar econômico-financeiro foi fechada; a próxima investigação metodológica é o pilar de conduta com o consumidor, sinistros e reclamações.  
> Este README funciona, nesta fase, como **contrato de projeto, guia de implementação e registro das decisões já tomadas**.  
> Regras marcadas como **EM VALIDAÇÃO** ou **EM CALIBRAÇÃO** não devem ser tratadas como metodologia definitiva nem incorporadas ao scoring sem os testes previstos neste documento.'''
if old_status not in text:
    raise SystemExit('status block not found')
text = text.replace(old_status, new_status, 1)

sections_9_14 = r'''## 9. Pilares da avaliação geral

A v2 passa a organizar a confiabilidade institucional da seguradora em dois pilares de natureza diferente:

```text
1. capacidade econômico-financeira
2. conduta com o consumidor, sinistros e reclamações
```

O primeiro pilar atingiu um **marco de arquitetura em 2026-08-19**: encerrou-se a procura por novos indicadores financeiros que pudessem virar eixos independentes de pontuação.

Isso não significa que os pesos e transformações matemáticas estejam fechados. Significa que a pergunta conceitual — **o que o pilar financeiro deve observar?** — está suficientemente delimitada para que o projeto avance sem continuar procurando novos índices até encontrar uma combinação conveniente.

A escolha do peso entre os dois pilares gerais permanece **EM VALIDAÇÃO**. A antiga hipótese 60/40 não deve funcionar como âncora metodológica: o peso só será definido depois que o segundo pilar possuir dados, semântica, cobertura e estabilidade suficientemente compreendidos.

---

## 10. Pilar econômico-financeiro — arquitetura conceitual fechada

Este pilar procura responder:

> **A seguradora apresenta sinais financeiros e operacionais compatíveis com a capacidade de honrar seus compromissos e sustentar sua atividade ao longo do tempo?**

A pergunta inclui obrigações perante segurados e outros terceiros, relações comerciais que dependem da continuidade da empresa e a própria capacidade de permanecer economicamente viável. Uma seguradora também precisa gerar resultado suficiente para preservar sua continuidade e capacidade futura.

O pilar não pretende prever o futuro nem certificar que determinada obrigação será paga. Ele reúne sinais públicos, atuais e históricos, para avaliar **capacidade, resiliência e sustentabilidade**, com limites explícitos.

### 10.1. Arquitetura aprovada

| Dimensão | Referência principal | Papel metodológico | Decisão |
|---|---|---|---|
| Capital regulatório | PLA/CMR | capacidade prudencial de absorção | eixo quantitativo principal |
| Liquidez | ILT | capacidade de honrar obrigações com recursos compatíveis | eixo quantitativo principal |
| Liquidez de curto prazo | ILC | diagnóstico complementar | não recebe eixo próprio de pontuação |
| Filme operacional | ICA, com IC e componentes explicativos | trajetória de equilíbrio/pressão da operação | estado longitudinal e explicação; não é um terceiro score bruto |
| Rentabilidade | ILPL | capacidade de geração de resultado sobre o patrimônio | diagnóstico complementar; rejeitado como eixo independente de scoring |

A arquitetura financeira, portanto, não será expandida com novos indicadores apenas porque eles existam na base.

### 10.2. O que ainda está em calibração

Permanecem abertos:

- transformação de PLA/CMR em avaliação limitada e interpretável;
- transformação de ILT com saturação de extremos;
- pesos relativos entre capital e liquidez;
- forma pela qual o filme operacional influencia cautela, explicação e/ou confiança da avaliação financeira sem virar, por acidente, um terceiro score redundante;
- tratamento final de histórico curto e evidência insuficiente;
- faixas verbais e `reason_codes` definitivos.

Esses pontos são **EM CALIBRAÇÃO**. Não reabrem a seleção de novos eixos financeiros.

---

## 11. Capital regulatório — PLA/CMR

A relação:

```text
PLA / CMR
```

permanece como a principal referência para a dimensão de capital regulatório.

### 11.1. Decisões aprovadas

- o indicador não será tratado linearmente;
- magnitude maior não significa benefício proporcionalmente maior;
- CMR zero ou evidência inutilizável não é convertido automaticamente em desempenho ruim;
- histórico curto é limitação de evidência, não penalidade de desempenho;
- a leitura deve respeitar saturação, estabilidade e contexto prudencial;
- porte absoluto não gera pontos por si só.

Não é aceitável inferir:

```text
PLA/CMR 2,0 = duas vezes melhor que PLA/CMR 1,0
```

### 11.2. EM CALIBRAÇÃO

Ainda devem ser fechados:

- função de transformação;
- zonas semânticas;
- peso relativo dentro do pilar financeiro;
- interação com histórico e confiança da evidência.

---

## 12. Liquidez — ILT como referência principal

A investigação de liquidez separou dois conceitos relacionados, mas não idênticos:

- **ILT — Índice de Liquidez Total:** referência principal da dimensão de liquidez;
- **ILC — Índice de Liquidez Corrente:** sinal complementar de curto prazo.

### 12.1. Decisões aprovadas

- ILT e ILC não receberão pesos independentes que contem liquidez duas vezes;
- ILT permanece como o principal candidato de pontuação da dimensão;
- ILC permanece diagnóstico explicativo;
- valores extremos de ILT não podem receber recompensa linear;
- a referência aritmética `1,0` pode ajudar a explicar a relação entre recursos e obrigações, mas não deve ser apresentada como selo ou corte regulatório da SUSEP;
- segmentação prudencial pode fornecer contexto de comparabilidade, mas não resolve sozinha os efeitos de denominadores pequenos nem vira bônus de porte.

### 12.2. EM CALIBRAÇÃO

Ainda devem ser fechados:

- saturação da magnitude;
- zonas semânticas;
- peso relativo frente ao capital;
- regra final de histórico/confiança.

---

## 13. Filme operacional — ICA, IC e componentes

A dimensão operacional não será tratada como uma fotografia mensal isolada nem como uma soma de vários índices que contem a mesma operação repetidamente.

O objetivo é observar **trajetória**:

```text
operação equilibrada de forma persistente
melhora
pressão recente
pressão persistente
histórico insuficiente
```

### 13.1. Papel das métricas

- **ICA** funciona como referência principal do estado operacional porque incorpora o efeito do resultado financeiro na capacidade de sustentar os custos da operação;
- **IC** ajuda a mostrar como a operação se comporta antes desse efeito financeiro;
- **ISR, IDC, IORDO, IRRES e IDA** explicam a composição do resultado e não devem virar cinco novos pilares de pontos;
- comparações temporais devem respeitar horizontes equivalentes, pois contas de resultado acumuladas no exercício não podem ser comparadas ingenuamente entre meses de maturidade diferente.

O filme operacional integra a leitura do pilar financeiro, mas **não é um terceiro score bruto independente** nesta arquitetura.

### 13.2. Investigação fechada do ILPL

O ILPL foi submetido a uma única investigação fechada, com critérios de sobrevivência definidos e registrados antes da primeira execução contra a BaseCompleta real.

Os gates exigiam, entre outros pontos:

- cobertura corrente mínima de 90%;
- cobertura pareada mínima de 75%;
- estabilidade mediana de ordenação de pelo menos `0,70` em meses equivalentes;
- estabilidade mediana de pelo menos `0,70` em fechamentos anuais;
- persistência de sinal lucro/prejuízo mínima de 70%;
- baixa associação com porte patrimonial;
- baixa redundância com PLA/CMR e ILT.

Resultado observado na competência madura `2026-05`:

```text
cobertura corrente                    94,27%   PASS
cobertura pareada mai/26 × mai/25     88,54%   PASS
estabilidade em meses equivalentes     0,581   FAIL
estabilidade em fechamentos anuais     0,748   PASS
persistência de sinal                 82,73%   PASS
|rho| ILPL × patrimônio médio          0,118   PASS
|rho| máximo com PLA/CMR ou ILT        0,343   PASS
```

O único gate reprovado foi justamente a estabilidade longitudinal da ordenação em meses equivalentes.

Decisão:

> **ILPL não sobrevive como componente independente de scoring e não receberá iteração de resgate pós-resultado.**

Ele pode permanecer como diagnóstico explicativo de geração de resultado, mas sua magnitude não terá um eixo próprio de pontos.

Essa decisão encerra a procura por novos componentes financeiros independentes nesta etapa.

---

## 14. Conduta com o consumidor, sinistros e reclamações — próximo pilar

O próximo pilar deve responder a uma pergunta diferente da financeira:

> **Como a seguradora se comporta diante do consumidor quando vende, administra e efetivamente precisa cumprir a proteção contratada?**

O interesse não é apenas saber se a empresa responde rápido a uma reclamação. É procurar sinais consistentes de **justiça, qualidade de conduta, cumprimento contratual, tratamento do sinistro e capacidade de aprender com problemas recorrentes**.

Reclamações são rastros observáveis desse comportamento. Elas podem revelar padrões relacionados, por exemplo, a:

- divergências de cobertura;
- negativas de sinistro;
- liquidação e regulação de sinistros;
- cancelamentos;
- venda, informação e adequação do produto;
- cobrança;
- demora operacional;
- descumprimento ou controvérsia contratual;
- qualidade da resposta e da solução oferecida ao consumidor.

A taxonomia final dependerá do que as fontes realmente permitirem distinguir de forma sustentável.

### 14.1. Reclamação não é prova automática de abuso

Uma reclamação individual registra uma insatisfação ou alegação. Sozinha, não prova que a empresa agiu de forma abusiva, que um produto foi deliberadamente desenhado para negar cobertura ou que determinada liquidação de sinistro foi incorreta.

A metodologia deve procurar **padrões**, não sentenças improvisadas.

Por isso, são proibidas inferências do tipo:

```text
houve reclamação → houve abuso
houve negativa → produto é ruim
respondeu rápido → conduta é boa
```

Quando a própria fonte trouxer classificação, procedência, resultado, decisão regulatória ou outro estado verificável, essa informação poderá ter significado diferente de uma alegação sem desfecho.

### 14.2. O pilar deve medir o filme, não a foto

O comportamento ao longo do tempo é central.

Uma empresa que apresenta um problema, identifica a causa, corrige produto ou conduta e reduz de forma sustentada a recorrência do problema deve ser distinguida de uma empresa que apenas responde reclamações individualmente enquanto o mesmo padrão continua reaparecendo.

Assim, a futura metodologia deverá estudar pelo menos quatro dimensões:

```text
incidência normalizada de problemas
natureza e gravidade dos temas
resposta / resolução dos casos
recorrência e adaptação ao longo do tempo
```

A **adaptação estrutural** pode ser tão ou mais informativa que a velocidade de resposta. Tempo de resposta, isoladamente, não será tratado como sinônimo de boa conduta.

### 14.3. Sinais candidatos — ainda sem pontuação

A investigação deverá verificar se as fontes permitem construir, sem forçar os dados:

- frequência de reclamações normalizada por uma medida defensável de exposição;
- concentração em temas materialmente ligados à proteção contratada e ao sinistro;
- proporção de casos resolvidos ou com desfecho favorável, quando a fonte oferecer esse conceito de forma comparável;
- satisfação posterior, quando houver amostra e semântica suficientes;
- reincidência do mesmo tipo de problema;
- tendência de melhora, estabilidade ou deterioração;
- persistência de problemas apesar das respostas individuais;
- divergência ou convergência entre diferentes canais;
- qualidade e velocidade da resposta, sem supervalorizar rapidez formal.

Estados longitudinais candidatos poderão assumir linguagem semelhante a:

```text
melhora sustentada
conduta estável sem pressão relevante
pressão recente
pressão persistente
deterioração
histórico insuficiente
```

Esses nomes são conceituais e ainda não constituem `reason_codes` definitivos.

### 14.4. Múltiplos canais exigem deduplicação e identidade

O projeto poderá estudar diversos canais, mas não deve simplesmente somar reclamações de fontes diferentes.

Antes de qualquer agregação será necessário resolver:

- identidade jurídica da empresa reclamada;
- diferenças de cobertura entre canais;
- duplicidade provável do mesmo caso;
- períodos comparáveis;
- taxonomias incompatíveis;
- diferenças entre reclamação, consulta, denúncia e processo;
- denominadores disponíveis;
- viés de seleção de cada plataforma.

Fontes regulatórias e públicas estruturadas têm prioridade. Outros canais só poderão influenciar a metodologia se forem sustentáveis, automatizáveis, auditáveis e semanticamente compatíveis.

### 14.5. Primeira investigação do novo pilar

A próxima etapa não é criar um score de reclamações.

É construir um **inventário fechado das fontes e da semântica disponível**, respondendo:

1. quais canais possuem dados estruturados e historicamente recuperáveis;
2. qual entidade jurídica cada registro representa;
3. quais temas de reclamação são distinguíveis;
4. quais desfechos são realmente observáveis;
5. qual denominador permite comparação justa;
6. qual histórico permite detectar recorrência e adaptação;
7. onde diferentes fontes se complementam e onde apenas duplicam o mesmo fenômeno;
8. quais sinais podem ser explicados ao consumidor sem extrapolar a evidência.

Somente depois desse inventário serão definidos critérios de sobrevivência para os indicadores candidatos do segundo pilar.

---

'''
pattern = re.compile(r'## 9\. Pilares candidatos da avaliação geral\n.*?(?=## 15\. Confiança da avaliação)', re.S)
text, count = pattern.subn(sections_9_14, text, count=1)
if count != 1:
    raise SystemExit(f'sections 9-14 replacement count={count}')

old_operations = '''          "operations": {
            "score": 84.0,
            "period": "2026-05"
          }
'''
new_operations = '''          "operations": {
            "score": null,
            "signal": "balanced_persistent",
            "assessment_role": "longitudinal_context",
            "period": "2026-05"
          },

          "profitability": {
            "score": null,
            "metric": "ILPL",
            "assessment_role": "diagnostic_only",
            "period": "2026-05"
          }
'''
if old_operations not in text:
    raise SystemExit('illustrative operations schema block not found')
text = text.replace(old_operations, new_operations, 1)

phase4 = r'''### Fase 4 — Matriz quantitativa

- [x] delimitar o universo regulatório elegível;
- [x] investigar PLA/CMR como referência de capital;
- [x] investigar ILC/ILT e selecionar ILT como referência principal de liquidez;
- [x] investigar efeito de porte/segmentação e extremos de denominador;
- [x] investigar IC/ICA e fechar o papel de filme operacional;
- [x] executar investigação fechada do ILPL e rejeitá-lo como eixo independente de scoring;
- [x] fechar a arquitetura conceitual do pilar econômico-financeiro;
- [ ] calibrar transformação de PLA/CMR;
- [ ] calibrar transformação de ILT;
- [ ] definir interação do filme operacional com avaliação/confiança sem criar terceiro score bruto;
- [ ] inventariar fontes do pilar de conduta com o consumidor;
- [ ] mapear identidade, taxonomia, desfechos, denominadores e histórico de reclamações;
- [ ] definir critérios de sobrevivência dos indicadores candidatos de conduta;
- [ ] testar amostras mínimas, outliers e estabilidade temporal do segundo pilar.

**Marco:** a seleção de dimensões do pilar financeiro está encerrada. A investigação quantitativa passa agora ao comportamento da seguradora perante o consumidor.

**Somente depois:** definir fórmulas e pesos definitivos da avaliação geral.

'''
pattern = re.compile(r'### Fase 4 — Matriz quantitativa\n.*?(?=### Fase 5 — Pesos e score)', re.S)
text, count = pattern.subn(phase4, text, count=1)
if count != 1:
    raise SystemExit(f'phase 4 replacement count={count}')

section46 = r'''## 46. Questões metodológicas ainda abertas

Até que sejam testadas, **não considerar como regras definitivas**:

### Pilar econômico-financeiro — arquitetura fechada, calibração aberta

- transformação de PLA/CMR;
- transformação/saturação de ILT;
- peso Capital × Liquidez;
- forma de incorporar o filme operacional à conclusão e/ou confiança sem convertê-lo em terceiro score bruto;
- tratamento final de histórico financeiro curto;
- `reason_codes` financeiros definitivos.

A seleção de novos indicadores financeiros independentes **não está mais aberta** nesta etapa. ILPL foi rejeitado como eixo de scoring pela investigação pré-registrada e não receberá iteração de resgate.

### Pilar de conduta com o consumidor — investigação aberta

- fonte ou conjunto de fontes definitivo;
- identidade jurídica entre canais;
- deduplicação entre reclamações provenientes de fontes diferentes;
- taxonomia de temas e gravidade;
- distinção entre alegação, procedência, resolução e outros desfechos;
- denominador para normalização;
- amostra mínima;
- janelas temporais comparáveis;
- forma de medir recorrência;
- forma de medir adaptação/melhora sustentada;
- papel da velocidade e qualidade de resposta;
- comparabilidade entre empresas com perfis de carteira diferentes;
- critérios de sobrevivência dos sinais candidatos.

### Avaliação geral

- peso Financeiro × Conduta;
- coortes finais necessárias;
- cálculo de `assessment_confidence`;
- faixas de rating;
- threshold de elegibilidade;
- `reason_codes` gerais;
- quais dados de ramos devem ser resumidos publicamente;
- formato final dos quatro JSONs;
- eventual necessidade de fragmentação por desempenho.

Esses pontos deverão ser resolvidos por análise de dados e testes, não por preferência estética nem por busca posterior de uma fórmula que produza um ranking desejado.

---

'''
pattern = re.compile(r'## 46\. Questões metodológicas ainda abertas\n.*?(?=## 47\. Diretriz final desta etapa)', re.S)
text, count = pattern.subn(section46, text, count=1)
if count != 1:
    raise SystemExit(f'section 46 replacement count={count}')

path.write_text(text, encoding='utf-8')
PY
