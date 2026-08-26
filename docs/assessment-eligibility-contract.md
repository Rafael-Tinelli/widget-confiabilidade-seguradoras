# Assessment Eligibility Contract — v2

Status: **fechado e validado; `assessment_eligible` está formalmente aberto para 85 seguradoras. `ranking_eligible` permanece fechado.**

Este contrato sucede `docs/cross-pillar-assessment-semantic-contract.md`.

O gate é **fail-closed**: divergência de população, contrato semântico não fechado ou confiança central insuficiente impede a abertura da avaliação em vez de produzir fallback.

A pergunta é:

> **Temos base regulatória, comparabilidade e evidência suficientes para publicar uma avaliação conjunta desta seguradora?**

A resposta não depende de a seguradora apresentar uma leitura favorável, atenção ou alerta. Elegibilidade mede a possibilidade de **avaliar**, não a qualidade do resultado.

## 1. Separação dos gates

A v2 passa a distinguir explicitamente:

```text
regulatory_universe_eligible
        ↓
semantic_public_assessment_supported
        ↓
assessment_eligible
        ↓
ranking_eligible
```

Os significados são diferentes:

- `regulatory_universe_eligible`: a entidade é uma seguradora ordinária atual apta a seguir no pipeline;
- `semantic_public_assessment_supported`: os pilares fechados sustentam uma leitura conjunta semanticamente segura;
- `assessment_eligible`: o contrato formal autoriza publicar a avaliação conjunta;
- `ranking_eligible`: eventual autorização posterior para participar de uma comparação ordenada/coorte de ranking.

Nenhum gate posterior é herdado automaticamente do anterior.

## 2. Regra de elegibilidade para avaliação

Uma entidade só pode receber:

```text
assessment_eligible = true
```

quando satisfaz simultaneamente:

1. pertence ao universo regulatório atual;
2. possui `joint_core_complete`;
3. possui suporte semântico público fechado;
4. possui núcleo Financeiro utilizável;
5. possui conclusão de Conduta comparável e suficientemente sustentada;
6. a confiança central não é insuficiente ou não classificada;
7. os artifacts regulatório e semântico usados são os últimos validados com sucesso na mesma branch.

Não existe requisito de “bom desempenho”.

## 3. Resultado adverso não bloqueia avaliação

Regra fundamental:

```text
favorable_reading
attention
prudential_warning
```

podem ser igualmente `assessment_eligible`.

Excluir uma seguradora porque ela apresenta um alerta enviesaria o produto: o gate passaria a esconder justamente as avaliações mais importantes ao consumidor.

Assim:

```text
assessment_eligible != selo de qualidade
assessment_eligible != recomendação
assessment_eligible != ausência de alerta
```

`assessment_eligible` significa apenas:

> **há evidência suficiente e comparável para publicar a avaliação definida pela metodologia.**

## 4. Histórico limitado

O contrato preserva a separação entre desempenho e confiança.

Estados aceitos:

```text
historico_estabelecido
historico_limitado
```

`historico_limitado`:

- não reduz desempenho;
- não cria penalidade;
- não bloqueia automaticamente `assessment_eligible`;
- deve ser exibido como disclosure de confiança.

Bloqueiam o gate:

```text
evidencia_central_insuficiente
confianca_nao_classificada
```

Isso não é uma nota ruim. Significa que o núcleo necessário para a avaliação ainda não possui base suficiente.

No snapshot atual, as 85 entidades semanticamente completas possuem `historico_estabelecido`. A regra para histórico limitado existe para que novas seguradoras não sejam punidas por idade quando o núcleo atual for utilizável.

## 5. Evidência incompleta permanece fora do gate

Os estados sem conclusão conjunta não recebem neutralidade nem avaliação parcial disfarçada de completa.

Rotas atuais incluem:

```text
Conduta não comparável com segurança
cobertura temporal insuficiente
conclusão sensível ao denominador
Financeiro central incompleto
```

Essas entidades continuam pesquisáveis e seus sinais disponíveis continuam visíveis conforme o contrato semântico.

Mas:

```text
joint_core_incomplete
→ assessment_eligible = false
```

até que a rota de recuperação correspondente seja resolvida.

## 6. Atualidade

O contrato não inventa um prazo arbitrário como “dados com menos de 30 dias”.

A política operacional é:

> o workflow do gate restaura os artifacts **mais recentes com execução bem-sucedida na mesma branch** para o universo regulatório e para o contrato semântico.

A competência e a janela metodológica continuam definidas pelos próprios contratos dos pilares.

Atualidade é requisito de proveniência do pipeline; não é desempenho e não gera pontos.

## 7. Coorte e ranking continuam separados

O gate de avaliação não define `comparison_cohort`.

Para as entidades elegíveis:

```text
ranking_state = pending_ranking_eligibility_contract
ranking_eligible = false
comparison_cohort = null
```

Para as demais:

```text
ranking_state = blocked_by_assessment_ineligibility
ranking_eligible = false
```

Abrir avaliação não resolve os 222 trade-offs normativos nem a cobertura insuficiente para uma alegação de ranking integral do mercado.

## 8. Snapshot validado

Na execução real do contrato:

```text
universo regulatório                         157
suporte semântico para avaliação              85
assessment_eligible                           85
assessment_not_eligible                       72
ranking_eligible                               0
```

Entre as 85 elegíveis:

```text
Leitura central favorável   46
Atenção                     30
Alerta prudencial            9
```

A presença das 9 avaliações com alerta prudencial dentro do gate é intencional e funciona como teste de independência entre **elegibilidade** e **resultado**.

## 9. Guardrails

O builder e os testes devem impedir:

- entidade fora do universo regulatório no gate;
- divergência de população entre universo regulatório e contrato semântico;
- avaliação completa sem estado semântico;
- `evidence_incomplete` tornando-se elegível;
- resultado favorável sendo requisito de elegibilidade;
- alerta prudencial bloqueando avaliação;
- histórico limitado virando penalidade;
- missingness virando neutralidade;
- score sendo criado;
- posição de ranking sendo criada;
- `ranking_eligible = true`;
- seleção silenciosa de coorte de ranking.

## 10. Implementação

Arquivos:

```text
api/v2/build_assessment_eligibility_contract.py
tests/test_v2_assessment_eligibility_contract.py
.github/workflows/v2-assessment-eligibility-contract.yml
```

Artifact:

```text
data/derived/v2/assessment_eligibility_contract.json
```

Workflow:

```text
V2 Assessment Eligibility Contract
```

## 10.1. Precedência dos artifacts de elegibilidade

`data/derived/v2/entity_eligibility_inventory.json` continua sendo o artifact **upstream do gate regulatório**. Ele deliberadamente não consome evidência Financeira/Conduta e, por isso, seus campos de avaliação representam o estado pendente naquela etapa do pipeline.

Após o fechamento deste contrato, a fonte autoritativa para a elegibilidade de **avaliação** passa a ser:

```text
data/derived/v2/assessment_eligibility_contract.json
```

Assim:

```text
entity_eligibility_inventory
→ responde ao gate regulatório

assessment_eligibility_contract
→ responde ao gate formal de avaliação
```

Não há conflito metodológico entre as camadas; há progressão de gates. O frontend/API final deverá consumir o estado downstream apropriado, e não inferir a elegibilidade final apenas do inventário regulatório.

## 11. Fechamento validado

Execução oficial:

```text
V2 Assessment Eligibility Contract
run                     33025120193
job                     98364518971
Ruff                    verde
testes                  6/6
build real              verde
boundaries              verdes
artifact                9628104960
SHA256 ZIP              7997da1c36999a99fdd6380f685b04c66cfb830737fcf4a12e678558db4efc12
```

O contrato fechou como:

```text
status = assessment_eligibility_contract_closed
assessment_eligibility_gate_opened = true
assessment_eligible = 85

ranking_eligibility_gate_opened = false
ranking_eligible = 0
```

O próximo passo metodológico é um **preflight de elegibilidade para ranking**, que deverá tratar coorte, representatividade, natureza da alegação pública e os trade-offs que a matriz deliberadamente não totaliza. A abertura do gate de avaliação não autoriza ranking integral ou parcial por si só.
