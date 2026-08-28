# Contrato público de busca e perfil — v2

Status: **fechado para integração do frontend público**.

Este documento define o contrato que liga identidade, lifecycle, relationships, Sandbox/Conduta e avaliação v2 à página pública da Sanida. O frontend não deve mais usar `insurer_explorer.json` como se ele fosse um perfil completo da empresa.

## 1. Pergunta de produto

A busca pública deve conseguir responder, nesta ordem:

1. **Quem é o nome pesquisado?**
2. **É seguradora, marca, Sandbox, empresa histórica ou outra entidade?**
3. **Existe outra empresa por trás da marca ou do risco?**
4. **Há sucessão, grupo econômico ou outra relação documentada relevante?**
5. **Que sinais atuais podem ser afirmados com segurança?**
6. **Que parte ainda não pode ser comparada e por quê?**
7. **Quais números e regras sustentam a resposta, se o leitor quiser aprofundar?**

A progressão pública obrigatória é:

```text
resposta rápida
→ sinais em português comum
→ identidade e relações relevantes
→ números técnicos
→ limites metodológicos
```

PLA/CMR, ILT, observed/expected e códigos internos não são a porta de entrada do perfil.

## 2. Fontes combinadas

`api/v2/build_public_search_profile_contract.py` combina, sem recalcular metodologia:

```text
v2_lifecycle_relationship_inventory
+
v2_public_insurer_explorer
+
v2_sandbox_brand_conduct_evidence
+
data/reference/v2/conduct_subject_relationships.json
```

Papéis:

- **lifecycle/relationships**: identidade, CNPJ, FIP, classificação, regime, status, lifecycle jurídico, sucessão, grupos econômicos, marcas e aliases;
- **insurer explorer**: avaliação semântica, Financeiro, Conduta e contexto econômico das seguradoras ordinárias já cobertas pelo contrato de assessment;
- **Sandbox Conduct**: evidência de Conduta de participantes Sandbox e contexto de marcas verificadas, como Loovi ↔ LTI;
- **Conduct subject relationships**: relações especiais entre o sujeito que recebe reclamações e carriers/portfólios, como Youse ↔ Caixa, Zurich e Bradesco.

`insurer_explorer.json` permanece útil para comparação e exploração, mas não é mais o contrato completo de identidade/perfil.

## 3. Escopo real da busca

A busca é deliberadamente mais ampla do que a avaliação ordinária.

No build real que fechou este contrato em 28/08/2026:

```text
entidades lifecycle                              492
marcas verificadas                                13
perfis públicos                                  505
entradas do índice de busca                      505
seguradoras ordinárias atuais no lifecycle       159
com payload de assessment atual                  157
ordinárias atuais ainda sem payload de assessment  2
participantes Sandbox                             12
Sandbox com contexto de Conduta                   12
```

Essas quantidades são **snapshot**, não constantes metodológicas. O gate correto é relacional: toda entidade lifecycle e toda marca verificada devem gerar perfil e entrada de busca. Uma nova seguradora pode aparecer no universo regulatório antes de acumular ou receber o payload de avaliação; nesse caso ela permanece pesquisável e o frontend não inventa avaliação.

### 3.1. Fuzzy matching

O frontend pode usar busca aproximada somente para ordenar candidatos:

```text
fuzzy search → localizar candidatos
fuzzy search ≠ decidir identidade
```

A abertura de um perfil deve corresponder a uma entrada determinística do índice público.

## 4. Tipos de perfil

### Seguradora ordinária atual

Pode receber identidade, situação regulatória, lifecycle, grupo econômico observado, relações corporativas verificadas, marcas relacionadas, relações especiais de Conduta e avaliação v2 quando o payload existir. Se a entidade for atual mas ainda estiver fora do snapshot de assessment, a busca continua funcionando sem imputar sinais.

### Participante Sandbox

Recebe identidade/regime, explicação do Sandbox e evidência de Conduta quando disponível. Não recebe avaliação ou ranking ordinário e não recebe pressão proporcional inventada.

### Marca

Marca é objeto de resolução, não pessoa jurídica que herda avaliação. O perfil informa aliases, relação (`brand_of`, `risk_carrier` etc.), entidade alvo, escopo e evidência.

```text
brand_inherits_entity_assessment = false
```

### Entidade histórica

A identidade histórica é preservada e, quando documentado, o perfil aponta a cadeia de sucessores. Avaliação atual não é transferida retroativamente.

### Previdência, capitalização, resseguro, regime especial e outros

Permanecem pesquisáveis para evitar confusão de nomes, mas ficam fora da comparação ordinária quando o contrato assim determina.

## 5. Semântica obrigatória de ausência

Esta regra é contratual:

```text
null ≠ 0
```

Métricas públicas relevantes usam envelope explícito:

```json
{
  "value": null,
  "availability": "unavailable",
  "public_use": "displayable",
  "zero_semantics": null,
  "meaning": "..."
}
```

Um zero realmente observado permanece zero e recebe semântica própria. O frontend não precisa — e não pode — inferir a diferença.

### Prêmio direto e tamanho da operação

Prêmio direto é contexto de volume econômico, nunca qualidade. Quando não existe relação 1:1 segura entre sujeito de reclamações e exposição, um zero bruto não pode virar “operação R$ 0”.

Exemplo Youse:

```text
value = 0.0
public_use = do_not_render_as_operation_size
zero_semantics = literal_source_zero_must_not_be_presented_as_zero_sized_business
```

## 6. Caso Youse ↔ Caixa

O perfil público preserva simultaneamente:

- 1.367 reclamações observadas no snapshot atual;
- relação documentada Youse → Caixa Seguradora como risk carrier no contexto de Conduta;
- `expected_complaints = null`;
- `pressure_ratio = null`;
- `comparable_months = null`;
- prêmio bruto zero marcado como **não exibível como tamanho da operação**.

Não dividimos as reclamações da Youse pela produção total da Caixa e não chamamos o prêmio próprio zero de “operação zero”.

## 7. Caso Loovi ↔ LTI

O contrato preserva identidades distintas:

```text
brand:loovi
entity:cnpj:47006254000180 → LTI Seguros S.A.
```

A relação verificada é `risk_carrier`. O contexto de Sandbox Conduct preserva **1.329 reclamações** contra a LTI na janela atual. A linguagem pública deve atribuir a evidência à LTI e explicar sua relação com a Loovi; não afirmar automaticamente “a Loovi teve 1.329 reclamações”.

A marca não herda assessment da entidade.

## 8. Caso HDI Seguros × HDI Global

O contrato mantém HDI Seguros S.A. e HDI Global Seguros S.A. como perfis regulatórios distintos, com CNPJs/FIPs distintos. O snapshot SUSEP os coloca no mesmo grupo econômico `TALANX AG`.

```text
same_group ≠ succession
same_group ≠ acquisition
same_group ≠ joint_venture
```

O grupo serve para contexto e desambiguação, não para fundir identidades.

## 9. Linguagem pública antes do segurês

### Capital

Antes de `PLA/CMR`, o perfil fornece frases como:

- “Na competência analisada, o patrimônio ajustado alcança o capital mínimo exigido.”
- “Na competência analisada, o patrimônio ajustado ficou abaixo do capital mínimo exigido.”
- “Não há dado utilizável suficiente para concluir a situação de capital.”

`PLA/CMR` fica na camada técnica.

### Liquidez

Antes de `ILT`, o perfil explica se o indicador mostrou ou não pressão segundo a referência aritmética usada pela metodologia e deixa explícito que 1,0 não é limite prudencial oficial da SUSEP.

### Conduta

O contrato reutiliza a semântica fechada: acima do esperado com evidência, abaixo do esperado com evidência, sem diferença clara, sensibilidade ao denominador, cobertura temporal insuficiente e não comparável.

Zero reclamações observadas não vira automaticamente sinal favorável. Responder ou finalizar reclamação não prova solução.

## 10. Guardrails de relationships

O perfil pode carregar sucessão, grupo observado, marcas, `risk_carrier` e relações especiais de sujeito/carrier/portfólio.

```text
brand ≠ legal_entity
same_group ≠ succession
risk_carrier_relation ≠ complaint_transfer
fuzzy_match ≠ identity_decision
```

## 11. Arquivos públicos

O builder publica:

```text
data/derived/v2/public/search_index.json
data/derived/v2/public/profile_manifest.json
data/derived/v2/public/profiles/*.json
```

`search_index.json` é o catálogo de busca/desambiguação. `profile_manifest.json` relaciona `profile_id` ao arquivo. Cada JSON em `profiles/` contém uma entidade ou marca preparada para apresentação progressiva.

Leaderboards e coleções continuam independentes no contrato exploratório existente.

## 12. Papel do frontend

O frontend pode pesquisar, ordenar candidatos por relevância textual, abrir perfis, formatar valores disponíveis e navegar pelas relações materializadas.

O frontend não pode:

- converter `null` em `0`;
- decidir identidade por fuzzy matching;
- transferir reclamações;
- calcular pressão;
- reconstruir PLA/CMR ou ILT;
- recalcular assessment;
- criar score ou ranking geral;
- inferir sucessão, grupo, joint venture ou risk carrier.

## 13. Validação de fechamento

Workflow:

`V2 Public Search Profile Contract`

Run de fechamento:

```text
run                                    33147565359
Ruff                                   verde
testes direcionados                    4/4
build real                              verde
validação real de fronteiras            verde
perfis públicos gerados                  505
arquivos no artifact                     508
artifact id                       9676392800
```

A validação real confirmou especificamente:

```text
Youse complaints                  1367
Youse expected                    null
Youse operation public_use        do_not_render_as_operation_size
Loovi/LTI complaints              1329
HDI economic group                TALANX AG
```

Com esses gates verdes, o próximo estágio de produto é:

```text
frontend_php_integration_against_closed_public_search_profile_contract
```
