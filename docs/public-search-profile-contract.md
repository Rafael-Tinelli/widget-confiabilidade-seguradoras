# Contrato público de busca e perfil — v2

Status: **fechado para integração do frontend público**.

Este documento define o contrato que liga a fundação de identidade, lifecycle, relações,
Sandbox/Conduta e avaliação v2 à página pública da Sanida.

O objetivo é impedir que o frontend precise reconstruir decisões que pertencem ao backend
e evitar a repetição dos erros observados nos protótipos de interface: ausência convertida
em zero, marca confundida com pessoa jurídica, relações empresariais omitidas e jargão
técnico exibido antes da resposta humana.

## 1. Pergunta de produto

A busca pública deve conseguir responder, nesta ordem:

1. **Quem é o nome pesquisado?**
2. **É seguradora, marca, Sandbox, empresa histórica ou outra entidade?**
3. **Existe outra empresa por trás da marca ou do risco?**
4. **Há sucessão, grupo econômico ou outra relação documentada que ajude a interpretar o caso?**
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

## 2. Fontes do contrato

O builder `api/v2/build_public_search_profile_contract.py` combina quatro camadas já
existentes, sem recalcular nenhuma delas:

```text
v2_lifecycle_relationship_inventory
+
v2_public_insurer_explorer
+
v2_sandbox_brand_conduct_evidence
+
data/reference/v2/conduct_subject_relationships.json
```

Funções:

- **lifecycle/relationships**: identidade, CNPJ, FIP, classificação, regime, status,
  lifecycle jurídico, sucessão, grupos econômicos, marcas e aliases;
- **insurer explorer**: avaliação semântica, Financeiro, Conduta e contexto econômico
  das seguradoras ordinárias atuais;
- **Sandbox Conduct**: evidência de Conduta de participantes Sandbox e contexto de
  marcas verificadas, como Loovi ↔ LTI;
- **Conduct subject relationships**: relações especiais entre o sujeito que recebe
  reclamações e carriers/portfólios, como Youse ↔ Caixa, Zurich e Bradesco.

O `insurer_explorer.json` continua válido para comparação e exploração das 157
seguradoras ordinárias, mas **não é mais o contrato completo de identidade/perfil**.

## 3. Escopo da busca

A busca é deliberadamente mais ampla do que a avaliação ordinária.

O índice público contém:

- todas as entidades materializadas pelo inventário lifecycle/relationships;
- todas as marcas verificadas;
- aliases de marcas;
- CNPJ e código FIP/SUSEP quando existirem;
- classificação e bucket de desambiguação;
- caminho determinístico para o perfil.

No snapshot de fechamento:

```text
entidades lifecycle                    490
marcas                                  13
perfis públicos                        503
entradas de busca                      503
seguradoras ordinárias atuais          157
participantes Sandbox                   12
```

Essas quantidades são snapshot, não constantes metodológicas.

### 3.1. Fuzzy matching

O frontend pode usar busca aproximada **somente para ordenar candidatos**.

```text
fuzzy search → localizar candidatos
fuzzy search ≠ decidir identidade
```

A escolha de qual perfil abrir deve sempre corresponder a uma entrada determinística do
índice público.

## 4. Tipos de perfil

### 4.1. Seguradora ordinária atual

Recebe:

- identidade e situação regulatória;
- lifecycle;
- grupo econômico observado;
- relações corporativas verificadas;
- marcas relacionadas;
- relações especiais de Conduta;
- avaliação v2, quando completa;
- sinais individuais quando a avaliação conjunta estiver incompleta;
- métricas técnicas com semântica explícita de disponibilidade.

### 4.2. Participante Sandbox

Recebe:

- identidade/regime;
- explicação de que Sandbox não é o universo ordinário;
- evidência de Conduta do artifact Sandbox quando existente;
- contexto de marca verificada;
- **nenhuma** avaliação/ranking ordinário;
- **nenhuma** pressão proporcional inventada.

### 4.3. Marca

Marca é objeto de resolução, não entidade que herda avaliação.

O perfil informa:

- nome e aliases;
- tipo de relação (`brand_of`, `risk_carrier` etc.);
- entidade(s) alvo;
- escopo;
- evidência;
- contexto de Conduta Sandbox quando o contrato específico autorizar.

Regra:

```text
brand_inherits_entity_assessment = false
```

### 4.4. Entidade histórica

O perfil preserva a identidade histórica e, quando documentado, aponta a cadeia de
sucessores até a entidade atual conhecida.

Avaliação atual não é transferida retroativamente.

### 4.5. Previdência, capitalização, resseguro, regime especial e outros

Permanecem pesquisáveis para evitar confusão de nomes, mas o perfil explica que estão
fora da comparação ordinária.

## 5. Semântica obrigatória de ausência

Esta regra é contratual:

```text
null ≠ 0
```

Cada métrica pública relevante usa um envelope:

```json
{
  "value": null,
  "availability": "unavailable",
  "public_use": "displayable",
  "zero_semantics": null,
  "meaning": "..."
}
```

Quando o valor é realmente zero na fonte:

```json
{
  "value": 0,
  "availability": "available",
  "public_use": "...",
  "zero_semantics": "...",
  "meaning": "..."
}
```

O frontend não precisa inferir a diferença.

### 5.1. Regra específica de prêmio direto

Prêmio direto é contexto de volume econômico e nunca mede qualidade.

Quando a entidade não possui relação 1:1 segura entre sujeito de reclamações e exposição,
um zero bruto de prêmio **não pode** virar “operação R$ 0”.

Exemplo: Youse.

O contrato preserva o valor bruto, mas marca:

```text
public_use = do_not_render_as_operation_size
zero_semantics = literal_source_zero_must_not_be_presented_as_zero_sized_business
```

Assim, a interface deve explicar a limitação em vez de exibir uma falsa dimensão da
operação.

## 6. Youse ↔ Caixa

A relação já documentada é trazida para o perfil público.

A leitura permitida é:

- há reclamações registradas contra a Youse;
- existe relação documentada com a Caixa Seguradora como risk carrier dos produtos
  considerados;
- não é válido dividir automaticamente as reclamações da Youse pela produção total da Caixa;
- também não é válido tratar o prêmio próprio zero como “tamanho zero da operação”;
- pressão proporcional permanece indisponível até existir exposição específica comparável.

`expected_complaints`, `pressure_ratio` e `comparable_months` permanecem `null`.

## 7. Loovi ↔ LTI

O contrato preserva duas identidades:

```text
brand:loovi
entity:cnpj:47006254000180  → LTI Seguros S.A.
```

A relação verificada é `risk_carrier`.

O artifact de Sandbox Conduct pode fornecer contexto de reclamações da LTI ao perfil da
Loovi, mas a atribuição pública deve preservar a origem:

> O Consumer.gov registra a evidência contra a LTI Seguros, entidade Sandbox vinculada
> aos seguros comercializados pela Loovi.

Não dizer automaticamente:

> “A Loovi teve X reclamações.”

No snapshot de validação, o contexto LTI/Loovi preserva 1.329 reclamações na janela de
12 meses.

## 8. HDI Seguros × HDI Global

O contrato não tenta resolver semelhança de nomes por heurística.

No snapshot atual:

- HDI Seguros S.A. e HDI Global Seguros S.A. são perfis regulatórios distintos;
- possuem CNPJs e FIPs distintos;
- aparecem no mesmo grupo econômico SUSEP `TALANX AG`;
- o grupo é contexto, não prova automática de incorporação, joint venture ou sucessão.

A busca pode mostrar ambos e usar a desambiguação do perfil.

## 9. Linguagem pública do Financeiro

### Capital

Antes de `PLA/CMR`, usar uma das mensagens:

- “Na competência analisada, o patrimônio ajustado alcança o capital mínimo exigido.”
- “Na competência analisada, o patrimônio ajustado ficou abaixo do capital mínimo exigido.”
- “Não há dado utilizável suficiente para concluir a situação de capital.”

`PLA/CMR` aparece somente em `technical`.

### Liquidez

Antes de `ILT`, usar:

- “O indicador de liquidez usado pela metodologia não mostrou pressão segundo sua
  referência aritmética.”
- “O indicador de liquidez ficou abaixo de sua referência aritmética e merece atenção.”
- “Não há dado utilizável suficiente para concluir a leitura de liquidez.”

A referência 1,0 do ILT continua explicitamente descrita como aritmética, não como limite
prudencial oficial da SUSEP.

## 10. Linguagem pública de Conduta

O contrato reutiliza a semântica fechada:

- acima do esperado com evidência;
- abaixo do esperado com evidência;
- sem diferença clara;
- conclusão sensível ao denominador;
- cobertura temporal insuficiente;
- não comparável.

Reclamações observadas podem ser zero. Isso continua sendo uma observação e **não vira
automaticamente sinal favorável**.

Resposta ou finalização no Consumer.gov não prova resolução.

## 11. Relações e grupos

O perfil pode trazer:

- `incorporated_into`;
- `successor_of`;
- grupo econômico observado;
- marcas;
- `risk_carrier`;
- relações especiais de sujeito/carrier/portfólio de Conduta.

Guardrails:

```text
same_group ≠ succession
same_group ≠ acquisition
same_group ≠ joint_venture
brand ≠ legal_entity
risk_carrier_relation ≠ complaint_transfer
```

## 12. Arquivos públicos

O builder publica:

```text
public/search_index.json
public/profile_manifest.json
public/profiles/*.json
```

`search_index.json` é o catálogo para busca/desambiguação.

`profile_manifest.json` relaciona `profile_id` → arquivo.

Cada arquivo em `public/profiles/` contém uma entidade ou marca já preparada para
apresentação progressiva.

Os leaderboards e coleções continuam sendo publicados pelo contrato exploratório
existente e permanecem independentes.

## 13. Papel do frontend

O frontend pode:

- pesquisar;
- ordenar candidatos por relevância textual;
- abrir perfis;
- formatar valores disponíveis;
- escolher componentes visuais;
- navegar por relações e perfis relacionados.

O frontend não pode:

- converter `null` em `0`;
- decidir identidade por fuzzy matching;
- transferir reclamações;
- calcular pressão;
- reconstruir PLA/CMR ou ILT;
- recalcular assessment;
- criar score;
- declarar vencedor;
- inferir sucessão, grupo ou risk carrier.

## 14. Critério de fechamento

O contrato é considerado fechado quando o workflow real confirma:

- todas as 490 entidades lifecycle materializadas;
- 13 marcas;
- 503 perfis/entradas de busca;
- 157 seguradoras ordinárias com payload de assessment;
- 12 participantes Sandbox;
- Loovi resolve para LTI e preserva 1.329 reclamações no contexto correto;
- Youse preserva reclamações, `expected/ratio/months = null` e não exibe prêmio zero como
  tamanho da operação;
- HDI Seguros e HDI Global permanecem perfis distintos, no mesmo grupo TALANX;
- nenhuma marca herda assessment;
- nenhuma ausência é convertida em zero;
- score/ranking geral continuam fora deste contrato.

Com esses gates verdes, o próximo estágio de produto é:

```text
frontend_php_integration_against_closed_public_search_profile_contract
```
