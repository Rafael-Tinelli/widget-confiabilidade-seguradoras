# Market relationship conduct — contrato geral

## Regra arquitetural

A separação entre quem comercializa/distribui um seguro e quem assume o risco não pertence ao conceito de Sandbox.

O modelo canônico é:

```text
marca pesquisada
  -> identidade de mercado / pessoa jurídica distribuidora
  -> papel de distribuição documentado
  -> relação(ões) de risco documentada(s)
  -> entidade(s) que assume(m) o risco
  -> regime regulatório de cada entidade de risco
  -> sujeito(s) de Conduta preservados separadamente
```

`Sandbox` é somente um possível `regulatory_regime` da entidade que assume o risco. Uma relação equivalente pode apontar para seguradora ordinária, entidade em regime especial ou mais de uma entidade em escopos distintos, desde que a relação esteja documentada no cadastro canônico.

## Invariantes

- `brand != legal_entity`;
- `market_identity != risk_bearer`;
- `risk_carrier_relation != complaint_transfer`;
- `complaint_subject != risk_carrier`;
- uma relação de distribuição não cria avaliação ou ranking para a distribuidora;
- uma marca não herda avaliação da entidade de risco;
- reclamações da distribuidora permanecem com a distribuidora;
- reclamações da entidade de risco permanecem com essa entidade;
- reclamações do carrier são contexto da relação e não são, por padrão, reclamações exclusivas da marca ou produto;
- CNPJ da fonte precede rótulo de marca na resolução do sujeito;
- na ausência de CNPJ, somente alias exato, único e previamente verificado pode atribuir Conduta à identidade de mercado;
- fuzzy matching nunca atribui reclamações;
- múltiplos carriers permanecem separados por relação/escopo; nunca se escolhe um carrier global por conveniência;
- papel de distribuição não verificado nunca é apresentado como verificado;
- ausência de dado não é convertida em zero.

## Artefato canônico

`data/derived/v2/market_relationship_conduct_evidence.json`

Gerador:

`api/v2/build_market_relationship_conduct_evidence.py`

Núcleo:

`api/v2/market_relationship_conduct_core.py`

O artefato parte do `entity_lifecycle_relationship_inventory.json`, portanto só materializa contexto público quando já existem `market_identity` e relação `risk_carrier` preservadas no registro canônico. Receita/CNAE, busca do widget e fuzzy matching podem ajudar a investigar um candidato, mas não criam automaticamente essa relação.

## Conduta

A unidade primária é o sujeito jurídico observado na fonte.

Para a identidade distribuidora:

1. CNPJ exato da fonte, quando presente;
2. somente se o CNPJ estiver ausente, nome/alias normalizado exato, único e verificado;
3. caso contrário, não atribuir.

Para a entidade que assume o risco:

1. resolver a própria entidade jurídica no universo classificado;
2. preservar a Conduta na própria entidade;
3. expor essa Conduta como contexto da relação, sem transferi-la para a distribuidora ou marca.

Quando a mesma pessoa jurídica distribuidora sustenta mais de uma marca, sua Conduta continua sendo da pessoa jurídica. O sistema não duplica a população de reclamações para fingir métricas independentes por marca.

## Multi-carrier

Uma marca pode possuir mais de uma relação `risk_carrier` atual quando o escopo de produto/tempo e a evidência sustentarem isso. O contrato público usa uma lista `risk_bearers[]`; cada item preserva:

- entidade e CNPJ;
- classe e regime regulatório reais;
- status e período da relação;
- escopo de produto;
- evidência;
- Conduta própria da entidade de risco.

Não existe `the_carrier` global quando a evidência mostra pluralidade.

## Contrato público

O perfil de marca recebe `market_relationship_context`, com:

- `distributor`;
- `distributor_conduct`;
- `risk_bearers[]`;
- `conduct_scope`;
- `multi_carrier`;
- `public_limit`.

O frontend deve renderizar essa estrutura diretamente, sem recomputar identidade, papel, carrier ou atribuição de reclamações.

## Compatibilidade Sandbox

`data/derived/v2/sandbox_brand_conduct_evidence.json` permanece temporariamente como projeção de compatibilidade para consumidores antigos. Ele não é mais o modelo autoritativo da relação.

A projeção só sintetiza um contexto de marca quando existe exatamente um carrier e ele é Sandbox. Um arranjo multi-carrier nunca é reduzido artificialmente a uma relação Sandbox singular.

O objetivo da camada de compatibilidade é permitir migração sem regressão da Loovi enquanto backend e frontend passam a consumir `market_relationship_context`.

## Exemplo de regressão

Loovi continua sendo um caso concreto de teste, não uma categoria arquitetural:

```text
Loovi
  -> CW Technology Ltda.
  -> representante de seguros
  -> LTI Seguros S.A.
  -> participante do Sandbox regulatório
```

O mesmo contrato deve comportar, quando houver evidência canônica suficiente, arranjos com carrier ordinário, múltiplos carriers ou outros regimes sem criar código específico por marca.
