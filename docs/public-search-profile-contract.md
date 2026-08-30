# Contrato público de busca e perfis — v2

Status: **fechado para identidade, busca e renderização pública; ranking geral continua bloqueado**.

Este contrato separa duas superfícies que não devem ser confundidas:

1. **busca de identidade**, ampla, que permite encontrar entidades supervisionadas, históricas, Sandbox, SSPEs e marcas verificadas;
2. **assessment/comparação ordinária**, restrita às seguradoras que passam pelo universo regulatório de consumo da v2.

## Princípio central

```text
search universe > ordinary assessment universe
```

Ser pesquisável não significa ser elegível ao comparador.

A interface pode usar fuzzy search para **ordenar candidatos de busca**, mas nunca para decidir identidade, sucessão, grupo, risk carrier ou atribuição de reclamações.

## Separação de identidades

O perfil público preserva:

- pessoa jurídica regulada;
- marca;
- grupo econômico;
- relações verificadas;
- lifecycle/sucessão;
- contexto Sandbox;
- contexto de Conduta.

Regras permanentes:

```text
brand_inherits_entity_assessment                     false
group_membership_implies_succession_or_joint_venture false
missing_value_may_be_coerced_to_zero                 false
raw_zero_may_be_relabelled_as_missing                false
zero_complaints_is_automatically_favorable           false
sandbox_enters_ordinary_ranking                      false
php_may_recompute_methodology                        false
```

PHP/JS renderiza artifacts pré-computados; não refaz metodologia.

## SSPE — semântica pública explícita

Uma Sociedade Seguradora de Propósito Específico continua pertencendo à superclasse jurídica `insurer` e pode ter `regulatory_regime = ordinary`. Isso **não** a transforma em seguradora ordinária do comparador de consumo.

O discriminador público é o estado regulatório já derivado pelo lifecycle:

```text
query_state = special_purpose_insurer
filter_bucket = other
```

A superfície pública deve mostrar:

```text
Seguradora de propósito específico (SSPE)
```

E o assessment deve ser:

```text
availability = not_applicable
reason = special_purpose_insurer_outside_consumer_assessment
```

Políticas:

```text
sspe_enters_ordinary_assessment false
sspe_enters_ordinary_ranking    false
```

A exclusão é de **escopo regulatório/metodológico**, não avaliação negativa de desempenho.

## Seguradora ordinária

Somente perfis com:

```text
entity_type = insurer
query_state = current_ordinary_insurer
```

podem corresponder ao `insurer_explorer` ordinário.

O conjunto de IDs dos perfis ordinários deve ser exatamente igual ao conjunto de IDs do explorer. Essa igualdade é uma invariante mais robusta que qualquer contagem fixa.

Assessment pode estar:

```text
available
incomplete
```

conforme o contrato de elegibilidade, sem converter missingness em resultado.

## Sandbox

Participantes do Sandbox permanecem pesquisáveis com sua natureza experimental explícita. O contexto de reclamações pode ser mostrado, mas não recebe automaticamente a mesma razão de pressão usada no universo ordinário.

O conjunto de perfis Sandbox deve reconciliar exatamente com os carriers do artifact de Conduct Sandbox.

## Marcas

Marca recebe perfil próprio e nunca herda assessment da entidade relacionada.

Relações marca ↔ risk carrier podem ser exibidas quando verificadas, mas não autorizam transferência automática de reclamações, produção ou conclusão de desempenho.

## Null e zero

Métrica pública usa semântica explícita:

```text
value
availability
public_use
meaning
zero_semantics
```

Se `availability = unavailable`, `value` deve ser `null`.

Zero literal preservado da fonte não pode ser silenciosamente reinterpretado como missing e também não pode virar selo favorável.

## Snapshot validado — 30/08/2026

Run:

```text
V2 Public Search Profile Contract
run 33323343760
head 35e509d31de68a9311ede57ac245de6b7d3c0e11
artifact 9735527221
SHA256 ZIP ec6562d88af95f5a0bad6b256e2d10b359b82486113c91efd9a98a304aed367f
```

População pública:

```text
lifecycle_entities                         492
brands                                      13
profiles                                   505
search_entries                             505
ordinary_current_insurer_profiles          156
ordinary_profiles_with_assessment_payload  156
special_purpose_insurer_profiles             3
sandbox_entity_profiles                     12
sandbox_profiles_with_conduct_context       12
```

SSPEs observadas e corretamente segregadas:

```text
fip:002747  GALÁPAGOS CAPITAL SOCIEDADE SEGURADORA DE PROPÓSITO ESPECÍFICO S.A.
fip:003191  BTG PACTUAL SOCIEDADE SEGURADORA DE PROPÓSITO ESPECÍFICO S.A.
fip:003221  ANDRINA SOCIEDADE SEGURADORA DE PROPOSITO ESPECIFICO S.A
```

Nas três:

```text
entity_type       insurer
regime            ordinary
query_state       special_purpose_insurer
filter_bucket     other
public label      Seguradora de propósito específico (SSPE)
assessment        not_applicable
```

Assim, a v2 preserva simultaneamente a natureza jurídica de seguradora e a exclusão correta do comparador ordinário.

## Validação estrutural

O validador real não congela mais casos individuais ou números temporais de reclamações. Ele exige:

- IDs de perfil únicos;
- um search entry por perfil;
- paths públicos únicos;
- `profiles = lifecycle_entities + brands`;
- conjunto ordinário = conjunto do explorer;
- conjunto Sandbox = conjunto de carriers Sandbox;
- SSPE disjunto do universo ordinário;
- marca sem assessment herdado;
- SSPE explicitamente rotulada e `not_applicable`;
- null semantics preservadas;
- ausência de `score`, `overall_score`, `ranking_position` e `winner`;
- quantidade de arquivos públicos igual à quantidade de perfis.

Casos como Youse, Loovi/LTI ou HDI podem continuar sendo usados em testes específicos de relacionamento quando necessário, mas seus números correntes não são boundaries do contrato público.

## Implementação

Builder base:

```text
api/v2/build_public_search_profile_contract.py
```

Finalização semântica regulatória do Gate 3:

```text
api/v2/public_profile_regulatory_semantics.py
```

Validação:

```text
api/v2/validate_public_search_profile_contract.py
tests/test_v2_public_search_profile_contract.py
tests/test_v2_public_profile_regulatory_semantics.py
.github/workflows/v2-public-search-profile-contract.yml
```

Outputs:

```text
data/derived/v2/public_search_profile_contract.json
data/derived/v2/public/search_index.json
data/derived/v2/public/profile_manifest.json
data/derived/v2/public/profiles/*.json
```

A camada `public_profile_regulatory_semantics.py` é uma finalização de contrato público sobre o builder existente, não uma metodologia paralela. A consolidação física desse código pode ser feita na limpeza de repositório sem alterar o contrato.

## Próxima etapa operacional

Os contratos públicos agora falham corretamente quando recebem gerações incompatíveis de artifacts. O Gate 4 deve eliminar a dependência operacional de “latest successful” por meio de uma orquestração transversal com geração/build ID, proveniência, freshness e fallback explícitos.
