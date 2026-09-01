# Contrato público de busca e perfis — v2

Status: **fechado para identidade, busca e renderização pública; ranking geral continua bloqueado**.

A busca de identidade é mais ampla que o universo de assessment. Ser pesquisável não significa ser elegível ao comparador.

## Invariantes

```text
search universe > ordinary assessment universe
brand_inherits_entity_assessment = false
group_membership_implies_succession_or_joint_venture = false
missing_value_may_be_coerced_to_zero = false
zero_complaints_is_automatically_favorable = false
sandbox_enters_ordinary_ranking = false
sspe_enters_ordinary_assessment = false
sspe_enters_ordinary_ranking = false
php_may_recompute_methodology = false
```

Fuzzy search pode ordenar candidatos de busca, mas nunca decidir identidade, sucessão, grupo, risk carrier ou atribuição de reclamações.

## SSPE

Uma Sociedade Seguradora de Propósito Específico permanece juridicamente uma seguradora, porém está fora do comparador ordinário da v2.

```text
query_state = special_purpose_insurer
filter_bucket = other
public label = Seguradora de propósito específico (SSPE)
assessment.availability = not_applicable
assessment.reason = special_purpose_insurer_outside_consumer_assessment
```

A exclusão é de escopo, não conclusão negativa de desempenho.

## Universo ordinário

Somente perfis com `query_state = current_ordinary_insurer` correspondem ao `insurer_explorer`. O conjunto de IDs dos perfis ordinários deve ser exatamente igual ao conjunto do explorer.

Assessment pode estar `available` ou `incomplete`; missingness nunca vira resultado neutro.

## Sandbox, marcas e relações

Sandbox permanece pesquisável e fora do benchmark ordinário. Seu conjunto de perfis deve reconciliar com os carriers do artifact de Conduct Sandbox.

Marcas têm perfil próprio e nunca herdam assessment da entidade. Relações de marca, risk carrier e sucessão podem ser exibidas quando verificadas, sem autorizar transferência automática de reclamações ou desempenho.

Toda referência pública a outro perfil deve apontar para um `profile_id` existente no mesmo pacote. Referência pendente é erro de publicação.

## Null e zero

Métricas públicas carregam `value`, `availability`, `public_use`, `meaning` e `zero_semantics`. Se `availability = unavailable`, `value` deve ser `null`. Zero factual não é missing nem selo favorável.

## Auditoria §19.1 — correção do caminho canônico

A Full Generation #44, anterior à correção transversal, produziu 777 entidades lifecycle, 13 marcas, 790 perfis, 156 perfis ordinários e 12 perfis Sandbox.

A auditoria encontrou uma divergência real: as 3 SSPEs estavam classificadas internamente como `special_purpose_insurer`, mas o stage canônico `public_profiles` chamava apenas o builder-base. A finalização regulatória e o validator existiam, porém não eram executados pelo DAG que montava o pacote. Por isso o JSON público ainda mostrava headline genérico `Seguradora` e reason genérico de ausência de assessment.

O Gate 4 foi corrigido para executar obrigatoriamente, na mesma geração:

```text
api.v2.public_profile_regulatory_semantics
→ api.v2.validate_public_search_profile_contract
```

O validator agora também rejeita referências de perfil sem destino no mesmo pacote.

## Validação estrutural

O contrato público exige:

- IDs de perfil e paths únicos;
- um search entry por perfil;
- `profiles = lifecycle_entities + brands`;
- conjunto ordinário igual ao explorer;
- conjunto Sandbox igual ao artifact Sandbox Conduct;
- SSPE disjunto do universo ordinário e explicitamente rotulado;
- marca sem assessment herdado;
- referências entre perfis resolvidas;
- null/zero semantics preservadas;
- ausência de score, posição de ranking geral ou winner;
- quantidade de arquivos públicos igual à população de perfis.

## Implementação

```text
api/v2/build_public_search_profile_contract.py
api/v2/public_profile_regulatory_semantics.py
api/v2/validate_public_search_profile_contract.py
tests/test_v2_public_search_profile_contract.py
tests/test_v2_public_profile_regulatory_semantics.py
```

Outputs:

```text
data/derived/v2/public_search_profile_contract.json
data/derived/v2/public/search_index.json
data/derived/v2/public/profile_manifest.json
data/derived/v2/public/profiles/*.json
```

A consolidação física entre builder-base e finalizador pode ser avaliada no §19.5. O contrato semântico já está integrado ao caminho canônico de publicação.
