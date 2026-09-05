# Contrato de elegibilidade de avaliação — v2

Status: **fechado; assessment individual é publicado quando a evidência satisfaz o contrato; ranking permanece independente e bloqueado**.

## Pergunta do gate

> **Temos base regulatória, comparabilidade e evidência suficientes para publicar uma avaliação conjunta desta seguradora?**

A elegibilidade mede se a avaliação pode ser feita com segurança. Ela **não mede se o resultado é bom ou ruim**.

## Regras permanentes

Para `assessment_eligible = true`:

- a entidade deve pertencer ao universo regulatório ordinário comparável;
- o núcleo conjunto deve estar completo;
- o contrato semântico deve suportar avaliação pública;
- a evidência financeira central deve ser suficiente;
- a conclusão de Conduta deve ser comparável e conclusiva.

Guardrails:

```text
performance_result_used_for_eligibility             false
adverse_result_blocks_assessment                     false
limited_financial_history_blocks_assessment          false
missingness_treated_as_neutral                       false
ranking_gate_independent                             true
```

Histórico financeiro limitado, quando ainda aceito pelo contrato de evidência, é disclosure de confiança e não penalidade de desempenho.

## Estados

```text
eligible_complete_joint_assessment
not_eligible_joint_evidence_incomplete
not_eligible_core_evidence_confidence
```

Uma entidade inelegível não recebe nota zero, posição inferior ou classe negativa. O motivo da ineligibilidade permanece explícito.

## População dinâmica

O contrato não exige quantidade fixa de entidades.

Invariantes executáveis:

```text
assessment_eligible + assessment_not_eligible = regulatory_universe
ranking_eligible = 0
entity rows = regulatory_universe
eligible rows = assessment_eligible
semantic_supported >= assessment_eligible
```

`evidence_incomplete` não pode aparecer entre entidades elegíveis.

## Snapshot integrado corrente

Na Full Generation #44 (`run_id = 33562392945`, `head = c95e8675f8a2363b325623b2f310886e81f1c027`):

```text
regulatory_universe                    156
semantic_public_assessment_supported    82
assessment_eligible                     82
assessment_not_eligible                 74
ranking_eligible                         0
```

Classes entre as 82 elegíveis:

```text
favorable_reading     45
attention              35
prudential_warning      2
```

Razões de evidência entre as 74 não elegíveis:

```text
conduta_nao_comparavel_com_seguranca        51
conduta_com_cobertura_temporal_insuficiente 11
conduta_sensivel_ao_denominador               5
financeiro_central_incompleto                 7
```

Essas contagens são fotografia da geração, não requisitos do gate.

## O que este gate não autoriza

`assessment_eligible = true` não significa:

- seguradora boa;
- seguradora recomendada;
- ranking elegível;
- posição relativa;
- ausência de alertas;
- garantia futura.

Uma avaliação adversa pode ser elegível se a evidência for suficiente.

## Operação canônica

O contrato antigo de Gate 3 restaurava artifacts bem-sucedidos de execuções distintas. Esse mecanismo não é mais a política operacional da v2.

No Gate 4 fechado, a política é:

```text
single_generation_workspace_required = true
cross_run_latest_successful_restore_forbidden = true
```

O artifact corrente registra:

```text
operational_freshness_policy =
single_generation_workspace_cross_run_latest_successful_restore_forbidden
```

Assessment, contratos entre pilares e publicação são gerados no mesmo workspace/build; uma recomposição cross-run não é autoridade para publicação.

## Implementação

```text
api/v2/build_assessment_eligibility_contract.py
tests/test_v2_assessment_eligibility_contract.py
```

Artifact:

```text
data/derived/v2/assessment_eligibility_contract.json
```

## Encadeamento já concluído

O `ranking_eligibility_preflight` é o contrato separado que pergunta se existe escopo e regra de ordenação defensáveis. Esse estágio já foi fechado e mantém `ranking_eligible = 0`.
