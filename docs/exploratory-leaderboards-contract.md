# Exploratory Leaderboards Contract — v2

Status: **contrato de exploração fechado para rankings unidimensionais e coleções semânticas; ranking geral permanece bloqueado**.

Este contrato sucede:

- `docs/cross-pillar-assessment-semantic-contract.md`;
- `docs/assessment-eligibility-contract.md`;
- `docs/ranking-eligibility-preflight.md`.

A direção de produto é deliberadamente assimétrica:

```text
produto principal
= avaliação semântica + comparador lado a lado

produto secundário
= leaderboards de métricas específicas + coleções exploratórias

ranking geral composto
= bloqueado
```

## 1. Pergunta humana

> **Quais formas de explorar e ordenar aspectos específicos do mercado são úteis e honestas sem transformar uma métrica isolada em ranking geral?**

Regra central:

> **um leaderboard numérico só existe quando a própria métrica define a ordem; conceitos compostos permanecem coleções semânticas ou não suportados.**

Isso permite satisfazer curiosidade pública e intenções de busca sem reabrir a discussão já encerrada pelo `Ranking Eligibility Preflight` sobre uma ordem total 1–85 ou 1–157.

## 2. Separação obrigatória de produtos

### Avaliação individual

Responde:

> O que sabemos sobre esta seguradora?

Usa o contrato semântico já fechado e preserva:

- identidade correta;
- elegibilidade de avaliação;
- leitura geral;
- capital;
- liquidez;
- contexto operacional;
- Conduta;
- persistência e tendência quando aplicáveis;
- confiança e missingness.

### Comparação lado a lado

Responde:

> Em que estas seguradoras diferem nos mesmos eixos?

A comparação não precisa declarar vencedora. O contrato autoriza o backend a entregar até quatro cards como recomendação de UX, mas não cria `winner`, score composto ou ordem geral.

### Exploração do mercado

Responde perguntas factuais ou semânticas estreitas, como:

- quais têm maior prêmio direto?;
- quais apresentam maior PLA/CMR?;
- quais apresentam maior ILT?;
- quais têm pressão de reclamações confirmadamente abaixo ou acima do esperado?;
- quais pertencem a uma coleção sem sinal financeiro central adverso?;
- quais têm leitura conjunta favorável?;
- quais mostram melhora recente em Conduta embora a pressão anual ainda seja adversa?

## 3. Leaderboards numéricos autorizados

O snapshot atual suporta cinco leaderboards públicos.

### 3.1. Maiores por prêmio direto

```text
id = largest_by_direct_premium
candidatas = 132
ordem = prêmio direto de seguros 12m, decrescente
```

Semântica:

> Maiores seguradoras por prêmio direto de seguros.

Não significa:

- mais confiáveis;
- melhores;
- mais populares;
- maior satisfação.

Prêmio mede volume econômico.

### 3.2. Maiores relações PLA/CMR

```text
id = highest_pla_cmr_ratio
candidatas = 155
ordem = PLA/CMR, decrescente
```

Semântica:

> Maiores relações entre Patrimônio Líquido Ajustado e Capital Mínimo Requerido na competência observada.

Não significa ranking financeiro geral. A avaliação Financeira continua sem recompensa ilimitada por magnitude acima do requisito.

### 3.3. Maiores ILTs

```text
id = highest_ilt
candidatas = 156
ordem = ILT, decrescente
```

Semântica:

> Maiores ILTs observados na competência de referência.

Não significa selo de liquidez da SUSEP nem ranking financeiro geral. A referência 1,0 permanece paridade aritmética da metodologia, não limite prudencial oficial.

### 3.4. Menor pressão relativa de reclamações

```text
id = lowest_conduct_pressure_ratio
candidatas = 41
escopo = below_expected_with_sufficient_evidence
ordem = observed / expected, crescente
```

Somente conclusões anuais abaixo do esperado com evidência suficiente e denominador estável podem entrar.

Razão menor:

- não prova melhor atendimento;
- não mede percentual de clientes insatisfeitos;
- não vira bônus no ranking geral.

### 3.5. Maior pressão relativa de reclamações

```text
id = highest_conduct_pressure_ratio
candidatas = 26
escopo = above_expected_with_sufficient_evidence
ordem = observed / expected, decrescente
```

Somente conclusões anuais acima do esperado com evidência suficiente e denominador estável podem entrar.

Razão maior descreve pressão proporcional no indicador. Não significa que todos os clientes terão problemas e não define sozinha a qualidade geral da seguradora.

## 4. Política de empate

Leaderboards não recebem desempate de mérito secundário.

```text
competition rank
mesmo valor da métrica → mesma posição
```

Exemplo:

```text
1
1
3
```

O nome jurídico é usado somente para estabilidade de serialização quando valores são iguais; ele não altera a posição.

O Top 10 significa **dez posições**, não necessariamente dez entidades. Se uma posição contém empate, todas as entidades empatadas naquela posição podem aparecer.

## 5. Coleções semânticas autorizadas

Coleções são explicitamente:

```text
ordered = false
```

Não existe 1º ou último lugar.

Snapshot atual:

```text
financial_core_without_current_adverse_signal   120
favorable_joint_assessment                       46
favorable_with_below_expected_conduct             33
conduct_improving_but_still_adverse                4
conduct_persistent_above_expected                 20
```

### `financial_core_without_current_adverse_signal`

Critério:

```text
core_financial_signal == core_indicators_without_current_shortfall
```

É a tradução segura mais próxima de uma intenção como “financeiro mais em dia”. Não é Top 10 porque os contratos não autorizam ordenar internamente esse grupo por mérito geral.

### `favorable_joint_assessment`

Critério:

```text
assessment_eligible == true
AND public_class == favorable_reading
```

### `favorable_with_below_expected_conduct`

Critério:

```text
favorable_joint_assessment
AND Conduct == below_expected_with_sufficient_evidence
```

A coincidência de sinais favoráveis não é selo de excelência.

### `conduct_improving_but_still_adverse`

Critério:

```text
Conduct == above_expected_with_sufficient_evidence
AND trend == improving_pressure
```

A melhora recente não apaga o estado anual adverso.

### `conduct_persistent_above_expected`

Critério:

```text
Conduct == above_expected_with_sufficient_evidence
AND persistence == persistent_above_expected
```

Persistência qualifica o sinal adverso; não cria ranking de “piores seguradoras”.

## 6. Conceitos pedidos, mas não sustentados pelo dado atual

O artifact contém um `concept_registry` para impedir que o frontend transforme intenção de busca em conclusão metodológica inexistente.

### “Mais popular”

```text
classification = not_supported
safe_alternative = largest_by_direct_premium
```

Prêmio mede tamanho econômico; reclamações medem atrito observado. Nenhum dos dois mede popularidade.

### “Emergente promissora”

```text
classification = not_supported
```

Exige contrato próprio para:

- crescimento;
- porte;
- maturidade mínima;
- janela histórica;
- significado público de “promissora”.

A v2 ainda não fechou um contrato longitudinal de crescimento de produção para essa finalidade.

### “Consagrada exemplar”

```text
classification = not_supported
safe_alternative = favorable_with_below_expected_conduct
```

“Consagrada” exige evidência de tenure/legado que a arquitetura atual não modela. “Exemplar” implicaria mérito geral além dos contratos fechados.

### “Mais reclamadas em volume absoluto”

```text
classification = context_only
```

O número bruto permanece disponível no `insurer_explorer.json`, mas não é publicado como ranking de qualidade porque tamanho da operação influencia fortemente o volume absoluto.

### “Ranking geral”

```text
classification = not_supported
ranking_eligible = 0
```

O `Ranking Eligibility Preflight` continua autoritativo: cobertura e ordem total permanecem insuficientes.

## 7. Pacote JSON público

O builder gera um artifact metodológico e um pacote destinado à camada PHP.

```text
data/derived/v2/exploratory_leaderboards_contract.json

data/derived/v2/public/
├── insurer_explorer.json
├── explore_index.json
├── leaderboards/
│   ├── largest_by_direct_premium.json
│   ├── highest_pla_cmr_ratio.json
│   ├── highest_ilt.json
│   ├── lowest_conduct_pressure_ratio.json
│   └── highest_conduct_pressure_ratio.json
└── collections/
    ├── financial_core_without_current_adverse_signal.json
    ├── favorable_joint_assessment.json
    ├── favorable_with_below_expected_conduct.json
    ├── conduct_improving_but_still_adverse.json
    └── conduct_persistent_above_expected.json
```

## 8. `insurer_explorer.json`

Preserva as 157 seguradoras e entrega dados semanticamente prontos para busca e comparação.

Cada entidade pode carregar:

```text
identity
assessment
financial
conduct
market_context
explore_memberships
```

A camada pública recebe, entre outros:

- nome jurídico e display name observado;
- `assessment_eligible`;
- classe, título, resumo e limites da avaliação;
- PLA/CMR e estado de capital;
- ILT e estado de liquidez;
- contexto operacional;
- confiança financeira;
- conclusão de Conduta;
- observed / expected quando comparável;
- meses comparáveis;
- persistência;
- tendência;
- prêmio direto 12m;
- reclamações observadas 12m;
- memberships em leaderboards e coleções.

Entidades sem avaliação conjunta continuam no explorer. Missingness não vira posição inferior.

## 9. Papel do PHP no HostGator

Regra formal:

```text
php_may_recompute_methodology = false
```

O PHP deve:

- carregar JSON;
- localizar a seguradora;
- filtrar/buscar;
- montar cards;
- apresentar os mesmos eixos lado a lado;
- renderizar leaderboards e coleções;
- exibir períodos, fontes, labels e caveats já definidos.

O PHP não deve:

- recalcular PLA/CMR;
- recalcular ILT;
- recalcular observed/expected;
- inventar score;
- converter coleções em ranking;
- criar desempate secundário;
- declarar vencedor geral;
- reinterpretar missingness.

## 10. Guardrails executáveis

O builder e os testes impedem:

- `ranking_eligible > 0`;
- abertura silenciosa do ranking geral;
- `overall_score`, `financial_score` ou `conduct_score`;
- leaderboard sem métrica declarada;
- uso de estado inconclusivo nos leaderboards de Conduta;
- missingness como bottom rank;
- desempate de mérito secundário;
- reclassificação de leaderboard métrico como “melhor seguradora”;
- síntese automática de “popular”, “promissora”, “consagrada exemplar” ou “ranking geral”;
- recálculo metodológico no PHP.

## 11. Resultado validado do snapshot atual

```text
universo regulatório              157
assessment_eligible                85
assessment_not_eligible            72
ranking_eligible                     0

leaderboards numéricos               5
coleções semânticas                   5
arquivos públicos gerados            12
```

O contrato abre:

```text
exploratory_leaderboards_gate_opened = true
semantic_comparator_ready_for_data_contract = true
metric_specific_public_leaderboards_ready = true
```

E mantém:

```text
general_ranking_gate_opened = false
ranking_eligible = 0
general_ranking_remains_blocked = true
```

### Validação real

```text
V2 Exploratory Leaderboards Contract
run                     33040347388
job                     98412282069
Ruff                    verde
testes                  7/7
build real              verde
boundaries              verdes
artifact                9633622703
SHA256 ZIP              ebedc4ea8d10959ab3dbb01000d923e4f57d1cb2db960dfbec7ff54a93598905
arquivos públicos       12
```

A validação acima foi executada contra os artifacts reais da branch. Alterações posteriores apenas de governança/documentação não mudam os contratos nem os dados derivados; o workflow oficial inclui `README.md` e este documento em seus gatilhos de validação.

## 12. Próximo estágio de produto

Com este contrato fechado, a próxima etapa deixa de ser escolha de score.

Ela passa a ser:

```text
public_api_json_packaging_and_frontend_php_integration
```

Ou seja:

1. estabilizar os JSONs públicos como interface entre backend e host;
2. definir versionamento/cache/publicação;
3. construir a busca por seguradora;
4. construir cards de avaliação;
5. construir seleção de 2–4 seguradoras para comparação;
6. montar as páginas/abas de exploração e Top 10;
7. preservar os caveats metodológicos no frontend.

O ranking geral continua fora desse caminho até que um contrato futuro, se desejado, resolva explicitamente escopo e ordenação.
