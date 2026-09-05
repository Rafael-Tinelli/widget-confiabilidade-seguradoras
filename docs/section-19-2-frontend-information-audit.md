# §19.2 — Auditoria do potencial informacional do frontend

Status: **FECHADO**.

Início: 01/09/2026.  
Fechamento: 02/09/2026.

Este documento registra a auditoria entre o que a v2 sabe, o que o pacote público expõe e o que o frontend versionado efetivamente consome.

A pergunta contratual do §19.2 foi:

> **A informação metodologicamente aprovada chega ao frontend sem ser perdida, reinterpretada ou recalculada?**

A resposta final é **sim para as 17 capacidades exigidas pelo README**, com os gaps encontrados durante a auditoria corrigidos no backend público ou no wiring do frontend.

---

## 1. Regra permanente

```text
frontend_gap -> corrigir wiring/renderização
backend_gap  -> ampliar payload público
nunca        -> recomputar metodologia em React/PHP/JS
```

Continuam proibidos no frontend:

- score composto;
- ranking geral;
- pesos ou contribuições de pilares;
- recálculo de PLA/CMR, ILT ou observed/expected;
- inferência de missingness/comparabilidade;
- transferência de reclamações entre marca e risk carrier;
- decisão de identidade por fuzzy matching;
- inferência de sucessão, aquisição ou grupo;
- transformação de `null` em `0`.

---

## 2. Baseline da auditoria

A fotografia inicial foi a Full Generation Proof #49:

```text
run        33567550092
head       7993dbabd1cf3cd21181c88d072aed4ce5573538
artifact   9824434275
sha256     d0ccb6ce274542015431ae9fde0084c12941d1983ce06527bf6872e442244431
conclusion success
```

Snapshot público observado:

```text
profiles                                      790
entity profiles                               777
brand profiles                                 13
ordinary current insurer profiles             156
assessment available                           82
assessment incomplete                          74
sandbox entity profiles                        12
special purpose insurer profiles                3
entities with legal lifecycle                 312
entities with economic-group context          179
entities with explicit successor                5
entities with incoming brands                  13
entities with direct relationships               9
entities with Conduct reconciliation             9
entities with Sandbox Conduct                  12
explorer entities in >=1 leaderboard           47
explorer entities in >=1 semantic collection  131
```

Essas contagens são fotografia da geração, não constantes metodológicas.

---

## 3. Diagnóstico inicial e migração realizada

No início do §19.2 o frontend ativo ainda era v1:

```text
/api/v1/insurers.json
score_desc
Melhor Nota
Nota
Solvência + Reputação + Open Insurance
weights/contributions
fallback de score em React
```

O gargalo, portanto, era principalmente **migração do contrato de dados**, não ausência generalizada de informação no backend.

O caminho ativo foi migrado para os contratos públicos v2:

```text
search_index.json
profile_manifest.json
insurer_explorer.json
explore_index.json
profiles/*.json
leaderboards/*.json
collections/*.json
```

Arquivos centrais da implementação:

```text
widget-ui/src/App.jsx
widget-ui/src/v2Data.js
widget-ui/src/components/InsurerCard.jsx
widget-ui/src/InsurerProfileModal.jsx
widget-ui/src/ComparisonPanel.jsx
widget-ui/src/ExplorePanel.jsx
```

O antigo `InsurerScoreModal.jsx` deixou de participar do caminho ativo. Sua remoção física é assunto de limpeza do §19.5, não condição metodológica do §19.2.

---

## 4. Gap de período de Conduta — corrigido no backend

A auditoria encontrou um gap público real: a janela de Conduta existia nos artifacts internos, mas o perfil público expunha somente contagem de meses comparáveis.

A janela preservada era:

```text
start_month 2025-07
end_month   2026-06
months      12
```

Foi criado:

`api/v2/public_information_projection.py`

A projeção passa a publicar explicitamente:

```json
{
  "start_month": "2025-07",
  "end_month": "2026-06",
  "months": 12,
  "semantics": "preserved_consumer_gov_window_not_inferred_by_frontend"
}
```

Ela é aplicada a:

- `conduct.reference_window` no Explorer;
- `assessment.conduct.reference_window` nos perfis ordinários;
- `sandbox_conduct.reference_window` nos perfis Sandbox;
- `sandbox_conduct_context.reference_window` em marcas Sandbox.

O backend falha fechado se a janela ordinária e a janela Sandbox divergirem. O frontend apenas renderiza esse contexto e não infere período a partir de `generated_at`, competência financeira ou contagem de meses.

A integração entrou antes da validação final do contrato público em `api/v2/public_profile_regulatory_semantics.py`.

Prova integrada relevante:

```text
V2 Gate 4 Full Generation Proof #51
run_id      33574721819
head        dd36b3dd54ea285dc48e8f5264bbc5eacdaa25c6
conclusion  success
artifact    9826811502
name        v2-gate4-full-generation-33574721819-a1
artifact sha256
7ac6776542ac1af56ffdf48c6023e612ab21640f3788643a78d3d8651cf21cc0
```

Commits posteriores à prova #51 até o fechamento desta seção afetaram somente frontend, testes e documentação, sem alterar a lógica de geração que ela comprovou.

---

## 5. Aliases — resolução sem contaminar identidade jurídica

A investigação de aliases confirmou que o registro canônico os associa principalmente aos **perfis de marca**, juntamente com relações verificadas marca → entidade/risk carrier.

Decisão final:

```text
alias de marca -> encontra perfil de marca
perfil de marca -> mostra relação verificada
alias de marca != alias automático da entidade jurídica
```

Não foi criado espelhamento artificial de aliases de marca sobre legal entities, pois isso violaria:

```text
brand != legal_entity
```

Portanto, aliases ficam **PUBLIC_READY e FRONTEND_USED pela semântica de marca**, sem criar nova decisão de identidade no navegador.

---

## 6. Busca por CNPJ e código SUSEP — aresta final corrigida

O `search_index` preserva identificadores em forma compacta. A primeira migração do frontend normalizava pontuação como espaços, de modo que um CNPJ digitado como:

```text
39.999.619/0001-97
```

poderia deixar de casar com:

```text
39999619000197
```

A correção em `App.jsx` adicionou correspondência estritamente determinística por dígitos compactos, sem fuzzy identity assignment:

```text
texto normalizado -> busca textual
OU
>= 4 dígitos informados -> comparação com dígitos compactos do search_text
```

Isso cobre CNPJ formatado e códigos regulatórios digitados com separadores sem transformar o frontend em resolvedor de identidade.

Regressão adicionada em:

`tests/test_v2_frontend_public_contract.py`

Commits:

```text
9adc37c42f69c574523b7e94cc24fb72a53c318d
  fix(v2-ui): match punctuated regulatory identifiers

7e5100120f4661ec81bb1b411fe18340257c3f69
  test(v2-ui): guard formatted identifier search
```

---

## 7. Matriz final backend → público → frontend

| Capacidade | Payload público | Uso ativo | Resultado |
|---|---|---|---|
| identidade | `search_index` + profiles | busca + perfil | OK |
| aliases | perfis/entradas de marca | busca de marca + relação verificada | OK |
| lifecycle | `profile.lifecycle` | perfil | OK |
| sucessões | lifecycle + relações diretas | perfil + navegação | OK |
| grupos | `economic_group` | contexto de perfil | OK |
| marcas | brand profiles + relationship context | busca + perfil | OK |
| risk carriers | relações verificadas | perfil | OK |
| relações de Conduta | `conduct_reconciliation` | perfil/evidências | OK |
| Sandbox Conduct | `sandbox_conduct*` | perfil/contexto próprio | OK |
| sinais financeiros | assessment/explorer | card + perfil + comparação | OK |
| sinais de reclamações | assessment/explorer | card + perfil + comparação | OK |
| períodos | financeiro + `reference_window` de Conduta | perfil + comparação | OK |
| comparabilidade | contrato de Conduta | linguagem pública | OK |
| confiança | evidência financeira | card + perfil + comparação | OK |
| limites | assessment/profile/metric semantics | perfil | OK |
| leaderboards | explore index + leaderboard files | exploração sob demanda | OK |
| coleções | explore index + collection files | exploração sob demanda | OK |

Resumo final:

```text
17 capacidades auditadas
17 PUBLIC_READY
17 FRONTEND_USED
 0 PUBLIC_PARTIAL conhecido
 0 BACKEND_GAP conhecido nesta seção
 0 FRONTEND_GAP conhecido nesta seção
```

---

## 8. Semântica da experiência ativa

O frontend ativo agora preserva:

- lista ordinária inicial em ordem alfabética, não por mérito;
- busca mais ampla que o universo de assessment;
- perfis de marca separados de entidades jurídicas;
- comparação lado a lado sem vencedor universal;
- leaderboards somente por métrica explicitamente declarada;
- coleções explicitamente não ordenadas quando o contrato assim determina;
- `public_use` antes de exibir valor bruto;
- linguagem pública para confidence, comparabilidade, persistência e tendências;
- períodos explícitos;
- ausência de score geral, pesos e recomposição metodológica no caminho ativo.

O `npm run build` incorporado ao CI serve como **prova de compilabilidade/integridade do React/Vite**. Ele não significa que GitHub Actions faça deploy do frontend para o HostGator.

---

## 9. Evidência de fechamento

No HEAD que concluiu a aresta funcional final:

```text
head 7e5100120f4661ec81bb1b411fe18340257c3f69
```

os checks automáticos terminaram verdes:

```text
CI #1406
run 33587333608
conclusion success
- Ruff: success
- pytest: success
- Node setup: success
- Vite build: success

V2 Gate 4 Evergreen Contract #246
run 33587333609
conclusion success
```

A projeção backend específica de período público já havia sido comprovada pela Full Generation #51, descrita na seção 4.

Não é necessária nova Full Generation apenas para os commits de frontend/teste, pois eles não alteram o DAG nem os artifacts públicos gerados pelo Gate 4.

---

## 10. Decisão de fechamento

O §19.2 está **formalmente fechado**.

O próximo trabalho é operacional:

```text
§19.3 Evergreen / zero manutenção
→ fontes e caches
→ dependências reais
→ eliminar download manual de artifacts
→ sincronização segura com HostGator
→ staging/rollback
→ recuperação com fonte oficial indisponível
```

A etapa seguinte deve manter a separação:

```text
GitHub/Gate 4 -> gera e valida um pacote público único
HostGator     -> hospeda/serve frontend e dados públicos
Vite no CI    -> valida compilabilidade; não é deploy por si só
```

O escopo desta consolidação termina no §19.5; não há avanço automático para itens posteriores.
