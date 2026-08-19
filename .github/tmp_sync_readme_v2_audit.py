from pathlib import Path
import re

path = Path("README.md")
text = path.read_text(encoding="utf-8")


def replace_once(old: str, new: str, label: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly 1 match, found {count}")
    text = text.replace(old, new)


def replace_section(start_heading: str, next_heading: str, body: str) -> None:
    global text
    pattern = re.compile(
        rf"^{re.escape(start_heading)}\n.*?(?=^{re.escape(next_heading)}\n)",
        flags=re.MULTILINE | re.DOTALL,
    )
    matches = list(pattern.finditer(text))
    if len(matches) != 1:
        raise SystemExit(
            f"section {start_heading}: expected exactly 1 match, found {len(matches)}"
        )
    text = pattern.sub(body.rstrip() + "\n\n", text, count=1)


# Architecture: document implemented official sources and clearly mark candidates.
old_sources = """FONTES OFICIAIS / PÚBLICAS
          │
          ├── SUSEP / SES
          ├── licenciamentos e cadastros SUSEP
          ├── Sandbox SUSEP
          ├── SusepCon / BDR, quando utilizável
          ├── Open Insurance
          └── Consumidor.gov, quando aplicável
"""
new_sources = """FONTES OFICIAIS / PÚBLICAS
          │
          ├── SUSEP / SES — BaseCompleta + LISTAEMPRESAS
          ├── SUSEP — entidades atualmente licenciadas
          ├── SUSEP — regimes especiais
          ├── SUSEP — Sandbox Regulatório
          ├── Receita Federal — Dados Abertos do CNPJ (lifecycle jurídico)
          ├── SusepCon / BDR — candidato do pilar de conduta
          ├── Consumidor.gov — candidato complementar
          └── Open Insurance — contexto, sem pontos
"""
replace_once(old_sources, new_sources, "architecture source list")

# Identity semantics implemented in v2.
anchor = """Sempre que possível, a identidade deve convergir para:

```text
entity_id
CNPJ
código FIP/SUSEP
razão social
tipo de entidade
status regulatório
```
"""
identity_detail = anchor + """

Na implementação v2 atual:

- para registros regulatórios SUSEP/SES, o identificador canônico é `entity_id = fip:XXXXXX`;
- CNPJ representa a **pessoa jurídica** e não substitui o FIP como chave regulatória;
- a fonte oficial de entidades licenciadas da SUSEP tem precedência para nome/CNPJ atuais quando disponível, preservando divergências em evidência de auditoria;
- `LISTAEMPRESAS.csv` funciona como ponte auxiliar FIP ↔ CNPJ/nome no SES e **não define o universo atual de seguradoras**;
- a Receita Federal é uma dimensão separada de lifecycle jurídico/cadastral e não sobrescreve o status regulatório SUSEP;
- participante do Sandbox sem FIP publicado pode ser materializado excepcionalmente por `entity_id = cnpj:<CNPJ>`, sempre com `entity_type = sandbox_participant` e fora do ranking ordinário;
- `normalize_cnpj_v2` aceita o contrato atual de CNPJ numérico ou alfanumérico, mantendo a normalização legada da v1 isolada.
"""
replace_once(anchor, identity_detail, "identity semantics")

# Matrix identity/source table: reflect actual authority boundaries.
old_table = """| Informação | Fonte principal | Pontua? | Função |
|---|---|---:|---|
| CNPJ | SUSEP | Não | Identidade |
| Código FIP/SUSEP | SUSEP/SES | Não | Integração |
| Razão social | SUSEP | Não | Identidade |
| Nome de exibição | Derivado/curado | Não | UX |
| Tipo da entidade | SUSEP + normalização | Não | Universo |
| Situação atual | SUSEP | Não | Gate |
| Regime regulatório | SUSEP | Não | Gate |
| Grupo econômico | SUSEP | Não | Contexto |
| Ramos de atuação | SES | Não | Contexto |
| Open Insurance | Fonte oficial | Não | Contexto |
| Marca/alias | Fonte verificável/curadoria | Não | Resolução de busca |
"""
new_table = """| Informação | Fonte principal | Pontua? | Função |
|---|---|---:|---|
| Código FIP/SUSEP | SUSEP/SES | Não | identidade regulatória preferida |
| CNPJ atual da entidade regulada | SUSEP licenciadas + SES/LISTAEMPRESAS | Não | vínculo com a pessoa jurídica |
| Situação cadastral do CNPJ | Receita Federal — Dados Abertos do CNPJ | Não | cross-check jurídico/lifecycle |
| Razão social atual | SUSEP licenciadas | Não | identidade regulatória corrente |
| Nome de exibição | Derivado/curado | Não | UX |
| Tipo da entidade | SUSEP licenciadas/regimes/Sandbox + normalização | Não | universo |
| Status regulatório | SUSEP licenciadas/regimes/Sandbox | Não | gate |
| Regime regulatório | SUSEP | Não | gate |
| Grupo econômico | SES / `Ses_grupos_economicos.csv` | Não | contexto |
| Ramos/atividade observada | SES | Não | contexto |
| Open Insurance | Fonte oficial, quando consumida | Não | contexto |
| Marca/alias | Registro verificável/curado | Não | resolução de busca |
"""
replace_once(old_table, new_table, "identity source table")

# Eligibility: separate implemented current state from future conceptual state.
start = "### 8.3. Elegibilidade"
end = "---\n\n## 9. Pilares da avaliação geral"
pattern = re.compile(
    rf"^{re.escape(start)}\n.*?(?=^---\n\n## 9\. Pilares da avaliação geral\n)",
    flags=re.MULTILINE | re.DOTALL,
)
if len(list(pattern.finditer(text))) != 1:
    raise SystemExit("eligibility subsection not uniquely found")
eligibility_body = """### 8.3. Elegibilidade

A implementação atual separa três estados que não podem ser confundidos:

```text
regulatory_universe_eligible
→ assessment_eligible
→ ranking_eligible
```

O universo regulatório atual é `ordinary_current_insurers`. No marco documentado nesta branch:

```json
{
  "regulatory_universe_eligible": 157,
  "assessment_eligible": 0,
  "ranking_eligible": 0,
  "comparison_cohort": null
}
```

Os 157 registros são seguradoras ordinárias atuais que **podem prosseguir aos gates de evidência**; não são 157 seguradoras já avaliadas ou ranqueadas.

A camada de evidência financeira já existe, mas ainda não abre `assessment_eligible`. Permanecem pendentes no gate: evidência de conduta/reclamações, calibração metodológica, confiança da avaliação e definição da coorte final.

A Receita funciona aqui como cross-check jurídico: um CNPJ conhecido como `closed`, `suspended`, `unfit` ou `null` impede a entrada no universo corrente; indisponibilidade do cross-check, porém, não revoga silenciosamente uma licença SUSEP válida.

#### Futuro — seguradora ordinária após todos os gates

```text
avaliação geral: sim
ranking: somente se também houver coorte comparável e metodologia completa
```

#### Seguradora com financeiro utilizável e conduta/reclamações insuficiente

```text
avaliação financeira: pode ser exibida
avaliação geral: não
ranking geral: não
```

#### Sandbox

```text
identificação: sim
status regulatório: sim
entity_type final: sandbox_participant
avaliação geral de seguradora ordinária: não
ranking de seguradoras ordinárias: nunca
```

#### Marca

```text
resolução: sim
nota própria: não
ranking próprio: não
direcionamento à entidade responsável: quando possível
```
"""
text = pattern.sub(eligibility_body.rstrip() + "\n\n", text, count=1)

# Financial evidence/maturity is implemented, not merely intended.
anchor = """Esses pontos são **EM CALIBRAÇÃO**. Não reabrem a seleção de novos eixos financeiros.

---

## 11. Capital regulatório — PLA/CMR
"""
insert = """Esses pontos são **EM CALIBRAÇÃO**. Não reabrem a seleção de novos eixos financeiros.

### 10.3. Evidência financeira e competência madura — IMPLEMENTADO

A branch já possui um `financial_evidence` separado de score. Ele lê, para o universo regulatório elegível:

- `Ses_pl_margem.csv` — PLA/CMR e histórico prudencial;
- `SES_Balanco.csv` — contas e CMPIDs para liquidez/operação;
- `Ses_seguros.csv` — presença histórica da operação securitária.

A implementação distingue **competência observada** de **competência financeira madura**. A política draft `2.0-draft-financial-period-maturity-1` considera os últimos 6 períodos comuns às três fontes e escolhe o mais recente cuja cobertura de PLA/CMR derivável seja pelo menos 95% do pico recente.

No conjunto usado nesta etapa:

```text
202601  148
202602  151
202603  153
202604  155
202605  155  ← competência madura selecionada
202606  130  ← observada, mas ainda imatura para a metodologia
```

Em `2026-05`:

```text
complete_core_history          143
limited_core_history            12
capital_metric_unavailable       1
insufficient_core_evidence       1
requires_source_investigation    0
```

PLA/CMR era derivável para 155/157. CMR zero, registro prudencial zerado ou dado inutilizável são tratados como **evidência indisponível**, nunca como desempenho zero. O horizonte de 12 meses é gate de completude; janelas de 24/36 meses permanecem descritivas para estabilidade/confiança.

---

## 11. Capital regulatório — PLA/CMR
"""
replace_once(anchor, insert, "financial evidence maturity")

# Operating states are already implemented experimentally.
old = """O filme operacional integra a leitura do pilar financeiro, mas **não é um terceiro score bruto independente** nesta arquitetura.

### 13.2. Investigação fechada do ILPL
"""
new = """O filme operacional integra a leitura do pilar financeiro, mas **não é um terceiro score bruto independente** nesta arquitetura.

### 13.2. Estados operacionais experimentais — IMPLEMENTADO

A branch já materializa estados semânticos experimentais em `operating_states.py`. Eles **não alteram score, `assessment_eligible` nem `ranking_eligible`**.

Contrato atual:

```text
formula_state
history_state
operating_signal
```

O histórico é considerado `established` apenas quando existem 12 competências consecutivas com base operacional positiva e uma competência equivalente do ano anterior comparável. O sinal usa ICA como referência e IC como apoio, sempre comparando horizontes equivalentes.

Na execução real em `2026-05`:

```text
formula ICA derivável                  139
sem base operacional positiva           16
competência corrente ausente              1
denominador ampliado não positivo         1

history_state = established            125
history_state = limited                 32

balanced_persistent                     90
improved                                13
recent_pressure                          7
persistent_pressure                     14
indeterminate                           33
```

O ponto `1,0` permanece referência aritmética de paridade da fórmula, **não threshold prudencial ou selo SUSEP**.

### 13.3. Investigação fechada do ILPL
"""
replace_once(old, new, "operating states implementation")

# Align preliminary JSON examples with actual canonical IDs/status and unfinished complaints pillar.
text = text.replace('"id": "cnpj:12345678000190"', '"id": "fip:012340"')
text = text.replace('"fip_code": "01234"', '"fip_code": "012340"')
text = text.replace('"regulatory_status": "active"', '"regulatory_status": "active_licensed"')
text = text.replace('"universe": "licensed_insurers"', '"universe": "ordinary_current_insurers"')
text = text.replace('"entity_id": "cnpj:12345678000190"', '"entity_id": "fip:012340"')
text = text.replace('"entity_id": "cnpj:11111111000111"', '"entity_id": "fip:111111"')
text = text.replace('"entity_id": "cnpj:22222222000122"', '"entity_id": "fip:222222"')

old_complaints_example = """        "complaints": {
          "score": 79.8,
          "source_id": "susep_complaints",
          "period": "2025-Q4",
          "comparison_group": "default"
        },
"""
new_complaints_example = """        "complaints": {
          "score": null,
          "status": "methodology_pending",
          "source_id": null,
          "period": null,
          "comparison_group": null
        },
"""
replace_once(old_complaints_example, new_complaints_example, "complaints schema example")
text = text.replace('          "complaints_better_than_reference"\n', '          "operating_signal_available"\n')

# Provenance example: do not pretend complaints source is finalized; add Receita lifecycle.
old_provenance = """  "sources": {
    "susep_pl_margem": {
      "provider": "SUSEP",
      "dataset": "Ses_pl_margem"
    },
    "susep_complaints": {
      "provider": "SUSEP",
      "dataset": "SusepCon/BDR"
    }
  }
"""
new_provenance = """  "sources": {
    "susep_pl_margem": {
      "provider": "SUSEP",
      "dataset": "Ses_pl_margem"
    },
    "receita_cnpj_lifecycle": {
      "provider": "Receita Federal do Brasil",
      "dataset": "Cadastro Nacional da Pessoa Jurídica (CNPJ) - Dados Abertos"
    }
  }
"""
replace_once(old_provenance, new_provenance, "provenance example")

# Full source registry synchronized with the code actually present in this refactor.
replace_section(
    "## 32. Fontes",
    "## 33. Estado da API v1",
    r'''## 32. Fontes — estado real da branch

Esta seção distingue **fontes já consumidas pela v2**, fontes contextuais já existentes no repositório e fontes ainda candidatas para o segundo pilar.

### 32.1. SUSEP / SES — IMPLEMENTADO

O SES é a base principal de evidência econômico-financeira e histórica. A implementação utiliza as URLs oficiais para `BaseCompleta.zip` e `LISTAEMPRESAS.csv`, com cache/fallback e validação do ZIP.

Na v2, `BaseCompleta.zip` sustenta, entre outros:

- `Ses_cias.csv` e fluxos SES — identidade regulatória histórica/presença de atividade;
- `Ses_pl_margem.csv` — PLA, CMR e histórico prudencial;
- `SES_Balanco.csv` — balanço/CMPIDs usados nos experimentos de liquidez e operação;
- `Ses_seguros.csv` — presença da operação securitária;
- `Ses_grupos_economicos.csv` — grupo econômico atual e histórico contextual.

A Base Completa **não é uma lista pronta de seguradoras atuais comparáveis**. Presença em fluxo SES prova presença de dado/atividade, não tipo jurídico nem licença atual.

### 32.2. LISTAEMPRESAS — IMPLEMENTADO COMO PONTE

`LISTAEMPRESAS.csv` é usado como mapa auxiliar entre `CodigoFIP`/`Coenti`, CNPJ e nome. Não define sozinho:

- universo atual;
- tipo da entidade;
- licença vigente;
- regime regulatório;
- elegibilidade para ranking.

### 32.3. Entidades atualmente licenciadas SUSEP — IMPLEMENTADO

A classificação corrente consulta o serviço oficial SUSEP:

```text
https://www2.susep.gov.br/menuatendimento/procura_2011.asp
```

A fonte publica FIP, CNPJ, razão social e tipo. A v2 reconhece separadamente seguradoras, previdência aberta, capitalização, resseguradores, corretores de resseguro e autorregulação conforme os códigos da própria fonte.

Para seguradoras ordinárias atuais, essa é a autoridade principal de **licenciamento/tipo/status regulatório**. O matching é por FIP; divergências com SES são preservadas em auditoria, não resolvidas por similaridade de nome.

### 32.4. Regimes especiais SUSEP — IMPLEMENTADO

A v2 consulta as listas oficiais de:

- direção fiscal;
- intervenção;
- liquidação extrajudicial;
- liquidação ordinária;
- falência.

Esses registros recebem regime/status próprios e não entram no ranking de seguradoras ordinárias. O regime especial não é traduzido em “nota ruim”; é uma condição regulatória que exige tratamento informativo próprio.

### 32.5. Sandbox SUSEP — IMPLEMENTADO

A fonte corrente é a página oficial consolidada de participantes do Sandbox. Ela publica CNPJ, edição, status, vigência e modalidades, mas não FIP.

Regras implementadas:

- associação a identidade FIP existente somente por **CNPJ exato**;
- fuzzy matching é proibido;
- participante oficial sem FIP publicado pode ser materializado por CNPJ como identidade própria;
- ambiguidades permanecem `unresolved`;
- o tipo final é sempre `sandbox_participant`;
- Sandbox nunca entra no universo/ranking ordinário.

### 32.6. Receita Federal — Dados Abertos do CNPJ — IMPLEMENTADO

A fonte jurídica/cadastral é o conjunto oficial:

```text
Receita Federal do Brasil
Cadastro Nacional da Pessoa Jurídica (CNPJ) — Dados Abertos
```

A coleta automática acessa o repositório oficial em `arquivos.receitafederal.gov.br` via compartilhamento público Nextcloud/WebDAV, descobre a competência mensal completa mais recente e processa `Motivos.zip` + partições `Estabelecimentos*.zip`. O pipeline filtra somente os CNPJs do universo regulatório alvo, sem guardar uma cópia integral do cadastro nacional.

Papel metodológico:

```text
SUSEP → autoridade sobre licença, tipo e regime regulatório
Receita → autoridade/cross-check sobre situação cadastral da pessoa jurídica
```

A situação Receita é armazenada em `legal_lifecycle` e **não sobrescreve** `regulatory_status`. Contradição material — por exemplo, CNPJ baixado na Receita e simultaneamente `active_licensed` na SUSEP — interrompe o pipeline para investigação.

A validação integral já realizada para a competência `2026-08` resolveu **310/310 CNPJs-alvo**, com as 10 partições de Estabelecimentos processadas e golden checks oficiais preservados.

Existe um pequeno `receita_lifecycle_verified.json` como **oráculo de regressão**. Quando o snapshot automático bulk está disponível, esse arquivo deixa de ser fonte principal e deve concordar com o resultado automático.

O workflow `Refresh Receita lifecycle`:

- roda por `workflow_dispatch` e semanalmente;
- descobre a competência oficial mais recente;
- exige cobertura mínima de 95%;
- rejeita CNPJ duplicado e quedas anormais de cobertura;
- valida o bulk contra os casos golden;
- atualiza apenas o snapshot filtrado quando necessário.

### 32.7. Grupos econômicos e relationships — IMPLEMENTADO

`Ses_grupos_economicos.csv` fornece observações mensais de grupo. A v2 comprime o histórico em períodos contíguos e distingue grupos específicos de buckets genéricos como `INDEPENDENTE` e `OUTROS GRUPOS`.

Buckets genéricos permanecem como evidência contextual e **não criam falsa relação societária compartilhada**.

Sucessões como `incorporated_into` só são materializadas a partir de `data/reference/v2/verified_relationships.json`, com evidência explícita. O backend gera a relação inversa `successor_of` e resolve cadeias de sucessão. Grupo econômico ou nome semelhante, isoladamente, nunca prova incorporação.

Marcas também são objetos de resolução: podem apontar para `risk_carrier`, mas **não herdam score** da entidade responsável.

### 32.8. Fórmulas econômico-financeiras SUSEP — IMPLEMENTADO COMO REFERÊNCIA

Os experimentos de ILC, ILT, IC, ICA e componentes usam os CMPIDs documentados em `api/sources/susep_financial_evidence.py` e a referência oficial **Índices para Análise Econômico-Financeira das Supervisionadas (SUSEP, 2018)**, ainda vinculada pela página atual de Solvência e Contabilidade.

A existência de uma fórmula oficial não transforma `1,0` em corte prudencial quando a SUSEP não o define como tal, nem obriga o ranking a pontuar todos os índices publicados.

### 32.9. Open Insurance — CONTEXTUAL / NÃO INTEGRADO AO SCORE v2

O repositório já possui infraestrutura histórica de Open Insurance, mas a v2 decidiu que participação não gera pontos de confiabilidade. Eventual consumo futuro será contextual e não deverá reabrir esse princípio.

### 32.10. SusepCon / BDR — CANDIDATO, AINDA NÃO IMPLEMENTADO NO PILAR v2

Continua sendo a fonte regulatória prioritária a investigar para conduta/reclamações, condicionada a:

- acesso sustentável;
- estrutura recuperável;
- identidade jurídica;
- granularidade e taxonomia;
- histórico;
- denominador comparável;
- automação e rastreabilidade.

A presença do nome no README **não significa que exista hoje um score ou source module v2 pronto**.

### 32.11. Consumidor.gov — INFRAESTRUTURA LEGADA/COMPLEMENTAR, PAPEL v2 ABERTO

O repositório contém coletor/agregação/matching históricos para Consumidor.gov. Isso não equivale a aprovação metodológica na v2.

Antes de reutilizar essa infraestrutura no segundo pilar será necessário revalidar identidade, cobertura, amostra, denominadores, semântica de resolução/satisfação e eventual duplicidade com outros canais. Seu papel definitivo permanece **EM VALIDAÇÃO**.

### 32.12. Hierarquia de autoridade

Quando duas fontes respondem perguntas diferentes, a v2 preserva as dimensões em vez de escolher arbitrariamente uma “fonte vencedora”:

```text
FIP / licença / tipo / regime atual      → SUSEP
atividade e evidência financeira         → SUSEP / SES
CNPJ jurídico e situação cadastral       → Receita Federal como cross-check
grupo econômico observado                → SUSEP / SES
sucessão societária                      → relação explicitamente verificada
marca / risk carrier                     → relação verificável
conduta/reclamações                       → ainda em investigação
```
'''
)

# Workflows: current branch has distinct validation, research, and refresh paths.
replace_section(
    "## 35. Workflows e automação",
    "## 36. Sequência de implementação",
    r'''## 35. Workflows e automação — estado atual

A v2 já possui workflows separados para não misturar ingestão, validação e pesquisa metodológica.

### 35.1. Validações automáticas do PR

Estão implementados:

- `CI` — lint dos Python alterados + suíte pytest;
- `V2 Foundation Validation` — identidade SES/FIP e invariantes básicos;
- `V2 Classification Validation` — licenciadas, regimes especiais e Sandbox;
- `V2 Lifecycle Relationships Validation` — Receita, sucessões, grupos, marcas e query context;
- `V2 Eligibility Validation` — universo regulatório e gates;
- `V2 Financial Evidence Validation` — evidência financeira e maturidade de competência;
- `V2 Liquidity Experiment` — diagnóstico reproduzível ILC/ILT na competência madura;
- `V2 Operating Experiment` — IC/ICA e estados operacionais experimentais.

Esses workflows geram artifacts internos de validação; não publicam score v2 nem abrem o ranking.

### 35.2. Experimentos preservados, fora do caminho automático

Dois estudos foram mantidos para auditabilidade, mas não devem continuar produzindo novas iterações por simples push:

- `V2 Liquidity Transform Experiment` — `workflow_dispatch` manual;
- `V2 ILPL Closed Experiment` — `workflow_dispatch` manual, com critérios pré-registrados e veredito já encerrado.

A preservação do código não significa que a investigação continue aberta.

### 35.3. Atualização da Receita

`Refresh Receita lifecycle` é um fluxo operacional independente, manual e agendado semanalmente. Ele reutiliza a BaseCompleta SUSEP já validada para construir o universo-alvo e atualiza somente o snapshot filtrado do CNPJ após os gates de cobertura/regressão.

### 35.4. Princípio operacional

A infraestrutura existente de cache, fallback, snapshots, testes, refresh e auditoria deve ser preservada e simplificada quando possível. O objetivo é reduzir complexidade acidental sem perder rastreabilidade.
'''
)

# Roadmap: reflect implemented foundation instead of leaving everything unchecked.
replace_section(
    "## 36. Sequência de implementação",
    "## 37. Testes quantitativos obrigatórios antes de fechar a metodologia",
    r'''## 36. Sequência de implementação — estado real

A ordem continua sendo parte do contrato, mas o checklist abaixo reflete o que já existe na branch.

### Fase 0 — README e contrato — CONCLUÍDA NESTA ETAPA

- [x] definir objetivo e fronteiras do produto;
- [x] separar GitHub e frontend;
- [x] estabelecer princípios metodológicos;
- [x] definir arquitetura preliminar da API v2;
- [x] registrar decisões versus pontos em validação.

### Fase 1 — Fontes e ingestão — IMPLEMENTAÇÃO SUBSTANCIAL / SEGUNDO PILAR PENDENTE

- [x] mapear BaseCompleta, LISTAEMPRESAS e chaves FIP/CNPJ;
- [x] mapear fontes SUSEP de licenciadas, regimes especiais e Sandbox;
- [x] implementar leitura financeira filtrada e períodos;
- [x] implementar Receita CNPJ bulk filtrada com descoberta de competência;
- [x] medir cobertura e proteger quedas anormais nas fontes já integradas;
- [x] classificar o papel das fontes atuais como regulatória, jurídica, financeira ou contextual;
- [ ] inventariar e validar as fontes do pilar de conduta/reclamações.

### Fase 2 — Identidade, classificação e lifecycle — IMPLEMENTADA EM DRAFT

- [x] FIP como chave regulatória preferida;
- [x] CNPJ como pessoa jurídica separada da identidade regulatória;
- [x] suporte v2 a CNPJ numérico/alfanumérico;
- [x] classificação corrente por fontes oficiais SUSEP;
- [x] regimes especiais separados;
- [x] Sandbox separado como `sandbox_participant`;
- [x] Sandbox-only por CNPJ quando não existe FIP publicado;
- [x] lifecycle Receita separado do status regulatório;
- [x] testes de unicidade, conflito e invariantes;
- [x] preservar divergências em vez de escondê-las.

**Estado:** base de identidade/classificação pronta para sustentar os próximos gates; ainda é versionada como draft até o fechamento da v2.

### Fase 3 — Relationships, grupos, marcas e resolvedor — IMPLEMENTADA EM DRAFT / CURADORIA AINDA EXPANSÍVEL

- [x] separar marca e entidade regulatória;
- [x] resolver `risk_carrier` por relação verificável;
- [x] impedir marca de herdar score;
- [x] aplicar sucessões explicitamente verificadas;
- [x] resolver cadeias de sucessão no backend;
- [x] integrar histórico de grupos econômicos SES sem inferir sucessão;
- [x] impedir buckets genéricos de criarem grupos corporativos falsos;
- [x] criar query buckets para insurer/Sandbox/historical/special/pension/capitalization/other;
- [ ] ampliar relações verificadas quando novos casos exigirem;
- [ ] medir e reduzir dependência de curadoria antes da publicação definitiva.

### Fase 4 — Matriz quantitativa — FINANCEIRO CONCEITUALMENTE FECHADO; CONDUTA É O PRÓXIMO TRABALHO

- [x] delimitar o universo regulatório elegível;
- [x] implementar financial evidence sem score;
- [x] distinguir competência observada de competência madura;
- [x] investigar PLA/CMR e confirmar capital como eixo principal;
- [x] investigar ILC/ILT e selecionar ILT como referência principal de liquidez;
- [x] investigar efeito de porte/segmentação e extremos de denominador;
- [x] investigar IC/ICA e implementar filme operacional experimental;
- [x] executar investigação fechada do ILPL e rejeitá-lo como eixo independente de scoring;
- [x] encerrar a procura por novos eixos financeiros;
- [ ] calibrar transformação de PLA/CMR;
- [ ] calibrar transformação/saturação de ILT;
- [ ] fechar como o filme operacional interfere em cautela/confiança sem terceiro score;
- [ ] inventariar fontes do pilar de conduta;
- [ ] mapear identidade, taxonomia, desfechos, denominadores e histórico de reclamações;
- [ ] pré-registrar e testar critérios de sobrevivência dos sinais de conduta.

### Fase 5 — Pesos e score — PENDENTE

Só começa depois que os dois pilares possuírem semântica, cobertura e estabilidade compreendidas.

- [ ] calibrar peso Capital × Liquidez;
- [ ] testar peso Financeiro × Conduta;
- [ ] executar análise de sensibilidade;
- [ ] comparar rankings e casos individuais;
- [ ] fechar `assessment_confidence`;
- [ ] definir faixas verbais e limitações.

### Fase 6 — Schema v2 definitivo — PENDENTE

- [ ] revisar necessidade real de cada campo público;
- [ ] congelar nomes e versionamento;
- [ ] escrever JSON Schemas e fixtures;
- [ ] validar `reason_codes` finais;
- [ ] remover exemplos que deixarem de representar o contrato aprovado.

### Fase 7 — Builders v2 — FUNDAÇÃO INTERNA IMPLEMENTADA; PUBLICAÇÃO FINAL PENDENTE

Já existem builders internos para:

```text
identity inventory
classification inventory
lifecycle + relationships inventory
eligibility inventory
financial evidence inventory
liquidity experiment
operating experiment
ILPL closed experiment
```

Ainda faltam os builders definitivos de assessment/ranking e os quatro contratos públicos finais:

- [ ] `meta.json`;
- [ ] `entities.json`;
- [ ] `brands.json`;
- [ ] `rankings.json`.

### Fase 8 — Testes e auditoria — SUBSTANCIALMENTE IMPLEMENTADA NA FUNDAÇÃO

Já existem testes para identidade, classificação, Sandbox, lifecycle Receita, relationships, elegibilidade, evidência financeira, maturidade, liquidez, operação e ILPL.

Continuam pendentes os testes que só podem existir após score/schema final:

- [ ] JSON Schema público;
- [ ] regressões de score;
- [ ] regressões de ranking;
- [ ] snapshots comparativos da avaliação final;
- [ ] comportamento do pilar de conduta com fonte indisponível/fallback.

### Fase 9 — Publicação dos JSONs — PENDENTE

- [ ] publicar `/api/v2/`;
- [ ] preservar `/api/v1/` durante a transição;
- [ ] validar staging e rollback.

### Fase 10 — Frontend no site — PENDENTE E FORA DA ARQUITETURA DO REPO

No site, PHP recebe dados semanticamente prontos; HTML/CSS/JavaScript cuidam da experiência. Nenhuma regra de scoring ou matching é reconstruída ali.

### Fase 11 — Migração — PENDENTE

- [ ] ativar v2 em produção;
- [ ] observar erros e comparar comportamento;
- [ ] remover dependência do bundle legado;
- [ ] desativar v1 quando seguro.

### Fase 12 — Limpeza — PENDENTE

- [ ] remover frontend legado do alvo final;
- [ ] remover aliases e compatibilidade temporária;
- [ ] remover código morto/experimentos que não precisem ser preservados para auditoria;
- [ ] simplificar workflows e dependências.

### Fase 13 — Revisão final deste README — PENDENTE

Ao final da refatoração, transformar os elementos ainda marcados como draft/calibração em documentação definitiva e registrar fórmulas, fontes finais, schema, deployment e limitações conhecidas.
'''
)

# Open questions: make legal/source boundaries explicit and avoid stale generic labels.
text = text.replace(
    "- fonte ou conjunto de fontes definitivo;\n",
    "- fonte ou conjunto de fontes definitivo (SusepCon/BDR deve ser investigado primeiro; Consumidor.gov permanece candidato complementar);\n",
)

path.write_text(text, encoding="utf-8")
print("README synchronized with current refactor/v2-data-foundation implementation")
