# §19.3–§19.4 — Evergreen, zero manutenção, cron e publicação HostGator

Status: **FECHADO COMO ARQUITETURA E IMPLEMENTAÇÃO; FRONTEND R5.3 EM PRODUÇÃO; AUTOMAÇÃO DE DADOS NÃO ATIVADA NO DRAFT**.

Data: 02/09/2026.

Hardening SSH pós-cutover: 05/09/2026.

Este documento fecha conjuntamente os itens §19.3 e §19.4 porque o mecanismo de cron só é seguro se consumir exatamente o mesmo contrato evergreen de geração e distribuição.

O objetivo não é mover o frontend para GitHub Actions. O frontend continua servido no HostGator. O que passa a ser automatizável é **a geração e a sincronização do pacote público de dados v2**.

---

## 1. Diagnóstico anterior

O Gate 4 já havia resolvido:

```text
uma publicação = uma geração
uma geração = um build_id
um build_id = um source_head_sha + run/attempt identificável
```

Também já existiam:

- DAG canônico de 25 stages;
- `distribution_manifest.json` com hash por arquivo e hash lógico do pacote;
- verificação fail-closed;
- instalação em diretório versionado;
- ponteiros `current` e `previous`;
- rollback testado;
- caches de fontes com hash e lineage;
- Full Generation como única prova integrada de dados/publicação.

A lacuna operacional era posterior à geração:

```text
Full Generation verde
→ artifact GitHub Actions por 14 dias
→ download/manualização ainda necessário para chegar ao HostGator
```

Isso não atendia ao objetivo de zero manutenção nem definia o cron final de produção.

---

## 2. Auditoria de fontes e caches

O caminho canônico usa `api/v2/source_cache.py` e `api/v2/source_snapshot.py`.

Semântica preservada:

```text
current fetch + validação ok -> fresh
current fetch falha + cache anterior íntegro/validado -> stale
sem fetch atual utilizável e sem cache validável -> unavailable
```

O fallback não reescreve `fetched_at` para parecer atual. O timestamp original e o hash do snapshot permanecem na lineage.

O cache é aceito somente depois de verificações como:

- `source_id` esperado;
- URL esperada;
- SHA-256 esperado;
- materialização local bem-sucedida;
- validação estrutural específica da fonte.

A fonte Receita possui ainda contrato próprio de compatibilidade, período e hash do universo regulatório.

Decisão operacional preservada:

> **stale validado é stale; nunca vira fresh por ter sido reutilizado. unavailable não pode ser disfarçado como zero, ausência econômica ou snapshot atual.**

Não foi introduzido um TTL arbitrário nesta consolidação. A idade real continua disponível em `fetched_at` e a política metodológica de aceitabilidade permanece no Gate 4. Alterar um limite temporal seria decisão de metodologia/operação de fonte, não limpeza de publicação.

---

## 3. Recuperação quando fonte oficial estiver fora do ar

Fluxo automático:

```text
1. tentar fonte oficial atual
2. validar resposta atual
3. se falhar, tentar cache anterior identificado e hash-validado
4. se cache for utilizável, lineage = stale
5. se cache também falhar ou faltar, lineage = unavailable
6. Full Generation só pode prosseguir dentro dos contratos Gate 4
7. somente uma Full Generation concluída com success pode chegar ao publicador
```

Consequência importante:

> **falha de fonte nunca exige apagar ou substituir a geração já servida no HostGator.**

Se a nova Full Generation não produzir um pacote validado, o workflow de publicação não recebe autorização para trocar `current`. A geração anterior continua ativa.

Esse comportamento é preferível a publicar JSON parcial, vazio ou de gerações misturadas.

---

## 4. Dependências operacionais finais

### Validação de desenvolvimento/PR

```text
CI
→ Ruff
→ pytest
→ build Vite de integridade

V2 Gate 4 Evergreen Contract
→ estrutura do DAG
→ publication readiness
→ guardrails operacionais
```

O build Vite continua sendo **validação de compilabilidade**, não deploy do frontend ao HostGator.

### Geração integrada

```text
V2 Gate 4 Full Generation Proof
→ aquisição/cache
→ 25-stage same-generation DAG
→ validação de lineage
→ pacote público único
→ prova local de install/rollback
→ artifact exato do próprio run
```

### Automação de produção preparada

```text
V2 Production Generation Schedule
→ dispara a Full Generation canônica em main

Full Generation success em main
→ V2 HostGator Public Package Sync
→ baixa SOMENTE o artifact do run que disparou o evento
→ verifica build_id + source_head_sha + package_sha256
→ envia a geração exata por SSH
→ verifica novamente no HostGator
→ instala via mesmo install_public_generation.py
→ current muda atomicamente
→ previous é preservado
```

Não existe dependência do tipo:

```text
gh run list -> pegar latest successful
```

nem combinação de artifacts independentes.

---

## 5. Workflow de publicação HostGator

Arquivo:

`.github/workflows/v2-hostgator-publication.yml`

O workflow aceita somente:

1. `workflow_run` da **V2 Gate 4 Full Generation Proof** com `success` e `head_branch = main`; ou
2. republicação manual de um `source_run_id` específico, que é novamente consultado pela API e precisa provar:
   - workflow correto;
   - `conclusion = success`;
   - `head_branch = main`;
   - `head_sha` válido;
   - `run_attempt` identificável.

O caminho automático por `workflow_run` exige `V2_HOSTGATOR_DEPLOY_ENABLED=true`. O caminho manual é deliberadamente independente desse gate automático, mas exige `workflow_dispatch`, um `source_run_id` exato e confirmação literal `confirm_publication=PUBLISH`. Assim é possível provar uma publicação manual controlada mantendo a publicação automática desligada.

O artifact esperado é derivado deterministicamente:

```text
v2-gate4-full-generation-<run_id>-a<run_attempt>
```

O `build_id` esperado também é derivado deterministicamente:

```text
v2-gate4-full-<run_id>-a<run_attempt>
```

Antes de qualquer SSH, a geração baixada é verificada localmente por:

`api/v2/install_public_generation.py verify`

O publicador compara ainda:

- `manifest.build.build_id`;
- `manifest.build.source_head_sha`;
- `public_package.package_sha256`.

Portanto, nem um artifact de outro run com nome semelhante pode ser instalado silenciosamente.

---

## 6. Transporte SSH

A implementação não usa `ssh-keyscan` durante cada publicação como fonte implícita de confiança.

A confiança do host deve ser previamente configurada no secret:

`V2_HOSTGATOR_KNOWN_HOSTS`

O workflow usa:

```text
BatchMode=yes
IdentitiesOnly=yes
StrictHostKeyChecking=yes
UserKnownHostsFile=<arquivo controlado pelo workflow>
GlobalKnownHostsFile=/dev/null
UpdateHostKeys=no
```

O material é escrito somente em um diretório efêmero sob `RUNNER_TEMP`, criado com `umask 077`. Antes da conexão, o workflow:

- valida que `V2_HOSTGATOR_SSH_KEY` contém uma chave privada válida e não interativa;
- valida a sintaxe das chaves públicas de host;
- exige que `V2_HOSTGATOR_KNOWN_HOSTS` contenha uma entrada para o host e a porta configurados;
- não usa `ssh-keyscan` como fonte de confiança durante a publicação;
- remove chave e `known_hosts` do diretório efêmero com `if: always()`.

Secrets necessários para a futura publicação controlada:

```text
V2_HOSTGATOR_HOST
V2_HOSTGATOR_USER
V2_HOSTGATOR_SSH_KEY
V2_HOSTGATOR_KNOWN_HOSTS
```

Variables necessárias:

```text
V2_HOSTGATOR_SSH_PORT             # opcional; default operacional 22
V2_HOSTGATOR_PUBLICATION_ROOT     # caminho absoluto privado/operacional
V2_HOSTGATOR_PUBLIC_PATH          # caminho absoluto servido como /ranking-seguradoras/data/v2/public
V2_HOSTGATOR_DEPLOY_ENABLED       # precisa ser "true" somente para publicação automática por workflow_run
V2_PRODUCTION_AUTOMATION_ENABLED  # precisa ser "true" para o cron disparar geração
```

A publicação manual controlada não exige `V2_HOSTGATOR_DEPLOY_ENABLED=true`; ela exige `workflow_dispatch`, `source_run_id` exato e `confirm_publication=PUBLISH`.

Nenhum valor de credencial é versionado no repositório.

A auditoria pós-cutover também confirmou que não existem chave privada, `known_hosts` operacional ou arquivo SSH rastreado. O repositório contém somente a lógica e os nomes dos secrets.

Provisionamento concluído em 05/09/2026:

```text
DEDICATED_HOSTGATOR_ACTIONS_KEY_LOGIN  PASS
V2_HOSTGATOR_SECRETS_CONFIGURED       true
V2_HOSTGATOR_VARIABLES_CONFIGURED     true
V2_HOSTGATOR_DEPLOY_ENABLED           false
V2_PRODUCTION_AUTOMATION_ENABLED      false
V2_MARKET_SENSOR_AUTOMATION_ENABLED   false
GITHUB_ACTIONS_SSH_RUNTIME_VALIDATION NOT_EXECUTED
```

A chave dedicada foi validada em autenticação pública não interativa diretamente contra o HostGator. A última linha não representa falha: o consumo dos secrets pelo runner permanece deliberadamente não executado enquanto o workflow não estiver no branch default e não houver autorização do gate operacional.

---

## 7. Instalação no HostGator

Arquivos envolvidos:

```text
api/v2/install_public_generation.py
ops/hostgator/install_v2_public_remote.sh
```

O pacote chega primeiro a:

```text
<PUBLICATION_ROOT>/incoming/<build_id>/package
```

Antes da troca pública, o host executa novamente:

```text
verify(package)
```

Depois valida o `build_id` e o `package_sha256` esperados pelo workflow de origem.

Somente então executa:

```text
install(package, PUBLICATION_ROOT)
```

Layout final:

```text
PUBLICATION_ROOT/
├── generations/
│   ├── <build anterior>/
│   └── <build atual>/
├── current  -> generations/<build atual>
├── previous -> generations/<build anterior>
├── tools/
│   └── install_public_generation.py
└── last-install.json
```

O instalador persistido em `tools/` permite rollback mesmo se GitHub estiver temporariamente indisponível.

---

## 8. URL pública sem publicação parcial

A URL consumida pelo frontend continua:

```text
/ranking-seguradoras/data/v2/public/
```

No filesystem do HostGator, `V2_HOSTGATOR_PUBLIC_PATH` deve ser provisionado **uma única vez** como symlink para:

```text
V2_HOSTGATOR_PUBLICATION_ROOT/current
```

Exemplo conceitual — os caminhos reais dependem da conta e não são versionados:

```bash
mkdir -p /CAMINHO/OPERACIONAL/v2-publication/{generations,incoming}
ln -s /CAMINHO/OPERACIONAL/v2-publication/current \
      /CAMINHO/PUBLIC_HTML/ranking-seguradoras/data/v2/public
```

O script remoto exige que esse symlink já exista e aponte para `current`. Ele **não cria nem substitui automaticamente o caminho público**. Isso mantém o primeiro cutover como ação deliberada.

Depois do cutover, trocar `current` por `os.replace()` do symlink é atômico. O navegador não observa uma fase em que metade dos JSONs pertença à geração antiga e metade à nova.

---

## 9. Rollback

A instalação de uma nova geração mantém a anterior em `previous`.

Rollback no HostGator:

```bash
python3 "$V2_HOSTGATOR_PUBLICATION_ROOT/tools/install_public_generation.py" \
  rollback "$V2_HOSTGATOR_PUBLICATION_ROOT"
```

O comando verifica as duas gerações antes da troca.

Como a URL pública aponta para `current`, o rollback não exige mover centenas de JSONs nem alterar o frontend.

O teste de integração `tests/test_v2_hostgator_publication.py` prova:

- instalação inicial;
- segunda geração;
- preservação de `previous`;
- resolução contínua do caminho público;
- rollback com o instalador persistido;
- rejeição de `package_sha256` inesperado antes da troca.

---

## 10. Cron de produção

Arquivo:

`.github/workflows/v2-production-generation-schedule.yml`

Cadência inicial preparada:

```text
segunda-feira 09:17 UTC
segunda-feira 06:17 America/Sao_Paulo enquanto UTC-3
```

A escolha semanal evita executar diariamente uma geração integrada longa para fontes cuja atualização relevante é muito mais lenta. A cadência pode ser alterada posteriormente sem mudar a arquitetura de publicação.

O cron não executa o DAG por conta própria. Ele dispara:

```text
v2-gate4-full-generation-proof.yml --ref main
```

Assim permanece uma única implementação canônica da geração.

O cron só funciona quando:

```text
V2_PRODUCTION_AUTOMATION_ENABLED = true
```

---

## 11. Estado pós-cutover do frontend

O mecanismo de automação não executou o cutover. O frontend R5.3 foi promovido manualmente em 05/09/2026 após autorização separada e permanece independente dos gates de geração/publicação.

Enquanto o PR #1 estiver Draft e as variáveis de habilitação não estiverem deliberadamente ligadas:

```text
frontend R5.3 em produção            PASS
v1                                   preservada como backup de rollback
/ranking-seguradoras/data/v2/public  aponta para a geração R5 aprovada
cron de produção v2                  desabilitado por gate
publicação SSH automática            desabilitada por gate
publicação SSH manual                somente dispatch explícito + confirmação PUBLISH
sensores evergreen                   desabilitados por gate
```

Além disso, eventos `workflow_run` e `schedule` tornam-se operacionais a partir da presença desses workflows no branch default. Portanto a definição versionada pode ser validada agora sem transformar o branch de refatoração em produção.

---

## 12. Falhas e efeito esperado

| Falha | Efeito |
|---|---|
| fonte atual falha, cache válido | geração pode prosseguir com lineage `stale` conforme Gate 4 |
| fonte e cache inutilizáveis | Full Generation não autoriza publicação |
| Full Generation falha | workflow HostGator não publica |
| artifact exato expirou em republicação manual | republicação falha; não procura outro run |
| build/head/hash divergem | publicação falha antes do SSH |
| SSH host key diverge | conexão falha |
| upload fica parcial | `verify` remoto falha; `current` não muda |
| symlink público não está previamente correto | instalação remota falha antes da troca |
| pacote remoto adulterado | hash/manifest falham |
| nova geração instalada mas precisa ser revertida | `previous` + instalador persistido permitem rollback |

---

## 13. Decisão de fechamento

### §19.3 — Evergreen / zero manutenção

**Fechado no nível de arquitetura e código.**

Foram resolvidos:

- dependência operacional canônica;
- consumo automático do artifact exato;
- eliminação do download manual no caminho de produção preparado;
- sincronização HostGator fail-closed;
- staging e rollback;
- recuperação documentada para indisponibilidade de fonte;
- cron preparado sem duplicar o DAG.

### §19.4 — Cron jobs e publicação

**Fechado no nível de arquitetura e código.**

`/ranking-seguradoras/data/v2/public/` passa a ter um mecanismo definido para resolver sempre a **uma geração inteira** por meio do ponteiro `current`.

O cron nunca tenta coordenar artifacts independentes.

### Próxima ativação operacional

O cutover do frontend está concluído. A automação de dados permanece deliberadamente pendente de:

1. merge futuro autorizado em `main`;
2. validação do consumo dos secrets/variables pelo runner, sem relaxar a pinagem de host;
3. Full Generation bem-sucedida em `main`;
4. autorização separada para publicação manual controlada dessa geração;
5. prova do comportamento de `previous` e do rollback normal do instalador;
6. somente depois, autorização separada para habilitar os gates de automação.

Isso é uma pendência de ativação de produção, não uma lacuna no mecanismo versionado.

O próximo item da consolidação é **§19.5 — Limpeza do repositório**.
