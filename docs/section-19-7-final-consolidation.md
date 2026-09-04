# §19.7 — Lapidação final e consolidação da v2

Data: 04/09/2026.

Status técnico do branch: **PRONTO PARA INSTALAÇÃO E QA NO STAGING**.

Recomendação binária de cutover: **NOT READY**.

O blocker remanescente não é uma lacuna de implementação conhecida: é a ausência
da prova final no ambiente real depois que o pacote R5 for instalado no HostGator.
Até existirem evidências de PHP/headers/DOM e QA desktop/mobile sobre essa mesma
geração, o §19.7 não pode recomendar o cutover de produção.

## 1. Fontes reconciliadas

A revisão comparou quatro estados concretos:

1. o branch `refactor/v2-data-foundation`;
2. a cópia recebida de `public_html` do HostGator;
3. o frontend R4.1 efetivamente instalado como `index2.php`;
4. a última geração pública Gate 4 disponível para diagnóstico.

A cópia do HostGator foi tratada como evidência de dependências externas, não como
fonte automática de verdade. Nenhum arquivo de produção foi alterado por esta
revisão.

## 2. Drifts necessários

| Drift objetivo | Evidência | Correção executada no branch |
|---|---|---|
| frontend validado no HostGator não estava versionado | `index2.php`, `ranking-v2.js`, `ranking-v2.css` e testes existiam somente na cópia de `public_html` | superfície final importada para `ranking-seguradoras/` e coberta pelo CI |
| staging produzia diretivas robots conflitantes | `index2.php` enviava X-Robots/meta `noindex`, enquanto `head-global.php` fixava `index, follow` | `head-global.php` agora aceita `$page_robots`, preserva o default legado e emite uma única meta; staging mantém X-Robots noindex |
| dados instalados estavam misturados/desatualizados | índices tinham datas/populações diferentes e não havia `distribution_manifest.json` | frontend passa a exigir manifesto, conjunto de arquivos da mesma geração, tipo de artifact e SHA-256 antes de renderizar cada JSON |
| frontend e geração final não eram testados juntos na entrega | workflow validava dados e frontend em trilhas separadas | regressão R2 recebe o `search_index.json` exato; o procedimento de release a executa sobre o artifact do Full Generation antes de montar o ZIP |
| cópia pública expunha vocabulário interno | centenas de `quick_answer` continham “v2”, “projeto” ou “snapshot”; contexto Youse continha “widget” | builders reescritos em linguagem do leitor e validator/teste impedem regressão |
| seis resumos públicos de Conduta perderam acentos | strings como “Ha mais reclamacoes...” saíam do builder | textos corrigidos e parametrizados em teste |
| limpeza §19.5 classificava incorretamente o bundle v1 como removível | hashes de `widget.js`/`widget.css` do Hostgator coincidem com `widget-ui/dist`; `index.php` ainda os consome | `dist` restaurado e reclassificado como LEGACY até o cutover e a aposentadoria comprovada do v1 |
| controles podiam ser usados antes de a geração íntegra estar pronta | carregamento parcial não tinha um estado de prontidão único | controles iniciam bloqueados; `aria-busy`/`data-load-state` só liberam a UI após os três índices íntegros |
| falha de pacote não oferecia recuperação clara | erro interrompia a inicialização sem ação direta | estado fail-closed com mensagem e botão “Tentar novamente” |
| abertura de perfil não estabelecia foco útil | usuário de teclado permanecia no controle anterior | título do perfil recebe foco programático sem deslocamento duplo |

Classificação final: **necessários executados no código**. A instalação e a prova
no HostGator permanecem necessárias antes de mudar a recomendação de cutover.

## 3. Drifts recomendados

| Item | Estado |
|---|---|
| cache bust dos assets R5 (`?v=15`) | executado |
| candidato de produção separado do staging | executado em `deployment/production-cutover/` |
| redirect 301 de parâmetros legados para estado por fragmento | preparado, não instalado |
| checklist explícito de backup, instalação, QA e rollback | executado |
| validação permanente de head, CSS, JS, helpers, rotas e integridade | executado |
| preservar staging em `noindex` durante observação pós-cutover | preparado no roteiro |

Esses itens reduzem risco operacional e dívida objetiva sem reabrir metodologia ou
transformar o §19.7 em redesign.

## 4. Itens opcionais

Não bloqueiam staging nem cutover quando os gates necessários passarem:

- criar um lockfile Node em uma mudança própria de toolchain;
- ampliar a matriz manual para versões adicionais de navegadores/dispositivos;
- acrescentar monitoramento sintético periódico do staging;
- aplicar política CSP mais restritiva após inventário completo de scripts globais;
- remover arquivos v1 somente depois da janela de rollback;
- ativar sensores evergreen após a decisão operacional correspondente.

Nenhum desses itens foi usado como justificativa para alteração especulativa.

## 5. Áreas deliberadamente não tocadas

- fórmulas, pesos e metodologia fechada;
- score geral ou ranking geral;
- `main` e merge do Draft PR;
- `index.php` da produção HostGator;
- cron/atualizador v1;
- cron/publicação automática v2;
- sensores evergreen;
- symlink ou filesystem real do HostGator;
- exclusão dos assets v1.

## 6. Contrato de dados do frontend final

O navegador segue esta ordem:

```text
distribution_manifest.json
  -> valida build/política/lista de arquivos
  -> search_index.json + insurer_explorer.json + explore_index.json
  -> perfis, leaderboards e coleções sob demanda
  -> valida caminho + SHA-256 + artifact antes de usar
```

O JavaScript não:

- recalcula classificação metodológica;
- combina Financeiro e Conduta em score;
- converte ausência em zero;
- corrige texto ou regra produzida pelo backend;
- aceita arquivo fora da geração manifestada;
- escolhe silenciosamente uma entidade ambígua.

## 7. Auditoria SEO final

| Controle | Staging | Candidato de produção |
|---|---|---|
| canonical | `https://sanida.com.br/ranking-seguradoras/` | mesmo canonical |
| meta robots | uma única `noindex, follow` | uma única `index, follow, max-image-preview:large` |
| X-Robots-Tag | `noindex, follow` | ausente |
| estados da aplicação | fragmentos `#perfil`, `#comparar`, `#consulta` | mesmos fragmentos |
| parâmetros legados | apenas compatibilidade de leitura no staging | 301 para o hub + fragmento |
| URLs por marca | não geradas | não geradas |
| dados estruturados | `WebPage` + `WebApplication` | mesmos tipos, URL do hub |
| headings | um H1; seções e perfil com hierarquia própria | igual |

Essa arquitetura concentra crawlability no hub e evita explosão de URLs por marca,
sem transformar demanda de busca em justificativa editorial automática.

## 8. Provas permanentes adicionadas

Evidência executada para esta consolidação:

```text
commit de implementação         a2fbded4ecffd30fd6095eefd50a982e27846be7
head do PR após lint mecânico    5ec2b4e6b7e9aa1f6dd551001671bc6535b21e75
CI #1479                         success
Evergreen Contract #319         success
Full Generation #69             success
workflow run                     33829195597
artifact                         9921622757
artifact ZIP SHA-256             0677bd2bf87c2b503c19871972fc9417f453fa548b529c9bb83ecea3bb1a4ad9
build_id                         v2-gate4-full-33829195597-a1
logical package SHA-256          be7c1da75a7cbfe14de836c97c2b0ecacb0703eeb89f18ddf647bd80d2bfa502
public JSON files                805
search profiles                  791
ordinary current insurers        156
ranking_eligible                 0
watchdog_blocking_drift          0
```

O commit `5ec2b4e` é descendente direto e altera somente lint mecânico em dois
scripts de teste estático. Geradores, contratos, PHP, CSS e JavaScript entregues
são exatamente os de `a2fbded`, que é o `source_head_sha` gravado no manifesto.

A regressão R2 foi executada novamente sobre o `search_index.json` baixado do
artifact #69: 791 entradas, 156 seguradoras ordinárias e 6 candidatos Allianz.
O instalador verificou todos os hashes e o hash lógico do pacote. A varredura da
cópia pública encontrou zero ocorrências de “v2”, “projeto”, “widget” e “snapshot”.

```text
ranking-seguradoras/tests/r2-static-check.py
ranking-seguradoras/tests/r2-regression.mjs
ranking-seguradoras/tests/r3-technical-helper.mjs
ranking-seguradoras/tests/r4-1-head-structure.py
ranking-seguradoras/tests/r4-css-structure.py
ranking-seguradoras/tests/r5-public-integrity.mjs
tests/test_v2_public_copy_quality.py
tests/test_v2_repository_cleanup.py
```

O CI comum valida a superfície versionada. O Full Generation existente valida a
geração e sua instalação/rollback; no release, o artifact desse run é baixado e a
regressão do frontend é executada contra seu `search_index.json` exato.

## 9. Matriz de QA obrigatória no ambiente real

O roteiro `INSTALLACAO-HOSTGATOR.md` contém a execução detalhada. O gate exige:

- PHP lint no release e no destino;
- `sha256sum -c` do pacote entregue;
- verificação completa do `distribution_manifest` pelo instalador;
- header X-Robots e DOM do staging sem conflito;
- busca simples e ambígua;
- Youse e outras identidades com relação marca/empresa/portador do risco;
- registros ordinário, histórico, Sandbox e cooperativa;
- helpers, comparação 2–4, lista, filtros, leaderboards e coleções;
- compartilhamento e Back/Forward;
- teclado e foco;
- desktop e mobile;
- falha de rede/pacote em estado fail-closed;
- Console sem erro e rede sem 404.

## 10. Cutover e rollback preparados

O pacote de entrega separa fisicamente:

```text
staging/                         instalar agora para QA
data-package/public/            instalar atomicamente
cutover-NAO-INSTALAR-AINDA/     guardar; exige autorização
tools/                           verificador/instalador/rollback
```

O rollback tem duas camadas:

1. restaurar os quatro arquivos de frontend/head a partir do backup;
2. trocar `current` e `previous` pelo instalador, ou restaurar o diretório público
   original na primeira migração.

A produção v1 e seu atualizador permanecem disponíveis durante a janela de
observação. Aposentadoria do v1/cron é mudança posterior e separada.

## 11. Decisão

```text
implementação do branch       PASS
contratos e regressões locais PASS
Full Generation #69           PASS
pacote de instalação          VERIFICADO / PREPARADO
produção/main/cron/sensores   INTOCADOS
QA real após instalação       PENDENTE
recomendação de cutover       NOT READY
```

Condição objetiva para reavaliar como **READY FOR CUTOVER**: instalar exatamente o
pacote R5 no staging, executar integralmente o roteiro e anexar evidências PASS de
headers, DOM, manifesto, matriz funcional, desktop/mobile e rollback. Isso ainda
não autoriza o cutover; apenas remove o blocker técnico do gate §19.7.
