# A09.6 — Prefetch controlado de duas partições (homologado, não ativado)

## Situação

**Implementação homologada por prova isolada de 18/11/2026, DESLIGADA por padrão.** O objetivo é
reduzir o tempo de transferência dos arquivos oficiais da Receita, sem
modificar ordem de processamento, regras de identificação, Conduta, SUSEP ou
formação do pacote público.

Prova A09.5 de 32 MiB, [run #44002733900](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/44002733900):
ganhos observados de 2,042× e 2,024× na vazão de duas conexões. Essa prova
usou apenas prefixos; **não é homologação de downloads completos**.

## Funcionamento proposto

- Na fase de **lifecycle**, antecipar duas partições distintas
  `Estabelecimentos0.zip` e `Estabelecimentos1.zip`, descarregando em
  diretório temporário privado da geração, com dois workers no máximo.
  O parser segue exatamente a ordem original; ao consumir cada arquivo,
  chama a função **A09.4 `acquire_run_establishment_zip`** como antes.
  A09.4 armazena SHA-256 local e reutiliza os ZIPs validados na fase de
  identidade, sem nova transferência.
- Na fase de **identidade**, antecipar `Empresas` (duas partições na janela),
  sem alterar o parser nem a correspondência por CNPJ/denominação.
  `Estabelecimentos` continua pelo cache A09.4 **sem prefetch remoto adicional**.
- Só haverá GETs adicionais no modo explicitamente habilitado.
  `RECEITA_A096_PREFETCH=1` opta pela experiência. Ausência do valor
  `1` significa execução **sequencial original**.
- A janela tem no máximo **duas partições**, programadas em ordem:
  0+1; depois que 0 é processada e removida, programa 2; e assim por diante.
  A operação de parsing é sempre uma única thread.
- Cada worker possui sessão HTTP exclusiva. Não usa HTTP Range nem API paga.
  Em 200 OK, valida Content-Length, ETag e Last-Modified quando informados,
  tamanho efetivo do arquivo, ZIP central directory e SHA-256 local do
  conteúdo recebido. O arquivo é novamente lido e comparado ao digest
  antes de ser entregue à thread de parsing. O SHA-256 não é apresentado
  como checksum publicado pela Receita — é um selo **local**.
- O total especulativo de dois arquivos não supera **4 GiB** e exige ao
  menos **4 GiB livres adicionais** no disco. Sem espaço, documentação
  insuficiente, tempo excedido, conexão falha, metadado divergente ou ZIP
  inválido, o programa usa o caminho de aquisição **sequencial**.
  O arquivo de fallback só é aceito se tamanho e ZIP estiverem íntegros
  quando metadados oficiais são confiáveis; caso contrário, falha fechada.
- Ao sair cedo ou ocorrer uma exceção de parser, o mecanismo sinaliza o
  cancelamento dos workers, aguarda seu encerramento e remove o diretório
  especulativo. Não mantém ZIPs especulativos entre execuções.
- Cada worker efetua operações isoladas em arquivo `.part`, promovido
  apenas depois de validado. Nenhum worker modifica arquivos publicados,
  scores, snapshots de saída ou lógica metodológica.

## Delimitações técnicas

- A estrutura ZIP é inspecionada via `zipfile.is_zipfile` e
  `ZipFile.infolist`, assim como na rotina existente; não se descomprime
  uma segunda vez todos os CSVs para recalcular CRC.
- Ausência de corpo de ZIP íntegro não é mascarada pela origem; jamais
  se fornece ao parser um arquivo especulativo incompleto.
- O cancelamento é cooperativo durante streaming; um worker preso em
  leitura de socket só retorna após o timeout de leitura configurado.
- Se o ambiente não tiver 4 GiB de folga além dos ZIPs especulativos,
  não há tentativa paralela.
- O teste A09.6 **não altera o workflow de Full Generation nem o scheduler**.
  Não há ativação automática na `main`, independentemente de eventual merge.

## Homologação — critérios antes de qualquer ativação

1. CI completa verde, inclusive testes preexistentes de lifecycle,
   identidade, A09.4, cache integrity, Gate 4 e novo modo experimental.
2. Teste end-to-end com pequenos ZIPs offline: mesmas linhas e cobertura,
   mesma ordem e mesma saída semântica em modo original vs opt-in.
3. Injeções adversariais: ZIP quebrado, SHA alterado, tamanho inválido,
   falta de metadata, exceção de transporte, disco insuficiente, erro ao
   promover staging, cancelamento e exclusão dos temporários.
4. Prova isolada **sem publicação**, com fontes/cache seguros e limite
   de duração, para medir tempo agregado e equivalência de artefatos
   numa mesma competência. Evitar uma segunda aquisição nacional
   desnecessária — não aprovar por simples alegação de economia.
5. Revisão explícita de custos GitHub Actions/tempo, limite de disco
   e política de rollback antes de editar a configuração produtiva.

**Critério de ativação produtiva futura:** decisão independente, com avaliação
explícita dos recursos e da reversão. Até essa decisão, manter
`RECEITA_A096_PREFETCH` ausente na produção.

## Reversão

Desativar a variável `RECEITA_A096_PREFETCH` (ou definir valor diferente de
`1`). O caminho sequencial original é o comportamento padrão, sem
migração de caches nem reversão de dados. A09.4 permanece independente.

## Arquivos afetados

- `api/sources/receita_cnpj_prefetch.py`: módulo isolado da experiência.
- `api/sources/receita_cnpj_bulk.py`: seleção opcional do caminho no lifecycle.
- `api/sources/receita_cnpj_identity.py`: seleção opcional para Empresas.
- `tests/test_v2_receita_a096_prefetch.py`: provas offline e falhas.
- Este documento.

Nenhuma publicação HostGator, Full Generation, alteração de Conduta,
registro de marca ou pontuação é necessária para revisar esta PR.


## Homologação adicional — referência de novembro e replay local (PR #71)

### Referência documental já materializada

A geração histórica de novembro, run
[`#43941197377`](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/43941197377),
tem sua referência em
[`docs/evidence/a096-november-2026-reference.json`](evidence/a096-november-2026-reference.json)
(arquivo dentro de `docs/evidence/`).

Foi extraída **do ZIP real da prova** (artifact ID `14277894275`), não de
números inventados. O SHA-256 do arquivo de prova foi fixado; o manifesto
de distribuição conferiu **816/816 arquivos públicos** por SHA-256 e tamanho.
Os dois snapshots da Receita estão identificados por hashes de sua linhagem:

- Lifecycle `9e32fef301c6104bcf3c3394a55636a0a5f783ea0b47a1dc9636760a483e5382`.
- Identidade `62928cf04a72f5476ac0ababc62e91124f8805cc5e2c3be966360b8e0f103e97`.

**Limite de evidência:** o pacote de prova de 1,68 MB NÃO contém os bytes
desses dois snapshots filtrados nem os 20 ZIPs nacionais. Seu arquivo
compacto de referência não contém CNPJs nem URL do compartilhamento WebDAV
(com token). O artifact histórico expira em **30/11/2026**; nenhum ZIP bruto
foi versionado nem publicado.

### Recuperação de snapshots por cache persistente

O workflow manual
`V2 A09.6 November Reference Homologation`
(`.github/workflows/v2-a096-november-reference.yml`) existe **somente
para revisão da referência histórica**: exige `REFERENCE`, restaura
EXATAMENTE os caches `v2-gate4-source-Linux-43941197377` e
`v2-gate4-conduct-Linux-43941197377`, sem usar restore-keys, e adquire
apenas o ZIP de evidências de 1,68 MB **do GitHub**, nunca os ZIPs da Receita.

O script `scripts/a096_homologation.py reference` reconstitui o manifesto
e confere a referência pinada, o contrato da geração, linhagem, proveniência,
fingerprint, A09.4 e 816 hashes do pacote. Em seguida, o subcomando `audit`
verifica o **SHA-256 real do arquivo restaurado e do recibo** do cache,
identidade da fonte, competência, artifact e chave de universo/provedores.
Qualquer cache ausente, divergente ou incompleto bloqueia a aprovação.
A execução não publica e não salva cache.

Mesmo com hashes e contratos verificados, **o status sem candidato é
`insufficient_evidence`**, enquanto o workflow pode encerrar verde para a
verificação da *referência* especificamente. Isso não autoriza o prefetch.

### Preparação dos ZIPs para a próxima aquisição realmente necessária

`RECEITA_A096_RETAIN_ZIPS=1` (não definido na produção) captura, de modo
**opcional e temporário**, os dez ZIPs de **Empresas** por `os.link` no
mesmo `RUNNER_TEMP`, antes do parser descartar cada arquivo. Não faz cópia
se o hardlink falhar e não altera a aquisição. Os dez ZIPs de
**Estabelecimentos** são os que a A09.4 já mantém no mesmo runner.
Os dois grupos têm recibos por `V2_BUILD_ID`, tamanho, metadados de origem
e SHA-256 local. O script:

```bash
python -m scripts.a096_finalize_local_capture \
  --source-cache data/cache/v2/source \
  --conduct-cache data/cache/v2/conduct \
  --output "$RUNNER_TEMP/a096-local-input-proof.json"
```

somente emite o manifesto se os **20 ZIPs** íntegros e os dois snapshots
da **mesma geração e competência** estiverem presentes. Os ZIPs ficam no
runner, nunca no artifact ou HostGator, e desaparecem quando o job acaba.
Esse comando deve ser executado depois das duas fases e **antes** de
desmontar o workspace. Não foi introduzido no workflow produtivo.

Para comparar o parser sequencial com a execução candidata sobre **os
mesmos ZIPs locais**, sem rede, após a retenção e o replay controlado:

```bash
python -m scripts.a096_homologation compare-local \
  --zip-proof "$RUNNER_TEMP/a096-local-input-proof.json" \
  --companies "$RUNNER_TEMP/a096-company-run-zips/$V2_BUILD_ID" \
  --establishments "$RUNNER_TEMP/receita-a09-run-zips/$V2_BUILD_ID" \
  --serial-lifecycle caminho/serial-lifecycle.json \
  --serial-identity caminho/serial-identity.json \
  --candidate-lifecycle caminho/prefetch-lifecycle.json \
  --candidate-identity caminho/prefetch-identity.json \
  --output "$RUNNER_TEMP/a096-semantic-verdict.json"
```

O verificador lê e confere **todos os 20 ZIPs por SHA-256**, inclusive
tamanho, integridade ZIP, identidade e competência, e confere o hash do
snapshot sequencial. Os resultados só podem ser
`equivalent`, `different` ou `insufficient_evidence`.
São ignorados **somente** `generated_at`, `source.retrieved_at` e
`source.release_checked_at` (sem descartar campos contratuais, registros,
coberturas ou resultados). O relatório de diferenças lista **caminhos de
campos**, não CNPJs ou valores privados.

**Para outra competência**, a referência de novembro não substitui o
baseline local; a comparação deve nascer dos mesmos bytes da nova
aquisição. O mecanismo de retenção e o comparador foram exercitados na prova nacional
real de 18/11/2026 descrita abaixo. A integração não habilita prefetch,
retenção ou publicação.


## Homologação nacional concluída em 18/11/2026 — autorização de merge, sem ativação

- [Prova isolada #44238341224](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/44238341224), head `827e4563a26fdd9630cfdcc6600011d6ff0ee56d`, concluiu o Gate 4 experimental, verificou 20/20 ZIPs reais e provou prefetch 10/10 Empresas e 10/10 Estabelecimentos (sem fallback nesta execução).
- A09.4: 10 ZIPs de Estabelecimentos armazenados e 10 reutilizados/verificados; 10 Empresas capturadas por hardlink temporário. Os ZIPs brutos não foram publicados.
- Replay independente com parser sequencial original sobre **os mesmos 20 ZIPs reais** terminou `equivalent` (`same_exact_zip_set_and_semantic_match`), sem diferenças semânticas no lifecycle ou na identidade. O relatório comparou hashes e preservou `records[].observed_at` como campo significativo.
- Artefato de evidências: `a096-real-prefetch-proof-44238341224-a1`, ID `14407163080`, retenção até 02/12/2026. O histórico de novembro/2026 é uma referência separada, não equivale por si só à prova de replay.
- [CI #44238347989](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/44238347989) aprovada no mesmo head.
- **Decisão:** autorização para integrar o código **sem ativação produtiva**. `RECEITA_A096_PREFETCH` e `RECEITA_A096_RETAIN_ZIPS` permanecem ausentes dos workflows de geração produtiva; nenhuma alteração ao fluxo de publicação do HostGator. Ativação posterior exige decisão independente.
- A prova de equivalência não constitui medição controlada de redução do tempo de Full Generation em produção; a decisão de ativação deve considerar duração, GitHub Actions e reserva de disco.
