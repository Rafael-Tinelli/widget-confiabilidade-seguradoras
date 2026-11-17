# A09.6 — Prefetch controlado de duas partições (candidato, não ativado)

## Situação

**Implementação experimental em PR, DESLIGADA por padrão.** O objetivo é
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

**Critério de liberação:** todas essas condições documentadas e aceitas.
Até lá, manter `RECEITA_A096_PREFETCH` ausente na produção.

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
