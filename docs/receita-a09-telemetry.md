# A09.1 — Telemetria e prova de competência da Receita

Escopo estritamente observacional sobre o builder `receita_consumer_identity`.
**Não** muda os arquivos oficiais utilizados, a chave de elegibilidade do
cache, a metodologia, a classificação de freshness, a Conduta, o Gate 4 DAG
ou a frequência de atualização. Não habilita HostGator como origem ou cache.

## Por que isso existe

- No [run #42687071354](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/42687071354)
  (09/11/2026), a identidade validou o cache em **19,020 s**.
- No [run #43246120800](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/43246120800)
  (12/11/2026), a identidade exigiu **9.811,402 s** para nova aquisição.
- O processo antigo não registrava o motivo de invalidação nem custo
  por ZIP, impedindo atribuir tempo a transporte versus parse.
- A competência era registrada no snapshot interno, mas não aparecia
  explicitamente no artefato pequeno da prova final.

## Eventos operacionais

Mensagens `RECEITA_A09 {JSON}` no log e, quando configurado,
um arquivo JSONL local. Somente categorias fixas, índices de partição,
horários, durações, contagens, hashes não reversíveis e números de
recursos são admitidos; o emissor descarta quaisquer chaves não
autorizadas. **Não emite CNPJ, razão social, fornecedor ou linha CSV.**

| Evento | Evidência |
|---|---|
| `cache_decision` | `hit`, `miss_snapshot`, `miss_contract`, `miss_period`, `miss_universe_hash`, `miss_provider_hash`, `forced_rebuild` |
| `cache_integrity` | Falha de materialização do cache derivado, sem mudar o fallback |
| `release_discovery` | Competência, fingerprint de **metadados**, quantidade de arquivos e tempo de descoberta |
| `zip_download` | Categoria/índice, tentativas, espera entre retries, bytes da transferência, duração e resultado |
| `dictionary_parse` | Tempo para baixar e ler dicionários e quantidade de códigos |
| `partition_parse` | Categoria/índice, tamanho anunciado/observado, linhas lidas/retidas, tempo de leitura |
| `candidate_finalize`, `identity_complete` | Finalização e duração da construção da identidade |
| `provenance_write` | Presença/consistência da prova compacta |

Os eventos também incluem `peak_rss_mib` (pico de RSS do processo
no Linux) e `disk_free_mib` (espaço livre no filesystem no momento do
evento). São **amostras**, não estimativas de ocupação total da conta
GitHub Actions nem de uso global do runner. O pico de disco pode ocorrer
entre amostras. O campo `received_bytes` de `zip_download` é dos bytes
do **GET final bem-sucedido**, não uma soma de bytes desperdiçados em
tentativas anteriores. `transferred_bytes_total` soma os bytes de todas\nas tentativas, incluindo transferências parciais fracassadas.\n`retry_wait_s` contém a espera programada;
`elapsed_download_s` inclui as tentativas.

## Evidência compacta no artefato Gate 4

O workflow existente passa a incluir:

- `data/derived/v2/receita_a09_events.jsonl`;
- `data/derived/v2/receita_identity_provenance_a09.json`.

A prova contém `reference_period`, `release_url`, o fingerprint registrado
pelo snapshot e o fingerprint parcial observado no catálogo, ambos
explicitamente rotulados, além de `release_checked_at` (quando consulta oficial
comprovadamente ocorreu), `last_remote_fetch_at` (data mantida no
snapshot materializado), versão do contrato, hashes da população
de CNPJs e de fornecedores, motivo de decisão e hash do snapshot.
A prova compara o hash do snapshot materializado com a linhagem,
além de verificar a coerência da competência e dos hashes registrados
no marcador de descoberta. Se a verificação não for possível, deixa
os campos de consulta atual vazios e indica `not_available` ou
`mismatch`, sem atribuir atualização recente a cache antigo.

`catalog_fingerprint` cobre metadados das partições disponíveis no
discovery (Motivos e Estabelecimentos). O `release_fingerprint` do
snapshot reconstruído cobre arquivos efetivamente utilizados por
identidade (Empresas, Estabelecimentos, CNAEs e Naturezas). **Nenhum é
SHA-256 dos bytes remotos**, nem prova contra reempacotamento silencioso
com metadados idênticos; isso será objeto da A09.2.

`current_validation` significa validação dos critérios de cache na
execução atual, **não novo download da base nacional**. O histórico
continua distinguindo `current_fetch`, `current_validation` e
`validated_cache_fallback`. O antigo campo `used_cache=false` mede
fallback e não deve ser interpretado como ausência de qualquer reuso.

## Limitações e homologação

A09.1 foi desenhado para CI com ZIPs sintéticos e mocks locais, sem
download da Receita e sem Full Generation. Para concluir a homologação
**operacional** da instrumentação, será necessário observar em
execução isolada ou na próxima execução deliberada:

1. motivo de decisão do cache e competência refletidos no manifesto;
2. um evento de download e parse por partição e dicionários;
3. contagem de tentativas, durações e bytes sem informações pessoais;
4. arquivos de prova pequenos e hash/proveniência coerentes;
5. nenhum efeito em Conduta, Gate 4 ou publicação.

A09.1 **não** inclui alteração da política de invalidação por
fingerprint (A09.2), melhoria de velocidade ou paralelismo (A09.3),
nem autoriza Full Generation apenas para testar instrumentação.
