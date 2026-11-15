# A09.4 — reuso temporário de ZIPs da Receita entre etapas do Gate 4

## Evidência operacional

A Full Generation [#43653748779](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/43653748779), executada em 14–15/11/2026, durou aproximadamente 4h49. A telemetria A09.1 demonstrou duas aquisições dos mesmos dez arquivos oficiais `Estabelecimentos*.zip`, cada uma com cerca de **5,42 GB e 1h50 de download**:

1. `receita_cnpj_lifecycle` baixou e leu as dez partições, descartando os arquivos locais.
2. `receita_consumer_identity` baixou e leu novamente as mesmas partições.

As duas etapas são processos diferentes do DAG; a simples reutilização de variáveis ou de `TemporaryDirectory` não resolveria o problema.

## Solução

Um arquivo temporário por **`V2_BUILD_ID`**, sob **`RUNNER_TEMP`**, guarda exclusivamente os ZIPs de Estabelecimentos obtidos pela primeira etapa. A segunda etapa pode reutilizá-los sem nova conexão HTTP se, e somente se:

- Ambas estão no mesmo `V2_BUILD_ID` e no mesmo workspace temporário do runner.
- Nome de partição, URL da release e competência são os mesmos.
- O manifesto do recurso oficial mantém tamanho positivo e ETag ou Last-Modified.
- O manifesto salvo coincide com os metadados oficiais observados pela etapa consumidora.
- O arquivo salvo é ZIP, seu tamanho coincide e seu **SHA-256 local** é igual ao registrado.

Caso qualquer condição falhe, executa-se o **download oficial existente**. Falhas em armazenamento temporário não substituem uma aquisição válida nem provocam reuso não verificado. Os caminhos de trabalho usam link físico (`os.link`), com cópia como fallback: o descarte do arquivo pela primeira etapa não elimina a cópia compartilhada.

A reutilização nunca consulta nem escreve os caches persistentes `data/cache/v2/source` e `data/cache/v2/conduct`. O workspace `RUNNER_TEMP` não integra os artefatos de publicação, não é exportado ao HostGator e é descartado ao encerrar o job. Nenhum ZIP nacional é versionado no repositório.

## Provas

Testes offline verificam reuso após descarte do arquivo da primeira etapa, inclusive **entre dois processos Python independentes**, alteração da competência, URL, ETag e build id, corrupção de mesmo tamanho, validação de tamanho e metadados ausentes. Testes existentes de paridade e método de identificação continuam aplicáveis.

O registro `RECEITA_A09` usa evento `zip_reuse` (`stored`, `verified`, `unverifiable` ou `storage_unavailable`). Isso permite distinguir arquivos baixados de arquivos localmente reaproveitados.

## Economia potencial e limites

Na execução de 14–15/11, a segunda transferência das partições consumiu cerca de **1h50**. Em uma geração em que as duas etapas precisem reconstruir resultados e a publicação não mude no intervalo, espera-se eliminar essa segunda transferência e manter o processamento CSV da segunda etapa. **Isso não reduz automaticamente uma Full Generation em 1h50:** a melhoria depende de necessidade real de ambas as aquisições, desempenho da fonte, integridade e espaço no runner.

O SHA-256 protege a **cópia local**; o fingerprint de metadados da A09.2 continua não sendo checksum oficial dos bytes remotos. A política de freshness permanece a mesma.

**Sem mudanças em:** Conduta, ranking/scoring, universo de CNPJs, identificação de seguradoras, SUSEP, caches persistentes, versão de contrato, horários, workflows ou HostGator.

**Não executar Full Generation para homologar esta PR.** Após integração, a próxima reconstrução necessária fornecerá a validação operacional por meio dos eventos `zip_download` e `zip_reuse`.
