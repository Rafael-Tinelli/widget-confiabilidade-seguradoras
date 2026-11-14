# A09.2 — Fingerprint de release e integridade dos caches da Receita

Esta PR protege a distinção entre **cache verificado como atual** e
**fallback de um snapshot anterior**. Não muda a autoridade da fonte, a
classificação da SUSEP, a metodologia de Conduta, a janela histórica
nem os critérios de elegibilidade de seguradoras.

## Problema demonstrado

A09.1 registrou o `release_fingerprint` observado na aquisição,
mas o Gate 4 ainda usava principalmente `reference_period` para
validar o cache de lifecycle e `reference_period + target_universe_hash
+ unresolved_provider_hash` para identidade. Um ZIP republicado na
**mesma competência** poderia passar como `current_validation` sem
confrontar sua identidade de publicação.

## Novas decisões de freshness

| Camada | Escopo do fingerprint WebDAV | Quando `current_validation` é permitido |
|---|---|---|
| Lifecycle | `Motivos.zip` + todas as partições `Estabelecimentos*.zip` | Competência, URL, arquivo requerido, tamanho positivo, ETag ou Last-Modified, fingerprint de metadados e compatibilidade do universo |
| Identidade | Todas as partições `Empresas*.zip` e `Estabelecimentos*.zip` + `Cnaes.zip` + `Naturezas.zip` | Chave Gate 4 válida **e** release URL, competência, método e fingerprint oficial de metadados idênticos |

**Consulta de validação:** descoberta WebDAV `PROPFIND`, sem download
dos ZIPs da Receita. A rotina de identidade faz uma consulta da pasta
completa quando o cache parece elegível; quando precisa reconstruir,
a aquisição existente continua responsável pelo processamento oficial.

**Ausência de metadados:** não é interpretada como igualdade.
Falta de tamanho, ausência simultânea de ETag e Last-Modified,
arquivos faltantes, repetidos ou inesperados tornam a comparação
`unverifiable`. Não é permitido afirmar `current_validation`.
O sistema pode tentar adquirir os arquivos oficiais, ou utilizar
**somente** o fallback já validado por SHA-256, identificado como
`validated_cache_fallback` (stale). Uma nova aquisição pode levar
horas: a PR **não executa** esse trabalho na homologação.

**Mudança dentro da mesma competência:** mudança em nome/quantidade
de arquivos, tamanho, ETag ou Last-Modified altera o fingerprint e
invalida a hipótese de `current_validation`; uma reconstrução passa
a ser necessária para restaurar o estado de fonte atual.

## Integridade criptográfica: o que é e o que não é

O fingerprint é SHA-256 de um **manifesto de metadados**, não dos ZIPs
remotos. Se o servidor alterar bytes sem modificar qualquer um
desses metadados, não há como afirmar que a rotina detectará a troca.
A09.2 **não** é uma assinatura de publicação emitida pela Receita.

O `CachedSource` existente verifica o **SHA-256 do snapshot local**
na materialização, inclusive contra adulteração de mesmo tamanho.
Essa validação continua obrigatória para o reuso dos caches do Gate 4.
Uma cópia antiga sem fingerprint pode continuar como fallback
validado, mas não pode representar uma validação atual. A política de
materialização, o método de hash local e o armazenamento não mudaram.

## Compatibilidade

- A versão de cache por universo/CNPJ **não é alterada**. O fingerprint
  funciona como condição adicional à afirmação de `fresh`, mantendo
  a possibilidade de fallback para snapshots anteriores.
- O lifecycle registra no snapshot novo o método e fingerprint dos
  metadados usados; o builder de identidade já o registrava desde A09.1.
- O comando standalone de lifecycle também deixa de ignorar revisão
  da publicação dentro da mesma competência.
- Logs de decisão identificam incompatibilidade de release. Não expõem
  CNPJ, nomes ou credenciais.
- A PR não incorpora as PRs antigas #44–#46, não usa HostGator como
  transporte e não altera workflow ou calendário.

## Homologação e limites

Os testes determinísticos verificam invariância à ordem, modificação
de metadados na mesma competência, arquivos duplicados/faltantes,
ausência de ETag e Last-Modified, snapshot pré-fingerprint, cache
corrompido com o mesmo tamanho, além das decisões de skip/rebuild dos
builders sem transferir ZIPs oficiais.

A homologação **operacional** do fingerprint depende de uma execução
de aquisição/validação posterior; testes verdes não substituem
essa prova. Não executar Full Generation somente para validar a PR.
