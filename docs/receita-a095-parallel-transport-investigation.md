# A09.5 — Investigação controlada de paralelismo na Receita Federal

## Escopo

**Exclusivamente diagnóstico**, sem implementação de downloader concorrente,
sem Full Generation, sem publicação e sem serviços comerciais.

Dados de base: [homologação A09 #43764654951](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/43764654951).
Naquela execução, os 10 ZIPs de Estabelecimentos totalizaram 5.416.929.969
bytes e aproximadamente 8.762,165 s de transferência, sequencialmente.
Os 10 ZIPs de Empresas adicionaram 1.386.551.928 bytes e 2.355,293 s.
A09.4 já impediu o download duplicado dos Estabelecimentos.

## Hipótese

Os arquivos são transferidos pelo `requests.Session.get(..., stream=True)`,
um por vez, em `api/sources/receita_cnpj_bulk.py:_download_zip`.
Não existe controle de taxa artificial. Duas conexões *podem* acelerar o
processo se houver limite por conexão; se o limite for global por IP,
servidor, CDN ou rede do runner, o paralelismo pode não trazer ganho.

## Prova mínima segura

Workflow manual `V2 A09.5 Bounded Transport Probe`, com confirmação
`PROBE`. O workflow executa `scripts/a095_bounded_transport_probe.py`:

1. Descobre a última publicação oficial por PROPFIND (sem scraping) e
   confirma sua identidade via fingerprint de metadados.
2. Seleciona dois ZIPs oficiais `Estabelecimentos0.zip` e
   `Estabelecimentos1.zip`, sem baixar os arquivos completos.
3. Exige HTTP **206 Partial Content**, `Content-Range` exato,
   comprimento esperado e codificação `identity`. Se a origem
   responder HTTP 200, interrompe antes de receber o corpo inteiro
   e relata `range_not_supported`.
4. Primeiro recebe 1 KiB como verificação técnica; depois dois trechos
   de **8 MiB sequencialmente** e os mesmos dois trechos com **no máximo
   duas conexões simultâneas**.
5. Compara os SHA-256 dos trechos recebidos; confirma que o fingerprint
   oficial não mudou entre o começo e o fim.
6. Produz somente JSON com tempos, vazão e status, sem CSV, nomes de
   pessoas, CNPJs ou URLs autenticadas.

**Limites:** aproximadamente 32 MiB de trechos HTTP, mais 1 KiB de
preflight, duas conexões no máximo, 100 s por requisição e 8 minutos
de teto do job. Sem persistência de ZIPs, alteração de caches ou
dependência de serviço pago.

Um resultado de 2 conexões mais rápido é **um indício, não prova
conclusiva** de benefício para transferências de GB. Uma rodada
repetida em horários diferentes pode ser necessária. O próximo
estágio, se houver evidência, será uma proposta de implementação
com testes de equivalência de bytes, ZIP, metadados e cálculo,
fallback serial e limites de disco e concorrência.

## Critérios de decisão

- `range_not_supported`: não há base para baixar por partes; investigar
  paralelismo de ZIPs distintos separadamente, sem insistir em Range.
- `measured` com ganho consistente em repetições: avaliar duas
  transferências distintas simultâneas com leitor sequencial.
- Vazão agregada similar ou menor, respostas 429/503, problemas de
  checksum ou throttling: manter downloader sequencial.
- Nunca adotar paralelismo por extrapolação direta dos 8 MiB para
  os 5,42 GB completos.

Este workflow não integra a lista de workflows que disparam o
publicador HostGator, não altera score e não inicia por push ou cron.
