# A09 — homologação operacional controlada, sem publicação

## Correção do diagnóstico anterior

A identidade da Receita **já possui cache persistente validado** em
`data/cache/v2/conduct/receita_consumer_identity.json` e respectiva metadata.
`api.v2.build_conduct_source_snapshot` utiliza `CachedSource.materialize`
(SHA-256, source_id, URL), e `api.v2.build_consumer_gov_receita_identity`
só declara cache atual quando competência, universo de CNPJs, fornecedores
e fingerprint da publicação oficial são compatíveis.

O workflow produtivo `v2-gate4-full-generation-proof.yml` já restaura e
salva `data/cache/v2/conduct`. Portanto, **não se altera o contrato do
cache de identidade** nesta frente: a hipótese de ausência de
persistência estava incorreta.

## Por que não disparar a geração produtiva para essa prova

Depois da execução [#43653748779](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/43653748779)
os dois caches da Receita podem estar atuais. Uma nova Full Generation
produtiva poderá reutilizar ambos e não exercitar a A09.4. Além disso,
quando o workflow produtivo termina com sucesso, o publicador do
HostGator pode ser acionado automaticamente.

A prova exige **reconstrução efetiva de lifecycle e identidade no mesmo
runner**. A A09.4 deve emitir exatamente um download oficial e um
`zip_reuse: stored` por `EstabelecimentosN.zip`, seguidos de
`zip_reuse: verified` na segunda rotina, sem novo GET HTTP.

## Workflow seguro

`.github/workflows/v2-a09-isolated-rebuild-proof.yml`:

- É manual, exige digitar `REBUILD`; sem cron, acionamento por PR ou
  scheduler.
- Executa **o DAG canônico** `python -m api.v2.gate4_pipeline --run-all`
  com a instrumentação A09.1–A09.4.
- Ambiente efêmero, sem restore/save do Actions cache. A fonte
  regulatória fica em diretório isolado; os dois caches da Receita
  são limpos somente no checkout do runner.
- O baseline versionado do Consumidor.gov é preparado normalmente.
- O workflow de prova **não** consta dos eventos observados pelo
  publicador do HostGator.
- Upload somente de relatório compacto e eventos A09; sem pacote
  público, ZIP nacional ou arquivos de cache.
- Não altera código de produção, metodologia, ranking, SUSEP,
  agendamento nem conteúdo publicado.

O workflow poderá ser executado **após o merge**, na `main`.
Não executar em paralelo a outra geração nacional para evitar
competição por banda. A prova pode consumir várias horas porque
faz varredura nacional real.

## Critério de aceite

O verificador `scripts/a09_validate_rebuild_proof.py` falha diante de
downloads, armazenamento ou reuso ausentes/duplicados, contagem
diferente do catálogo oficial, tamanho inconsistente, falha de
integridade, novo download após início do reuso, fontes stale/cache
ou falta de telemetria.

O relatório comprova **quantos GETs e bytes foram evitados** na
execução observada. Não declara uma economia temporal causal a partir
de execuções com condições de rede diferentes. No run anterior, a
segunda transferência levou 6.614,152 segundos (5,42 GB): ponto
histórico de comparação, não previsão garantida.

## Procedimento

Após CI aprovada e integração à `main`, acionar o workflow
**V2 A09 Isolated Full Rebuild Proof**, branch `main`, com
`confirm_rebuild=REBUILD`. Analisar os eventos `stored`/`verified` e
o relatório publicado como artifact. Não interpretar a homologação
como autorização para atualizar o HostGator.
