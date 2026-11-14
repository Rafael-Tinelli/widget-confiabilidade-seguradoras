# A09.3 — Otimização conservadora de CPU na identidade Receita

## Contexto operacional

A execução [Full Generation de 12/11/2026](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/43246120800)
registrou **9.811,402 segundos** na aquisição/processamento da
`receita_consumer_identity`. A09.1 está na `main`, mas **nenhuma
aquisição nacional instrumentada depois de A09.1 havia sido
homologada** quando esta PR foi criada. Portanto, o tempo por download,
por ZIP e por partição ainda não está medido operacionalmente.

Não é seguro concluir que o processamento de CPU explica a maior
parte dos 9.811 segundos. Esta PR mede e melhora apenas uma operação
cara demonstrável no hot path; paralelismo, downloads, retenção e
cache permanecem fora do escopo até existirem medições reais.

**Dependências:** PR #63 (A09.2) continua aberta/sem merge no início
deste trabalho. A09.3 foi criada diretamente da `main` (commit
`c6c75d57ca3fd54d23b64c274c9fd65ef68a10f7`) e **não incorpora
A09.2**. Não alterar contratos de fingerprints ou freshness aqui.

## Otimizações conservadoras

1. `_match_name_to_queries` normalizava duas vezes **cada nome**:
   `_surface_name(name)` e `_business_core(name)` chamavam ambas
   `normalize_name_key(name)` (NFKD Unicode, remoção de diacríticos,
   regex e caixa baixa). O novo código realiza essa normalização
   **uma única vez** e deriva as duas chaves dos mesmos tokens.
2. Para cada linha de `Empresas`, `nature_code` era normalizado antes
   de verificar se a empresa ou seu nome correspondia ao universo
   procurado. Agora isso é feito somente **após** a seleção. Isso não
   altera a normalização da natureza jurídica dos registros retidos.

A lógica de correspondência (superfície exata, núcleo de negócio,
stopwords, mínimo de caracteres), a precedência das correspondências,
o tratamento de nomes comerciais, a identificação por CNPJ e os
candidatos ambíguos são **iguais à versão anterior**.

## Provas e benchmark

- `tests/test_v2_receita_a09_matching_performance.py`: compara a
  nova função a um **oráculo congelado do algoritmo anterior**,
  inclusive 4.000 nomes sintéticos determinísticos, nomes especiais e
  os dois prefixos de correspondência. Prova chamada única à
  normalização. Compara o snapshot completo de um processamento de ZIPs
  locais de Empresas e Estabelecimentos, exceto carimbos de observação.
- `scripts/bench_receita_identity_matching.py`: benchmark sintético
  **sem HTTP, sem dados externos e sem Full Generation**. Executar:

  ```bash
  python -m scripts.bench_receita_identity_matching --rows 30000 --repeats 4
  ```

  O resultado imprime medianas, amostra, número de repetições,
  equivalência de candidatos e redução **somente do tempo da função
  de matching**. Não há limiar mínimo de desempenho que torne a CI
  instável em runners compartilhados.

O ensaio preliminar local, com aproximadamente 25 mil nomes sintéticos,
registrou medianas próximas de **0,297 s para o algoritmo anterior**
e **0,161 s para a estratégia de normalização única** (redução da
ordem de 46% nesse microteste, não em uma Full Generation).
Medições em outras máquinas, cargas e períodos podem divergir.

## Limites e próximos passos

A09.3 não acelera o download dos arquivos nacionais, não altera a
quantidade de partições ou a necessidade de varredura para identificar
nomes comerciais de fornecedores, não descarta dados da Receita e não
introduz processamento concorrente. **Não se presume uma redução
percentual do tempo total da Full Generation.**

O próximo diagnóstico substancial depende de uma aquisição real com
A09.1 para separar `zip_download` e `partition_parse`. Se o gargalo
for rede, a otimização de matching terá efeito limitado; se for CPU,
poderemos justificar outras alterações por dados. Essa aquisição
não deve ser disparada somente para produzir um benchmark.

**Segurança e produto:** sem alteração da Conduta, SUSEP, Gate 4 DAG,
HostGator, horários, hashes de cache, persistência, pontuações ou
taxonomia. Nenhum Full Generation, download oficial nem deploy.
