# §19.5 — Limpeza do repositório

Status: **FECHADO**.

Data: 02/09/2026.

O objetivo desta etapa não foi apagar tudo o que parece antigo. A regra aplicada foi a do README:

```text
KEEP       — produção/fundação atual
DIAGNOSTIC — auditoria ainda útil
LEGACY     — histórico reprodutível
DELETE     — redundante/obsoleto/temporário
```

A exclusão só ocorreu quando foi possível demonstrar que o caminho não participa da arquitetura v2 ativa nem precisa ser preservado para a produção v1 atual ou para reproduzir decisões metodológicas.

---

## 1. DELETE — removidos

### `widget-ui/src/InsurerScoreModal.jsx`

Motivo:

- implementava a experiência v1 de score/weights/contributions;
- não era mais importado pelo `App.jsx` ativo;
- o contrato de frontend v2 proíbe recomposição de score;
- os testes já protegiam a ausência desse modal no caminho ativo.

Classificação: **DELETE**.

### `widget-ui/dist/`

Arquivos removidos:

```text
widget-ui/dist/index.html
widget-ui/dist/assets/widget.css
widget-ui/dist/assets/widget.js
```

Motivo:

- eram build compilado antigo, anterior à migração v2;
- `widget-ui/dist/` já estava explicitamente ignorado no `.gitignore`;
- Vite reconstrói esse diretório a partir das fontes versionadas;
- o CI compila novamente o frontend e não depende desse snapshot histórico.

Classificação: **DELETE — output reproduzível e obsoleto**.

### `data/raw/consumidor_gov/tmp68ikpili.csv`

Motivo:

- arquivo temporário com nome não canônico;
- aproximadamente 68 MB;
- `data/raw/*` já é ignorado;
- a aquisição v2 trabalha com os nomes e snapshots canônicos da fonte;
- nenhuma dependência atual foi encontrada apontando para esse arquivo específico.

Classificação: **DELETE — temporário rastreado acidentalmente**.

### `teste_consumidor.py`

Motivo:

- scratch manual de investigação de Consumer.gov;
- não era teste automatizado;
- não participava do Gate 4;
- não era fonte de verdade ou builder atual.

Classificação: **DELETE — scratch obsoleto**.

A regressão está protegida por:

`tests/test_v2_repository_cleanup.py`

---

## 2. KEEP — fundação v2 atual

Permanecem como **KEEP**, entre outros:

```text
api/v2/gate4_pipeline.py
api/v2/generation.py
api/v2/source_cache.py
api/v2/source_snapshot.py
api/v2/install_public_generation.py
api/v2/public_information_projection.py
api/v2/public_profile_regulatory_semantics.py
api/v2/validate_public_search_profile_contract.py

data/reference/v2/

tests/test_v2_*.py

docs/section-19-1-methodology-data-audit-closure.md
docs/section-19-2-frontend-information-audit.md
docs/section-19-3-19-4-evergreen-hostgator-publication.md

widget-ui/src/App.jsx
widget-ui/src/v2Data.js
widget-ui/src/InsurerProfileModal.jsx
widget-ui/src/ComparisonPanel.jsx
widget-ui/src/ExplorePanel.jsx
widget-ui/src/components/InsurerCard.jsx

.github/workflows/v2-gate4-evergreen-contract.yml
.github/workflows/v2-gate4-full-generation-proof.yml
.github/workflows/v2-hostgator-publication.yml
.github/workflows/v2-production-generation-schedule.yml

ops/hostgator/install_v2_public_remote.sh
```

Esses caminhos compõem a arquitetura v2 consolidada ou suas provas contratuais.

---

## 3. DIAGNOSTIC — preservados deliberadamente

Os experimentos e diagnósticos v2 não foram removidos em massa.

Exemplos:

- diagnósticos de liquidez;
- experimentos de operating result;
- calibrações de Conduct;
- credibility e portfolio mix;
- investigações cross-pillar;
- experimentos de Consumer.gov/Receita;
- workflows e testes que reproduzem essas análises.

Motivo:

> o fato de uma investigação não ser o caminho de publicação final não a torna lixo; ela preserva a justificativa causal das decisões metodológicas já consolidadas.

Também permanece:

`scripts/audit_insurers_json.py`

como ferramenta diagnóstica do legado/v1.

Classificação: **DIAGNOSTIC**.

---

## 4. LEGACY — preservado enquanto o cutover não ocorreu

A produção atualmente servida no HostGator ainda não foi substituída pela v2 durante este Draft PR.

Por isso permanecem como **LEGACY**, e não DELETE:

- builders v1 em `api/`;
- `api/v1/*.json`;
- módulos e snapshots de Open Insurance/OPIN usados pelo histórico v1;
- workflows antigos de atualização da produção v1;
- testes v1 correspondentes;
- snapshots que sustentam auditabilidade histórica;
- `verify_extraction.sh`.

### `verify_extraction.sh`

Foi mantido especificamente como **LEGACY / manual only**.

Ele executa verificações destrutivas e reconstruções do pipeline antigo e, portanto:

- não deve ser usado como mecanismo operacional v2;
- não deve ser chamado pelo cron v2;
- ainda pode auxiliar na reprodução de problemas do pipeline anterior enquanto o cutover não for concluído.

Removê-lo agora reduziria a capacidade de investigar a produção ainda existente sem benefício para a arquitetura v2.

---

## 5. Duplicações mantidas conscientemente

`LISTAEMPRESAS.csv` aparece em mais de um local histórico, inclusive:

```text
api/static/LISTAEMPRESAS.csv
data/raw/ses/LISTAEMPRESAS.csv
```

Os blobs observados são iguais, mas os caminhos pertencem a gerações arquiteturais diferentes.

Não foi feita deduplicação nesta etapa porque:

1. a produção v1 ainda não sofreu cutover;
2. referências históricas podem depender do caminho antigo;
3. a economia de espaço é irrelevante perto do risco de quebrar reproducibilidade.

Classificação atual: **LEGACY/KEEP conforme o consumidor**.

Uma eventual deduplicação física deve ocorrer somente depois do cutover e de nova busca de dependências.

---

## 6. Workflows antigos

Nenhum workflow foi apagado simplesmente por possuir `experiment`, `diagnostic`, `preflight` ou por não ser parte do cron final.

A distinção aplicada é:

```text
workflow que compõe Gate 4/publicação v2 -> KEEP
workflow que reproduz investigação v2   -> DIAGNOSTIC
workflow que sustenta produção v1       -> LEGACY
workflow sem consumidor e sem valor     -> DELETE
```

Não foi identificado nesta passagem um workflow em que a última categoria estivesse demonstrada com segurança suficiente para exclusão.

Isso é intencionalmente mais conservador do que uma limpeza por nome de arquivo.

---

## 7. Dependências do frontend

Foi observado que `widget-ui/package.json` utiliza ranges semver e o repositório ainda não possui `package-lock.json`; o CI usa `npm install --package-lock=false`.

Isso significa que a prova de compilabilidade do frontend não possui lock exato de dependências Node.

A observação **não invalida o Gate 4**, porque:

- Vite não participa da geração do pacote público de dados;
- o build Vite no CI é uma verificação de compilação;
- a publicação HostGator criada em §19.3–§19.4 sincroniza somente os dados públicos v2;
- o próprio projeto Python também adota ranges em `requirements.txt`, portanto transformar toda a árvore em dependências herméticas seria uma política de packaging mais ampla.

Classificação: **aresta de toolchain, não blocker do fechamento §19.5**.

Não foram fixadas versões arbitrárias nem criado lockfile artificial sem resolução real do npm.

---

## 8. O que não foi removido

Deliberadamente não foram apagados:

- metodologia fechada mas ainda reproduzível;
- experimentos que explicam por que determinados sinais foram aceitos ou rejeitados;
- v1 que ainda sustenta a produção não cortada;
- snapshots regulatórios canônicos;
- SES raw canônico;
- referências verificadas de identidade/relacionamento;
- testes que defendem invariantes metodológicos;
- workflows que podem reproduzir investigação histórica.

A limpeza não deve reduzir a capacidade de responder:

> “como chegamos a esta decisão?”

---

## 9. Estado final do escopo §19.1–§19.5

```text
§19.1 metodologia/dados              FECHADO
§19.2 potencial informacional/UI     FECHADO
§19.3 evergreen/zero manutenção      FECHADO em arquitetura/código
§19.4 cron/publicação                FECHADO em arquitetura/código
§19.5 limpeza do repositório         FECHADO
```

Há uma distinção operacional importante:

> §19.3–§19.4 estarem fechados em arquitetura/código não significa que o cutover de produção foi executado.

O cutover continua deliberadamente bloqueado até merge autorizado, configuração SSH/paths no HostGator e habilitação explícita das variables de produção.

---

## 10. Limite desta consolidação

O escopo solicitado termina no §19.5.

**§19.6 não é iniciado automaticamente por este fechamento.**

Qualquer revisão posterior de experiência, SEO ou deploy visual deve ser aberta como uma etapa separada e deliberada.
