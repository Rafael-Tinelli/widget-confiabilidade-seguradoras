# Cold start de identidades de mercado — setembro de 2026

## Objetivo

Esta rodada semeia no registro canônico identidades de mercado que ajudam o consumidor a entender quem comercializa/distribui o produto, quem presta serviços auxiliares e quem assume o risco securitário.

A admissão é **curada e durável**. Ela não depende do volume de buscas internas da Sanida. Telemetria do widget pode descobrir ou priorizar candidatos futuros, mas não remove silenciosamente uma identidade já verificada.

As seguradoras garantidoras **não são cadastradas novamente como marcas** nesta rodada. Elas continuam sendo entidades reguladas vindas do universo SUSEP e aparecem apenas como alvos de `risk_carrier` quando a relação está documentada.

## Decisões de admissão

| Entrada | Pessoa jurídica / identidade de mercado | Papel preservado | Risk carrier no registro | Decisão |
|---|---|---|---|---|
| Affinity Seguro Viagem | ASPAS TURISMO, VIAGENS E ASSISTENCIA INTERNACIONAL S/A — 17.846.647/0001-31 | Sub-representante de seguros | SABEMI SEGURADORA S.A. — 87.163.234/0001-38 | Admitir |
| Assistente de Viagem | 3RI VIAGENS E TURISMO LTDA — 13.711.169/0001-00 | Comparador / consolidadora | Nenhum carrier global | Admitir |
| AllSafe | PACTA GESTAO DE NEGOCIOS LTDA — 33.897.006/0001-62 | Representante de seguros | Split Risk Seguradora S.A. — 43.505.273/0001-09 | Admitir |
| Assist Card | ASSIST-CARD DO BRASIL LTDA — 00.027.571/0001-10 | Representante de seguros e distribuidora de assistência | STARR International Brasil Seguradora S.A. — 17.341.270/0001-69 | Admitir |
| Ituran | ITURAN SERVICOS LTDA — 16.613.061/0001-64 | Papel jurídico de distribuição ainda não verificado; produto combina monitoramento e seguros parceiros | Nenhum carrier global | Admitir com papel não verificado |
| Universal Assistance | ASISTBRAS - ASSISTENCIA AO VIAJANTE LTDA — 07.139.957/0001-62 | Representante de seguros e assistência ao viajante | Zurich Minas Brasil Seguros S.A. — 17.197.385/0001-21 | Admitir |
| Vital Card | SCHULTZ-INGA TURISMO LTDA — 04.628.135/0001-57 | Representante de seguros / agência de viagens | ALLSEG SEGURADORA S.A. — 67.865.360/0001-27 | Admitir |

## Affinity: duas coisas diferentes

`Affinity` da Yelum não é materializado como nova pessoa jurídica ou nova marca independente nesta rodada. A página da própria Yelum o apresenta como seu programa de benefícios/solução comercial. A seguradora Yelum já pertence ao universo regulatório SUSEP.

`Affinity Seguro Viagem`, por outro lado, é preservado como identidade de mercado separada da ASPAS. Para evitar colisão semântica, o alias simples `Affinity` não é cadastrado para essa entrada; os nomes públicos preservados são `Affinity Seguro Viagem` e `Affinity Viagem`.

`Affinity Seguro Viagem` também não é fundido com `Assistente de Viagem`. São entradas diferentes, com pessoas jurídicas diferentes.

## Assistente de Viagem

A plataforma se descreve como comparador/consolidadora de seguradoras e operadoras. Por isso não é correto fixar SABEMI, ou qualquer outra seguradora, como carrier global da marca. A relação com o carrier depende do plano efetivamente escolhido.

A existência de uma seguradora em um produto específico poderá ser acrescentada futuramente com escopo de produto/tempo e evidência apropriados, sem transformar esse carrier em seguradora universal da plataforma.

## Assist Card

A pessoa jurídica de mercado é a ASSIST-CARD DO BRASIL LTDA. O registro preserva separadamente:

- o papel da Assist-Card do Brasil como representante de seguros;
- a STARR como entidade que assume o risco no componente securitário documentado;
- ASSIST CARD SMALLINE S.A. como contexto de assistência internacional;
- GLOBAL APOIO ADMINISTRATIVO A EMPRESA LTDA como contexto de assistência nacional.

Smalline e Global não são cadastradas como risk carriers por essa evidência.

## Ituran

A Ituran é admitida porque sua existência separada ajuda a explicar o arranjo comercial `Ituran com Seguro`. A evidência atual mostra múltiplas seguradoras parceiras e não sustenta uma única seguradora garantidora global nem uma classificação jurídica única da Ituran como corretora ou representante.

Consequentemente:

- `distribution_role_state = unverified`;
- não há `risk_carrier` global;
- CNAE ou intermediação comercial, isoladamente, não transformam a empresa em corretora.

## Vital Card

A identidade comercial é vinculada à Schultz-Ingá Turismo. A Vital Card informa atuação como representante da ALLSEG; a STZ Corretora de Seguros é preservada separadamente como contexto de corretagem. Isso evita a regra incorreta `quem comercializa = corretora`.

## Não duplicação

Esta rodada deliberadamente não cria novas entradas de marca para:

- Loovi e Azos, que já estavam no registro canônico;
- SABEMI, Split Risk, STARR, Zurich Minas Brasil e ALLSEG, pois são entidades reguladas tratadas pelo pipeline SUSEP;
- ASSIST CARD SMALLINE e GLOBAL APOIO como seguradoras, pois nesta evidência são participantes da camada de assistência;
- STZ Corretora como marca de seguro viagem; ela é contexto de corretagem do arranjo Vital Card;
- Yelum Affinity como identidade empresarial separada.

## Invariantes

1. `brand != legal_entity`.
2. `market_identity != risk_carrier`.
3. `risk_carrier_relation != complaint_transfer`.
4. `search_alias != complaint_alias`.
5. Papel comercial documentado não é automaticamente classe regulatória.
6. Ausência de carrier global não impede a admissão de uma identidade de mercado útil.
7. Marca cold-start não depende de telemetria interna para permanecer no registro.
8. Mudança futura de relação deve atualizar estado/período/evidência; não apagar silenciosamente o histórico.
