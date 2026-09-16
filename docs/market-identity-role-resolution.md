# Market identity role resolution

Status: implementation branch `feat/market-identity-role-resolution`.

## Objective

Resolve unknown consumer-facing names conservatively enough to answer:

```text
who is this?
→ is there a distinct legal entity behind the name?
→ is that entity an authorized insurer?
→ if not, what role does it play in the insurance arrangement?
→ which insurer assumes the risk, when that relationship is explicitly documented?
```

The purpose is not to create profiles for every product, campaign, affinity program or commercial label used by an insurer already present in the registry.

## Admission boundary

A searched name is not automatically a market identity.

```text
consumer-facing name != market_identity
```

A distinct public identity requires evidence of an autonomous and materially relevant subject in the insurance arrangement.

Therefore, a product, plan, campaign, internal affinity program, coverage name or commercial vertical does not become a separate public identity merely because users search for it.

A name such as an insurer's internal affinity program may remain an unresolved/private candidate or later become only a navigation alias to the already-known insurer. It does not create a new legal entity, assessment subject or relationship.

## GSC

Google Search Console remains optional and not configured. This implementation does not require GSC and does not change that operational decision.

The primary demand signal is the already-active privacy-minimized widget zero-result telemetry:

```text
unknown widget query
→ SQLite aggregate
→ candidate
→ directed investigation when justified
```

Demand controls investigation priority. It never proves identity, classification or relationship.

## Receita/CNAE semantics

Receita Federal evidence is used for two separate purposes:

1. resolving a possible distinct legal entity by CNPJ/legal name/trade name;
2. producing non-authoritative role hypotheses from CNAE.

It must not be used to assert a public role by itself.

Examples:

```text
6622-3/00
→ strong hypothesis: insurance broker/agent activity
→ NOT automatic public role = broker

7490-1/04
→ weak hypothesis: general business intermediation
→ NOT broker
→ NOT insurance representative

79.xx travel activity
→ hypothesis: travel/distribution context
→ NOT insurance intermediary classification by itself

insurance CNAE
→ corroborating signal only
→ SUSEP remains authoritative for insurer status
```

## Three independent evidence states

The implementation separates:

```text
identity_state
classification_state
relationship_state
```

This permits, for example:

```text
identity_state = verified_legal_entity
classification_state = role_unresolved
relationship_state = unresolved
```

That state is valid and publishable only after the publication contract explicitly permits it. It must not be silently promoted into broker, representative, distributor or carrier semantics.

## Intermediation is not synonymous with broker

A non-insurer involved in selling insurance is not automatically a broker.

The investigation may later prove roles such as:

- broker;
- insurance representative;
- distributor;
- marketplace/platform;
- assistance provider;
- travel agency/operator involved in distribution;
- technology/service provider;
- another source-backed market role.

Absence of a broker does not imply a missing link. A representative arrangement may be legitimate and materially different from brokerage.

## Risk carrier

CNAE, corporate group, name similarity or commercial distribution never creates a `risk_carrier` relationship.

A carrier relationship requires source-backed product/arrangement evidence and may need explicit scope by product and/or period.

```text
distribution relationship != risk_carrier
risk_carrier_relation != complaint_transfer
```

Multi-carrier arrangements must remain multi-carrier. The system must not collapse them into a single global carrier merely for convenience.

## Automatic effects forbidden

Receita enrichment preserves:

```text
assertion_effect = none
automatic_registry_mutation = forbidden
assessment_inheritance = forbidden
complaint_transfer_effect = none
```

It may accelerate review; it cannot manufacture a public fact.

## Implemented modules

`api/v2/market_role_signals.py`

- extracts bounded CNAE role signals;
- separates exact legal identity from market role;
- gives exact current SUSEP CNPJ precedence for insurer status;
- refuses automatic role or carrier claims;
- keeps zero/multiple Receita legal roots unresolved.

`api/v2/enrich_market_identity_candidates.py`

- enriches the existing candidate registry with a directed Receita snapshot when such a snapshot exists;
- preserves lifecycle/review state and all candidate guardrails;
- does not require GSC;
- does not mutate canonical registries.

## Operational boundary still intentional

The evergreen sensor does not yet perform a fresh national Receita bulk scan for every unknown widget query. Duplicating the full Receita acquisition inside the market-sensor workflow would be a disproportionate and tightly coupled design.

The new enrichment layer is therefore source-ready but only executes when a directed Receita candidate snapshot is supplied. The next pipeline decision should determine how those snapshots are acquired/reused without making the lightweight weekly sensor re-download the national corpus.

This limitation is explicit rather than hidden: current production discovery remains observational until stronger evidence is available.
