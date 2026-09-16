from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import normalize_name_key
from api.v2.market_role_signals import resolve_unknown_query_with_receita_candidates


def _provider_match_index(receita_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in receita_payload.get("provider_matches") or []:
        if not isinstance(row, dict):
            continue
        raw = str(row.get("provider") or row.get("provider_key") or "").strip()
        key = normalize_name_key(raw)
        if not key:
            continue
        if key in out:
            raise ValueError(f"duplicate Receita provider match for {raw}")
        out[key] = row
    return out


def _licensed_cnpjs(payload: Any) -> set[str]:
    if payload is None:
        return set()
    if isinstance(payload, dict):
        rows = payload.get("entities") or payload.get("records") or []
    elif isinstance(payload, list):
        rows = payload
    else:
        raise TypeError("licensed payload must be a list or object")
    out: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        cnpj = normalize_cnpj_v2(row.get("cnpj"))
        if cnpj:
            out.add(cnpj)
    return out


def enrich_candidate_registry_with_receita(
    candidate_registry: dict[str, Any],
    receita_payload: dict[str, Any],
    *,
    current_licensed_payload: Any = None,
) -> dict[str, Any]:
    """Attach non-authoritative Receita investigation evidence to candidates.

    This function never changes lifecycle state, review state, assertion effect,
    score effect, complaint attribution or canonical-registry mutation policy.
    It only enriches candidates for faster source-backed review.
    """

    if candidate_registry.get("artifact") != "v2_market_identity_candidate_registry":
        raise ValueError("unexpected market identity candidate artifact")
    if receita_payload.get("artifact") not in {None, "v2_receita_cnpj_identity"}:
        raise ValueError("unexpected Receita identity artifact")

    provider_index = _provider_match_index(receita_payload)
    licensed_cnpjs = _licensed_cnpjs(current_licensed_payload)

    output = json.loads(json.dumps(candidate_registry))
    enriched = 0
    unresolved = 0

    for candidate in output.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        observations = candidate.get("observations") or []
        query_values = [
            str(row.get("normalized_query") or row.get("observed_value") or "").strip()
            for row in observations
            if isinstance(row, dict)
            and str(row.get("sensor_class") or "") == "demand"
        ]
        query_values = [value for value in query_values if value]
        if not query_values:
            continue

        # Demand observations for one candidate share an exact normalized key.
        query = query_values[0]
        match = provider_index.get(normalize_name_key(query))
        if match is None:
            candidate["investigation"] = {
                "source": "receita_cnpj",
                "state": "not_available_for_query",
                "query": query,
                "assertion_effect": "none",
            }
            unresolved += 1
            continue

        rows = [row for row in match.get("candidates") or [] if isinstance(row, dict)]
        resolution = resolve_unknown_query_with_receita_candidates(
            query,
            rows,
            current_susep_insurer_cnpjs=licensed_cnpjs,
        )
        candidate["investigation"] = {
            "source": "receita_cnpj",
            "state": "enriched",
            "provider_candidate_state": match.get("candidate_state"),
            "provider_match_method": match.get("match_method"),
            "resolution": resolution,
            "assertion_effect": "none",
        }
        enriched += 1

    output["investigation_enrichment"] = {
        "receita": {
            "mode": "directed_candidate_enrichment",
            "authority_semantics": "identity_and_role_hypothesis_only",
            "may_assert_public_market_role": False,
            "may_assert_risk_carrier": False,
            "may_mutate_registry": False,
            "enriched_candidate_count": enriched,
            "unresolved_candidate_count": unresolved,
        }
    }
    return output


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Attach conservative Receita/CNAE investigation evidence to market candidates."
    )
    parser.add_argument("candidate_registry", type=Path)
    parser.add_argument("receita_snapshot", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--current-licensed", type=Path)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    candidate_registry = _load(args.candidate_registry)
    receita_payload = _load(args.receita_snapshot)
    licensed = _load(args.current_licensed) if args.current_licensed else None
    output = enrich_candidate_registry_with_receita(
        candidate_registry,
        receita_payload,
        current_licensed_payload=licensed,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(output["investigation_enrichment"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
