from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from api.build_consumidor_gov import (
    RAW_DIR,
    TARGET_SEGMENT,
    _download,
    _iter_rows,
    _list_basecompleta_resources,
    _norm,
)
from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import get_name_tokens, normalize_name_key

ELIGIBILITY_PATH = Path("data/derived/v2/entity_eligibility_inventory.json")
OUTPUT_PATH = Path("data/derived/v2/consumer_gov_157_experiment.json")
MONTHS_BACK = 12

PROVIDER_NAME_COLUMNS = ("Nome Fantasia", "Empresa", "Fornecedor", "Nome do Fornecedor")
RESPONDED_COLUMNS = ("Respondida", "Respondida?", "Empresa Respondeu", "Respondeu")
SITUATION_COLUMNS = ("Situação", "Situacao", "Status", "Situação da Reclamação")
EVALUATION_COLUMNS = ("Avaliação Reclamação", "Avaliacao Reclamacao", "Avaliação")
SCORE_COLUMNS = ("Nota do Consumidor", "Nota Consumidor", "Nota")
AREA_COLUMNS = ("Área", "Area")
SUBJECT_COLUMNS = ("Assunto",)
GROUP_PROBLEM_COLUMNS = ("Grupo Problema", "Grupo de Problema")
PROBLEM_COLUMNS = ("Problema",)

GENERIC_NAME_TOKENS = {
    "sa", "s", "a", "ltda", "cia", "companhia", "sociedade", "brasil", "brasileira",
    "seguro", "seguros", "seguradora", "seguradoras", "gerais", "de", "da", "do", "das",
    "dos", "e", "em", "para", "participacoes", "holding", "grupo",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _pick(row: dict[str, str], columns: tuple[str, ...]) -> str:
    for column in columns:
        value = row.get(column)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _truthy_pt(value: str) -> bool:
    return _norm(value) in {"s", "sim", "1", "true", "yes"}


def _safe_float(value: str) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        number = float(text.replace(",", "."))
    except ValueError:
        return None
    return number


def _core_name(name: str) -> str:
    tokens = [
        token for token in normalize_name_key(name).split()
        if token not in GENERIC_NAME_TOKENS and len(token) > 1
    ]
    return " ".join(tokens)


def _build_unique_index(pairs: list[tuple[str, str]]) -> tuple[dict[str, str], dict[str, list[str]]]:
    raw: dict[str, set[str]] = defaultdict(set)
    for key, entity_id in pairs:
        if key:
            raw[key].add(entity_id)
    unique = {key: next(iter(ids)) for key, ids in raw.items() if len(ids) == 1}
    ambiguous = {key: sorted(ids) for key, ids in raw.items() if len(ids) > 1}
    return unique, ambiguous


def _entity_names(entity: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for field in ("legal_name", "display_name", "name", "regulatory_name", "source_name"):
        value = str(entity.get(field) or "").strip()
        if value:
            names.append(value)
    evidence = entity.get("evidence") or {}
    for source in ("licensed", "ses_identity", "ses"):
        block = evidence.get(source) or {}
        if isinstance(block, dict):
            for field in ("legal_name", "name", "source_name"):
                value = str(block.get(field) or "").strip()
                if value:
                    names.append(value)
    return sorted(set(names))


def _build_indexes(payload: dict[str, Any]) -> dict[str, Any]:
    eligible = [
        entity for entity in payload.get("entities") or []
        if (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
    ]
    entity_by_id = {str(entity["entity_id"]): entity for entity in eligible}
    cnpj_to_entity = {
        normalize_cnpj_v2(entity.get("cnpj")): str(entity["entity_id"])
        for entity in eligible
        if normalize_cnpj_v2(entity.get("cnpj"))
    }

    exact_pairs: list[tuple[str, str]] = []
    core_pairs: list[tuple[str, str]] = []
    all_names_by_entity: dict[str, list[str]] = defaultdict(list)
    for entity in eligible:
        entity_id = str(entity["entity_id"])
        for name in _entity_names(entity):
            all_names_by_entity[entity_id].append(name)
            exact_pairs.append((normalize_name_key(name), entity_id))
            core_pairs.append((_core_name(name), entity_id))

    verified_brand_pairs: list[tuple[str, str]] = []
    for brand in payload.get("brands") or []:
        targets = {
            str(relation.get("target_entity_id"))
            for relation in brand.get("relationships") or []
            if relation.get("target_entity_id") in entity_by_id
            and relation.get("relationship_type") in {"brand_of", "risk_carrier"}
        }
        if len(targets) != 1:
            continue
        entity_id = next(iter(targets))
        for name in [brand.get("name"), *(brand.get("aliases") or [])]:
            value = str(name or "").strip()
            if value:
                verified_brand_pairs.append((normalize_name_key(value), entity_id))
                core_pairs.append((_core_name(value), entity_id))
                all_names_by_entity[entity_id].append(value)

    exact, exact_ambiguous = _build_unique_index(exact_pairs)
    brand, brand_ambiguous = _build_unique_index(verified_brand_pairs)
    core, core_ambiguous = _build_unique_index(core_pairs)
    return {
        "eligible": eligible,
        "entity_by_id": entity_by_id,
        "cnpj": cnpj_to_entity,
        "exact_name": exact,
        "verified_brand": brand,
        "core_name": core,
        "all_names_by_entity": {k: sorted(set(v)) for k, v in all_names_by_entity.items()},
        "ambiguities": {
            "exact_name": exact_ambiguous,
            "verified_brand": brand_ambiguous,
            "core_name": core_ambiguous,
        },
    }


def _detect_cnpj_columns(headers: list[str]) -> list[str]:
    return sorted({header for header in headers if "cnpj" in normalize_name_key(header)})


def _match_provider(
    row: dict[str, str], indexes: dict[str, Any], cnpj_columns: list[str]
) -> tuple[str | None, str, str]:
    for column in cnpj_columns:
        cnpj = normalize_cnpj_v2(row.get(column))
        if cnpj and cnpj in indexes["cnpj"]:
            return indexes["cnpj"][cnpj], "cnpj_exact", cnpj

    provider = _pick(row, PROVIDER_NAME_COLUMNS)
    key = normalize_name_key(provider)
    if key and key in indexes["exact_name"]:
        return indexes["exact_name"][key], "legal_name_exact", provider
    if key and key in indexes["verified_brand"]:
        return indexes["verified_brand"][key], "verified_brand_exact", provider

    core = _core_name(provider)
    if core and len(core) >= 4 and core in indexes["core_name"]:
        return indexes["core_name"][core], "unique_core_name", provider
    return None, "unmatched", provider


def _candidate_suggestions(provider: str, indexes: dict[str, Any], limit: int = 3) -> list[dict[str, Any]]:
    provider_key = normalize_name_key(provider)
    provider_tokens = set(get_name_tokens(provider))
    candidates: list[tuple[float, str, str]] = []
    for entity_id, names in indexes["all_names_by_entity"].items():
        best = 0.0
        best_name = ""
        for name in names:
            name_key = normalize_name_key(name)
            seq = SequenceMatcher(None, provider_key, name_key).ratio()
            target_tokens = set(get_name_tokens(name))
            union = provider_tokens | target_tokens
            jac = (len(provider_tokens & target_tokens) / len(union)) if union else 0.0
            score = max(seq, jac)
            if score > best:
                best = score
                best_name = name
        candidates.append((best, entity_id, best_name))
    return [
        {"entity_id": entity_id, "name": name, "diagnostic_similarity": round(score, 4)}
        for score, entity_id, name in sorted(candidates, reverse=True)[:limit]
        if score >= 0.55
    ]


def _new_entity_stats(entity: dict[str, Any]) -> dict[str, Any]:
    return {
        "entity_id": entity.get("entity_id"),
        "fip_code": entity.get("fip_code"),
        "cnpj": entity.get("cnpj"),
        "legal_name": entity.get("legal_name"),
        "months_with_complaints": 0,
        "complaints": 0,
        "responded": 0,
        "finalized": 0,
        "evaluated": 0,
        "consumer_resolved": 0,
        "consumer_not_resolved": 0,
        "satisfaction_sum": 0.0,
        "match_methods": {},
        "by_month": {},
        "areas": {},
        "subjects": {},
        "problem_groups": {},
        "problems": {},
    }


def _top(counter: Counter[str], n: int = 20) -> dict[str, int]:
    return dict(counter.most_common(n))


def build_experiment() -> dict[str, Any]:
    eligibility = json.loads(ELIGIBILITY_PATH.read_text(encoding="utf-8"))
    indexes = _build_indexes(eligibility)
    eligible = indexes["eligible"]
    if len(eligible) != 157:
        raise RuntimeError(f"expected 157 regulatory eligible insurers, got {len(eligible)}")

    resources = _list_basecompleta_resources()
    selected_months = sorted(resources)[-MONTHS_BACK:]
    if len(selected_months) < MONTHS_BACK:
        raise RuntimeError(f"expected at least {MONTHS_BACK} Consumer.gov months, got {len(selected_months)}")

    entity_stats = {
        str(entity["entity_id"]): _new_entity_stats(entity) for entity in eligible
    }
    monthly_meta: list[dict[str, Any]] = []
    all_cnpj_columns: set[str] = set()
    match_method_counts: Counter[str] = Counter()
    unmatched_provider_rows: Counter[str] = Counter()
    unmatched_provider_names: Counter[str] = Counter()
    insurance_rows_total = 0
    matched_rows_total = 0
    areas = Counter()
    subjects = Counter()
    problem_groups = Counter()
    problems = Counter()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for month in selected_months:
        resource = resources[month]
        suffix = Path(resource.url.split("?", 1)[0]).suffix or ".csv"
        raw_path = RAW_DIR / f"consumer_gov_experiment_{month}{suffix}"
        _download(resource.url, raw_path)

        headers: list[str] = []
        month_rows = month_matched = 0
        month_entity_counts: Counter[str] = Counter()
        month_methods: Counter[str] = Counter()
        try:
            for row in _iter_rows(raw_path):
                if not headers:
                    headers = list(row.keys())
                if _norm(row.get("Segmento de Mercado", "")) != _norm(TARGET_SEGMENT):
                    continue
                if "cancelada" in _norm(_pick(row, SITUATION_COLUMNS)):
                    continue

                month_rows += 1
                insurance_rows_total += 1
                cnpj_columns = _detect_cnpj_columns(headers)
                all_cnpj_columns.update(cnpj_columns)
                entity_id, method, provider = _match_provider(row, indexes, cnpj_columns)
                match_method_counts[method] += 1
                month_methods[method] += 1
                if not entity_id:
                    if provider:
                        unmatched_provider_rows[provider] += 1
                        unmatched_provider_names[provider] += 1
                    continue

                month_matched += 1
                matched_rows_total += 1
                month_entity_counts[entity_id] += 1
                stats = entity_stats[entity_id]
                stats["complaints"] += 1
                stats["match_methods"][method] = stats["match_methods"].get(method, 0) + 1

                responded = _truthy_pt(_pick(row, RESPONDED_COLUMNS))
                situation = _norm(_pick(row, SITUATION_COLUMNS))
                evaluation = _norm(_pick(row, EVALUATION_COLUMNS))
                score = _safe_float(_pick(row, SCORE_COLUMNS))
                finalized = "finalizada" in situation or "encerrada" in situation
                evaluated = bool(evaluation) or (score is not None and score > 0)
                resolved = evaluation == "resolvida"
                not_resolved = "nao resolvida" in evaluation or "não resolvida" in evaluation

                stats["responded"] += int(responded)
                stats["finalized"] += int(finalized)
                stats["evaluated"] += int(evaluated)
                stats["consumer_resolved"] += int(resolved)
                stats["consumer_not_resolved"] += int(not_resolved)
                if score is not None and score > 0:
                    stats["satisfaction_sum"] += score

                for columns, label, global_counter in (
                    (AREA_COLUMNS, "areas", areas),
                    (SUBJECT_COLUMNS, "subjects", subjects),
                    (GROUP_PROBLEM_COLUMNS, "problem_groups", problem_groups),
                    (PROBLEM_COLUMNS, "problems", problems),
                ):
                    value = _pick(row, columns)
                    if value:
                        global_counter[value] += 1
                        bucket = stats[label]
                        bucket[value] = bucket.get(value, 0) + 1

            for entity_id, count in month_entity_counts.items():
                stats = entity_stats[entity_id]
                stats["by_month"][month] = count
                stats["months_with_complaints"] += 1

            monthly_meta.append({
                "month": month,
                "resource_name": resource.name,
                "resource_url": resource.url,
                "headers": headers,
                "public_cnpj_columns": _detect_cnpj_columns(headers),
                "insurance_segment_rows": month_rows,
                "matched_rows": month_matched,
                "matched_row_ratio": month_matched / month_rows if month_rows else None,
                "observed_insurers": len(month_entity_counts),
                "match_methods": dict(month_methods),
            })
        finally:
            raw_path.unlink(missing_ok=True)

    for stats in entity_stats.values():
        evaluated = int(stats["evaluated"])
        scored = int(stats["consumer_resolved"]) + int(stats["consumer_not_resolved"])
        stats["evaluated_resolution_rate"] = (
            stats["consumer_resolved"] / scored if scored else None
        )
        stats["average_satisfaction"] = (
            stats["satisfaction_sum"] / evaluated if evaluated else None
        )
        for label in ("areas", "subjects", "problem_groups", "problems"):
            stats[label] = dict(sorted(stats[label].items(), key=lambda item: (-item[1], item[0]))[:15])

    observed = [stats for stats in entity_stats.values() if stats["complaints"] > 0]
    thresholds = [1, 5, 10, 20, 30, 50, 100]
    threshold_counts = {
        str(threshold): sum(stats["complaints"] >= threshold for stats in entity_stats.values())
        for threshold in thresholds
    }
    evaluated_thresholds = [1, 5, 10, 20, 30]
    evaluated_counts = {
        str(threshold): sum(stats["evaluated"] >= threshold for stats in entity_stats.values())
        for threshold in evaluated_thresholds
    }

    top_unmatched: list[dict[str, Any]] = []
    for provider, count in unmatched_provider_rows.most_common(40):
        top_unmatched.append({
            "provider": provider,
            "rows": count,
            "candidate_suggestions_non_authoritative": _candidate_suggestions(provider, indexes),
        })

    return {
        "artifact": "v2_consumer_gov_157_experiment",
        "generated_at": _utc_now(),
        "status": "experimental",
        "methodology_note": (
            "Coverage experiment only. No score, assessment eligibility or ranking eligibility is changed. "
            "Accepted matching is deterministic: exact public CNPJ when available; exact normalized legal/current "
            "entity name; exact verified brand; or a unique exact core-name key. Fuzzy similarity is emitted only "
            "as a non-authoritative diagnostic for unmatched provider names. Absence from Consumer.gov is not zero "
            "complaints because company participation in that platform is not universal."
        ),
        "source": {
            "provider": "Senacon / Ministerio da Justica e Seguranca Publica",
            "dataset": "reclamacoes-do-consumidor-gov-br",
            "ckan_api": "https://dados.mj.gov.br/api/3/action",
            "target_segment": TARGET_SEGMENT,
            "months": selected_months,
            "public_cnpj_columns_detected": sorted(all_cnpj_columns),
        },
        "universe": {
            "regulatory_universe": "ordinary_current_insurers",
            "eligible_insurers": len(eligible),
            "observed_insurers": len(observed),
            "unobserved_insurers": len(eligible) - len(observed),
            "coverage_ratio": len(observed) / len(eligible),
            "insurers_by_min_complaints_12m": threshold_counts,
            "insurers_by_min_evaluated_complaints_12m": evaluated_counts,
        },
        "rows": {
            "insurance_segment_rows": insurance_rows_total,
            "matched_rows": matched_rows_total,
            "matched_row_ratio": matched_rows_total / insurance_rows_total if insurance_rows_total else None,
            "match_methods": dict(match_method_counts),
            "unmatched_rows": insurance_rows_total - matched_rows_total,
            "distinct_unmatched_provider_names": len(unmatched_provider_names),
        },
        "taxonomy": {
            "top_areas": _top(areas),
            "top_subjects": _top(subjects),
            "top_problem_groups": _top(problem_groups),
            "top_problems": _top(problems, 30),
        },
        "monthly": monthly_meta,
        "entities": sorted(entity_stats.values(), key=lambda item: (-item["complaints"], str(item["entity_id"]))),
        "top_unmatched_providers": top_unmatched,
        "matching_ambiguities": indexes["ambiguities"],
    }


def main() -> None:
    payload = build_experiment()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp = OUTPUT_PATH.with_suffix(OUTPUT_PATH.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(OUTPUT_PATH)
    print(json.dumps({
        "artifact": payload["artifact"],
        "months": payload["source"]["months"],
        "public_cnpj_columns_detected": payload["source"]["public_cnpj_columns_detected"],
        "universe": payload["universe"],
        "rows": payload["rows"],
        "top_unmatched_providers": payload["top_unmatched_providers"][:10],
    }, ensure_ascii=False, indent=2))
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
