from __future__ import annotations

import gzip
import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any

MONTHLY_DIR = Path("data/derived/consumidor_gov/monthly")
MONTHS = [
    "2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12",
    "2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06",
]

CASES = {
    "Loovi": {
        "labels": ["Loovi", "Loovi Seguros", "Loovi Technology", "CW Technology", "CW Technology Ltda"],
        "contains": ["loovi", "cw technology"],
        "cnpjs": ["34504257000100"],
    },
    "Affinity Viagem": {
        "labels": ["Affinity Viagem", "Affinity", "ASPAS Turismo Viagens e Assistencia Internacional S/A", "ASPAS Turismo Viagens e Assistencia Internacional"],
        "contains": ["affinity", "aspas turismo"],
        "cnpjs": ["17846647000131"],
    },
    "Assistente de Viagem": {
        "labels": ["Assistente de Viagem", "3RI Viagens e Turismo LTDA", "3RI Viagens e Turismo", "Sabemi Seguradora S.A.", "Sabemi Seguradora", "Sabemi"],
        "contains": ["assistente de viagem", "3ri viagens", "sabemi"],
        "cnpjs": ["13711169000100", "87163234000138"],
    },
    "AllSafe": {
        "labels": ["AllSafe", "All Safe", "PACTA Gestao de Negocios Ltda", "PACTA Gestao de Negocios", "Split Risk Seguradora S.A.", "Split Risk Seguradora", "Split Risk"],
        "contains": ["allsafe", "all safe", "pacta gestao", "split risk"],
        "cnpjs": ["33897006000162", "43505273000109"],
    },
    "Assist Card": {
        "labels": ["Assist Card", "Assist-Card", "Assist Card do Brasil", "Assist-Card do Brasil Ltda", "Assist Card Smalline S.A.", "Assist Card Smalline", "Starr International Brasil Seguradora S.A.", "Starr International Brasil Seguradora", "Starr Brasil", "Global Apoio Administrativo a Empresa Ltda", "Global Apoio Administrativo a Empresa"],
        "contains": ["assist card", "assist-card", "smalline", "starr international brasil", "starr brasil", "global apoio administrativo"],
        "cnpjs": ["00027571000110", "17341270000169", "31772960000101"],
    },
    "Ituran": {
        "labels": ["Ituran", "Ituran Servicos Ltda", "Ituran Servicos"],
        "contains": ["ituran"],
        "cnpjs": ["16613061000164"],
    },
    "Universal Assistance": {
        "labels": ["Universal Assistance", "Asistbras - Assistencia ao Viajante Ltda", "Asistbras Assistencia ao Viajante Ltda", "Asistbras", "Zurich Minas Brasil Seguros S.A.", "Zurich Minas Brasil Seguros", "Zurich Minas Brasil"],
        "contains": ["universal assistance", "asistbras", "zurich minas brasil"],
        "cnpjs": ["07139957000162", "17197385000121"],
    },
    "Vital Card": {
        "labels": ["Vital Card", "VitalCard", "Schultz-Inga Turismo Ltda", "Schultz Inga Turismo Ltda", "Schultz-Inga Turismo", "Schultz Inga Turismo", "STZ Corretora de Seguros Ltda", "STZ Corretora de Seguros", "Allseg Seguradora S/A", "Allseg Seguradora", "Allseg"],
        "contains": ["vital card", "vitalcard", "schultz inga", "stz corretora", "allseg"],
        "cnpjs": ["04628135000157", "12353171000183", "67865360000127"],
    },
}


def norm(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def cnpj_digits(value: Any) -> str | None:
    d = re.sub(r"\D+", "", str(value or ""))
    return d if len(d) == 14 else None


def load_month(month: str) -> tuple[dict[str, Any], dict[str, Any], Path]:
    path = MONTHLY_DIR / f"consumidor_gov_{month}.json.gz"
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        root = json.load(fh)
    if not isinstance(root, dict):
        raise RuntimeError(f"invalid monthly root: {path}")
    entries = root.get("by_name_key_raw") or root.get("by_name_key") or root.get("by_name") or {}
    if not isinstance(entries, dict) or not entries:
        raise RuntimeError(f"monthly snapshot has no provider entries: {path}")
    return root, entries, path


def provider_label(key: str, entry: dict[str, Any]) -> str:
    return str(entry.get("display_name") or entry.get("name") or entry.get("displayName") or key).strip()


def get_stats(entry: dict[str, Any]) -> dict[str, Any]:
    raw = entry.get("statistics") if isinstance(entry.get("statistics"), dict) else entry
    def number(*names: str) -> int:
        for name in names:
            if raw.get(name) not in (None, ""):
                try:
                    return int(float(raw[name]))
                except (TypeError, ValueError):
                    continue
        return 0
    return {
        "complaints": number("complaintsCount", "total_claims", "totalClaims", "total"),
        "responded": number("respondedCount", "responded_claims", "respondidas"),
        "finalized": number("finalizedCount", "finalized_claims", "finalizadas"),
        "resolved": number("resolvedCount", "resolved_claims", "resolvidas_indicador"),
        "satisfaction_count": number("satisfactionCount", "evaluatedCount", "satisfaction_count", "nota_count"),
    }


def classify_match(case: dict[str, Any], label: str, cnpj: str | None) -> tuple[str | None, list[str]]:
    if cnpj and cnpj in case["cnpjs"]:
        return "exact_cnpj", [cnpj]
    label_n = norm(label)
    exact_map = {norm(v): v for v in case["labels"]}
    if label_n in exact_map:
        return "exact_candidate_label", [exact_map[label_n]]
    hits = [phrase for phrase in case["contains"] if norm(phrase) and norm(phrase) in label_n]
    if hits:
        return "discovery_contains_phrase", hits
    return None, []


def main() -> None:
    occurrences: list[dict[str, Any]] = []
    inventory: list[dict[str, Any]] = []

    for month in MONTHS:
        root, entries, path = load_month(month)
        valid_cnpjs: set[str] = set()
        providers_with_cnpj = 0
        for key, raw_entry in entries.items():
            entry = raw_entry if isinstance(raw_entry, dict) else {}
            label = provider_label(str(key), entry)
            cnpj = cnpj_digits(entry.get("cnpj") or entry.get("cnpjKey") or entry.get("cnpj_key"))
            if cnpj:
                providers_with_cnpj += 1
                valid_cnpjs.add(cnpj)
            for case_name, case in CASES.items():
                basis, matched = classify_match(case, label, cnpj)
                if basis is None:
                    continue
                occurrences.append({
                    "case": case_name,
                    "month": month,
                    "provider_label": label,
                    "provider_key": str(key),
                    "provider_cnpj": cnpj,
                    "match_basis": basis,
                    "matched_queries": matched,
                    **get_stats(entry),
                })
        inventory.append({
            "month": month,
            "path": str(path),
            "provider_entries": len(entries),
            "providers_with_valid_cnpj": providers_with_cnpj,
            "unique_valid_cnpjs": len(valid_cnpjs),
            "meta": root.get("meta") if isinstance(root.get("meta"), dict) else {},
        })

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in occurrences:
        grouped[row["case"]].append(row)

    cases_out = []
    for case_name, case in CASES.items():
        rows = grouped.get(case_name, [])
        identities: dict[tuple[str, str | None], dict[str, Any]] = {}
        for row in rows:
            key = (row["provider_label"], row["provider_cnpj"])
            agg = identities.setdefault(key, {
                "provider_label": row["provider_label"],
                "provider_cnpj": row["provider_cnpj"],
                "months": [],
                "complaints": 0,
                "responded": 0,
                "finalized": 0,
                "resolved": 0,
                "satisfaction_count": 0,
                "match_bases": set(),
            })
            agg["months"].append(row["month"])
            for field in ("complaints", "responded", "finalized", "resolved", "satisfaction_count"):
                agg[field] += int(row.get(field) or 0)
            agg["match_bases"].add(row["match_basis"])
        identity_rows = []
        for agg in identities.values():
            agg["months"] = sorted(set(agg["months"]))
            agg["match_bases"] = sorted(agg["match_bases"])
            identity_rows.append(agg)
        identity_rows.sort(key=lambda r: (-r["complaints"], r["provider_label"].lower()))

        label_cnpjs: dict[str, set[str | None]] = defaultdict(set)
        for row in rows:
            label_cnpjs[norm(row["provider_label"])].add(row["provider_cnpj"])
        collisions = []
        for label_n, values in label_cnpjs.items():
            if len(values) > 1:
                collisions.append({
                    "normalized_label": label_n,
                    "cnpjs": sorted(v for v in values if v),
                    "has_missing_cnpj": None in values,
                })

        cases_out.append({
            "case": case_name,
            "target_cnpjs": case["cnpjs"],
            "candidate_labels": case["labels"],
            "occurrence_rows": len(rows),
            "distinct_provider_identities": len(identity_rows),
            "total_complaints_across_matched_rows": sum(int(r.get("complaints") or 0) for r in rows),
            "provider_identities": identity_rows,
            "label_cnpj_collisions": collisions,
        })

    payload = {
        "audit": "consumer_gov_12_month_provider_label_occurrences",
        "months": MONTHS,
        "scope_note": "Discovery audit only; textual occurrence does not prove legal identity, role, carrier, or complaint transfer.",
        "matching_policy": {
            "exact_cnpj": "strong source identity signal when the snapshot preserves CNPJ",
            "exact_candidate_label": "exact normalized equality against the supplied commercial/legal candidate labels",
            "discovery_contains_phrase": "recall-only discovery, never sufficient by itself for attribution",
            "fuzzy_matching": False,
        },
        "month_inventory": inventory,
        "cases": cases_out,
        "occurrences": sorted(occurrences, key=lambda r: (r["case"], r["month"], r["provider_label"].lower())),
    }
    Path("consumer_gov_provider_label_audit.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    lines = [
        "# Auditoria objetiva — rótulos de fornecedores no Consumer.gov",
        "",
        f"Janela: `{MONTHS[0]}` a `{MONTHS[-1]}`.",
        "",
        "A auditoria registra ocorrências. Não transfere reclamações e não converte coincidência textual em identidade jurídica.",
        "",
        "## Cobertura dos 12 snapshots",
        "",
        "| Mês | Entradas | Entradas com CNPJ | CNPJs únicos |",
        "|---|---:|---:|---:|",
    ]
    for row in inventory:
        lines.append(f"| {row['month']} | {row['provider_entries']} | {row['providers_with_valid_cnpj']} | {row['unique_valid_cnpjs']} |")
    for summary in cases_out:
        lines.extend(["", f"## {summary['case']}", ""])
        if not summary["provider_identities"]:
            lines.append("Nenhuma ocorrência encontrada pelos critérios da auditoria.")
            continue
        lines.extend([
            "| Rótulo observado | CNPJ preservado | Meses | Reclamações | Match |",
            "|---|---|---|---:|---|",
        ])
        for row in summary["provider_identities"]:
            lines.append(
                f"| {row['provider_label']} | {row['provider_cnpj'] or '—'} | {', '.join(row['months'])} | {row['complaints']} | {', '.join(row['match_bases'])} |"
            )
        lines.append("")
        lines.append(f"Total de reclamações nas linhas encontradas: **{summary['total_complaints_across_matched_rows']}**.")
        if summary["label_cnpj_collisions"]:
            lines.append("")
            lines.append("Colisões de rótulo/CNPJ observadas:")
            for collision in summary["label_cnpj_collisions"]:
                values = ", ".join(collision["cnpjs"]) or "sem CNPJ"
                if collision["has_missing_cnpj"]:
                    values += ", também sem CNPJ"
                lines.append(f"- `{collision['normalized_label']}` → {values}")
    Path("consumer_gov_provider_label_audit.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"inventory": inventory, "cases": cases_out}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
