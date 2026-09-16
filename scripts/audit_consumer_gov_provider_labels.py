from __future__ import annotations

import json
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from api.v2.consumer_gov_conduct_core import load_monthly_entries

MONTHS = [
    "2025-07",
    "2025-08",
    "2025-09",
    "2025-10",
    "2025-11",
    "2025-12",
    "2026-01",
    "2026-02",
    "2026-03",
    "2026-04",
    "2026-05",
    "2026-06",
]

CASES = {
    "Loovi": {
        "labels": [
            "Loovi",
            "Loovi Seguros",
            "Loovi Technology",
            "CW Technology",
            "CW Technology Ltda",
        ],
        "contains": ["loovi", "cw technology"],
        "cnpjs": ["34504257000100"],
    },
    "Affinity Viagem": {
        "labels": [
            "Affinity Viagem",
            "Affinity",
            "ASPAS Turismo, Viagens e Assistencia Internacional S/A",
            "ASPAS Turismo Viagens e Assistencia Internacional",
        ],
        "contains": ["affinity", "aspas turismo"],
        "cnpjs": ["17846647000131"],
    },
    "Assistente de Viagem": {
        "labels": [
            "Assistente de Viagem",
            "3RI Viagens e Turismo LTDA",
            "3RI Viagens e Turismo",
            "Sabemi Seguradora S.A.",
            "Sabemi Seguradora",
            "Sabemi",
        ],
        "contains": ["assistente de viagem", "3ri viagens", "sabemi"],
        "cnpjs": ["13711169000100", "87163234000138"],
    },
    "AllSafe": {
        "labels": [
            "AllSafe",
            "All Safe",
            "PACTA Gestao de Negocios Ltda",
            "PACTA Gestao de Negocios",
            "Split Risk Seguradora S.A.",
            "Split Risk Seguradora",
            "Split Risk",
        ],
        "contains": ["allsafe", "all safe", "pacta gestao", "split risk"],
        "cnpjs": ["33897006000162", "43505273000109"],
    },
    "Assist Card": {
        "labels": [
            "Assist Card",
            "Assist-Card",
            "Assist Card do Brasil",
            "Assist-Card do Brasil Ltda",
            "Assist Card Smalline S.A.",
            "Assist Card Smalline",
            "Starr International Brasil Seguradora S.A.",
            "Starr International Brasil Seguradora",
            "Starr Brasil",
            "Global Apoio Administrativo a Empresa Ltda",
            "Global Apoio Administrativo a Empresa",
        ],
        "contains": [
            "assist card",
            "assist-card",
            "smalline",
            "starr international brasil",
            "starr brasil",
            "global apoio administrativo",
        ],
        "cnpjs": ["00027571000110", "17341270000169", "31772960000101"],
    },
    "Ituran": {
        "labels": ["Ituran", "Ituran Servicos Ltda", "Ituran Servicos"],
        "contains": ["ituran"],
        "cnpjs": ["16613061000164"],
    },
    "Universal Assistance": {
        "labels": [
            "Universal Assistance",
            "Asistbras - Assistencia ao Viajante Ltda",
            "Asistbras Assistencia ao Viajante Ltda",
            "Asistbras",
            "Zurich Minas Brasil Seguros S.A.",
            "Zurich Minas Brasil Seguros",
            "Zurich Minas Brasil",
        ],
        "contains": [
            "universal assistance",
            "asistbras",
            "zurich minas brasil",
        ],
        "cnpjs": ["07139957000162", "17197385000121"],
    },
    "Vital Card": {
        "labels": [
            "Vital Card",
            "VitalCard",
            "Schultz-Inga Turismo Ltda",
            "Schultz Inga Turismo Ltda",
            "Schultz-Inga Turismo",
            "Schultz Inga Turismo",
            "STZ Corretora de Seguros Ltda",
            "STZ Corretora de Seguros",
            "Allseg Seguradora S/A",
            "Allseg Seguradora",
            "Allseg",
        ],
        "contains": [
            "vital card",
            "vitalcard",
            "schultz inga",
            "stz corretora",
            "allseg",
        ],
        "cnpjs": ["04628135000157", "12353171000183", "67865360000127"],
    },
}

OUTPUT_JSON = Path("consumer_gov_provider_label_audit.json")
OUTPUT_MD = Path("consumer_gov_provider_label_audit.md")


def norm(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def digits(value: Any) -> str | None:
    d = re.sub(r"\D+", "", str(value or ""))
    return d if len(d) == 14 else None


def provider_name(raw_key: str, entry: dict[str, Any]) -> str:
    return str(
        entry.get("display_name")
        or entry.get("name")
        or entry.get("displayName")
        or raw_key
    ).strip()


def stats(entry: dict[str, Any]) -> dict[str, Any]:
    raw = entry.get("statistics") if isinstance(entry.get("statistics"), dict) else {}
    def integer(*keys: str) -> int:
        for key in keys:
            if raw.get(key) not in (None, ""):
                try:
                    return int(raw[key])
                except (TypeError, ValueError):
                    pass
        return 0

    return {
        "complaints": integer("complaintsCount", "total_claims", "totalClaims"),
        "responded": integer("respondedCount", "responded_claims"),
        "finalized": integer("finalizedCount", "finalized_claims"),
        "resolved": integer("resolvedCount", "resolved_claims"),
        "satisfaction_count": integer(
            "satisfactionCount", "evaluatedCount", "satisfaction_count"
        ),
        "score_sum": raw.get("scoreSum", raw.get("score_sum")),
    }


def match_case(
    case: dict[str, Any], provider: str, cnpj: str | None
) -> tuple[str | None, list[str]]:
    if cnpj and cnpj in case["cnpjs"]:
        return "exact_cnpj", [cnpj]

    provider_n = norm(provider)
    exact_labels = {norm(label): label for label in case["labels"]}
    if provider_n in exact_labels:
        return "exact_verified_candidate_label", [exact_labels[provider_n]]

    hits = []
    for phrase in case["contains"]:
        phrase_n = norm(phrase)
        if phrase_n and phrase_n in provider_n:
            hits.append(phrase)
    if hits:
        return "discovery_contains_phrase", hits
    return None, []


def main() -> None:
    occurrences: list[dict[str, Any]] = []
    month_inventory: list[dict[str, Any]] = []
    all_provider_labels: Counter[str] = Counter()
    all_provider_cnpjs: Counter[str] = Counter()

    for month in MONTHS:
        root, entries, path = load_monthly_entries(month)
        providers_with_cnpj = 0
        valid_cnpj_values: set[str] = set()

        for raw_key, raw_entry in entries.items():
            entry = raw_entry if isinstance(raw_entry, dict) else {}
            provider = provider_name(str(raw_key), entry)
            cnpj = digits(entry.get("cnpj") or entry.get("cnpjKey") or entry.get("cnpj_key"))
            if cnpj:
                providers_with_cnpj += 1
                valid_cnpj_values.add(cnpj)
            all_provider_labels[provider] += 1
            if cnpj:
                all_provider_cnpjs[cnpj] += 1

            for case_name, case in CASES.items():
                match_basis, matched = match_case(case, provider, cnpj)
                if match_basis is None:
                    continue
                occurrence = {
                    "case": case_name,
                    "month": month,
                    "provider_label": provider,
                    "provider_key": str(raw_key),
                    "provider_cnpj": cnpj,
                    "match_basis": match_basis,
                    "matched_queries": matched,
                    **stats(entry),
                }
                occurrences.append(occurrence)

        month_inventory.append(
            {
                "month": month,
                "path": str(path),
                "provider_entries": len(entries),
                "providers_with_valid_cnpj": providers_with_cnpj,
                "unique_valid_cnpjs": len(valid_cnpj_values),
                "root_meta": root.get("meta") if isinstance(root, dict) else None,
            }
        )

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for occurrence in occurrences:
        grouped[occurrence["case"]].append(occurrence)

    case_summaries = []
    for case_name in CASES:
        rows = grouped.get(case_name, [])
        by_identity: dict[tuple[str, str | None], dict[str, Any]] = {}
        for row in rows:
            key = (row["provider_label"], row["provider_cnpj"])
            current = by_identity.setdefault(
                key,
                {
                    "provider_label": row["provider_label"],
                    "provider_cnpj": row["provider_cnpj"],
                    "months": [],
                    "complaints": 0,
                    "responded": 0,
                    "finalized": 0,
                    "resolved": 0,
                    "satisfaction_count": 0,
                    "match_bases": set(),
                },
            )
            current["months"].append(row["month"])
            for key_name in (
                "complaints",
                "responded",
                "finalized",
                "resolved",
                "satisfaction_count",
            ):
                current[key_name] += int(row.get(key_name) or 0)
            current["match_bases"].add(row["match_basis"])

        identities = []
        for identity in by_identity.values():
            identity["months"] = sorted(set(identity["months"]))
            identity["match_bases"] = sorted(identity["match_bases"])
            identities.append(identity)
        identities.sort(key=lambda x: (-x["complaints"], x["provider_label"].lower()))

        labels_to_cnpjs: dict[str, set[str | None]] = defaultdict(set)
        for row in rows:
            labels_to_cnpjs[norm(row["provider_label"])].add(row["provider_cnpj"])
        collisions = [
            {
                "normalized_label": label,
                "cnpjs": sorted(c for c in cnpjs if c),
                "has_missing_cnpj": None in cnpjs,
            }
            for label, cnpjs in labels_to_cnpjs.items()
            if len(cnpjs) > 1
        ]

        case_summaries.append(
            {
                "case": case_name,
                "target_cnpjs": CASES[case_name]["cnpjs"],
                "candidate_labels": CASES[case_name]["labels"],
                "occurrence_rows": len(rows),
                "distinct_provider_identities": len(identities),
                "total_complaints_across_matched_rows": sum(
                    int(row.get("complaints") or 0) for row in rows
                ),
                "provider_identities": identities,
                "label_cnpj_collisions": collisions,
            }
        )

    payload = {
        "audit": "consumer_gov_12_month_provider_label_occurrences",
        "months": MONTHS,
        "scope_note": (
            "Discovery audit only. Exact CNPJ or label/phrase occurrence does not by itself "
            "establish legal identity, distribution role, risk carrier, or complaint transfer."
        ),
        "matching_policy": {
            "exact_cnpj": "discovery signal with strongest source identity evidence",
            "exact_verified_candidate_label": (
                "exact normalized equality against supplied candidate labels; still requires "
                "source-scoped identity review before public attribution"
            ),
            "discovery_contains_phrase": (
                "broad recall-only discovery; never sufficient for complaint attribution"
            ),
            "fuzzy_matching": False,
        },
        "month_inventory": month_inventory,
        "cases": case_summaries,
        "occurrences": sorted(
            occurrences,
            key=lambda row: (
                row["case"],
                row["month"],
                row["provider_label"].lower(),
            ),
        ),
    }
    OUTPUT_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    lines = [
        "# Auditoria de rótulos de fornecedores — Consumer.gov (12 snapshots)",
        "",
        f"Período: `{MONTHS[0]}` a `{MONTHS[-1]}`.",
        "",
        "Esta é uma auditoria de ocorrência e descoberta. Uma coincidência textual não transfere reclamações nem prova identidade jurídica.",
        "",
        "## Cobertura dos snapshots",
        "",
        "| Mês | Entradas | Com CNPJ válido | CNPJs únicos |",
        "|---|---:|---:|---:|",
    ]
    for row in month_inventory:
        lines.append(
            f"| {row['month']} | {row['provider_entries']} | "
            f"{row['providers_with_valid_cnpj']} | {row['unique_valid_cnpjs']} |"
        )

    for summary in case_summaries:
        lines.extend(
            [
                "",
                f"## {summary['case']}",
                "",
                f"Ocorrências mensais encontradas: **{summary['occurrence_rows']}**. "
                f"Reclamações somadas nas linhas encontradas: **{summary['total_complaints_across_matched_rows']}**.",
                "",
            ]
        )
        identities = summary["provider_identities"]
        if not identities:
            lines.append("Nenhuma ocorrência encontrada pelos critérios desta auditoria.")
            continue
        lines.extend(
            [
                "| Rótulo na fonte | CNPJ na entrada | Meses | Reclamações | Base de match |",
                "|---|---|---:|---:|---|",
            ]
        )
        for identity in identities:
            months = ", ".join(identity["months"])
            bases = ", ".join(identity["match_bases"])
            lines.append(
                f"| {identity['provider_label']} | {identity['provider_cnpj'] or '—'} | "
                f"{months} | {identity['complaints']} | {bases} |"
            )
        if summary["label_cnpj_collisions"]:
            lines.extend(["", "Colisões de rótulo/CNPJ detectadas:"])
            for collision in summary["label_cnpj_collisions"]:
                values = ", ".join(collision["cnpjs"]) or "sem CNPJ"
                if collision["has_missing_cnpj"]:
                    values += ", também sem CNPJ"
                lines.append(f"- `{collision['normalized_label']}` → {values}")

    OUTPUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload["month_inventory"], ensure_ascii=False, indent=2))
    print(f"wrote {OUTPUT_JSON}")
    print(f"wrote {OUTPUT_MD}")


if __name__ == "__main__":
    main()
