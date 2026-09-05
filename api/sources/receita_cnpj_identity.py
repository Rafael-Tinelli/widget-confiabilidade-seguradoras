from __future__ import annotations

import csv
import io
import re
import tempfile
import zipfile
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests

from api.sources.receita_cnpj_bulk import (
    OFFICIAL_CNPJ_SHARE_URL,
    ReceitaOpenDataError,
    ReceitaOpenDataRelease,
    _configure_official_session,
    _download_zip,
    _first_csv_member,
    _propfind,
    discover_latest_release,
)
from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import normalize_name_key

COMPANY_FILE_RE = re.compile(r"Empresas\d+\.zip$", re.IGNORECASE)
CNAE_FILE = "Cnaes.zip"
LEGAL_NATURE_FILE = "Naturezas.zip"

# Generic legal/business tokens may be removed only for the conservative
# fallback key. Surface-name equality remains the preferred match.
_BUSINESS_CORE_STOPWORDS = {
    "sa",
    "s",
    "a",
    "ltda",
    "cia",
    "companhia",
    "sociedade",
    "de",
    "da",
    "do",
    "das",
    "dos",
    "e",
    "seguro",
    "seguros",
    "seguradora",
    "seguradoras",
    "vida",
    "previdencia",
    "previdenciaria",
    "complementar",
    "privada",
    "capitalizacao",
    "corporativo",
    "corporativos",
    "corporativa",
    "corporativas",
}

_LEGAL_FORM_SUFFIXES = {
    "sa",
    "s",
    "a",
    "ltda",
    "cia",
    "companhia",
    "sociedade",
}

# Receita CNAE is a corroborating/exclusion signal only. It never licenses an
# insurer; current ordinary-insurer admission remains SUSEP-authoritative.
SAFE_OUTSIDE_PRIMARY_CNAES = {
    "6450600": "receita_capitalization_activity",
    "6541300": "receita_closed_pension_activity",
    "6542100": "receita_open_pension_activity_not_current_susep_insurer",
    "6622300": "receita_insurance_broker_or_agent_activity",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _digits(value: Any) -> str:
    return re.sub(r"\D+", "", str(value or ""))


def _surface_name(name: str) -> str:
    tokens = normalize_name_key(name).split()
    while tokens and tokens[-1] in _LEGAL_FORM_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def _business_core(name: str) -> str:
    tokens = [
        token
        for token in normalize_name_key(name).split()
        if token not in _BUSINESS_CORE_STOPWORDS and len(token) > 1
    ]
    if not tokens:
        return ""
    # A one-token core must be reasonably distinctive. This deliberately
    # rejects generic keys such as "uniao" while allowing PREVISUL/EQUATORIAL.
    if len(tokens) == 1 and len(tokens[0]) < 7:
        return ""
    value = " ".join(tokens)
    return value if len(value.replace(" ", "")) >= 7 else ""


def _build_query_indexes(provider_names: Iterable[str]) -> dict[str, dict[str, set[str]]]:
    surface: dict[str, set[str]] = defaultdict(set)
    core: dict[str, set[str]] = defaultdict(set)
    display: dict[str, str] = {}
    for provider in provider_names:
        name = str(provider or "").strip()
        if not name:
            continue
        provider_key = normalize_name_key(name)
        if not provider_key:
            continue
        display.setdefault(provider_key, name)
        surface_key = _surface_name(name)
        if surface_key:
            surface[surface_key].add(provider_key)
        core_key = _business_core(name)
        if core_key:
            core[core_key].add(provider_key)
    return {"surface": surface, "core": core, "display": display}


def _match_name_to_queries(
    name: str,
    query_indexes: dict[str, dict[str, set[str]]],
    *,
    prefix: str,
) -> list[tuple[str, str]]:
    if not name:
        return []
    out: list[tuple[str, str]] = []
    surface = _surface_name(name)
    for provider_key in query_indexes["surface"].get(surface, set()):
        out.append((provider_key, f"{prefix}_surface_exact"))
    core = _business_core(name)
    for provider_key in query_indexes["core"].get(core, set()):
        out.append((provider_key, f"{prefix}_business_core"))
    return out


def _load_code_map(zip_path: Path) -> dict[str, str]:
    if not zipfile.is_zipfile(zip_path):
        raise ReceitaOpenDataError(f"Invalid Receita dictionary ZIP: {zip_path}")
    with zipfile.ZipFile(zip_path) as archive:
        member = _first_csv_member(archive)
        with archive.open(member) as raw:
            text = io.TextIOWrapper(raw, encoding="latin1", newline="")
            reader = csv.reader(text, delimiter=";", quotechar='"')
            out: dict[str, str] = {}
            for row in reader:
                if len(row) < 2:
                    continue
                code = _digits(row[0])
                label = str(row[1]).strip()
                if code and label:
                    out[code] = label
            return out


def _release_files(
    session: requests.Session,
    release: ReceitaOpenDataRelease,
) -> dict[str, Any]:
    resources = _propfind(session, release.release_url)
    files = {
        str(item["name"]): item
        for item in resources
        if not item.get("is_collection")
    }
    companies = sorted(
        (name for name in files if COMPANY_FILE_RE.fullmatch(name)),
        key=lambda name: int(re.search(r"(\d+)", name).group(1)),
    )
    if not companies:
        raise ReceitaOpenDataError("Receita release has no Empresas partitions")
    if CNAE_FILE not in files or LEGAL_NATURE_FILE not in files:
        raise ReceitaOpenDataError(
            "Receita release is missing Cnaes.zip or Naturezas.zip required for identity enrichment"
        )
    return {
        "companies": companies,
        "cnaes": CNAE_FILE,
        "legal_natures": LEGAL_NATURE_FILE,
        "metadata": files,
    }


def _company_base(cnpj: str | None) -> str | None:
    normalized = normalize_cnpj_v2(cnpj)
    return normalized[:8] if normalized else None


def _establishment_record(
    row: list[str],
    *,
    company: dict[str, Any] | None,
    cnae_map: dict[str, str],
    nature_map: dict[str, str],
) -> dict[str, Any] | None:
    if len(row) < 13:
        return None
    cnpj = normalize_cnpj_v2("".join(str(part).strip() for part in row[:3]))
    if not cnpj:
        return None
    primary_cnae = _digits(row[11])
    secondary_codes = [
        code
        for code in (_digits(item) for item in str(row[12] or "").split(","))
        if code
    ]
    nature_code = _digits((company or {}).get("legal_nature_code"))
    status_code = str(row[5]).strip().zfill(2)
    return {
        "cnpj": cnpj,
        "cnpj_base": cnpj[:8],
        "is_head_office": str(row[3]).strip() == "1",
        "trade_name": str(row[4] or "").strip() or None,
        "cadastral_status_code": status_code,
        "cadastral_status": {
            "01": "NULA",
            "02": "ATIVA",
            "03": "SUSPENSA",
            "04": "INAPTA",
            "05": "ATIVA NÃO REGULAR",
            "08": "BAIXADA",
        }.get(status_code),
        "status_date_raw": str(row[6] or "").strip() or None,
        "activity_start_date_raw": str(row[10] or "").strip() or None,
        "primary_cnae_code": primary_cnae or None,
        "primary_cnae": cnae_map.get(primary_cnae) if primary_cnae else None,
        "secondary_cnaes": [
            {"code": code, "description": cnae_map.get(code)}
            for code in secondary_codes
        ],
        "legal_name_receita": (company or {}).get("legal_name_receita"),
        "legal_nature_code": nature_code or None,
        "legal_nature": nature_map.get(nature_code) if nature_code else None,
    }


def _candidate_rank(method: str) -> int:
    return {
        "trade_name_surface_exact": 1,
        "legal_name_surface_exact": 2,
        "trade_name_business_core": 3,
        "legal_name_business_core": 4,
    }.get(method, 99)


def _finalize_provider_matches(
    query_indexes: dict[str, dict[str, set[str]]],
    candidates: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    display = query_indexes["display"]
    for provider_key, provider_name in sorted(display.items()):
        rows = candidates.get(provider_key) or []
        if not rows:
            out.append(
                {
                    "provider": provider_name,
                    "provider_key": provider_key,
                    "candidate_state": "no_candidate",
                    "match_method": None,
                    "candidates": [],
                }
            )
            continue

        best_rank = min(_candidate_rank(str(row.get("match_method") or "")) for row in rows)
        best = [row for row in rows if _candidate_rank(str(row.get("match_method") or "")) == best_rank]
        by_base: dict[str, dict[str, Any]] = {}
        for row in best:
            base = str(row.get("cnpj_base") or "")
            if not base:
                continue
            previous = by_base.get(base)
            if previous is None or (not previous.get("is_head_office") and row.get("is_head_office")):
                by_base[base] = row
        selected = sorted(by_base.values(), key=lambda row: (str(row.get("cnpj") or ""), str(row.get("legal_name_receita") or "")))
        state = "unique_candidate" if len(selected) == 1 else "ambiguous_candidates"
        out.append(
            {
                "provider": provider_name,
                "provider_key": provider_key,
                "candidate_state": state,
                "match_method": selected[0].get("match_method") if len(selected) == 1 else None,
                "candidates": selected,
            }
        )
    return out


def build_filtered_identity_snapshot(
    target_entities: Iterable[dict[str, Any]],
    provider_names: Iterable[str],
    *,
    release: ReceitaOpenDataRelease | None = None,
    work_dir: Path | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Build a bounded Receita identity snapshot for v2 entities and provider labels.

    The official national files are streamed partition by partition and deleted
    immediately. Only current project CNPJs and deterministic name candidates
    for the supplied provider labels are retained.
    """
    targets: dict[str, dict[str, Any]] = {}
    for entity in target_entities:
        cnpj = normalize_cnpj_v2(entity.get("cnpj"))
        if cnpj:
            targets[cnpj] = {
                "entity_id": entity.get("entity_id"),
                "project_legal_name": entity.get("legal_name"),
            }
    target_bases = {cnpj[:8] for cnpj in targets}
    queries = _build_query_indexes(provider_names)

    own_session = session is None
    sess = session or requests.Session()
    _configure_official_session(sess)
    resolved_release = release or discover_latest_release(sess)
    files = _release_files(sess, resolved_release)

    temp_context = None
    if work_dir is None:
        temp_context = tempfile.TemporaryDirectory(prefix="receita-identity-v2-")
        root = Path(temp_context.name)
    else:
        root = Path(work_dir)
        root.mkdir(parents=True, exist_ok=True)

    try:
        cnae_name = str(files["cnaes"])
        nature_name = str(files["legal_natures"])
        cnae_zip = _download_zip(sess, urljoin(resolved_release.release_url, cnae_name), root / cnae_name)
        nature_zip = _download_zip(sess, urljoin(resolved_release.release_url, nature_name), root / nature_name)
        cnae_map = _load_code_map(cnae_zip)
        nature_map = _load_code_map(nature_zip)
        cnae_zip.unlink(missing_ok=True)
        nature_zip.unlink(missing_ok=True)

        companies_by_base: dict[str, dict[str, Any]] = {}
        legal_matches_by_base: dict[str, list[tuple[str, str]]] = defaultdict(list)
        company_files_scanned: list[str] = []

        for name in files["companies"]:
            local = _download_zip(sess, urljoin(resolved_release.release_url, name), root / name)
            try:
                with zipfile.ZipFile(local) as archive:
                    member = _first_csv_member(archive)
                    with archive.open(member) as raw:
                        text = io.TextIOWrapper(raw, encoding="latin1", newline="")
                        reader = csv.reader(text, delimiter=";", quotechar='"')
                        for row in reader:
                            if len(row) < 3:
                                continue
                            base = _digits(row[0])
                            if len(base) != 8:
                                continue
                            legal_name = str(row[1] or "").strip()
                            nature_code = _digits(row[2])
                            matches = _match_name_to_queries(legal_name, queries, prefix="legal_name")
                            if base in target_bases or matches:
                                companies_by_base[base] = {
                                    "cnpj_base": base,
                                    "legal_name_receita": legal_name or None,
                                    "legal_nature_code": nature_code or None,
                                    "legal_nature": nature_map.get(nature_code) if nature_code else None,
                                }
                            for provider_key, method in matches:
                                legal_matches_by_base[base].append((provider_key, method))
                company_files_scanned.append(name)
            finally:
                local.unlink(missing_ok=True)

        canonical_records: dict[str, dict[str, Any]] = {}
        provider_candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
        establishment_files_scanned: list[str] = []

        for name in resolved_release.establishment_files:
            local = _download_zip(sess, urljoin(resolved_release.release_url, name), root / name)
            try:
                with zipfile.ZipFile(local) as archive:
                    member = _first_csv_member(archive)
                    with archive.open(member) as raw:
                        text = io.TextIOWrapper(raw, encoding="latin1", newline="")
                        reader = csv.reader(text, delimiter=";", quotechar='"')
                        for row in reader:
                            if len(row) < 13:
                                continue
                            cnpj = normalize_cnpj_v2("".join(str(part).strip() for part in row[:3]))
                            if not cnpj:
                                continue
                            base = cnpj[:8]
                            is_head_office = str(row[3]).strip() == "1"
                            need_target = cnpj in targets
                            legal_matches = legal_matches_by_base.get(base) or []
                            trade_name = str(row[4] or "").strip()
                            trade_matches = _match_name_to_queries(trade_name, queries, prefix="trade_name") if is_head_office else []
                            if not need_target and not (is_head_office and (legal_matches or trade_matches)):
                                continue
                            record = _establishment_record(
                                row,
                                company=companies_by_base.get(base),
                                cnae_map=cnae_map,
                                nature_map=nature_map,
                            )
                            if record is None:
                                continue
                            if need_target:
                                record["entity_id"] = targets[cnpj].get("entity_id")
                                record["project_legal_name"] = targets[cnpj].get("project_legal_name")
                                canonical_records[cnpj] = record
                            if is_head_office:
                                for provider_key, method in [*legal_matches, *trade_matches]:
                                    candidate = dict(record)
                                    candidate["match_method"] = method
                                    provider_candidates[provider_key].append(candidate)
                establishment_files_scanned.append(name)
            finally:
                local.unlink(missing_ok=True)

        provider_matches = _finalize_provider_matches(queries, provider_candidates)
        unique = sum(row["candidate_state"] == "unique_candidate" for row in provider_matches)
        ambiguous = sum(row["candidate_state"] == "ambiguous_candidates" for row in provider_matches)
        none = sum(row["candidate_state"] == "no_candidate" for row in provider_matches)

        return {
            "artifact": "v2_receita_cnpj_identity",
            "generated_at": _utc_now(),
            "status": "experimental",
            "source": {
                "authority": "Receita Federal do Brasil",
                "dataset": "Cadastro Nacional da Pessoa Jurídica (CNPJ) - Dados Abertos",
                "public_share_url": OFFICIAL_CNPJ_SHARE_URL,
                "release_url": resolved_release.release_url,
                "reference_period": resolved_release.period,
                "ingestion_method": "official_nextcloud_webdav_bulk_filtered_identity",
                "retrieved_at": _utc_now(),
            },
            "identity_semantics": {
                "receita_role": "identify_legal_entity_trade_name_activity_and_legal_nature",
                "susep_role": "authoritative_for_current_insurer_licensing_and_157_membership",
                "cnae_role": "corroborating_or_safe_exclusion_only_never_insurer_admission",
                "fuzzy_matching": "not_used",
                "sales_channel_rule": "never_transfer_complaints_to_a_carrier_from_distribution_relationship_alone",
            },
            "meta": {
                "target_cnpjs": len(targets),
                "target_cnpjs_enriched": len(canonical_records),
                "provider_queries": len(queries["display"]),
                "provider_unique_candidates": unique,
                "provider_ambiguous_candidates": ambiguous,
                "provider_no_candidate": none,
                "company_files_scanned": company_files_scanned,
                "establishment_files_scanned": establishment_files_scanned,
            },
            "safe_outside_primary_cnaes": SAFE_OUTSIDE_PRIMARY_CNAES,
            "canonical_records": [canonical_records[key] for key in sorted(canonical_records)],
            "provider_matches": provider_matches,
        }
    finally:
        if temp_context is not None:
            temp_context.cleanup()
        if own_session:
            sess.close()
