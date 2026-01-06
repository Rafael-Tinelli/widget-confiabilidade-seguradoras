# api/build_insurers.py
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Importações internas
from api.matching.consumidor_gov_match import NameMatcher, format_cnpj
from api.utils.identifiers import normalize_cnpj
from api.sources.opin_participants import load_opin_participant_cnpjs
from api.sources.ses import extract_ses_master_and_financials
from api.intelligence import calculate_score

OUTPUT_FILE = Path("api/v1/insurers.json")
CONSUMIDOR_GOV_FILE = Path("data/derived/consumidor_gov/aggregated.json")

# Constantes de Validação
TARGET_UNIVERSE_COUNT = 233
# O DoD de match do Open Insurance agora é dinâmico, mas exigimos um piso mínimo de sanidade
MIN_OPIN_MATCH_FLOOR = 10


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> None:
    # 1. COLETA FINANCEIRA (SUSEP)
    print("\n--- INICIANDO COLETA SUSEP (FINANCEIRO) ---")
    _ses_meta, companies = extract_ses_master_and_financials()

    # 2. PREPARAÇÃO DO MATCHER (CONSUMIDOR.GOV)
    print("\n--- INICIANDO COLETA CONSUMIDOR.GOV ---")
    reputation_root = {}
    if CONSUMIDOR_GOV_FILE.exists():
        try:
            reputation_root = json.loads(CONSUMIDOR_GOV_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Aviso: Erro ao ler {CONSUMIDOR_GOV_FILE}: {e}")

    matcher = NameMatcher(reputation_root)

    # 3. CARGA DE PARTICIPANTES OPEN INSURANCE (SET CNPJ)
    print("\n--- INICIANDO CRUZAMENTO OPEN INSURANCE ---")
    opin_cnpjs = load_opin_participant_cnpjs()

    # 4. CONSOLIDAÇÃO
    print("\n--- CONSOLIDANDO E CALCULANDO SCORES ---")
    insurers = []
    matched_reputation = 0
    matched_opin = 0

    # Rastreamento para validação dinâmica
    susep_cnpjs_seen = set()

    for raw_id, comp in companies.items():
        # A. Normalização para Join
        cnpj_candidate = comp.get("cnpj") or comp.get("cnpj_fmt") or str(raw_id)
        cnpj_clean = normalize_cnpj(cnpj_candidate)

        # Rastreia CNPJs válidos no universo SUSEP para cálculo de interseção
        if cnpj_clean:
            susep_cnpjs_seen.add(cnpj_clean)

        # Formatação visual
        cnpj_fmt = format_cnpj(cnpj_clean) if cnpj_clean else str(cnpj_candidate)

        name = comp.get("name") or comp.get("corporate_name") or comp.get("razao_social") or cnpj_fmt

        # B. Match com Consumidor.gov
        rep_entry, match_meta = matcher.get_entry(str(name), cnpj=cnpj_clean or cnpj_fmt)

        reputation_data = None
        if rep_entry:
            stats = rep_entry.get("statistics") or {}

            def get_val(k):
                v = stats.get(k)
                if v in (None, ""):
                    return None
                try:
                    return float(v)
                except (ValueError, TypeError):
                    return None

            sat_avg = get_val("overallSatisfaction")
            res_rate = get_val("solutionIndex")
            complaints = int(get_val("complaintsCount") or 0)

            if sat_avg is not None and res_rate is not None:
                matched_reputation += 1
                reputation_data = {
                    "source": "consumidor.gov",
                    "match_score": match_meta.score if match_meta else 0,
                    "display_name": rep_entry.get("display_name") or rep_entry.get("name"),
                    "metrics": {
                        "satisfaction_avg": sat_avg,
                        "resolution_rate": res_rate,
                        "complaints_total": complaints
                    }
                }

        # C. Join Open Insurance (Flag Booleana)
        # Verifica interseção de Sets usando CNPJ normalizado
        is_participant = bool(cnpj_clean and cnpj_clean in opin_cnpjs)
        if is_participant:
            matched_opin += 1

        # D. Objeto Mestre
        # CRÍTICO: Preserva o ID original do extrator SUSEP
        final_id = comp.get("id") or raw_id

        insurer_obj = {
            "id": str(final_id),
            "name": str(name),
            "cnpj": cnpj_fmt,
            "flags": {
                "openInsuranceParticipant": is_participant
            },
            "data": {
                "net_worth": comp.get("net_worth", 0.0),
                "premiums": comp.get("premiums", 0.0),
                "claims": comp.get("claims", 0.0),
            },
            "reputation": reputation_data,
            "products": []
        }

        # E. Inteligência
        processed_insurer = calculate_score(insurer_obj)

        if "financial_score" not in processed_insurer["data"]:
            comp_fin = processed_insurer["data"].get("components", {}).get("solvency", 0.0)
            processed_insurer["data"]["financial_score"] = comp_fin

        insurers.append(processed_insurer)

    # Ordenação
    insurers.sort(key=lambda x: x["data"].get("score", 0), reverse=True)

    # 5. Validação DoD (FAIL FAST & DINÂMICO)
    total_count = len(insurers)

    # Validação 1: Universo SUSEP (Estrito)
    if total_count != TARGET_UNIVERSE_COUNT:
        error_msg = (
            f"FATAL: Universo SUSEP inconsistente. "
            f"Esperado: {TARGET_UNIVERSE_COUNT}, Encontrado: {total_count}. "
            "Verifique filtro de entidades supervisionadas na fonte SES."
        )
        print(error_msg, file=sys.stderr)
        raise SystemExit(1)

    # Validação 2: Cruzamento Open Insurance (Dinâmico)
    # Calcula a interseção matemática esperada
    expected_matches = len(opin_cnpjs.intersection(susep_cnpjs_seen))

    if matched_opin != expected_matches:
        error_msg = (
            f"FATAL: Inconsistência no join Open Insurance. "
            f"Matches reais: {matched_opin}. Matches esperados (interseção): {expected_matches}. "
            "Erro lógico no loop de construção."
        )
        print(error_msg, file=sys.stderr)
        raise SystemExit(1)

    # Sanity Check: Se a normalização falhar silenciosamente, a interseção seria 0 e passaria no check acima.
    # Exigimos um piso mínimo para garantir que o ETL está saudável.
    if matched_opin < MIN_OPIN_MATCH_FLOOR:
        error_msg = (
            f"FATAL: Baixa contagem de participantes OPIN ({matched_opin}). "
            f"Abaixo do piso de sanidade ({MIN_OPIN_MATCH_FLOOR}). "
            "Provável falha massiva na normalização de CNPJs."
        )
        print(error_msg, file=sys.stderr)
        raise SystemExit(1)

    # 6. Geração do JSON Final
    out = {
        "schemaVersion": "1.0.0",
        "generatedAt": _utc_now_iso(),
        "period": "2024",
        "sources": ["SUSEP (SES)", "Open Insurance Brasil", "Consumidor.gov.br"],
        "meta": {
            "count": total_count,
            "stats": {
                "totalInsurers": total_count,
                "reputationMatched": matched_reputation,
                "openInsuranceParticipants": matched_opin,
            }
        },
        "insurers": insurers
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(out, ensure_ascii=False, indent=0, separators=(',', ':')), encoding="utf-8")

    print(f"SUCCESS: Generated {OUTPUT_FILE}")
    print(f"Integrity Check Passed: {total_count} insurers, {matched_opin} OPIN participants (Expected: {expected_matches}).")


if __name__ == "__main__":
    main()
