# api/build_consumidor_gov.py
from __future__ import annotations

import json
import os
import re
from dataclasses import asdict
from datetime import datetime
from typing import Any

from api.sources.consumidor_gov import (
    Agg,
    aggregate_month_dual,
    discover_basecompleta_urls,
    download_csv_to_gz,
)

CACHE_DIR = "data/.cache/consumidor_gov"
MONTHLY_DIR = "data/derived/consumidor_gov/monthly"
OUT_LATEST = "data/derived/consumidor_gov/consumidor_gov_agg_latest.json"


def _utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _ensure_dirs() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(MONTHLY_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(OUT_LATEST), exist_ok=True)


def _monthly_path(ym: str) -> str:
    return os.path.join(MONTHLY_DIR, f"consumidor_gov_{ym}.json")


def _load_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, payload: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _read_current_as_of() -> str | None:
    if not os.path.exists(OUT_LATEST):
        return None
    try:
        meta = _load_json(OUT_LATEST).get("meta", {})
        as_of = meta.get("as_of")
        return str(as_of) if as_of else None
    except Exception:
        return None


def _latest_available_month() -> str | None:
    try:
        latest_urls = discover_basecompleta_urls(months=1)
        return next(iter(latest_urls.keys()), None)
    except Exception:
        return None


def _agg_from_raw(raw: dict[str, Any]) -> Agg:
    return Agg(
        display_name=str(raw.get("display_name") or ""),
        total=int(raw.get("total", 0) or 0),
        finalizadas=int(raw.get("finalizadas", 0) or 0),
        respondidas=int(raw.get("respondidas", 0) or 0),
        resolvidas_indicador=int(raw.get("resolvidas_indicador", 0) or 0),
        nota_sum=float(raw.get("nota_sum", 0.0) or 0.0),
        nota_count=int(raw.get("nota_count", 0) or 0),
        tempo_sum=float(raw.get("tempo_sum", 0.0) or 0.0),
        tempo_count=int(raw.get("tempo_count", 0) or 0),
    )


def _merge_raw_into(target: dict[str, Agg], key: str, raw: dict[str, Any]) -> None:
    cur = _agg_from_raw(raw)
    if key not in target:
        target[key] = cur
        return
    target[key].merge(cur)


def _prune_monthly(retain: int) -> None:
    if not os.path.isdir(MONTHLY_DIR):
        return
    files = sorted(
        f
        for f in os.listdir(MONTHLY_DIR)
        if f.startswith("consumidor_gov_") and f.endswith(".json")
    )
    if len(files) <= retain:
        return
    for fname in files[: len(files) - retain]:
        try:
            os.remove(os.path.join(MONTHLY_DIR, fname))
        except OSError:
            pass


def _existing_months_from_disk() -> list[str]:
    if not os.path.isdir(MONTHLY_DIR):
        return []
    yms: list[str] = []
    for fname in os.listdir(MONTHLY_DIR):
        m = re.match(r"consumidor_gov_(\d{4}-\d{2})\.json$", fname)
        if m:
            yms.append(m.group(1))
    return sorted(set(yms))


def main(months: int = 12) -> None:
    """
    Build de agregados Consumidor.gov (janela móvel, padrão 12 meses).

    Regras de robustez:
    - Se discovery (CKAN/HTML) falhar, tenta usar meses já existentes em disco.
    - Se um download/parse gerar agregado vazio, o mês é ignorado (não salva JSON mensal vazio).
    - A janela final é montada a partir dos meses efetivamente existentes em disco.
    - Se a janela final ficar vazia mas OUT_LATEST existir, mantém OUT_LATEST e encerra com sucesso.
    """
    _ensure_dirs()

    # Fast-path: se estiver em dia, pula.
    if os.environ.get("CONSUMIDOR_GOV_FORCE") != "1":
        latest = _latest_available_month()
        current = _read_current_as_of()
        if latest and current and latest == current and os.path.exists(_monthly_path(latest)):
            print(f"OK: Consumidor.gov já atualizado (as_of={current}). Pulando rebuild.")
            return

    discovery_error: Exception | None = None
    try:
        urls = discover_basecompleta_urls(months=months)
    except Exception as exc:
        discovery_error = exc
        urls = {}

    # Se não conseguiu descobrir URLs, tenta usar meses já prontos em disco.
    yms = sorted(urls.keys())
    if not urls:
        local_months = _existing_months_from_disk()
        if local_months:
            yms = local_months[-months:]
            msg = "Aviso: sem rede/descoberta para Consumidor.gov; usando meses já existentes em disco."
            if discovery_error:
                msg += f" Motivo original: {discovery_error}"
            print(msg)
        else:
            # Sem URLs e sem meses locais: se existe OUT_LATEST, não derruba o refresh.
            if os.path.exists(OUT_LATEST):
                msg = "Aviso: sem URLs e sem meses locais; mantendo OUT_LATEST existente e encerrando."
                if discovery_error:
                    msg += f" Motivo original: {discovery_error}"
                print(msg)
                return
            detail = f" Motivo original: {discovery_error}" if discovery_error else ""
            raise SystemExit(f"Nenhuma URL de Base Completa encontrada (Consumidor.gov).{detail}")

    produced: list[str] = []
    for ym in yms:
        out_month = _monthly_path(ym)
        if os.path.exists(out_month):
            continue

        # Se estamos em modo "disco" (sem urls), não tentamos baixar.
        if ym not in urls:
            print(f"Aviso: nenhuma URL nova para {ym}; mantendo arquivo local se existir.")
            continue

        url = urls[ym]
        gz_path = os.path.join(CACHE_DIR, f"basecompleta_{ym}.csv.gz")

        try:
            info = download_csv_to_gz(url, gz_path)
        except Exception as exc:
            print(f"WARN: {ym}: falha ao baixar base ({exc}). Ignorando mês.")
            try:
                if os.path.exists(gz_path):
                    os.remove(gz_path)
            except OSError:
                pass
            continue

        try:
            by_name, by_cnpj = aggregate_month_dual(gz_path)
        except Exception as exc:
            print(f"WARN: {ym}: falha ao agregar CSV ({exc}). Ignorando mês.")
            try:
                os.remove(gz_path)
            except OSError:
                pass
            continue

        # Se veio vazio, é sinal de schema/HTML/erro. NÃO salva mês vazio.
        if not by_name:
            print(f"WARN: {ym}: agregado vazio (by_name_key_raw). Ignorando mês (não será salvo).")
            try:
                os.remove(gz_path)
            except OSError:
                pass
            continue

        payload_month: dict[str, Any] = {
            "meta": {
                "ym": ym,
                "source": "dados.mj.gov.br",
                "dataset": "reclamacoes-do-consumidor-gov-br",
                "source_url": url,
                "download": info,
                "generated_at": _utc_now(),
            },
            "by_name_key_raw": {k: asdict(v) for k, v in by_name.items()},
            "by_cnpj_key_raw": {k: asdict(v) for k, v in by_cnpj.items()},
        }

        _write_json(out_month, payload_month)
        produced.append(ym)

        try:
            os.remove(gz_path)
        except OSError:
            pass

    # Monta a janela a partir do que existe em disco (não do que "era esperado" por URLs)
    merge_yms = _existing_months_from_disk()[-months:]
    if not merge_yms:
        if os.path.exists(OUT_LATEST):
            print("WARN: Nenhum mês válido disponível; mantendo OUT_LATEST existente e encerrando.")
            return
        raise SystemExit("Nenhum agregado mensal disponível para montar a janela (nenhum mês em disco).")

    merged_name: dict[str, Agg] = {}
    merged_cnpj: dict[str, Agg] = {}
    used_months: list[str] = []

    for ym in merge_yms:
        p = _monthly_path(ym)
        if not os.path.exists(p):
            continue
        try:
            month = _load_json(p)
        except Exception:
            continue

        raw_name = month.get("by_name_key_raw") or {}
        raw_cnpj = month.get("by_cnpj_key_raw") or {}
        if not isinstance(raw_name, dict) or not raw_name:
            continue

        for k, raw in raw_name.items():
            if isinstance(raw, dict):
                _merge_raw_into(merged_name, str(k), raw)

        if isinstance(raw_cnpj, dict):
            for k, raw in raw_cnpj.items():
                if isinstance(raw, dict):
                    _merge_raw_into(merged_cnpj, str(k), raw)

        used_months.append(ym)

    if not merged_name:
        if os.path.exists(OUT_LATEST):
            print("WARN: Merge resultou vazio; mantendo OUT_LATEST existente e encerrando.")
            return
        raise SystemExit("Nenhum agregado mensal disponível para montar a janela (by_name_key_raw vazio).")

    as_of = used_months[-1] if used_months else merge_yms[-1]

    out: dict[str, Any] = {
        "meta": {
            "source": "dados.mj.gov.br",
            "dataset": "reclamacoes-do-consumidor-gov-br",
            "as_of": as_of,
            "window_months": len(used_months) if used_months else len(merge_yms),
            "months": used_months if used_months else merge_yms,
            "generated_at": _utc_now(),
            "produced_months": produced,
        },
        "by_name_key": {k: v.to_public() for k, v in merged_name.items()},
        "by_cnpj_key": {k: v.to_public() for k, v in merged_cnpj.items()},
    }

    _write_json(OUT_LATEST, out)

    # Mantém uma folga (24) pra evitar ficar recriando mês antigo e pra dar fallback local
    _prune_monthly(retain=max(24, months))
    print(f"OK: Consumidor.gov agregado atualizado (as_of={as_of}).")


if __name__ == "__main__":
    # Permite override simples por env var.
    env_months = os.environ.get("CONSUMIDOR_GOV_MONTHS")
    if env_months:
        try:
            main(months=int(env_months))
        except ValueError:
            main()
    else:
        main()
