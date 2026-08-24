from __future__ import annotations

import argparse
import json
import unicodedata
from pathlib import Path

from api.sources.consumer_gov_direct import (
    MIN_MONTH_BYTES,
    RAW_DIR,
    ConsumerGovPublicationNotFound,
    ConsumerGovRawError,
    Publication,
    discover_publications,
    ensure_months,
)

DEFAULT_IDENTITY = Path("data/derived/v2/consumer_gov_identity_experiment.json")


def _norm(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return text.encode("ascii", "ignore").decode("ascii").lower()


def is_explicit_basecompleta(publication: Publication) -> bool:
    blob = _norm(f"{publication.title} {publication.filename or ''}")
    return "base completa" in blob or "basecompleta" in blob


def _uncached_months(months: list[str]) -> list[str]:
    return [
        month
        for month in months
        if not (
            (path := RAW_DIR / f"basecompleta_{month}.csv").exists()
            and path.stat().st_size >= MIN_MONTH_BYTES
        )
    ]


def require_explicit_basecompleta_candidates(months: list[str]) -> None:
    missing = _uncached_months(months)
    if not missing:
        return

    publications = discover_publications(set(missing))
    explicit_months = {
        publication.month
        for publication in publications
        if publication.month and is_explicit_basecompleta(publication)
    }
    unresolved = [month for month in missing if month not in explicit_months]
    if unresolved:
        raise ConsumerGovPublicationNotFound(
            "Base Completa is unavailable for months "
            f"{unresolved}; generic 'Dados' and finalizadas publications are not "
            "accepted substitutes"
        )


def _months_from_identity(path: Path) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    source = payload.get("source") if isinstance(payload, dict) else None
    if isinstance(source, dict):
        months = source.get("months")
        if isinstance(months, list) and months:
            return [str(month) for month in months]

    months = payload.get("months") if isinstance(payload, dict) else None
    if isinstance(months, list) and months:
        return [str(month) for month in months]

    raise RuntimeError(f"could not resolve source months from {path}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--identity", type=Path, default=DEFAULT_IDENTITY)
    parser.add_argument("--months", nargs="*")
    args = parser.parse_args()

    months = args.months or _months_from_identity(args.identity)
    try:
        require_explicit_basecompleta_candidates(months)
        acquired = ensure_months(months)
    except ConsumerGovRawError as exc:
        raise SystemExit(str(exc)) from exc

    summary = {
        month: {
            "acquisition": item.get("acquisition"),
            "discovery_method": item.get("discovery_method"),
            "bytes": item.get("bytes"),
            "resource_url": item.get("resource_url"),
        }
        for month, item in acquired.items()
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
