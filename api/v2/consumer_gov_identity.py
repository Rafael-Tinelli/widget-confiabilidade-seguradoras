from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import normalize_name_key

DEFAULT_PROVIDER_RESOLUTIONS_PATH = Path(
    "data/reference/v2/consumer_gov_provider_resolutions.json"
)
DEFAULT_PROVIDER_RESOLUTION_EXTENSIONS_PATH = Path(
    "data/reference/v2/consumer_gov_provider_resolution_extensions.json"
)

ALLOWED_STATES = {"matched_current_insurer", "outside_157", "ambiguous"}


class ConsumerGovIdentityError(ValueError):
    """Raised when curated Consumer.gov identity evidence is inconsistent."""


@dataclass(frozen=True)
class ProviderResolution:
    provider_name: str
    resolution_state: str
    resolution_kind: str
    target_cnpj: str | None
    reason_code: str | None
    evidence: tuple[dict[str, Any], ...]


def _resolution_rows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("resolutions") or []
    if not isinstance(rows, list):
        raise ConsumerGovIdentityError(f"resolutions must be a list: {path}")
    if not all(isinstance(row, dict) for row in rows):
        raise ConsumerGovIdentityError(f"resolution row must be an object: {path}")
    return rows


def load_provider_resolution_registry(
    path: Path = DEFAULT_PROVIDER_RESOLUTIONS_PATH,
    *,
    extension_path: Path | None = None,
) -> dict[str, ProviderResolution]:
    paths = [path]
    if extension_path is not None:
        paths.append(extension_path)
    elif path == DEFAULT_PROVIDER_RESOLUTIONS_PATH and (
        DEFAULT_PROVIDER_RESOLUTION_EXTENSIONS_PATH.exists()
    ):
        paths.append(DEFAULT_PROVIDER_RESOLUTION_EXTENSIONS_PATH)

    registry: dict[str, ProviderResolution] = {}
    for source_path in paths:
        for raw in _resolution_rows(source_path):
            provider_name = str(raw.get("provider_name") or "").strip()
            key = normalize_name_key(provider_name)
            state = str(raw.get("resolution_state") or "").strip()
            kind = str(raw.get("resolution_kind") or "").strip()
            if not provider_name or not key:
                raise ConsumerGovIdentityError("provider_name is required")
            if key in registry:
                raise ConsumerGovIdentityError(
                    f"duplicate provider resolution: {provider_name}"
                )
            if state not in ALLOWED_STATES:
                raise ConsumerGovIdentityError(
                    f"unsupported resolution_state for {provider_name}: {state}"
                )
            if not kind:
                raise ConsumerGovIdentityError(
                    f"resolution_kind is required for {provider_name}"
                )

            target_cnpj = normalize_cnpj_v2(raw.get("target_cnpj"))
            if state == "matched_current_insurer" and not target_cnpj:
                raise ConsumerGovIdentityError(
                    f"matched current insurer requires target_cnpj: {provider_name}"
                )
            if state != "matched_current_insurer" and target_cnpj:
                raise ConsumerGovIdentityError(
                    f"non-matched resolution must not assign target_cnpj: {provider_name}"
                )

            evidence_raw = raw.get("evidence") or []
            if not isinstance(evidence_raw, list) or not evidence_raw:
                raise ConsumerGovIdentityError(
                    f"source-backed evidence is required for {provider_name}"
                )
            evidence = tuple(item for item in evidence_raw if isinstance(item, dict))
            if len(evidence) != len(evidence_raw):
                raise ConsumerGovIdentityError(
                    f"invalid evidence entry for {provider_name}"
                )

            registry[key] = ProviderResolution(
                provider_name=provider_name,
                resolution_state=state,
                resolution_kind=kind,
                target_cnpj=target_cnpj,
                reason_code=(str(raw.get("reason_code") or "").strip() or None),
                evidence=evidence,
            )
    return registry


def resolve_curated_provider(
    provider_name: str,
    cnpj_to_current_entity: dict[str, str],
    registry: dict[str, ProviderResolution],
) -> dict[str, Any] | None:
    resolution = registry.get(normalize_name_key(provider_name))
    if resolution is None:
        return None

    output: dict[str, Any] = {
        "resolution_state": resolution.resolution_state,
        "resolution_kind": resolution.resolution_kind,
        "reason_code": resolution.reason_code,
        "provider_name": resolution.provider_name,
        "evidence": [dict(item) for item in resolution.evidence],
        "entity_id": None,
    }
    if resolution.resolution_state == "matched_current_insurer":
        assert resolution.target_cnpj is not None
        entity_id = cnpj_to_current_entity.get(resolution.target_cnpj)
        if not entity_id:
            raise ConsumerGovIdentityError(
                "curated provider target is not in the current ordinary-insurer universe: "
                f"{resolution.provider_name} -> {resolution.target_cnpj}"
            )
        output["entity_id"] = entity_id
        output["target_cnpj"] = resolution.target_cnpj
    return output
