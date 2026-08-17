from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from api.utils.identifiers import normalize_cnpj

_ACTIVITY_SOURCE_MAP = {
    "SEGUROS": "insurance",
    "PREVIDENCIA": "pension",
    "CAPITALIZACAO": "capitalization",
    "RESSEGURO": "reinsurance",
}


class IdentityConflictError(ValueError):
    """Raised when two source records would collapse into one canonical identity."""


@dataclass(frozen=True)
class CanonicalIdentity:
    entity_id: str
    fip_code: str
    cnpj: str | None
    legal_name: str
    entity_type: str
    regulatory_regime: str
    regulatory_status: str
    activities: dict[str, bool]
    evidence: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_id": self.entity_id,
            "fip_code": self.fip_code,
            "cnpj": self.cnpj,
            "legal_name": self.legal_name,
            "entity_type": self.entity_type,
            "regulatory_regime": self.regulatory_regime,
            "regulatory_status": self.regulatory_status,
            "activities": dict(self.activities),
            "evidence": dict(self.evidence),
        }


def canonical_fip_code(raw: Any) -> str:
    """Return a six-digit FIP/SES code, or an empty string when unsupported."""
    if raw is None:
        return ""

    text = str(raw).strip().lower()
    if not text:
        return ""

    text = text.replace("ses:", "").replace("susep:", "").removesuffix(".0")

    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""

    return digits.zfill(6)


def _source_labels(record: Mapping[str, Any]) -> list[str]:
    raw = record.get("sources_found") or []
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, Iterable):
        return []

    labels: list[str] = []
    for item in raw:
        label = str(item or "").strip().upper()
        if label and label not in labels:
            labels.append(label)
    return labels


def _activities_from_sources(labels: Iterable[str]) -> dict[str, bool]:
    activities = {
        "insurance": False,
        "pension": False,
        "capitalization": False,
        "reinsurance": False,
    }
    for label in labels:
        key = _ACTIVITY_SOURCE_MAP.get(str(label).upper())
        if key:
            activities[key] = True
    return activities


def build_canonical_identity(record: Mapping[str, Any], *, source_key: Any | None = None) -> CanonicalIdentity:
    """Build a conservative v2 identity from one SES-normalized record.

    This function deliberately does **not** infer legal entity type, licensing
    status or regulatory regime from company names or from the presence of
    financial files. Those attributes remain ``unknown`` until a dedicated
    authoritative source is integrated.

    Activity flags are evidence that the entity appears in a corresponding SES
    data flow; they are not a legal classification.
    """
    raw_fip = (
        record.get("fip_code")
        or record.get("susep_id")
        or record.get("susepId")
        or record.get("id")
        or source_key
    )
    fip_code = canonical_fip_code(raw_fip)
    if not fip_code:
        raise ValueError("Canonical identity requires a valid FIP/SES code")

    cnpj = normalize_cnpj(record.get("cnpj"))
    legal_name = str(record.get("legal_name") or record.get("name") or "").strip()
    if not legal_name:
        raise ValueError(f"Canonical identity {fip_code} requires a legal name")

    entity_id = f"cnpj:{cnpj}" if cnpj else f"fip:{fip_code}"
    labels = _source_labels(record)

    return CanonicalIdentity(
        entity_id=entity_id,
        fip_code=fip_code,
        cnpj=cnpj,
        legal_name=legal_name,
        entity_type="unknown",
        regulatory_regime="unknown",
        regulatory_status="unknown",
        activities=_activities_from_sources(labels),
        evidence={
            "ses_present": True,
            "listaempresas_cnpj_present": cnpj is not None,
            "activity_sources": labels,
        },
    )


def build_canonical_entities(ses_companies: Any) -> list[dict[str, Any]]:
    """Build unique canonical identities without changing the v1 universe.

    ``ses_companies`` may be the dict returned by ``extract_ses_master_and_financials``
    or a list of records. Duplicate canonical IDs are rejected instead of
    being silently deduplicated in a frontend.
    """
    if isinstance(ses_companies, Mapping):
        iterator = ((key, value) for key, value in ses_companies.items() if isinstance(value, Mapping))
    elif isinstance(ses_companies, list):
        iterator = ((None, value) for value in ses_companies if isinstance(value, Mapping))
    else:
        raise TypeError("ses_companies must be a mapping or list of mappings")

    by_entity_id: dict[str, CanonicalIdentity] = {}
    by_fip: dict[str, str] = {}
    by_cnpj: dict[str, str] = {}

    for source_key, record in iterator:
        identity = build_canonical_identity(record, source_key=source_key)

        previous_fip_entity = by_fip.get(identity.fip_code)
        if previous_fip_entity and previous_fip_entity != identity.entity_id:
            raise IdentityConflictError(
                f"FIP {identity.fip_code} maps to multiple canonical entities: "
                f"{previous_fip_entity} and {identity.entity_id}"
            )

        if identity.cnpj:
            previous_cnpj_entity = by_cnpj.get(identity.cnpj)
            if previous_cnpj_entity and previous_cnpj_entity != identity.entity_id:
                raise IdentityConflictError(
                    f"CNPJ {identity.cnpj} maps to multiple canonical entities: "
                    f"{previous_cnpj_entity} and {identity.entity_id}"
                )

        previous = by_entity_id.get(identity.entity_id)
        if previous and (
            previous.fip_code != identity.fip_code
            or previous.legal_name != identity.legal_name
        ):
            raise IdentityConflictError(
                f"Canonical entity {identity.entity_id} has conflicting source records"
            )

        by_entity_id[identity.entity_id] = identity
        by_fip[identity.fip_code] = identity.entity_id
        if identity.cnpj:
            by_cnpj[identity.cnpj] = identity.entity_id

    return [by_entity_id[key].to_dict() for key in sorted(by_entity_id)]
