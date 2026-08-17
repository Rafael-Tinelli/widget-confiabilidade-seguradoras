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
    """Raised when source records conflict for the same regulatory identity."""


@dataclass(frozen=True)
class CanonicalIdentity:
    entity_id: str
    fip_code: str
    cnpj: str | None
    legal_entity_id: str | None
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
            "legal_entity_id": self.legal_entity_id,
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


def build_canonical_identity(
    record: Mapping[str, Any],
    *,
    source_key: Any | None = None,
) -> CanonicalIdentity:
    """Build a conservative v2 regulatory identity from one SES record.

    FIP is the stable identifier for a SUSEP regulatory record. CNPJ is an
    attribute of the legal entity behind that record and may differ across
    official sources, notably for foreign reinsurers and Brazilian
    representative offices. Therefore CNPJ must never change ``entity_id``.

    This function deliberately does not infer legal entity type, licensing
    status or regulatory regime from company names or financial-file presence.
    Activity flags are evidence of SES data-flow presence, not legal class.
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

    labels = _source_labels(record)

    return CanonicalIdentity(
        entity_id=f"fip:{fip_code}",
        fip_code=fip_code,
        cnpj=cnpj,
        legal_entity_id=f"cnpj:{cnpj}" if cnpj else None,
        legal_name=legal_name,
        entity_type="unknown",
        regulatory_regime="unknown",
        regulatory_status="unknown",
        activities=_activities_from_sources(labels),
        evidence={
            "ses_present": True,
            "listaempresas_cnpj_present": cnpj is not None,
            "activity_sources": labels,
            "ses_identity": {
                "legal_name": legal_name,
                "cnpj": cnpj,
            },
        },
    )


def build_canonical_entities(ses_companies: Any) -> list[dict[str, Any]]:
    """Build unique FIP-stable regulatory identities without changing v1.

    ``ses_companies`` may be the dict returned by
    ``extract_ses_master_and_financials`` or a list of records. Duplicate FIP
    identities with conflicting source records are rejected instead of being
    silently deduplicated in a frontend. Repeated CNPJs are not rejected here:
    CNPJ is a legal-entity attribute, not the regulatory-record primary key.
    """
    if isinstance(ses_companies, Mapping):
        iterator = (
            (key, value)
            for key, value in ses_companies.items()
            if isinstance(value, Mapping)
        )
    elif isinstance(ses_companies, list):
        iterator = (
            (None, value)
            for value in ses_companies
            if isinstance(value, Mapping)
        )
    else:
        raise TypeError("ses_companies must be a mapping or list of mappings")

    by_fip: dict[str, CanonicalIdentity] = {}

    for source_key, record in iterator:
        identity = build_canonical_identity(record, source_key=source_key)
        previous = by_fip.get(identity.fip_code)
        if previous and (
            previous.cnpj != identity.cnpj
            or previous.legal_name != identity.legal_name
        ):
            raise IdentityConflictError(
                f"FIP {identity.fip_code} has conflicting source records"
            )
        by_fip[identity.fip_code] = identity

    return [by_fip[key].to_dict() for key in sorted(by_fip)]
