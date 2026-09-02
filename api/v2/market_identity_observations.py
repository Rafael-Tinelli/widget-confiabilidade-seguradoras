from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2

CANDIDATE_ASSERTION_EFFECT = "none"
CANDIDATE_SCORE_EFFECT = "none"
CANDIDATE_COMPLAINT_TRANSFER_EFFECT = "none"
AUTOMATIC_REGISTRY_MUTATION = "forbidden"

CANDIDATE_LIFECYCLE_STATES = {
    "observed",
    "review_required",
    "resolved_existing_identity",
    "verified_new_identity",
    "dismissed_noise",
    "dismissed_non_market_query",
}

NOISE_QUERIES = {
    "seguro",
    "seguros",
    "seguradora",
    "seguradoras",
    "ranking",
    "ranking seguradoras",
    "melhor seguradora",
    "melhores seguradoras",
    "confiabilidade",
}

FORBIDDEN_TELEMETRY_KEYS = {
    "ip",
    "ip_address",
    "remote_addr",
    "email",
    "e_mail",
    "cpf",
    "cookie",
    "cookies",
    "user_id",
    "userid",
    "username",
    "user_name",
    "session_id",
    "sessionid",
}


@dataclass(frozen=True)
class DemandReviewThresholds:
    """Configurable promotion thresholds for demand-only observations."""

    widget_min_count: int = 2
    widget_min_distinct_days: int = 2
    gsc_min_impressions: int = 5
    gsc_min_clicks: int = 1

    def __post_init__(self) -> None:
        for value in (
            self.widget_min_count,
            self.widget_min_distinct_days,
            self.gsc_min_impressions,
            self.gsc_min_clicks,
        ):
            if value < 0:
                raise ValueError("market observation thresholds must be non-negative")


def normalize_market_query(value: Any) -> str:
    text = str(value or "").strip().casefold()
    if not text:
        return ""
    compact_digits = re.sub(r"\D+", "", text)
    if len(compact_digits) == 14 and len(re.sub(r"[\d./\-\s]+", "", text)) == 0:
        return compact_digits
    text = "".join(
        char
        for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def is_eligible_market_query(value: Any) -> bool:
    normalized = normalize_market_query(value)
    if not normalized or normalized in NOISE_QUERIES:
        return False
    if normalized.isdigit():
        return len(normalized) == 14
    return len(normalized) >= 3


def _exact_search_keys(search_index: Iterable[dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for entry in search_index:
        terms = [
            entry.get("name"),
            *(entry.get("aliases") or []),
            entry.get("cnpj"),
            entry.get("fip_code"),
        ]
        for term in terms:
            normalized = normalize_market_query(term)
            if normalized:
                keys.add(normalized)
    return keys


def _known_identity_phrases(search_index: Iterable[dict[str, Any]]) -> set[str]:
    """Return textual identity phrases suitable for high-precision GSC suppression.

    Search Console queries often contain intent around an already-known company,
    such as ``porto seguro e confiavel``. Those are useful SEO queries, but they are
    not evidence of a new market identity. The GSC sensor therefore suppresses a
    query when it contains a known name/alias on token boundaries. This precision
    rule is deliberately *not* applied to widget zero-result telemetry: the internal
    search remains the higher-recall sensor for genuinely new compound brand names.
    """
    phrases: set[str] = set()
    for entry in search_index:
        terms = [entry.get("name"), *(entry.get("aliases") or [])]
        for term in terms:
            normalized = normalize_market_query(term)
            if (
                normalized
                and normalized not in NOISE_QUERIES
                and not normalized.isdigit()
                and len(normalized) >= 3
            ):
                phrases.add(normalized)
    return phrases


def query_mentions_known_identity(
    value: Any,
    search_index: Iterable[dict[str, Any]],
) -> bool:
    normalized = normalize_market_query(value)
    if not normalized:
        return False
    padded_query = f" {normalized} "
    return any(
        f" {phrase} " in padded_query
        for phrase in _known_identity_phrases(search_index)
    )


def query_resolves_exactly(value: Any, search_index: Iterable[dict[str, Any]]) -> bool:
    normalized = normalize_market_query(value)
    return bool(normalized and normalized in _exact_search_keys(search_index))


def _assert_privacy_minimized_mapping(row: dict[str, Any]) -> None:
    keys = {str(key).strip().casefold() for key in row}
    forbidden = sorted(keys & FORBIDDEN_TELEMETRY_KEYS)
    if forbidden:
        raise ValueError(
            "unknown-search telemetry contains forbidden personal/session fields: "
            + ", ".join(forbidden)
        )


def _candidate_key(namespace: str, anchor: str) -> str:
    digest = hashlib.sha256(f"{namespace}:{anchor}".encode()).hexdigest()[:20]
    return f"market:{namespace}:{digest}"


def widget_unknown_search_observations(
    rows: Iterable[dict[str, Any]],
    search_index: Iterable[dict[str, Any]],
    *,
    thresholds: DemandReviewThresholds | None = None,
) -> list[dict[str, Any]]:
    thresholds = thresholds or DemandReviewThresholds()
    exact_keys = _exact_search_keys(search_index)
    observations: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        _assert_privacy_minimized_mapping(row)
        normalized = normalize_market_query(
            row.get("normalized_query") if "normalized_query" in row else row.get("query")
        )
        if not is_eligible_market_query(normalized) or normalized in exact_keys:
            continue
        count = max(int(row.get("count") or 0), 0)
        distinct_days = max(int(row.get("distinct_day_count") or 0), 0)
        state = (
            "review_required"
            if count >= thresholds.widget_min_count
            and distinct_days >= thresholds.widget_min_distinct_days
            else "observed"
        )
        observations.append(
            {
                "candidate_key": _candidate_key("query", normalized),
                "candidate_anchor": f"query:{normalized}",
                "source": "widget_unknown_search",
                "sensor_class": "demand",
                "confidence_semantics": "relevance_signal_only",
                "observed_value": normalized,
                "normalized_query": normalized,
                "first_seen": row.get("first_seen"),
                "last_seen": row.get("last_seen"),
                "count": count,
                "distinct_day_count": distinct_days,
                "lifecycle_state": state,
                "threshold": {
                    "min_count": thresholds.widget_min_count,
                    "min_distinct_days": thresholds.widget_min_distinct_days,
                },
            }
        )
    return observations


def gsc_query_observations(
    rows: Iterable[dict[str, Any]],
    search_index: Iterable[dict[str, Any]],
    *,
    thresholds: DemandReviewThresholds | None = None,
) -> list[dict[str, Any]]:
    thresholds = thresholds or DemandReviewThresholds()
    search_entries = list(search_index)
    exact_keys = _exact_search_keys(search_entries)
    observations: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        normalized = normalize_market_query(row.get("query"))
        if (
            not is_eligible_market_query(normalized)
            or normalized in exact_keys
            or query_mentions_known_identity(normalized, search_entries)
        ):
            continue
        impressions = max(int(row.get("impressions") or 0), 0)
        clicks = max(int(row.get("clicks") or 0), 0)
        state = (
            "review_required"
            if impressions >= thresholds.gsc_min_impressions
            or clicks >= thresholds.gsc_min_clicks
            else "observed"
        )
        observations.append(
            {
                "candidate_key": _candidate_key("query", normalized),
                "candidate_anchor": f"query:{normalized}",
                "source": "gsc_query",
                "sensor_class": "demand",
                "confidence_semantics": "relevance_signal_only",
                "observed_value": str(row.get("query") or ""),
                "normalized_query": normalized,
                "first_seen": row.get("first_seen"),
                "last_seen": row.get("last_seen"),
                "impressions": impressions,
                "clicks": clicks,
                "lifecycle_state": state,
                "threshold": {
                    "min_impressions": thresholds.gsc_min_impressions,
                    "min_clicks": thresholds.gsc_min_clicks,
                },
            }
        )
    return observations


def _regulated_identity_anchor(row: dict[str, Any]) -> str | None:
    fip = re.sub(r"\D+", "", str(row.get("fip_code") or ""))
    if fip:
        return f"fip:{fip.zfill(6)}"
    cnpj = normalize_cnpj_v2(row.get("cnpj"))
    if cnpj:
        return f"cnpj:{cnpj}"
    return None


def _regulated_observation(
    *,
    anchor: str,
    row: dict[str, Any],
    candidate_type: str,
    observed_value: Any,
    previous_value: Any = None,
) -> dict[str, Any]:
    observation = {
        "candidate_key": _candidate_key("regulated", anchor),
        "candidate_anchor": anchor,
        "source": "susep_licensed_delta",
        "sensor_class": "regulatory",
        "confidence_semantics": "official_regulatory_existence",
        "candidate_type": candidate_type,
        "observed_value": observed_value,
        "fip_code": row.get("fip_code"),
        "cnpj": normalize_cnpj_v2(row.get("cnpj")),
        "entity_type": row.get("entity_type"),
        "lifecycle_state": "review_required",
    }
    if previous_value is not None:
        observation["previous_value"] = previous_value
    return observation


def regulated_entity_delta_observations(
    previous_rows: Iterable[dict[str, Any]],
    current_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    previous = {
        anchor: dict(row)
        for row in previous_rows
        if (anchor := _regulated_identity_anchor(dict(row)))
    }
    current = {
        anchor: dict(row)
        for row in current_rows
        if (anchor := _regulated_identity_anchor(dict(row)))
    }
    observations: list[dict[str, Any]] = []
    for anchor in sorted(current):
        row = current[anchor]
        before = previous.get(anchor)
        if before is None:
            observations.append(
                _regulated_observation(
                    anchor=anchor,
                    row=row,
                    candidate_type="new_regulated_entity",
                    observed_value=row.get("legal_name"),
                )
            )
            continue

        before_cnpj = normalize_cnpj_v2(before.get("cnpj"))
        current_cnpj = normalize_cnpj_v2(row.get("cnpj"))
        if before_cnpj and current_cnpj and before_cnpj != current_cnpj:
            observations.append(
                _regulated_observation(
                    anchor=anchor,
                    row=row,
                    candidate_type="regulated_cnpj_change",
                    observed_value=current_cnpj,
                    previous_value=before_cnpj,
                )
            )

        before_name = normalize_market_query(before.get("legal_name"))
        current_name = normalize_market_query(row.get("legal_name"))
        if before_name and current_name and before_name != current_name:
            observations.append(
                _regulated_observation(
                    anchor=anchor,
                    row=row,
                    candidate_type="regulated_name_change",
                    observed_value=row.get("legal_name"),
                    previous_value=before.get("legal_name"),
                )
            )

        before_status = str(before.get("regulatory_status") or "")
        current_status = str(row.get("regulatory_status") or "")
        if before_status and current_status and before_status != current_status:
            observations.append(
                _regulated_observation(
                    anchor=anchor,
                    row=row,
                    candidate_type="regulated_status_change",
                    observed_value=current_status,
                    previous_value=before_status,
                )
            )
    return observations


def sandbox_delta_observations(
    previous_rows: Iterable[dict[str, Any]],
    current_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    def index(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for raw in rows:
            row = dict(raw)
            cnpj = normalize_cnpj_v2(row.get("cnpj"))
            if cnpj:
                result[cnpj] = row
        return result

    previous = index(previous_rows)
    current = index(current_rows)
    observations: list[dict[str, Any]] = []
    for cnpj in sorted(set(current) - set(previous)):
        row = current[cnpj]
        anchor = f"cnpj:{cnpj}"
        observations.append(
            {
                "candidate_key": _candidate_key("sandbox", anchor),
                "candidate_anchor": anchor,
                "source": "susep_sandbox_delta",
                "sensor_class": "regulatory",
                "confidence_semantics": "official_regulatory_existence",
                "candidate_type": "new_sandbox_participant",
                "observed_value": row.get("legal_name") or row.get("name"),
                "cnpj": cnpj,
                "lifecycle_state": "review_required",
            }
        )
    return observations


def candidate_registry_from_observations(
    observations: Iterable[dict[str, Any]],
    *,
    sensor_status: dict[str, str] | None = None,
) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in observations:
        observation = dict(raw)
        key = str(observation.get("candidate_key") or "")
        state = str(observation.get("lifecycle_state") or "observed")
        if not key:
            raise ValueError("market observation without candidate_key")
        if state not in CANDIDATE_LIFECYCLE_STATES:
            raise ValueError(f"unsupported candidate lifecycle state: {state}")
        grouped[key].append(observation)

    candidates: list[dict[str, Any]] = []
    for candidate_key in sorted(grouped):
        rows = grouped[candidate_key]
        states = {str(row.get("lifecycle_state") or "observed") for row in rows}
        review_state = "review_required" if "review_required" in states else "observed"
        candidate_types = sorted(
            {
                str(row.get("candidate_type") or "unknown_market_query")
                for row in rows
            }
        )
        candidates.append(
            {
                "candidate_id": candidate_key,
                "candidate_type": (
                    candidate_types[0]
                    if len(candidate_types) == 1
                    else "multi_signal_market_candidate"
                ),
                "candidate_types": candidate_types,
                "candidate_anchor": rows[0].get("candidate_anchor"),
                "review_state": review_state,
                "lifecycle_state": review_state,
                "priority": "P1" if review_state == "review_required" else "P2",
                "observations": rows,
                "assertion_effect": CANDIDATE_ASSERTION_EFFECT,
                "score_effect": CANDIDATE_SCORE_EFFECT,
                "complaint_transfer_effect": CANDIDATE_COMPLAINT_TRANSFER_EFFECT,
                "automatic_registry_mutation": AUTOMATIC_REGISTRY_MUTATION,
                "blocking": False,
            }
        )

    statuses = dict(sensor_status or {})
    return {
        "artifact": "v2_market_identity_candidate_registry",
        "status": "observational_non_authoritative",
        "sensor_status": statuses,
        "policy": {
            "detection_is_assertion": False,
            "fuzzy_identity_resolution": "forbidden",
            "candidate_assertion_effect": CANDIDATE_ASSERTION_EFFECT,
            "candidate_score_effect": CANDIDATE_SCORE_EFFECT,
            "candidate_complaint_transfer_effect": CANDIDATE_COMPLAINT_TRANSFER_EFFECT,
            "automatic_registry_mutation": AUTOMATIC_REGISTRY_MUTATION,
            "sensor_unavailable_invalidates_gate4": False,
        },
        "summary": {
            "observation_count": sum(len(rows) for rows in grouped.values()),
            "candidate_count": len(candidates),
            "review_required_count": sum(
                candidate["review_state"] == "review_required" for candidate in candidates
            ),
        },
        "candidates": candidates,
    }
