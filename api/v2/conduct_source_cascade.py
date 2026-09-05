from __future__ import annotations

from dataclasses import dataclass
from typing import Any

CASCADE_VERSION = "2.0-draft-conduct-source-cascade-1"

SOURCE_TIERS: tuple[tuple[int, str, str], ...] = (
    (1, "bdr_susepcon", "bdr_primary"),
    (2, "consumer_gov_basecompleta", "consumer_gov_taxonomy_primary"),
    (3, "consumer_gov_core_plus_ses", "consumer_gov_rppa_proxy"),
)

REQUIRED_CAPABILITIES = (
    "current",
    "public",
    "structured",
    "consumable",
    "coverage_sufficient",
)


class ConductSourceCascadeInvariantError(ValueError):
    """Raised when a conduct source decision violates the cascade contract."""


@dataclass(frozen=True)
class ConductSourceProbe:
    source: str
    current: bool
    public: bool
    structured: bool
    consumable: bool
    coverage_sufficient: bool
    state: str = "observed"
    reason_codes: tuple[str, ...] = ()
    metadata: dict[str, Any] | None = None

    @property
    def eligible(self) -> bool:
        return all(bool(getattr(self, field)) for field in REQUIRED_CAPABILITIES)

    def as_dict(self) -> dict[str, Any]:
        missing = [
            field for field in REQUIRED_CAPABILITIES if not bool(getattr(self, field))
        ]
        return {
            "source": self.source,
            "state": self.state,
            "eligible": self.eligible,
            "capabilities": {
                field: bool(getattr(self, field)) for field in REQUIRED_CAPABILITIES
            },
            "missing_capabilities": missing,
            "reason_codes": list(self.reason_codes),
            "metadata": dict(self.metadata or {}),
        }


def unavailable_probe(source: str, reason: str) -> ConductSourceProbe:
    return ConductSourceProbe(
        source=source,
        current=False,
        public=False,
        structured=False,
        consumable=False,
        coverage_sufficient=False,
        state="unavailable",
        reason_codes=(reason,),
    )


def select_conduct_source(
    probes: dict[str, ConductSourceProbe],
) -> dict[str, Any]:
    """Select exactly one conduct evidence regime by strict priority.

    A lower tier is considered only when every higher-priority source fails its
    capability gate. The selected methodology is never stitched into a time
    series from a different tier.
    """
    evaluated: dict[str, dict[str, Any]] = {}
    selected: tuple[int, str, str, ConductSourceProbe] | None = None

    for tier, source, methodology in SOURCE_TIERS:
        probe = probes.get(source) or unavailable_probe(source, "probe_missing")
        evaluated[source] = probe.as_dict()
        if selected is None and probe.eligible:
            selected = (tier, source, methodology, probe)

    if selected is None:
        decision = {
            "cascade_version": CASCADE_VERSION,
            "state": "conduct_evidence_unavailable",
            "selected_tier": None,
            "selected_source": None,
            "methodology": None,
            "series_policy": "no_cross_source_stitching",
            "scoring_state": "unavailable",
            "sources": evaluated,
        }
        validate_cascade_decision(decision)
        return decision

    tier, source, methodology, _ = selected
    decision = {
        "cascade_version": CASCADE_VERSION,
        "state": "source_selected",
        "selected_tier": tier,
        "selected_source": source,
        "methodology": methodology,
        "series_policy": "no_cross_source_stitching",
        "scoring_state": "calibration_required",
        "sources": evaluated,
        "higher_priority_sources": {
            name: evaluated[name]
            for priority, name, _ in SOURCE_TIERS
            if priority < tier
        },
    }
    validate_cascade_decision(decision)
    return decision


def validate_cascade_decision(decision: dict[str, Any]) -> None:
    sources = decision.get("sources") or {}
    selected_source = decision.get("selected_source")
    selected_tier = decision.get("selected_tier")

    if decision.get("series_policy") != "no_cross_source_stitching":
        raise ConductSourceCascadeInvariantError(
            "conduct evidence must not stitch time series across source tiers"
        )

    if selected_source is None:
        if selected_tier is not None or decision.get("methodology") is not None:
            raise ConductSourceCascadeInvariantError(
                "unavailable conduct decision cannot expose a selected tier or methodology"
            )
        if any(bool(item.get("eligible")) for item in sources.values()):
            raise ConductSourceCascadeInvariantError(
                "eligible source exists but cascade selected none"
            )
        return

    selected = sources.get(str(selected_source))
    if not isinstance(selected, dict) or selected.get("eligible") is not True:
        raise ConductSourceCascadeInvariantError(
            "selected conduct source must have passed the capability gate"
        )

    expected = next(
        (
            (tier, methodology)
            for tier, source, methodology in SOURCE_TIERS
            if source == selected_source
        ),
        None,
    )
    if expected is None:
        raise ConductSourceCascadeInvariantError("unknown selected conduct source")
    expected_tier, expected_methodology = expected
    if selected_tier != expected_tier or decision.get("methodology") != expected_methodology:
        raise ConductSourceCascadeInvariantError(
            "selected conduct tier/methodology does not match source priority contract"
        )

    for tier, source, _ in SOURCE_TIERS:
        if tier >= expected_tier:
            continue
        if bool((sources.get(source) or {}).get("eligible")):
            raise ConductSourceCascadeInvariantError(
                "lower-priority conduct source selected while a higher tier is eligible"
            )
