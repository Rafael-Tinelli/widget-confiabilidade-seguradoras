from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any


def _safe(value: Any, limit: int = 240) -> str:
    """Render untrusted observational text as inert Markdown table content.

    Sensor values can originate from user-entered widget queries or Search Console.
    They are evidence labels only and must never be able to inject Markdown/HTML or
    break the review queue layout. SQL injection is separately prevented at the
    HostGator bridge by prepared statements; this guard protects the GitHub surface.
    """
    text = str(value if value is not None else "—")
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", " ", text)
    text = text.replace("\r", " ").replace("\n", " ")
    text = text.replace("\\", "\\\\")
    text = text.replace("|", "\\|")
    text = text.replace("`", "\\`")
    text = text.replace("[", "\\[").replace("]", "\\]")
    text = text.replace("<", "&lt;").replace(">", "&gt;")
    return text[:limit]


def render_review_queue(
    payload: dict[str, Any],
    *,
    source_run_id: str,
    source_run_url: str,
) -> tuple[str, dict[str, int]]:
    if payload.get("artifact") != "v2_relationship_watchdog":
        raise ValueError("unexpected relationship watchdog artifact")
    policy = payload.get("policy") or {}
    for key, expected in (
        ("candidate_assertion_effect", "none"),
        ("candidate_complaint_transfer_effect", "none"),
        ("automatic_registry_mutation", "forbidden"),
    ):
        if policy.get(key) != expected:
            raise ValueError(f"relationship review queue policy mismatch: {key}")

    candidates = list(payload.get("candidates") or [])
    review_candidates = [
        row
        for row in candidates
        if row.get("review_state") in {"review_required", "registry_drift"}
    ]
    review_count = len(review_candidates)
    blocking_count = sum(bool(row.get("blocking")) for row in review_candidates)
    by_type = Counter(str(row.get("candidate_type") or "unknown") for row in review_candidates)
    market_count = sum(
        row.get("candidate_domain") == "emerging_market_identity"
        for row in review_candidates
    )

    lines = [
        "# Relationship review queue",
        "",
        f"Source Full Generation: [{source_run_id}]({source_run_url})",
        "",
        f"Current review candidates: **{review_count}**  ",
        f"Emerging-market candidates: **{market_count}**  ",
        f"Blocking registry drift: **{blocking_count}**",
        "",
        "> This queue is observational. `assertion_effect = none`, `score_effect = none`, complaint transfer is forbidden, and no canonical registry mutation is automatic.",
        "",
    ]

    sensor_status = payload.get("market_sensor_status") or {}
    if sensor_status:
        lines.extend([
            "## Market sensor status",
            "",
            "| Sensor | Status |",
            "|---|---|",
        ])
        for sensor, status in sorted(sensor_status.items()):
            lines.append(f"| `{_safe(sensor)}` | `{_safe(status)}` |")
        lines.append("")

    lines.extend([
        "## Counts by candidate type",
        "",
        "| Candidate type | Count |",
        "|---|---:|",
    ])
    if by_type:
        for candidate_type, count in sorted(by_type.items()):
            lines.append(f"| `{_safe(candidate_type)}` | {count} |")
    else:
        lines.append("| — | 0 |")

    if review_candidates:
        lines.extend([
            "",
            "<details>",
            f"<summary>Show {review_count} current candidates</summary>",
            "",
            "| Candidate | Type | State | Priority | Sources | Observed identity/context |",
            "|---|---|---|---|---|---|",
        ])
        for row in review_candidates:
            signals = row.get("signals") or {}
            sources = signals.get("sources") or []
            if isinstance(sources, str):
                sources = [sources]
            if not sources:
                sources = [
                    row.get("candidate_domain")
                    or row.get("source")
                    or "relationship_watchdog"
                ]
            observations = row.get("observations") or []
            observed = (
                row.get("provider_label")
                or row.get("brand_id")
                or row.get("entity_id")
                or row.get("relationship_id")
                or signals.get("provider_label")
                or signals.get("display_name")
                or (observations[0].get("observed_value") if observations else None)
                or row.get("reason")
                or "—"
            )
            lines.append(
                "| `{}` | `{}` | `{}` | {} | {} | {} |".format(
                    _safe(row.get("candidate_id")),
                    _safe(row.get("candidate_type")),
                    _safe(row.get("review_state")),
                    _safe(row.get("priority")),
                    _safe(", ".join(str(value) for value in sources if value)),
                    _safe(observed),
                )
            )
        lines.extend(["", "</details>"])

    lines.extend([
        "",
        "## Review rule",
        "",
        "A candidate may suggest investigation or evidence collection. Detection does not assert identity or relationship. Public identity, risk carrier, complaint attribution and canonical registry changes still require deterministic/source-backed resolution under the existing v2 contracts.",
    ])
    return "\n".join(lines) + "\n", {
        "review_count": review_count,
        "blocking_count": blocking_count,
        "market_count": market_count,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render the singleton v2 review queue Markdown.")
    parser.add_argument("watchdog", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("source_run_id")
    parser.add_argument("source_run_url")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    payload = json.loads(args.watchdog.read_text(encoding="utf-8"))
    markdown, summary = render_review_queue(
        payload,
        source_run_id=args.source_run_id,
        source_run_url=args.source_run_url,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(markdown, encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
