from api.v2.render_relationship_review_queue import render_review_queue


def _payload(observed_value: str) -> dict:
    return {
        "artifact": "v2_relationship_watchdog",
        "policy": {
            "candidate_assertion_effect": "none",
            "candidate_complaint_transfer_effect": "none",
            "automatic_registry_mutation": "forbidden",
        },
        "candidates": [
            {
                "candidate_id": "market:query:test",
                "candidate_type": "unknown_market_query",
                "candidate_domain": "emerging_market_identity",
                "review_state": "review_required",
                "priority": "P1",
                "blocking": False,
                "observations": [
                    {
                        "source": "gsc_query",
                        "observed_value": observed_value,
                    }
                ],
            }
        ],
    }


def test_untrusted_sensor_text_cannot_inject_html_or_markdown_links():
    markdown, summary = render_review_queue(
        _payload("</details>|[click](https://evil.example)`code`\x00"),
        source_run_id="123",
        source_run_url="https://github.com/example/run/123",
    )

    assert summary["market_count"] == 1
    assert "</details>" not in markdown
    assert "[click](https://evil.example)" not in markdown
    assert "&lt;/details&gt;" in markdown
    assert "\\[click\\](https://evil.example)" in markdown
    assert "\\`code\\`" in markdown
    assert "\x00" not in markdown
