from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_hostgator_bridge_persists_only_aggregate_query_fields():
    source = (ROOT / "ops/hostgator/unknown-market-query-endpoint.php").read_text(
        encoding="utf-8"
    )
    assert "normalized_query TEXT PRIMARY KEY" in source
    assert "distinct_day_count INTEGER NOT NULL" in source
    assert "REMOTE_ADDR" not in source
    assert "HTTP_COOKIE" not in source
    assert "session_start" not in source
    assert "query_only_contract_required" in source


def test_frontend_telemetry_is_disabled_by_default_and_same_origin_only():
    source = (ROOT / "widget-ui/src/unknownMarketTelemetry.js").read_text(
        encoding="utf-8"
    )
    assert "VITE_UNKNOWN_MARKET_QUERY_ENDPOINT" in source
    assert "endpoint.origin !== window.location.origin" in source
    assert "credentials: 'omit'" in source
    assert "JSON.stringify({ query: normalized })" in source
    assert "document.cookie" not in source
    assert "localStorage" not in source
    assert "sessionStorage" not in source


def test_market_sensor_workflow_is_guarded_and_uses_exact_full_generation():
    source = (
        ROOT / ".github/workflows/v2-emerging-market-identity-sensors.yml"
    ).read_text(encoding="utf-8")
    assert "V2_MARKET_SENSOR_AUTOMATION_ENABLED == 'true'" in source
    assert "source_run_id" in source
    assert "source Full Generation did not conclude successfully" in source
    assert "latest successful" not in source.lower()
    assert "V2_PRODUCTION_AUTOMATION_ENABLED" not in source
    assert "google-github-actions/auth@v3" in source
    assert "https://www.googleapis.com/auth/webmasters.readonly" in source
    assert "automatic_registry_mutation" not in source or "forbidden" in source


def test_gsc_collector_never_contains_credentials_in_payload_contract():
    source = (ROOT / "tools/fetch_gsc_market_queries.py").read_text(encoding="utf-8")
    assert 'os.getenv("GSC_ACCESS_TOKEN", "")' in source
    assert '"Authorization": f"Bearer {access_token}"' in source
    assert '"query": keys[0]' in source
    assert '"access_token":' not in source
    assert '"credentials":' not in source
