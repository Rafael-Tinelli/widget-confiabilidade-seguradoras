from datetime import date

import pytest

from tools.fetch_gsc_market_queries import GscSensorError, fetch_queries


class _Response:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def post(self, url, *, headers, json, timeout):
        self.requests.append(
            {"url": url, "headers": headers, "json": json, "timeout": timeout}
        )
        return self.responses.pop(0)


def test_gsc_collector_filters_exact_page_and_returns_only_query_metrics():
    session = _Session(
        [
            _Response(
                payload={
                    "rows": [
                        {
                            "keys": ["Azos seguros"],
                            "clicks": 3.0,
                            "impressions": 23.0,
                            "ctr": 0.13,
                            "position": 5.2,
                        }
                    ]
                }
            )
        ]
    )
    rows = fetch_queries(
        site_url="sc-domain:sanida.com.br",
        page_url="https://sanida.com.br/ranking-seguradoras/",
        access_token="test-token",
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 28),
        session=session,
    )

    assert rows == [{"query": "Azos seguros", "clicks": 3, "impressions": 23}]
    request = session.requests[0]
    assert request["json"]["dimensions"] == ["query"]
    assert request["json"]["dataState"] == "final"
    assert request["json"]["dimensionFilterGroups"][0]["filters"] == [
        {
            "dimension": "page",
            "operator": "equals",
            "expression": "https://sanida.com.br/ranking-seguradoras/",
        }
    ]
    assert "test-token" not in request["json"].values()


def test_gsc_collector_requires_auth_and_surfaces_unavailable_response():
    with pytest.raises(GscSensorError, match="access token"):
        fetch_queries(
            site_url="sc-domain:sanida.com.br",
            page_url="https://sanida.com.br/ranking-seguradoras/",
            access_token="",
            start_date=date(2026, 8, 1),
            end_date=date(2026, 8, 28),
        )

    session = _Session([_Response(status_code=503, payload={}, text="unavailable")])
    with pytest.raises(GscSensorError, match="HTTP 503"):
        fetch_queries(
            site_url="sc-domain:sanida.com.br",
            page_url="https://sanida.com.br/ranking-seguradoras/",
            access_token="test-token",
            start_date=date(2026, 8, 1),
            end_date=date(2026, 8, 28),
            session=session,
        )
