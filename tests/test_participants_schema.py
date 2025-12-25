# tests/test_participants_schema.py
import json
from pathlib import Path

PARTICIPANTS_PATH = Path("api/v1/participants.json")


def _load_data():
    assert PARTICIPANTS_PATH.exists(), "api/v1/participants.json must exist"
    return json.loads(PARTICIPANTS_PATH.read_text(encoding="utf-8"))


def test_basic_structure():
    data = _load_data()
    assert set(data.keys()) >= {"source", "participants", "meta"}

    source = data["source"]
    assert isinstance(source.get("url"), str) and source["url"]
    assert isinstance(source.get("fetchedAt"), str) and source["fetchedAt"]

    meta = data["meta"]
    assert isinstance(meta.get("count"), int)
    assert meta["count"] > 0

    participants = data["participants"]
    assert isinstance(participants, list)
    assert len(participants) == meta["count"]


def test_participants_shape_and_sizes():
    data = _load_data()

    allowed_participant_keys = {
        "id",
        "name",
        "registrationNumber",
        "status",
        "roles",
        "authorizationServers",
    }

    allowed_as_keys = {
        "id",
        "issuer",
        "openid",
        "status",
        "apiResourcesCount",
        "apiFamiliesCount",
        "apiFamilies",
    }

    # Guard rails: não deixar vazar estruturas gigantes do payload FULL para o SLIM
    forbidden_payload_keys = {
        "apiresources",
        "apiendpoints",
        "apidiscoveryendpoints",
        "resources",
        "apis",
    }

    for participant in data["participants"]:
        assert isinstance(participant, dict)
        assert set(participant.keys()) <= allowed_participant_keys

        for key in ("id", "name", "registrationNumber", "status"):
            value = participant.get(key)
            assert value is None or isinstance(value, str)

        roles = participant.get("roles")
        assert isinstance(roles, list)
        assert all(isinstance(r, str) for r in roles)

        auth_servers = participant.get("authorizationServers")
        assert isinstance(auth_servers, list)

        for auth in auth_servers:
            assert isinstance(auth, dict)
            assert set(auth.keys()) <= allowed_as_keys

            for k, v in auth.items():
                if k in {"apiResourcesCount", "apiFamiliesCount"}:
                    assert isinstance(v, int)
                    assert v >= 0
                elif k == "apiFamilies":
                    assert isinstance(v, list)
                    assert all(isinstance(f, str) for f in v)
                else:
                    assert v is None or isinstance(v, str)

            for key in auth.keys():
                assert (
                    key.lower() not in forbidden_payload_keys
                ), f"Found large payload key {key} in authorization server"
