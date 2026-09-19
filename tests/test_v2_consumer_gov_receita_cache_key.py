import json

from api.v2.build_consumer_gov_receita_identity import (
    CACHE_KEY_VERSION,
    TARGET_UNIVERSE_HASH_METHOD,
    _cache_key,
    _existing_cache_key,
    target_universe_hash,
    unresolved_provider_hash,
)


def _entity(entity_id: str, cnpj: str | None, legal_name: str = "SEGURADORA") -> dict:
    return {"entity_id": entity_id, "cnpj": cnpj, "legal_name": legal_name}


def test_target_universe_hash_is_bound_only_to_valid_cnpj_population() -> None:
    first = [
        _entity("fip:000001", "11.111.111/0001-11", "SEGURADORA A"),
        _entity("fip:000002", "22.222.222/0001-22", "SEGURADORA B"),
    ]
    equivalent = [
        _entity("changed-id", "22.222.222/0001-22", "OUTRO NOME"),
        _entity("another-id", "11.111.111/0001-11", "NOME ALTERADO"),
        _entity("fip:sem-cnpj", None, "SEM CNPJ"),
    ]
    changed = [*first, _entity("fip:000003", "33.333.333/0001-33")]

    assert target_universe_hash(first) == target_universe_hash(equivalent)
    assert target_universe_hash(first) != target_universe_hash(changed)


def test_unresolved_provider_hash_normalizes_order_case_and_accents() -> None:
    first = ["Previsul", "Itaú Unibanco Capitalização"]
    equivalent = ["ITAU UNIBANCO CAPITALIZACAO", "PREVISUL"]
    changed = [*first, "Outro Provedor"]

    assert unresolved_provider_hash(first) == unresolved_provider_hash(equivalent)
    assert unresolved_provider_hash(first) != unresolved_provider_hash(changed)


def test_existing_snapshot_is_reusable_only_with_complete_gate4_cache_key(tmp_path) -> None:
    path = tmp_path / "receita.json"
    expected = _cache_key(
        reference_period="2026-08",
        release_fingerprint_value="c" * 64,
        universe_hash="a" * 64,
        provider_hash="b" * 64,
    )
    path.write_text(
        json.dumps({"meta": {"gate4_cache_key": expected}}),
        encoding="utf-8",
    )

    assert _existing_cache_key(path) == expected
    assert expected["version"] == CACHE_KEY_VERSION
    assert expected["release_fingerprint"] == "c" * 64
    assert expected["target_universe_hash_method"] == TARGET_UNIVERSE_HASH_METHOD

    path.write_text(
        json.dumps(
            {
                "source": {"reference_period": "2026-08"},
                "meta": {},
            }
        ),
        encoding="utf-8",
    )
    assert _existing_cache_key(path) is None
