from api.v2.sandbox_lifecycle_history import (
    seed_sandbox_lifecycle_history,
    update_sandbox_lifecycle_history,
)


def _lti_sandbox(*, extended=True):
    return {
        "legal_name": "LTI Seguros S.A.",
        "cnpj": "47.006.254/0001-80",
        "edition": "2ª edição do Sandbox",
        "regulatory_status": "temporary_authorized",
        "raw_status": "Autorizada (Autorização prorrogada)",
        "authorization_start": "2023-01-20",
        "authorization_end": "2026-03-20",
        "authorization_end_raw": "20/03/2026 (Autorização prorrogada)",
        "authorization_extended": extended,
        "modalities": "Seguros de Danos",
        "source": "https://www.gov.br/susep/sandbox",
    }


def _lti_license():
    return {
        "fip_code": "001988",
        "cnpj": "47006254000180",
        "legal_name": "LTI SEGUROS S.A.",
        "entity_type": "insurer",
    }


def test_lti_history_survives_sandbox_exit_and_records_graduation():
    history = seed_sandbox_lifecycle_history(
        [_lti_sandbox()],
        observed_at="2026-09-15T02:26:32Z",
    )

    updated = update_sandbox_lifecycle_history(
        history,
        current_rows=[],
        current_licensed_rows=[_lti_license()],
        observed_at="2026-09-21T16:20:00Z",
        source_state="fresh",
    )

    record = updated["records"][0]
    assert record["cnpj"] == "47006254000180"
    assert record["current_sandbox_present"] is False
    assert record["current_outcome"] == "graduated_to_ordinary_license"
    assert record["ordinary_license"]["fip_code"] == "001988"
    assert record["last_snapshot"]["regulatory_status"] == "temporary_authorized"
    assert {
        event["event_type"] for event in record["events"]
    } >= {
        "sandbox_participant_first_observed",
        "sandbox_graduated_to_ordinary_license",
    }


def test_repeating_same_fresh_snapshot_does_not_duplicate_graduation_event():
    history = seed_sandbox_lifecycle_history(
        [_lti_sandbox()],
        observed_at="2026-09-15T02:26:32Z",
    )
    first = update_sandbox_lifecycle_history(
        history,
        current_rows=[],
        current_licensed_rows=[_lti_license()],
        observed_at="2026-09-21T16:20:00Z",
        source_state="fresh",
    )
    second = update_sandbox_lifecycle_history(
        first,
        current_rows=[],
        current_licensed_rows=[_lti_license()],
        observed_at="2026-09-22T16:20:00Z",
        source_state="fresh",
    )

    events = [
        event
        for event in second["records"][0]["events"]
        if event["event_type"] == "sandbox_graduated_to_ordinary_license"
    ]
    assert len(events) == 1


def test_stale_source_never_turns_absence_into_exit_or_graduation():
    history = seed_sandbox_lifecycle_history(
        [_lti_sandbox()],
        observed_at="2026-09-15T02:26:32Z",
    )

    updated = update_sandbox_lifecycle_history(
        history,
        current_rows=[],
        current_licensed_rows=[_lti_license()],
        observed_at="2026-09-21T16:20:00Z",
        source_state="stale",
    )

    record = updated["records"][0]
    assert record["current_sandbox_present"] is True
    assert record["current_outcome"] == "active_sandbox"
    assert not any(
        event["event_type"] == "sandbox_graduated_to_ordinary_license"
        for event in record["events"]
    )


def test_extension_change_is_preserved_as_event_without_overwriting_prior_event():
    history = seed_sandbox_lifecycle_history(
        [_lti_sandbox(extended=False)],
        observed_at="2026-03-20T00:00:00Z",
    )

    updated = update_sandbox_lifecycle_history(
        history,
        current_rows=[_lti_sandbox(extended=True)],
        current_licensed_rows=[],
        observed_at="2026-03-27T00:00:00Z",
        source_state="fresh",
    )

    record = updated["records"][0]
    event = next(
        event
        for event in record["events"]
        if event["event_type"] == "sandbox_authorization_extended_changed"
    )
    assert event["previous_value"] is False
    assert event["observed_value"] is True
    assert record["last_snapshot"]["authorization_extended"] is True


def test_removal_without_current_license_is_not_called_discontinuation():
    history = seed_sandbox_lifecycle_history(
        [_lti_sandbox()],
        observed_at="2026-03-20T00:00:00Z",
    )

    updated = update_sandbox_lifecycle_history(
        history,
        current_rows=[],
        current_licensed_rows=[],
        observed_at="2026-04-01T00:00:00Z",
        source_state="fresh",
    )

    record = updated["records"][0]
    assert record["current_sandbox_present"] is False
    assert record["current_outcome"] == "not_present_in_current_sandbox"
    assert "discontinued" not in str(record).casefold()
