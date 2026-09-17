from pathlib import Path

WORKFLOW = Path(".github/workflows/v2-hostgator-publication.yml")


def test_hostgator_publication_uses_single_verified_archive_transport():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    for required in (
        'transport_archive="$RUNNER_TEMP/v2-public-package.tar.gz"',
        'tar -C "$PACKAGE_DIR" -czf "$transport_archive" .',
        'python - "$transport_archive" <<\'PY\'',
        'unsafe transport archive member',
        'transport archive contains link',
        '"$transport_archive" \\',
        "tar -xzf '$incoming/v2-public-package.tar.gz' -C '$incoming/package'",
        "bash '$incoming/install_v2_public_remote.sh'",
        "'$PACKAGE_SHA'",
        "ServerAliveInterval=30",
        "ServerAliveCountMax=6",
    ):
        assert required in workflow

    assert 'scp "${scp_opts[@]}" -r "$PACKAGE_DIR"/*' not in workflow


def test_transport_change_preserves_exact_package_verification_and_atomic_installer():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    verification = 'python -m api.v2.install_public_generation verify "$package_dir" >/dev/null'
    archive = 'tar -C "$PACKAGE_DIR" -czf "$transport_archive" .'
    installer = "bash '$incoming/install_v2_public_remote.sh'"

    assert verification in workflow
    assert archive in workflow
    assert installer in workflow
    assert workflow.index(verification) < workflow.index(archive) < workflow.index(installer)


def test_hostgator_publication_reuses_one_ssh_transport_session():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    for required in (
        'control_path="$RUNNER_TEMP/v2-hostgator-ssh/control-%C"',
        "ControlMaster=auto",
        "ControlPersist=120",
        'ControlPath="$control_path"',
    ):
        assert required in workflow

    # ssh and scp must share the same control socket instead of opening a fresh
    # TCP/SSH handshake for each publication phase.
    assert workflow.count('ControlPath="$control_path"') == 2


def test_initial_ssh_retry_is_bounded_and_only_for_observed_transient_resets():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "for attempt in 1 2 3; do" in workflow
    assert "[[ $attempt -lt 3 ]]" in workflow
    assert "kex_exchange_identification:" in workflow
    assert "Connection reset by peer" in workflow
    assert "sleep $(( attempt * 5 ))" in workflow
    assert 'exit "$status"' in workflow

    # Preserve the failing ssh exit code before any logging/grep changes $?.
    assert "else\n              status=$?\n            fi" in workflow

    # Authentication and host-key failures must not be explicitly classified
    # as retryable conditions.
    assert "Permission denied" not in workflow
    assert "HOST IDENTIFICATION HAS CHANGED" not in workflow
