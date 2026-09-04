from pathlib import Path

GITIGNORE = Path(".gitignore")

REMOVED_PATHS = (
    Path("widget-ui/src/InsurerScoreModal.jsx"),
    Path("data/raw/consumidor_gov/tmp68ikpili.csv"),
    Path("teste_consumidor.py"),
)

LEGACY_PRODUCTION_PATHS = (
    Path("widget-ui/dist/index.html"),
    Path("widget-ui/dist/assets/widget.css"),
    Path("widget-ui/dist/assets/widget.js"),
)


def test_section_19_5_removed_only_confirmed_dead_or_temporary_paths():
    for path in REMOVED_PATHS:
        assert not path.exists(), f"obsolete path returned to the repository: {path}"


def test_reproducible_outputs_and_raw_temporaries_remain_ignored():
    gitignore = GITIGNORE.read_text(encoding="utf-8")

    assert "widget-ui/dist/" in gitignore
    assert "data/raw/*" in gitignore
    assert "*.tmp" in gitignore


def test_v1_frontend_bundle_remains_available_until_production_cutover():
    for path in LEGACY_PRODUCTION_PATHS:
        assert path.is_file(), f"v1 production dependency disappeared before cutover: {path}"
