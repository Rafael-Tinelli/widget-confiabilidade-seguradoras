from pathlib import Path

GITIGNORE = Path(".gitignore")

REMOVED_PATHS = (
    Path("widget-ui/src/InsurerScoreModal.jsx"),
    Path("widget-ui/dist/index.html"),
    Path("widget-ui/dist/assets/widget.css"),
    Path("widget-ui/dist/assets/widget.js"),
    Path("data/raw/consumidor_gov/tmp68ikpili.csv"),
    Path("teste_consumidor.py"),
)


def test_section_19_5_removed_only_confirmed_dead_or_temporary_paths():
    for path in REMOVED_PATHS:
        assert not path.exists(), f"obsolete path returned to the repository: {path}"


def test_reproducible_outputs_and_raw_temporaries_remain_ignored():
    gitignore = GITIGNORE.read_text(encoding="utf-8")

    assert "widget-ui/dist/" in gitignore
    assert "data/raw/*" in gitignore
    assert "*.tmp" in gitignore
