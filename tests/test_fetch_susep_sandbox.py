from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import requests

MODULE_PATH = Path(__file__).parents[1] / "tools/fetch_susep_sandbox.py"
SPEC = importlib.util.spec_from_file_location("fetch_susep_sandbox", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"não foi possível carregar {MODULE_PATH}")
sandbox = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sandbox)


FIRST_EDITION_HTML = """
<html><body>
<p>Os projetos selecionados foram:</p>
<div>1</div><div>88i</div><div>descrição</div>
<div>2</div><div>COOVER</div><div>descrição</div>
<div>3</div><div>IZA</div><div>descrição</div>
<p>Links de compartilhamento</p>
</body></html>
"""

SECOND_EDITION_HTML = """
<html><body>
<table>
  <tr><th>Empresa</th><th>Linhas de negócio</th></tr>
  <tr><td>Clubfix</td><td>Garantia estendida</td></tr>
  <tr><td>Darwin Seguros</td><td>Celular</td></tr>
  <tr><td>Simple2u</td><td>Bicicletas</td></tr>
</table>
</body></html>
"""


def test_second_edition_uses_company_column() -> None:
    items = sandbox.parse_page(SECOND_EDITION_HTML, "2ª edição do Sandbox")
    names = [item["name"] for item in items]

    assert names == ["Clubfix", "Darwin Seguros", "Simple2u"]
    assert "Celular" not in names
    assert "Bicicletas" not in names


def test_first_edition_numbered_fallback() -> None:
    items = sandbox.parse_page(FIRST_EDITION_HTML, "1ª edição do Sandbox")

    assert [item["name"] for item in items] == ["88i", "COOVER", "IZA"]
    assert all(item["cnpj"] == "" for item in items)


def test_validation_rejects_business_line_as_participant() -> None:
    items = [
        sandbox.make_item("88i", "1ª edição do Sandbox"),
        sandbox.make_item("COOVER", "1ª edição do Sandbox"),
        sandbox.make_item("IZA", "1ª edição do Sandbox"),
        sandbox.make_item("Clubfix", "2ª edição do Sandbox"),
        sandbox.make_item("Celular", "2ª edição do Sandbox"),
    ]

    errors = sandbox.validate_items(items)

    assert any("linhas de negócio" in error for error in errors)


def test_main_writes_only_valid_complete_collection(
    monkeypatch,
    tmp_path: Path,
) -> None:
    output = tmp_path / "api/v1/susep-sandbox-participants.json"
    monkeypatch.setattr(sandbox, "OUT", output)
    monkeypatch.setattr(
        sandbox,
        "URLS",
        [
            ("1ª edição do Sandbox", "first"),
            ("2ª edição do Sandbox", "second"),
            ("3ª edição do Sandbox", "third"),
        ],
    )
    monkeypatch.setattr(sandbox, "_build_session", object)

    html_by_url = {
        "first": FIRST_EDITION_HTML,
        "second": SECOND_EDITION_HTML,
        "third": "<html><body>Sem lista publicada.</body></html>",
    }
    monkeypatch.setattr(
        sandbox,
        "_fetch_html",
        lambda _session, url: html_by_url[url],
    )

    assert sandbox.main() == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    names = {item["name"] for item in payload["items"]}
    assert names == {"88i", "COOVER", "IZA", "Clubfix", "Darwin Seguros", "Simple2u"}


def test_main_preserves_previous_file_on_fetch_failure(
    monkeypatch,
    tmp_path: Path,
) -> None:
    output = tmp_path / "api/v1/susep-sandbox-participants.json"
    output.parent.mkdir(parents=True)
    original = {
        "source": "anterior",
        "fetchedAt": "2026-01-01T00:00:00+00:00",
        "items": [
            sandbox.make_item("Anterior A", "1ª edição do Sandbox"),
            sandbox.make_item("Anterior B", "1ª edição do Sandbox"),
            sandbox.make_item("Anterior C", "1ª edição do Sandbox"),
            sandbox.make_item("Anterior D", "2ª edição do Sandbox"),
            sandbox.make_item("Anterior E", "2ª edição do Sandbox"),
        ],
    }
    output.write_text(json.dumps(original), encoding="utf-8")
    original_bytes = output.read_bytes()

    monkeypatch.setattr(sandbox, "OUT", output)
    monkeypatch.setattr(
        sandbox,
        "URLS",
        [
            ("1ª edição do Sandbox", "first"),
            ("2ª edição do Sandbox", "second"),
            ("3ª edição do Sandbox", "third"),
        ],
    )
    monkeypatch.setattr(sandbox, "_build_session", object)

    def fake_fetch(_session, url: str) -> str:
        if url == "second":
            raise requests.ConnectionError("DNS indisponível")
        return FIRST_EDITION_HTML if url == "first" else "<html></html>"

    monkeypatch.setattr(sandbox, "_fetch_html", fake_fetch)

    assert sandbox.main() == 1
    assert output.read_bytes() == original_bytes
