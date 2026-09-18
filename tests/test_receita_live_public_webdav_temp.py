from __future__ import annotations

import json
import subprocess

HOST = "https://arquivos.receitafederal.gov.br"
TOKEN = "gn672Ad4CF8N6TK"
CNPJ_PATH = "Dados/Cadastros/CNPJ"


def _curl(args: list[str]) -> dict:
    command = [
        "curl",
        "--silent",
        "--show-error",
        "--max-time",
        "30",
        "--output",
        "/tmp/receita-probe-body",
        "--write-out",
        "%{http_code}|%{url_effective}|%{size_download}",
        *args,
    ]
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def test_receita_live_transport_probe() -> None:
    diagnostics = {
        "root_get": _curl(["--location", f"{HOST}/"]),
        "share_get": _curl(
            ["--location", f"{HOST}/index.php/s/{TOKEN}"]
        ),
        "legacy_webdav": _curl(
            [
                "--request",
                "PROPFIND",
                "--header",
                "Depth: 1",
                "--user",
                f"{TOKEN}:",
                f"{HOST}/public.php/webdav/{CNPJ_PATH}/",
            ]
        ),
        "modern_public_no_auth": _curl(
            [
                "--request",
                "PROPFIND",
                "--header",
                "Depth: 1",
                "--header",
                "X-Requested-With: XMLHttpRequest",
                f"{HOST}/public.php/dav/files/{TOKEN}/{CNPJ_PATH}/",
            ]
        ),
        "modern_public_token_auth": _curl(
            [
                "--request",
                "PROPFIND",
                "--header",
                "Depth: 1",
                "--header",
                "X-Requested-With: XMLHttpRequest",
                "--user",
                f"{TOKEN}:",
                f"{HOST}/public.php/dav/files/{TOKEN}/{CNPJ_PATH}/",
            ]
        ),
    }
    raise AssertionError(json.dumps(diagnostics, ensure_ascii=False, indent=2))
