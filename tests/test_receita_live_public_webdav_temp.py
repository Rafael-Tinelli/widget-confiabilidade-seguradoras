from __future__ import annotations

import json
import re

import requests


def _probe(session: requests.Session, method: str, url: str, **kwargs) -> dict:
    try:
        response = session.request(method, url, timeout=(10, 30), **kwargs)
        return {
            "status": response.status_code,
            "url": response.url,
            "location": response.headers.get("location"),
            "content_type": response.headers.get("content-type"),
            "bytes": len(response.content),
        }
    except requests.RequestException as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def test_receita_live_public_webdav_probe() -> None:
    bootstrap = requests.Session()
    root = bootstrap.get(
        "https://arquivos.receitafederal.gov.br/",
        timeout=(10, 30),
        allow_redirects=True,
    )
    token_match = re.search(r"/index\.php/s/([^/?#]+)", root.url)
    token = token_match.group(1) if token_match else ""

    diagnostics = {
        "root_status": root.status_code,
        "root_final_url": root.url,
        "token": token,
    }

    if token:
        legacy = requests.Session()
        legacy.auth = (token, "")
        diagnostics["legacy_webdav"] = _probe(
            legacy,
            "PROPFIND",
            "https://arquivos.receitafederal.gov.br/public.php/webdav/"
            "Dados/Cadastros/CNPJ/",
            headers={"Depth": "1"},
        )

        modern_public = requests.Session()
        diagnostics["modern_public_no_auth"] = _probe(
            modern_public,
            "PROPFIND",
            "https://arquivos.receitafederal.gov.br/public.php/dav/files/"
            f"{token}/Dados/Cadastros/CNPJ/",
            headers={"Depth": "1", "X-Requested-With": "XMLHttpRequest"},
        )

        modern_auth = requests.Session()
        modern_auth.auth = (token, "")
        diagnostics["modern_public_token_auth"] = _probe(
            modern_auth,
            "PROPFIND",
            "https://arquivos.receitafederal.gov.br/public.php/dav/files/"
            f"{token}/Dados/Cadastros/CNPJ/",
            headers={"Depth": "1", "X-Requested-With": "XMLHttpRequest"},
        )

    raise AssertionError(json.dumps(diagnostics, ensure_ascii=False, indent=2))
