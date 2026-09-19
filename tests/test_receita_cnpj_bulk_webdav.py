from __future__ import annotations

import requests

from api.sources.receita_cnpj_bulk import (
    _configure_official_session,
    _discover_public_share,
    _propfind,
)


class _Response:
    status_code = 207
    content = b"""<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:">
  <d:response>
    <d:href>/public.php/dav/files/token/2026-09/</d:href>
    <d:propstat>
      <d:prop>
        <d:displayname>2026-09</d:displayname>
        <d:resourcetype><d:collection/></d:resourcetype>
      </d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
  </d:response>
</d:multistatus>
"""


class _Session:
    def __init__(self) -> None:
        self.calls = []
        self.auth = ("legacy-token", "")
        self.cookies = requests.cookies.RequestsCookieJar()
        self.cookies.set("legacy", "cookie")

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return _Response()


def test_receita_public_webdav_propfind_sends_nextcloud_ajax_header() -> None:
    session = _Session()

    resources = _propfind(
        session,
        "https://arquivos.receitafederal.gov.br/public.php/dav/files/token/",
        retries=1,
    )

    assert resources[0]["name"] == "2026-09"
    method, _, kwargs = session.calls[0]
    assert method == "PROPFIND"
    assert kwargs["headers"]["Depth"] == "1"
    assert kwargs["headers"]["X-Requested-With"] == "XMLHttpRequest"


def test_receita_public_webdav_session_uses_current_public_share_contract() -> None:
    session = requests.Session()
    session.auth = ("legacy-token", "")
    session.cookies.set("legacy", "cookie")

    _configure_official_session(session)

    assert session.auth is None
    assert not session.cookies


def test_receita_public_webdav_propfind_clears_auth_and_cookies_before_request() -> None:
    session = _Session()

    _propfind(
        session,
        "https://arquivos.receitafederal.gov.br/public.php/dav/files/token/",
        retries=1,
    )

    assert session.auth is None
    assert not session.cookies


class _RedirectResponse:
    url = "https://arquivos.receitafederal.gov.br/index.php/s/currentShareToken"

    @staticmethod
    def raise_for_status() -> None:
        return None


class _RedirectSession:
    def __init__(self) -> None:
        self.auth = ("legacy-token", "")
        self.cookies = requests.cookies.RequestsCookieJar()
        self.cookies.set("legacy", "cookie")

    def get(self, url, **kwargs):
        assert url == "https://arquivos.receitafederal.gov.br/"
        assert kwargs["allow_redirects"] is True
        return _RedirectResponse()


def test_receita_public_share_is_discovered_from_current_root_redirect() -> None:
    share_url, dav_root = _discover_public_share(_RedirectSession())

    assert share_url.endswith("/index.php/s/currentShareToken")
    assert dav_root.endswith(
        "/public.php/dav/files/currentShareToken/Dados/Cadastros/CNPJ/"
    )
