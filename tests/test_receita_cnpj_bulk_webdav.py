from __future__ import annotations

import requests

from api.sources.receita_cnpj_bulk import (
    ReceitaOpenDataRelease,
    _configure_official_session,
    _discover_public_share,
    _propfind,
    release_fingerprint,
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


def _fingerprint_release(etag: str, size: int = 1180) -> ReceitaOpenDataRelease:
    return ReceitaOpenDataRelease(
        base_url="https://arquivos.receitafederal.gov.br/index.php/s/current",
        release_url=(
            "https://arquivos.receitafederal.gov.br/public.php/dav/files/current/"
            "Dados/Cadastros/CNPJ/2026-09/"
        ),
        period="2026-09",
        establishment_files=("Estabelecimentos0.zip",),
        resource_metadata=(
            {
                "name": "Motivos.zip",
                "size": size,
                "last_modified": "Mon, 14 Sep 2026 15:06:40 GMT",
                "etag": etag,
            },
            {
                "name": "Estabelecimentos0.zip",
                "size": 2243389254,
                "last_modified": "Mon, 14 Sep 2026 15:02:59 GMT",
                "etag": '"d4efe5ace49c8948c4946b5978a62559"',
            },
            {
                "name": "Empresas0.zip",
                "size": 563070974,
                "last_modified": "Mon, 14 Sep 2026 15:03:00 GMT",
                "etag": '"empresas-etag"',
            },
            {
                "name": "Cnaes.zip",
                "size": 22078,
                "last_modified": "Mon, 14 Sep 2026 15:03:01 GMT",
                "etag": '"cnaes-etag"',
            },
            {
                "name": "Naturezas.zip",
                "size": 1563,
                "last_modified": "Mon, 14 Sep 2026 15:03:02 GMT",
                "etag": '"naturezas-etag"',
            },
        ),
    )


def test_receita_release_fingerprint_detects_same_period_etag_change() -> None:
    first = release_fingerprint(
        _fingerprint_release('"61c9b2d879ce0e232d4e2cb589d9abba"'),
        purpose="lifecycle",
    )
    changed = release_fingerprint(
        _fingerprint_release('"changed-etag-same-period"'),
        purpose="lifecycle",
    )

    assert first != changed


def test_receita_identity_fingerprint_includes_company_and_dictionaries() -> None:
    release = _fingerprint_release('"motivos"')
    first = release_fingerprint(release, purpose="identity")
    metadata = list(release.resource_metadata)
    metadata[2] = {**metadata[2], "etag": '"changed-company-etag"'}
    changed_release = ReceitaOpenDataRelease(
        base_url=release.base_url,
        release_url=release.release_url,
        period=release.period,
        establishment_files=release.establishment_files,
        resource_metadata=tuple(metadata),
    )

    assert first != release_fingerprint(changed_release, purpose="identity")
