from __future__ import annotations

import zipfile
from pathlib import Path

import requests

from api.sources.receita_cnpj_bulk import (
    ReceitaOpenDataRelease,
    _configure_official_session,
    _discover_public_share,
    _download_zip,
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


def test_release_fingerprint_changes_when_same_period_etag_changes() -> None:
    first = ReceitaOpenDataRelease(
        base_url="https://arquivos.receitafederal.gov.br/index.php/s/current",
        release_url="https://arquivos.receitafederal.gov.br/public.php/dav/files/current/Dados/Cadastros/CNPJ/2026-09/",
        period="2026-09",
        establishment_files=("Estabelecimentos0.zip",),
        resource_metadata=(
            {
                "name": "Estabelecimentos0.zip",
                "size": 123,
                "etag": '"aaa"',
                "last_modified": "Mon, 14 Sep 2026 15:02:59 GMT",
            },
        ),
    )
    changed = ReceitaOpenDataRelease(
        base_url=first.base_url,
        release_url=first.release_url,
        period=first.period,
        establishment_files=first.establishment_files,
        resource_metadata=(
            {
                "name": "Estabelecimentos0.zip",
                "size": 123,
                "etag": '"bbb"',
                "last_modified": "Mon, 14 Sep 2026 15:02:59 GMT",
            },
        ),
    )

    assert release_fingerprint(first) != release_fingerprint(changed)


def test_download_zip_uses_validated_local_mirror_without_http(
    tmp_path: Path,
    monkeypatch,
) -> None:
    mirror = tmp_path / "mirror"
    mirror.mkdir()
    source = mirror / "Motivos.zip"
    with zipfile.ZipFile(source, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("Motivos.csv", "00;SEM MOTIVO\n")

    class _NoNetwork:
        def get(self, *args, **kwargs):
            raise AssertionError("HTTP must not be used when local mirror is configured")

    monkeypatch.setenv("V2_RECEITA_LOCAL_MIRROR_DIR", str(mirror))
    destination = tmp_path / "work" / "Motivos.zip"
    result = _download_zip(
        _NoNetwork(),
        "https://arquivos.receitafederal.gov.br/public.php/dav/files/token/Dados/Cadastros/CNPJ/2026-09/Motivos.zip",
        destination,
    )

    assert result == destination
    assert zipfile.is_zipfile(destination)
    assert destination.read_bytes() == source.read_bytes()
