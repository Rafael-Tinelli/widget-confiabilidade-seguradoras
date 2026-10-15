from __future__ import annotations

from api.sources.receita_cnpj_bulk import _propfind


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
