"""Deterministic text decoding for legacy Consumer.gov CSV readers."""

from __future__ import annotations

import codecs
from pathlib import Path


def detect_csv_encoding(path: Path) -> str:
    """Choose the encoding before any consumer can emit a row.

    Reading and restarting a CSV generator after a late UTF-8 decode error
    would emit some rows twice. Validate UTF-8 over the complete file in a
    streaming pass, then choose either UTF-8 (BOM-aware) or legacy Latin-1.
    A UTF-8 BOM declares UTF-8 and must not silently fall back to Latin-1.
    """
    with path.open("rb") as handle:
        has_bom = handle.read(3) == codecs.BOM_UTF8
        handle.seek(0)
        decoder = codecs.getincrementaldecoder("utf-8-sig")("strict")
        try:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                decoder.decode(chunk)
            decoder.decode(b"", final=True)
        except UnicodeDecodeError:
            if has_bom:
                raise
            return "latin1"
    return "utf-8-sig"
