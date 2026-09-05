from pathlib import Path

MODAL = Path("widget-ui/src/InsurerProfileModal.jsx")


def test_market_identity_is_rendered_separately_from_related_insurer():
    modal = MODAL.read_text(encoding="utf-8")

    assert "identity.market_identity" in modal
    assert "marketIdentity?.public_label || 'Marca / identidade de mercado'" in modal
    assert "Pessoa jurídica da identidade de mercado" in modal
    assert "CNPJ da identidade de mercado" in modal
    assert "marketIdentity?.evidence" in modal
