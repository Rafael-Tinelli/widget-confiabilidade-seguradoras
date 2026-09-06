from api.sources.consumer_gov_direct import Publication
from api.v2.complaint_source_watch import (
    BDR_GUIDANCE_URL,
    SUSEPCON_FREEZE_MARKER,
    evaluate_bdr_html,
    evaluate_consumer_ckan,
    evaluate_consumer_publications,
    evaluate_susepcon_html,
)


def test_consumer_ckan_requires_usable_resources() -> None:
    assert evaluate_consumer_ckan({"success": True, "result": {"resources": []}}) is None

    event = evaluate_consumer_ckan(
        {
            "success": True,
            "result": {
                "title": "Reclamações do Consumidor.gov.br",
                "metadata_modified": "2026-09-06T10:00:00",
                "resources": [{"url": "https://dados.mj.gov.br/resource.csv"}],
            },
        }
    )
    assert event is not None
    assert event.key == "consumer-gov-ckan-operational"
    assert event.title == "Fontes de Reclamações Disponíveis — Consumer.gov"


def test_consumer_publications_only_alert_for_new_explicit_base_completa() -> None:
    generic = Publication(
        code="202609010000",
        title="Dados Setembro 2026",
        filename="dados_2026-09.csv",
        published_at=None,
        month="2026-09",
        discovery_method="test",
    )
    old_complete = Publication(
        code="202606010000",
        title="Base Completa Junho 2026",
        filename="basecompleta_2026-06.csv",
        published_at=None,
        month="2026-06",
        discovery_method="test",
    )
    assert evaluate_consumer_publications([generic, old_complete]) is None

    new_complete = Publication(
        code="202607010000",
        title="Base Completa Julho 2026",
        filename="basecompleta_2026-07.csv",
        published_at=None,
        month="2026-07",
        discovery_method="test",
    )
    event = evaluate_consumer_publications([generic, old_complete, new_complete])
    assert event is not None
    assert event.evidence["newer_months"] == ["2026-07"]


def test_susepcon_freeze_notice_suppresses_notification() -> None:
    frozen = f"<html><body>SusepCon Reclamações {SUSEPCON_FREEZE_MARKER}</body></html>"
    assert evaluate_susepcon_html(frozen) is None


def test_susepcon_notice_removal_is_material_change() -> None:
    event = evaluate_susepcon_html(
        "<html><body>SusepCon - Painel e Ranking de Reclamações atualizado.</body></html>"
    )
    assert event is not None
    assert event.key == "susepcon-no-longer-frozen-at-2025-q4"


def test_bdr_ignores_schema_manual_and_existing_susepcon() -> None:
    html = """
    <html><body>
      <a href="https://www2.susep.gov.br/download/bdr/JsonSchema.json">Arquivo schema BDR</a>
      <a href="/manual-bdr.pdf">Manual BDR</a>
      <a href="https://www.gov.br/susep/pt-br/central-de-conteudos/central-de-paineis/painel-susepcon">Painel de reclamações SusepCon</a>
    </body></html>
    """
    assert evaluate_bdr_html([(BDR_GUIDANCE_URL, html)]) is None


def test_bdr_public_data_candidate_creates_stable_event() -> None:
    html = """
    <html><body>
      <a href="https://www.gov.br/susep/dados-abertos/bdr">Dados abertos BDR</a>
    </body></html>
    """
    first = evaluate_bdr_html([(BDR_GUIDANCE_URL, html)])
    second = evaluate_bdr_html([(BDR_GUIDANCE_URL, html)])
    assert first is not None
    assert second is not None
    assert first.key == second.key
    assert first.title == "Fontes de Reclamações Disponíveis — BDR/SUSEP"
