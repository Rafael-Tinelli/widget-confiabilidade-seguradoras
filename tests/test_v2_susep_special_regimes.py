from api.sources.susep_special_regimes import parse_special_regime_html


def test_parse_special_regime_assigns_type_from_official_section():
    document = """
    <html><body>
      <div>I. SOCIEDADES SEGURADORAS</div>
      <a href="info_empresa_2011.asp?codempresa=03948">SEGURADORA SA INFINITE</a>
      <div>II. ENTIDADES DE PREVIDÊNCIA COMPLEMENTAR ABERTA</div>
      <a href="info_empresa_2011.asp?codempresa=1147">FEDERAL VIDA</a>
      <div>III. SOCIEDADES DE CAPITALIZAÇÃO</div>
      <a href="info_empresa_2011.asp?codempresa=20826">CAP EXEMPLO</a>
    </body></html>
    """

    records = parse_special_regime_html(document, "extrajudicial_liquidation")

    assert records == [
        {
            "fip_code": "003948",
            "legal_name": "SEGURADORA SA INFINITE",
            "entity_type": "insurer",
            "regulatory_regime": "special",
            "regulatory_status": "extrajudicial_liquidation",
            "source": "https://www2.susep.gov.br/menuatendimento/regimesespeciais/liq_extrajudicial_2011.asp",
        },
        {
            "fip_code": "001147",
            "legal_name": "FEDERAL VIDA",
            "entity_type": "open_pension_entity",
            "regulatory_regime": "special",
            "regulatory_status": "extrajudicial_liquidation",
            "source": "https://www2.susep.gov.br/menuatendimento/regimesespeciais/liq_extrajudicial_2011.asp",
        },
        {
            "fip_code": "020826",
            "legal_name": "CAP EXEMPLO",
            "entity_type": "capitalization_company",
            "regulatory_regime": "special",
            "regulatory_status": "extrajudicial_liquidation",
            "source": "https://www2.susep.gov.br/menuatendimento/regimesespeciais/liq_extrajudicial_2011.asp",
        },
    ]


def test_bankruptcy_without_sections_keeps_type_unknown():
    document = """
    <a href="info_empresa_2011.asp?codempresa=01147">FEDERAL VIDA E PREVIDÊNCIA S.A.</a>
    """

    record = parse_special_regime_html(document, "bankruptcy")[0]

    assert record["fip_code"] == "001147"
    assert record["entity_type"] == "unknown"
    assert record["regulatory_status"] == "bankruptcy"
