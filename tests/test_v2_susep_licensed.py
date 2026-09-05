from api.sources.susep_licensed import parse_licensed_entities_html


def test_parse_insurer_result_table():
    document = """
    <html><body>
      <table>
        <tr><td>88I SEGURADORA DIGITAL S.A.</td></tr>
        <tr><td>CNPJ: 29.846.286/0001-02</td></tr>
        <tr><td>Código FIP: 04120</td></tr>
        <tr><td>Endereço: AVENIDA TESTE</td></tr>
      </table>
    </body></html>
    """

    records = parse_licensed_entities_html(document, "2")

    assert records == [
        {
            "fip_code": "004120",
            "cnpj": "29846286000102",
            "legal_name": "88I SEGURADORA DIGITAL S.A.",
            "entity_type": "insurer",
            "source_type_code": "2",
            "source": "https://www2.susep.gov.br/menuatendimento/procura_2011.asp",
        }
    ]


def test_parse_occasional_reinsurer_without_cnpj():
    document = """
    <table>
      <tr><td>ACE PROPERTY AND CASUALTY INSURANCE COMPANY</td></tr>
      <tr><td>CNPJ: ../-</td></tr>
      <tr><td>Código FIP: 54666</td></tr>
    </table>
    """

    records = parse_licensed_entities_html(document, "7")

    assert records[0]["fip_code"] == "054666"
    assert records[0]["cnpj"] is None
    assert records[0]["entity_type"] == "occasional_reinsurer"


def test_parser_ignores_layout_tables_without_fip():
    document = """
    <table><tr><td>Menu</td></tr></table>
    <table>
      <tr><td>APLICAP CAPITALIZAÇÃO S.A</td></tr>
      <tr><td>CNPJ: 13.122.801/0001-71</td></tr>
      <tr><td>Código FIP: 24813</td></tr>
    </table>
    """

    records = parse_licensed_entities_html(document, "6")

    assert len(records) == 1
    assert records[0]["entity_type"] == "capitalization_company"
    assert records[0]["fip_code"] == "024813"
