from api.sources.susep_sandbox import parse_sandbox_participants_html


def test_parse_sandbox_distinguishes_authorized_and_cancelled():
    document = """
    <h2>1ª edição do Sandbox</h2>
    <p>Coover Seguradora S.A.</p>
    <p>STATUS | Autorização temporária cancelada, conforme Portaria</p>
    <p>CNPJ | 39.768.897/0001-33</p>
    <p>DATA DE INICIO DA AUTORIZAÇÃO TEMPORÁRIA | 01/03/2021</p>
    <p>DATA FINAL DA AUTORIZAÇÃO TEMPORÁRIA | 28/05/2024</p>
    <p>MODALIDADES | Seguros de Danos</p>
    <h2>2ª edição do Sandbox</h2>
    <p>Clubfix Seguradora S.A.</p>
    <p>STATUS | Autorizada (Autorização prorrogada em razão do artigo 35-A)</p>
    <p>CNPJ | 43.095.690/0001-12</p>
    <p>DATA DE INICIO DA AUTORIZAÇÃO TEMPORÁRIA | 21/03/2023</p>
    <p>DATA FINAL DA AUTORIZAÇÃO TEMPORÁRIA | 19/05/2026 (Autorização prorrogada)</p>
    <p>MODALIDADES | Seguros de Danos e Pessoas</p>
    """

    records = parse_sandbox_participants_html(document)

    assert len(records) == 2
    coover = next(item for item in records if item["cnpj"] == "39768897000133")
    clubfix = next(item for item in records if item["cnpj"] == "43095690000112")

    assert coover["edition"] == "1ª edição do Sandbox"
    assert coover["regulatory_status"] == "sandbox_authorization_cancelled"
    assert coover["authorization_end"] == "2024-05-28"

    assert clubfix["edition"] == "2ª edição do Sandbox"
    assert clubfix["regulatory_status"] == "temporary_authorized"
    assert clubfix["authorization_start"] == "2023-03-21"
    assert clubfix["authorization_end"] == "2026-05-19"
    assert "prorrogada" in clubfix["authorization_end_raw"].casefold()


def test_parse_sandbox_split_table_cells_like_current_gov_renderer():
    document = """
    <h2>2ª edição do Sandbox</h2>
    <p>Novo Seguros S.A.</p>
    <table>
      <tr><td>STATUS</td><td>|</td><td>Autorizada</td></tr>
      <tr><td>CNPJ</td><td>|</td><td>50.182.327/0001-08</td></tr>
      <tr><td>DATA DE INICIO DA AUTORIZAÇÃO TEMPORÁRIA</td><td>|</td><td>31/10/2023</td></tr>
      <tr><td>DATA FINAL DA AUTORIZAÇÃO TEMPORÁRIA</td><td>|</td><td>30/10/2026</td></tr>
      <tr><td>MODALIDADES</td><td>|</td><td>Seguros de Danos</td></tr>
    </table>
    """

    record = parse_sandbox_participants_html(document)[0]

    assert record["legal_name"] == "Novo Seguros S.A."
    assert record["cnpj"] == "50182327000108"
    assert record["regulatory_status"] == "temporary_authorized"
    assert record["authorization_start"] == "2023-10-31"
    assert record["authorization_end"] == "2026-10-30"


def test_status_text_not_date_decides_current_authorization():
    document = """
    <h2>2ª edição do Sandbox</h2>
    <p>Kakau Seguradora S.A.</p>
    <p>STATUS | Autorizada (Autorização prorrogada)</p>
    <p>CNPJ | 43.409.064/0001-53</p>
    <p>DATA FINAL DA AUTORIZAÇÃO TEMPORÁRIA | 16/12/2025 (Autorização prorrogada)</p>
    """

    record = parse_sandbox_participants_html(document)[0]

    assert record["authorization_end"] == "2025-12-16"
    assert record["regulatory_status"] == "temporary_authorized"
