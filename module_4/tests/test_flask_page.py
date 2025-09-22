import pytest
from bs4 import BeautifulSoup

@pytest.mark.web
def test_analysis_page_loads_and_has_required_elements(client):
    resp = client.get("/analysis")
    assert resp.status_code == 200

    soup = BeautifulSoup(resp.data, "html.parser")
    # Title / header
    assert soup.find("h1") and "Analysis" in soup.find("h1").get_text()
    # Buttons by stable selectors (SHALL)
    assert soup.select_one('[data-testid="pull-data-btn"]') is not None
    assert soup.select_one('[data-testid="update-analysis-btn"]') is not None
    # At least one “Answer:” label on page (SHALL)
    assert any("Answer:" in el.get_text() for el in soup.select(".answer-item"))
