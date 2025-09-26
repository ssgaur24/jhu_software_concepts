"""import re
import pytest
from bs4 import BeautifulSoup

@pytest.mark.analysis
def test_answer_label_and_percent_two_decimals(monkeypatch, client):
    # GIVEN: patch the exact functions used by /analysis -> _build_rows()
    monkeypatch.setattr("src.flask_app.q2_pct_international", lambda: 12.3)
    monkeypatch.setattr("src.flask_app.q5_pct_accept_fall2025", lambda: 45.678)

    # WHEN
    resp = client.get("/analysis")
    assert resp.status_code == 200

    soup = BeautifulSoup(resp.data, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    # THEN: at least one “Answer:” appears
    assert "Answer:" in page_text

    # THEN: all percentages that appear use exactly two decimals
    found = re.findall(r"\b\d+\.\d{2}%\b", page_text)
    assert "12.30%" in page_text
    assert "45.68%" in page_text
    # Optional stricter check: if any % symbol appears, ensure it matches two-decimal pattern
    all_perc = re.findall(r"\b\d+(\.\d+)?%\b", page_text)
    assert len(found) == len(all_perc)

"""