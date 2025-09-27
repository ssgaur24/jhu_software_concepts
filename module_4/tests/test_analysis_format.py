# module_4/tests/test_analysis_format.py
"""
Analysis formatting tests for Module 4.
Checks that:
- Each analysis line includes an 'Answer:' label
- Any percentages shown are formatted with exactly two decimals (e.g., '12.34%')
We drive the page with fake rows (from conftest.fake_get_rows) so no DB is touched.
"""

import re
import pytest


# Replace the regex in both tests:
# from:
#   rb"\b\d+\.\d{2}%\b"
# to:
PERCENT_2DP = rb"\d+\.\d{2}%"

@pytest.mark.analysis
def test_answers_have_label_and_two_decimal_percentages_single_row(client, fake_get_rows):
    """
    Minimal case: one analysis row that contains a percentage with two decimals.
    """
    import re
    fake_get_rows.set([("Q: Admit rate?", "Answer: 12.34%")])

    resp = client.get("/")
    body = resp.data
    assert resp.status_code == 200
    assert b"Answer:" in body
    assert re.search(PERCENT_2DP, body) is not None


@pytest.mark.analysis
def test_answers_multiple_rows_each_with_two_decimal_percentages(client, fake_get_rows):
    """
    Multiple rows scenario: ensure every percentage we show follows the two-decimal rule.
    """
    import re
    fake_get_rows.set([
        ("Q1: Admit rate?", "Answer: 7.00%"),
        ("Q2: Yield rate?", "Answer: 53.10%"),
        ("Q3: International share?", "Answer: 0.25%"),
    ])

    resp = client.get("/")
    body = resp.data
    assert resp.status_code == 200

    matches = re.findall(PERCENT_2DP, body)
    assert len(matches) == 3, "Every shown percentage must have exactly two decimals"
    assert body.count(b"Answer:") == 3