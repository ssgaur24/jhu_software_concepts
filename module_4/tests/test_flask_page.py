# test_flask_page_basic.py
# Goal: verify the Flask page loads and renders simple Q/A rows.
# Uses fake_get_rows (bypass SQL) and client (Flask test client).

def test_home_renders_rows(client, fake_get_rows):
    # Arrange: provide two simple rows for the page to render
    fake_get_rows.set([("Q1: Applicants?", "Answer: 10"),
                       ("Q2: Admit %", "Answer: 12.34%")])

    # Act
    resp = client.get("/")

    # Assert
    assert resp.status_code == 200
    body = resp.data
    assert b"Q1: Applicants?" in body
    assert b"Answer: 10" in body
    assert b"Q2: Admit %" in body
    assert b"12.34%" in body

def test_status_elements_present(client, fake_get_rows):
    # Keep rows minimal; we’re checking the status scaffolding
    fake_get_rows.set([("Q", "A")])

    resp = client.get("/")
    assert resp.status_code == 200
    # These come from the patched render_template in conftest
    assert b"id='status'" in resp.data
    assert b"id='pull_running'" in resp.data
    assert b"id='report_exists'" in resp.data

def test_health_endpoint(client):
    # Optional but common: if your app has /health, ensure it works.
    resp = client.get("/health")
    # If your app doesn't have /health, comment this test out.
    assert resp.status_code in (200, 204)
