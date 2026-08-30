from app import app


def test_healthz():
    client = app.test_client()
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "ok"


def test_version_present():
    client = app.test_client()
    resp = client.get("/version")
    assert resp.status_code == 200
    assert "version" in resp.get_json()


def test_home():
    client = app.test_client()
    resp = client.get("/")
    assert resp.status_code == 200
    assert "message" in resp.get_json()
