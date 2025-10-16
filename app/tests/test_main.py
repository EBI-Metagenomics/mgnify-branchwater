from flask import Flask
from google.auth.transport import urllib3

from app.main import check_health


# test for /health endpoint
def test_health_route(monkeypatch):
    import types

    class DummyResponse:
        def __init__(self, status, data=b''):
            self.status = status
            self.data = data

    def dummy_request(self, method, url, headers=None):
        return DummyResponse(200)

    monkeypatch.setattr(urllib3.PoolManager, "request", dummy_request)

    app = Flask(__name__)
    app.route('/health', methods=["GET"])(check_health)
    client = app.test_client()
    response = client.get('/health')
    assert response.status_code == 200
    assert response.get_json() == {'status': 'ok'}