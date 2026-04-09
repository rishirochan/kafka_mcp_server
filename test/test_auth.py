import os
import sys
import unittest
from unittest.mock import patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from starlette.testclient import TestClient
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from src.auth import APIKeyMiddleware


def _homepage(request):
    return JSONResponse({"ok": True})


def _make_app():
    app = Starlette(routes=[Route("/", _homepage)])
    app.add_middleware(APIKeyMiddleware)
    return app


class TestAPIKeyMiddleware(unittest.TestCase):
    @patch.dict(os.environ, {}, clear=False)
    def test_no_api_key_configured_allows_all(self):
        """When MCP_API_KEY is not set, all requests should pass."""
        os.environ.pop("MCP_API_KEY", None)
        client = TestClient(_make_app())
        resp = client.get("/")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"ok": True})

    @patch.dict(os.environ, {"MCP_API_KEY": "secret-key-123"})
    def test_valid_x_api_key_header(self):
        client = TestClient(_make_app())
        resp = client.get("/", headers={"x-api-key": "secret-key-123"})
        self.assertEqual(resp.status_code, 200)

    @patch.dict(os.environ, {"MCP_API_KEY": "secret-key-123"})
    def test_valid_bearer_token(self):
        client = TestClient(_make_app())
        resp = client.get("/", headers={"authorization": "Bearer secret-key-123"})
        self.assertEqual(resp.status_code, 200)

    @patch.dict(os.environ, {"MCP_API_KEY": "secret-key-123"})
    def test_missing_key_returns_401(self):
        client = TestClient(_make_app())
        resp = client.get("/")
        self.assertEqual(resp.status_code, 401)
        self.assertIn("error", resp.json())

    @patch.dict(os.environ, {"MCP_API_KEY": "secret-key-123"})
    def test_wrong_key_returns_401(self):
        client = TestClient(_make_app())
        resp = client.get("/", headers={"x-api-key": "wrong-key"})
        self.assertEqual(resp.status_code, 401)

    @patch.dict(os.environ, {"MCP_API_KEY": "secret-key-123"})
    def test_wrong_bearer_returns_401(self):
        client = TestClient(_make_app())
        resp = client.get("/", headers={"authorization": "Bearer wrong-key"})
        self.assertEqual(resp.status_code, 401)

    @patch.dict(os.environ, {"MCP_API_KEY": "secret-key-123"})
    def test_non_bearer_auth_header_returns_401(self):
        """Authorization header without 'Bearer ' prefix should fail."""
        client = TestClient(_make_app())
        resp = client.get("/", headers={"authorization": "Basic abc123"})
        self.assertEqual(resp.status_code, 401)


if __name__ == "__main__":
    unittest.main()
