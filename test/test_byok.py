"""Tests for BYOK (Bring Your Own Key) session-based Kafka configuration."""

import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.server import AppContext, _kafka, _session_key


def _make_byok_ctx(app: AppContext):
    """Create a mock MCP context tied to a specific AppContext."""
    ctx = MagicMock()
    ctx.request_context.lifespan_context = app
    return ctx


class TestKafkaResolution(unittest.TestCase):
    """Test the _kafka() connector resolution logic."""

    def test_global_connector_returned_when_no_session(self):
        connector = MagicMock()
        app = AppContext(global_connector=connector)
        ctx = _make_byok_ctx(app)
        self.assertEqual(_kafka(ctx), connector)

    def test_session_connector_preferred_over_global(self):
        global_conn = MagicMock()
        session_conn = MagicMock()
        app = AppContext(global_connector=global_conn)
        ctx = _make_byok_ctx(app)
        key = _session_key(ctx)
        app.session_connectors[key] = session_conn
        self.assertEqual(_kafka(ctx), session_conn)

    def test_raises_when_no_connector_available(self):
        app = AppContext()  # no global, no session
        ctx = _make_byok_ctx(app)
        with self.assertRaises(ValueError) as cm:
            _kafka(ctx)
        self.assertIn("configure_kafka", str(cm.exception))

    def test_byok_mode_no_global(self):
        """When REQUIRE_BYOK is on, global_connector is None."""
        app = AppContext(global_connector=None)
        ctx = _make_byok_ctx(app)
        with self.assertRaises(ValueError):
            _kafka(ctx)


class TestConfigureKafkaTool(unittest.IsolatedAsyncioTestCase):
    """Test the configure_kafka tool."""

    @patch("src.server.KafkaConnector")
    async def test_configure_creates_session_connector(self, MockConnector):
        from src.server import configure_kafka

        mock_instance = MagicMock()
        MockConnector.return_value = mock_instance

        app = AppContext()
        ctx = _make_byok_ctx(app)
        result = await configure_kafka(ctx, "broker1:9092", "http://registry:8081")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["bootstrap_servers"], "broker1:9092")
        self.assertEqual(result["schema_registry_url"], "http://registry:8081")
        MockConnector.assert_called_once_with(
            bootstrap_servers="broker1:9092",
            schema_registry_url="http://registry:8081",
        )
        # Connector should be stored in session registry
        key = _session_key(ctx)
        self.assertEqual(app.session_connectors[key], mock_instance)

    @patch("src.server.KafkaConnector")
    async def test_configure_replaces_existing(self, MockConnector):
        from src.server import configure_kafka

        old_conn = AsyncMock()
        app = AppContext()
        ctx = _make_byok_ctx(app)
        key = _session_key(ctx)
        app.session_connectors[key] = old_conn

        new_conn = MagicMock()
        MockConnector.return_value = new_conn

        result = await configure_kafka(ctx, "new-broker:9092")
        self.assertEqual(result["status"], "ok")
        old_conn.close.assert_called_once()
        self.assertEqual(app.session_connectors[key], new_conn)

    @patch("src.server.KafkaConnector")
    async def test_configure_without_schema_registry(self, MockConnector):
        from src.server import configure_kafka

        MockConnector.return_value = MagicMock()
        app = AppContext()
        ctx = _make_byok_ctx(app)
        result = await configure_kafka(ctx, "broker:9092")
        self.assertIsNone(result["schema_registry_url"])

    @patch("src.server.KafkaConnector")
    async def test_configure_old_close_error_handled(self, MockConnector):
        """Error closing old connector should not prevent new one from being set."""
        from src.server import configure_kafka

        old_conn = AsyncMock()
        old_conn.close.side_effect = Exception("close failed")
        app = AppContext()
        ctx = _make_byok_ctx(app)
        key = _session_key(ctx)
        app.session_connectors[key] = old_conn

        MockConnector.return_value = MagicMock()
        result = await configure_kafka(ctx, "broker:9092")
        self.assertEqual(result["status"], "ok")


class TestDisconnectKafkaTool(unittest.IsolatedAsyncioTestCase):
    """Test the disconnect_kafka tool."""

    async def test_disconnect_existing_session(self):
        from src.server import disconnect_kafka

        conn = AsyncMock()
        app = AppContext()
        ctx = _make_byok_ctx(app)
        key = _session_key(ctx)
        app.session_connectors[key] = conn

        result = await disconnect_kafka(ctx)
        self.assertEqual(result["status"], "ok")
        self.assertIn("closed", result["message"])
        conn.close.assert_called_once()
        self.assertNotIn(key, app.session_connectors)

    async def test_disconnect_no_session(self):
        from src.server import disconnect_kafka

        app = AppContext()
        ctx = _make_byok_ctx(app)
        result = await disconnect_kafka(ctx)
        self.assertEqual(result["status"], "ok")
        self.assertIn("No session", result["message"])


class TestMultipleSessions(unittest.IsolatedAsyncioTestCase):
    """Test that multiple sessions can coexist independently."""

    @patch("src.server.KafkaConnector")
    async def test_two_sessions_independent(self, MockConnector):
        from src.server import configure_kafka

        conn_a = MagicMock()
        conn_b = MagicMock()
        MockConnector.side_effect = [conn_a, conn_b]

        app = AppContext()
        ctx_a = _make_byok_ctx(app)
        ctx_b = _make_byok_ctx(app)
        # Ensure different session objects so they get different keys
        ctx_a.session = MagicMock()
        ctx_b.session = MagicMock()

        await configure_kafka(ctx_a, "broker-a:9092")
        await configure_kafka(ctx_b, "broker-b:9092")

        key_a = _session_key(ctx_a)
        key_b = _session_key(ctx_b)
        self.assertNotEqual(key_a, key_b)
        self.assertEqual(_kafka(ctx_a), conn_a)
        self.assertEqual(_kafka(ctx_b), conn_b)


class TestToolsWithBYOKSession(unittest.IsolatedAsyncioTestCase):
    """Test that tools use the session connector when BYOK is configured."""

    def test_tool_uses_session_connector(self):
        from src.server import get_topics

        session_conn = MagicMock()
        session_conn.get_topics.return_value = ["byok-topic"]
        app = AppContext(global_connector=MagicMock())
        ctx = _make_byok_ctx(app)
        key = _session_key(ctx)
        app.session_connectors[key] = session_conn

        result = get_topics(ctx)
        self.assertEqual(result, ["byok-topic"])
        session_conn.get_topics.assert_called_once()
        # Global connector should not have been called
        app.global_connector.get_topics.assert_not_called()


class TestLifespanCleanup(unittest.IsolatedAsyncioTestCase):
    """Test that server_lifespan cleans up session connectors."""

    @patch("src.server.REQUIRE_BYOK", True)
    async def test_lifespan_byok_mode(self):
        from src.server import server_lifespan

        mock_server = MagicMock()
        async with server_lifespan(mock_server) as app:
            self.assertIsNone(app.global_connector)
            # Simulate a session connector
            conn = AsyncMock()
            app.session_connectors[123] = conn
        # After exit, session connectors should be closed
        conn.close.assert_called_once()

    @patch("src.server.REQUIRE_BYOK", False)
    @patch("src.server.KafkaConnector")
    async def test_lifespan_global_mode(self, MockConnector):
        from src.server import server_lifespan

        mock_conn = AsyncMock()
        MockConnector.return_value = mock_conn
        mock_server = MagicMock()
        async with server_lifespan(mock_server) as app:
            self.assertEqual(app.global_connector, mock_conn)
        mock_conn.close.assert_called_once()

    @patch("src.server.REQUIRE_BYOK", True)
    async def test_lifespan_session_close_error_handled(self):
        from src.server import server_lifespan

        mock_server = MagicMock()
        async with server_lifespan(mock_server) as app:
            conn = AsyncMock()
            conn.close.side_effect = Exception("close fail")
            app.session_connectors[999] = conn
        # Should not raise, error is logged


class TestKafkaHeaderMiddleware(unittest.TestCase):
    """Test that KafkaHeaderMiddleware extracts headers to request state."""

    def test_extracts_all_headers(self):
        from starlette.testclient import TestClient
        from starlette.applications import Starlette
        from starlette.responses import JSONResponse
        from starlette.routing import Route
        from src.auth import KafkaHeaderMiddleware

        def _handler(request):
            config = getattr(request.state, "kafka_config", {})
            return JSONResponse(config)

        app = Starlette(routes=[Route("/", _handler)])
        app.add_middleware(KafkaHeaderMiddleware)
        client = TestClient(app)

        resp = client.get(
            "/",
            headers={
                "x-kafka-bootstrap-servers": "broker:9092",
                "x-kafka-api-key": "my-key",
                "x-kafka-api-secret": "my-secret",
                "x-schema-registry-url": "http://registry:8081",
            },
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["bootstrap_servers"], "broker:9092")
        self.assertEqual(data["sasl_api_key"], "my-key")
        self.assertEqual(data["sasl_api_secret"], "my-secret")
        self.assertEqual(data["schema_registry_url"], "http://registry:8081")

    def test_no_headers_no_state(self):
        from starlette.testclient import TestClient
        from starlette.applications import Starlette
        from starlette.responses import JSONResponse
        from starlette.routing import Route
        from src.auth import KafkaHeaderMiddleware

        def _handler(request):
            has_config = hasattr(request.state, "kafka_config")
            return JSONResponse({"has_config": has_config})

        app = Starlette(routes=[Route("/", _handler)])
        app.add_middleware(KafkaHeaderMiddleware)
        client = TestClient(app)

        resp = client.get("/")
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(resp.json()["has_config"])

    def test_partial_headers(self):
        from starlette.testclient import TestClient
        from starlette.applications import Starlette
        from starlette.responses import JSONResponse
        from starlette.routing import Route
        from src.auth import KafkaHeaderMiddleware

        def _handler(request):
            config = getattr(request.state, "kafka_config", {})
            return JSONResponse(config)

        app = Starlette(routes=[Route("/", _handler)])
        app.add_middleware(KafkaHeaderMiddleware)
        client = TestClient(app)

        resp = client.get("/", headers={"x-kafka-bootstrap-servers": "broker:9092"})
        data = resp.json()
        self.assertEqual(data["bootstrap_servers"], "broker:9092")
        self.assertNotIn("sasl_api_key", data)


if __name__ == "__main__":
    unittest.main()
