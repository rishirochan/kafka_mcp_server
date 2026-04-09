import sys
import os
import unittest
from unittest.mock import AsyncMock, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.server import AppContext


def _make_ctx(schema_registry=None, connector=None):
    """Create a mock MCP context wired to an AppContext with a global connector."""
    ctx = MagicMock()
    if connector is None:
        connector = MagicMock()
    connector.schema_registry = schema_registry
    app = AppContext(global_connector=connector)
    ctx.request_context.lifespan_context = app
    # _session_key uses id(ctx.session), so session_connectors won't match
    # and _kafka() will fall through to global_connector.
    return ctx, connector


class TestSchemaToolGuards(unittest.IsolatedAsyncioTestCase):
    """Test that schema registry tools return proper errors when registry is not configured."""

    def test_list_schemas_no_registry(self):
        from src.server import list_schemas
        ctx, _ = _make_ctx(schema_registry=None)
        result = list_schemas(ctx)
        self.assertEqual(result["status"], "error")
        self.assertIn("SCHEMA_REGISTRY_URL", result["message"])

    def test_get_schema_no_registry(self):
        from src.server import get_schema
        ctx, _ = _make_ctx(schema_registry=None)
        result = get_schema(ctx, "test-value")
        self.assertEqual(result["status"], "error")

    def test_register_schema_no_registry(self):
        from src.server import register_schema
        ctx, _ = _make_ctx(schema_registry=None)
        result = register_schema(ctx, "test-value", '{"type":"string"}')
        self.assertEqual(result["status"], "error")

    async def test_delete_schema_no_registry(self):
        from src.server import delete_schema
        ctx, _ = _make_ctx(schema_registry=None)
        ctx.elicit = AsyncMock()
        result = await delete_schema(ctx, "test-value")
        self.assertEqual(result["status"], "error")
        # Elicitation should not be called when registry is not configured
        ctx.elicit.assert_not_called()

    def test_list_schemas_with_registry(self):
        from src.server import list_schemas
        mock_registry = MagicMock()
        mock_registry.get_subjects.return_value = ["topic1-value"]
        ctx, _ = _make_ctx(schema_registry=mock_registry)
        result = list_schemas(ctx)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["subjects"], ["topic1-value"])

    def test_get_schema_with_registry(self):
        from src.server import get_schema
        mock_registry = MagicMock()
        mock_registry.get_schema.return_value = {
            "schema_id": 1,
            "schema_str": "{}",
            "schema_type": "AVRO",
            "version": 1,
            "subject": "t-value",
        }
        ctx, _ = _make_ctx(schema_registry=mock_registry)
        result = get_schema(ctx, "t-value")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["schema_id"], 1)

    def test_register_schema_with_registry(self):
        from src.server import register_schema
        mock_registry = MagicMock()
        mock_registry.register_schema.return_value = 42
        ctx, _ = _make_ctx(schema_registry=mock_registry)
        result = register_schema(ctx, "t-value", '{"type":"string"}')
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["schema_id"], 42)

    async def test_delete_schema_with_registry(self):
        from src.server import delete_schema
        mock_registry = MagicMock()
        mock_registry.delete_subject.return_value = [1, 2]
        ctx, _ = _make_ctx(schema_registry=mock_registry)
        # Simulate a client that does not support elicitation (raises exception)
        ctx.elicit = AsyncMock(side_effect=Exception("elicitation not supported"))
        result = await delete_schema(ctx, "t-value")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["deleted_versions"], [1, 2])


class TestToolInputValidation(unittest.IsolatedAsyncioTestCase):
    """Test that tools reject invalid input before reaching KafkaConnector."""

    def test_describe_topic_rejects_empty_name(self):
        from src.server import describe_topic
        ctx, _ = _make_ctx()
        with self.assertRaises(ValueError):
            describe_topic(ctx, "")

    def test_describe_topic_rejects_invalid_chars(self):
        from src.server import describe_topic
        ctx, _ = _make_ctx()
        with self.assertRaises(ValueError):
            describe_topic(ctx, "topic with spaces")

    def test_create_topic_rejects_long_name(self):
        from src.server import create_topic
        ctx, _ = _make_ctx()
        with self.assertRaises(ValueError):
            create_topic(ctx, "a" * 250)

    async def test_publish_rejects_oversized_message(self):
        from src.server import publish
        ctx, _ = _make_ctx()
        ctx.log = AsyncMock()
        with self.assertRaises(ValueError):
            await publish(ctx, "valid-topic", "x" * 1_048_577)

    def test_register_schema_rejects_invalid_json(self):
        from src.server import register_schema
        ctx, _ = _make_ctx()
        with self.assertRaises(ValueError):
            register_schema(ctx, "test-value", "not valid json")


class TestToolHappyPaths(unittest.IsolatedAsyncioTestCase):
    """Test that tools delegate correctly to KafkaConnector on valid input."""

    def test_get_topics(self):
        from src.server import get_topics
        ctx, connector = _make_ctx()
        connector.get_topics.return_value = ["topic-a", "topic-b"]
        result = get_topics(ctx)
        self.assertEqual(result, ["topic-a", "topic-b"])

    def test_describe_topic(self):
        from src.server import describe_topic
        ctx, connector = _make_ctx()
        connector.describe_topic.return_value = {"topic": "my-topic", "partitions": []}
        result = describe_topic(ctx, "my-topic")
        self.assertEqual(result["topic"], "my-topic")
        connector.describe_topic.assert_called_once_with("my-topic")

    def test_get_partitions(self):
        from src.server import get_partitions
        ctx, connector = _make_ctx()
        connector.get_partitions.return_value = {"partitions_count": 3}
        result = get_partitions(ctx, "my-topic")
        self.assertEqual(result["partitions_count"], 3)
        connector.get_partitions.assert_called_once_with("my-topic")

    def test_is_topic_exists_true(self):
        from src.server import is_topic_exists
        ctx, connector = _make_ctx()
        connector.is_topic_exists.return_value = True
        self.assertTrue(is_topic_exists(ctx, "my-topic"))

    def test_is_topic_exists_false(self):
        from src.server import is_topic_exists
        ctx, connector = _make_ctx()
        connector.is_topic_exists.return_value = False
        self.assertFalse(is_topic_exists(ctx, "my-topic"))

    def test_create_topic_delegates(self):
        from src.server import create_topic
        ctx, connector = _make_ctx()
        connector.create_topic.return_value = True
        result = create_topic(ctx, "new-topic", 3, 1, {"retention.ms": "1000"})
        self.assertTrue(result)
        connector.create_topic.assert_called_once_with("new-topic", 3, 1, {"retention.ms": "1000"})

    async def test_delete_topic_confirmed(self):
        from src.server import delete_topic
        ctx, connector = _make_ctx()
        connector.delete_topic.return_value = True
        elicit_result = MagicMock()
        elicit_result.action = "accept"
        elicit_result.content = {"confirmed": True}
        ctx.elicit = AsyncMock(return_value=elicit_result)
        result = await delete_topic(ctx, "doomed-topic")
        self.assertTrue(result)
        connector.delete_topic.assert_called_once_with("doomed-topic")

    async def test_delete_topic_cancelled(self):
        from src.server import delete_topic
        ctx, connector = _make_ctx()
        elicit_result = MagicMock()
        elicit_result.action = "accept"
        elicit_result.content = {"confirmed": False}
        ctx.elicit = AsyncMock(return_value=elicit_result)
        result = await delete_topic(ctx, "doomed-topic")
        self.assertEqual(result["status"], "cancelled")
        connector.delete_topic.assert_not_called()

    async def test_delete_topic_elicitation_rejected(self):
        from src.server import delete_topic
        ctx, connector = _make_ctx()
        elicit_result = MagicMock()
        elicit_result.action = "reject"
        elicit_result.content = {}
        ctx.elicit = AsyncMock(return_value=elicit_result)
        result = await delete_topic(ctx, "doomed-topic")
        self.assertEqual(result["status"], "cancelled")

    async def test_delete_topic_elicitation_unsupported(self):
        """When client doesn't support elicitation, deletion proceeds."""
        from src.server import delete_topic
        ctx, connector = _make_ctx()
        connector.delete_topic.return_value = True
        ctx.elicit = AsyncMock(side_effect=Exception("not supported"))
        result = await delete_topic(ctx, "doomed-topic")
        self.assertTrue(result)

    async def test_publish_delegates(self):
        from src.server import publish
        ctx, connector = _make_ctx()
        connector.publish = AsyncMock(return_value={"status": "ok", "offset": 42})
        ctx.log = AsyncMock()
        result = await publish(ctx, "my-topic", "hello", "k1", "sess1", "AVRO")
        self.assertEqual(result["status"], "ok")
        connector.publish.assert_called_once_with("my-topic", "hello", "k1", "sess1", "AVRO")

    async def test_consume_delegates(self):
        from src.server import consume
        ctx, connector = _make_ctx()
        connector.consume = AsyncMock(return_value=["msg1", "msg2"])
        ctx.log = AsyncMock()
        result = await consume(ctx, "my-topic", "grp", "sess1", 5)
        self.assertEqual(result, ["msg1", "msg2"])
        connector.consume.assert_called_once_with("my-topic", "grp", "sess1", 5)

    async def test_delete_schema_confirmed(self):
        from src.server import delete_schema
        mock_registry = MagicMock()
        mock_registry.delete_subject.return_value = [1, 2, 3]
        ctx, _ = _make_ctx(schema_registry=mock_registry)
        elicit_result = MagicMock()
        elicit_result.action = "accept"
        elicit_result.content = {"confirmed": True}
        ctx.elicit = AsyncMock(return_value=elicit_result)
        result = await delete_schema(ctx, "t-value")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["deleted_versions"], [1, 2, 3])

    async def test_delete_schema_cancelled(self):
        from src.server import delete_schema
        mock_registry = MagicMock()
        ctx, _ = _make_ctx(schema_registry=mock_registry)
        elicit_result = MagicMock()
        elicit_result.action = "accept"
        elicit_result.content = {"confirmed": False}
        ctx.elicit = AsyncMock(return_value=elicit_result)
        result = await delete_schema(ctx, "t-value")
        self.assertEqual(result["status"], "cancelled")
        mock_registry.delete_subject.assert_not_called()


class TestPrompts(unittest.TestCase):
    """Test that prompt functions return proper message structure."""

    def test_inspect_topic_prompt(self):
        from src.server import prompt_inspect_topic
        result = prompt_inspect_topic("my-topic")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["role"], "user")
        self.assertIn("my-topic", result[0]["content"])

    def test_register_and_publish_prompt(self):
        from src.server import prompt_register_and_publish
        result = prompt_register_and_publish("t1", '{"type":"string"}', '{"name":"Alice"}')
        self.assertEqual(len(result), 1)
        self.assertIn("t1", result[0]["content"])
        self.assertIn('{"type":"string"}', result[0]["content"])
        self.assertIn('{"name":"Alice"}', result[0]["content"])


if __name__ == "__main__":
    unittest.main()
