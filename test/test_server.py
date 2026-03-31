import unittest
from unittest.mock import MagicMock
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class TestSchemaToolGuards(unittest.TestCase):
    """Test that schema registry tools return proper errors when registry is not configured."""

    def _make_ctx(self, schema_registry=None):
        """Create a mock MCP context with a KafkaConnector that has the given schema_registry."""
        ctx = MagicMock()
        connector = MagicMock()
        connector.schema_registry = schema_registry
        ctx.request_context.lifespan_context.kafka_connector = connector
        return ctx, connector

    def test_list_schemas_no_registry(self):
        from src.server import list_schemas
        ctx, _ = self._make_ctx(schema_registry=None)
        result = list_schemas(ctx)
        self.assertIn("error", result)
        self.assertIn("SCHEMA_REGISTRY_URL", result["error"])

    def test_get_schema_no_registry(self):
        from src.server import get_schema
        ctx, _ = self._make_ctx(schema_registry=None)
        result = get_schema(ctx, "test-value")
        self.assertIn("error", result)

    def test_register_schema_no_registry(self):
        from src.server import register_schema
        ctx, _ = self._make_ctx(schema_registry=None)
        result = register_schema(ctx, "test-value", '{"type":"string"}')
        self.assertIn("error", result)

    def test_delete_schema_no_registry(self):
        from src.server import delete_schema
        ctx, _ = self._make_ctx(schema_registry=None)
        result = delete_schema(ctx, "test-value")
        self.assertIn("error", result)

    def test_list_schemas_with_registry(self):
        from src.server import list_schemas
        mock_registry = MagicMock()
        mock_registry.get_subjects.return_value = ["topic1-value"]
        ctx, _ = self._make_ctx(schema_registry=mock_registry)
        result = list_schemas(ctx)
        self.assertEqual(result, ["topic1-value"])

    def test_get_schema_with_registry(self):
        from src.server import get_schema
        mock_registry = MagicMock()
        mock_registry.get_schema.return_value = {"schema_id": 1, "schema_str": "{}", "schema_type": "AVRO", "version": 1, "subject": "t-value"}
        ctx, _ = self._make_ctx(schema_registry=mock_registry)
        result = get_schema(ctx, "t-value")
        self.assertEqual(result["schema_id"], 1)

    def test_register_schema_with_registry(self):
        from src.server import register_schema
        mock_registry = MagicMock()
        mock_registry.register_schema.return_value = 42
        ctx, _ = self._make_ctx(schema_registry=mock_registry)
        result = register_schema(ctx, "t-value", '{"type":"string"}')
        self.assertEqual(result["schema_id"], 42)

    def test_delete_schema_with_registry(self):
        from src.server import delete_schema
        mock_registry = MagicMock()
        mock_registry.delete_subject.return_value = [1, 2]
        ctx, _ = self._make_ctx(schema_registry=mock_registry)
        result = delete_schema(ctx, "t-value")
        self.assertEqual(result["deleted_versions"], [1, 2])


if __name__ == "__main__":
    unittest.main()
