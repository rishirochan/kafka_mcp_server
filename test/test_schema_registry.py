import unittest
from unittest.mock import MagicMock, patch
import struct
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.schema_registry import SchemaRegistryService


class TestSchemaRegistryService(unittest.TestCase):
    @patch('src.schema_registry.SchemaRegistryClient')
    def setUp(self, MockClient):
        self.mock_client = MockClient.return_value
        self.service = SchemaRegistryService("http://localhost:8081")
        self.service.client = self.mock_client

    def test_get_subjects(self):
        self.mock_client.get_subjects.return_value = ["topic1-value", "topic2-value"]
        result = self.service.get_subjects()
        self.assertEqual(result, ["topic1-value", "topic2-value"])

    def test_get_schema_latest(self):
        mock_registered = MagicMock()
        mock_registered.schema_id = 1
        mock_registered.schema.schema_str = '{"type": "record", "name": "Test", "fields": []}'
        mock_registered.schema.schema_type = "AVRO"
        mock_registered.version = 1
        mock_registered.subject = "topic1-value"
        self.mock_client.get_latest_version.return_value = mock_registered

        result = self.service.get_schema("topic1-value")
        self.assertEqual(result["schema_id"], 1)
        self.assertEqual(result["schema_type"], "AVRO")
        self.assertEqual(result["version"], 1)
        self.mock_client.get_latest_version.assert_called_once_with("topic1-value")

    def test_get_schema_specific_version(self):
        mock_registered = MagicMock()
        mock_registered.schema_id = 2
        mock_registered.schema.schema_str = '{"type": "string"}'
        mock_registered.schema.schema_type = "AVRO"
        mock_registered.version = 3
        mock_registered.subject = "topic1-value"
        self.mock_client.get_version.return_value = mock_registered

        result = self.service.get_schema("topic1-value", version="3")
        self.assertEqual(result["version"], 3)
        self.mock_client.get_version.assert_called_once_with("topic1-value", 3)

    def test_register_schema(self):
        self.mock_client.register_schema.return_value = 42
        result = self.service.register_schema(
            "topic1-value",
            '{"type": "record", "name": "Test", "fields": []}',
            "AVRO"
        )
        self.assertEqual(result, 42)
        self.mock_client.register_schema.assert_called_once()

    def test_delete_subject(self):
        self.mock_client.delete_subject.return_value = [1, 2, 3]
        result = self.service.delete_subject("topic1-value")
        self.assertEqual(result, [1, 2, 3])
        self.mock_client.delete_subject.assert_called_once_with("topic1-value")

    def test_subject_name_value(self):
        self.assertEqual(self.service._get_subject_name("my-topic"), "my-topic-value")

    def test_subject_name_key(self):
        self.assertEqual(self.service._get_subject_name("my-topic", is_key=True), "my-topic-key")

    def test_has_subject_true(self):
        self.mock_client.get_subjects.return_value = ["my-topic-value"]
        self.assertTrue(self.service._has_subject("my-topic-value"))

    def test_has_subject_false(self):
        self.mock_client.get_subjects.return_value = ["other-topic-value"]
        self.assertFalse(self.service._has_subject("my-topic-value"))

    def test_serialize_no_schema(self):
        self.mock_client.get_subjects.return_value = []
        result = self.service.serialize("my-topic", {"key": "value"})
        self.assertIsNone(result)

    def test_deserialize_none_input(self):
        result = self.service.deserialize("topic", None)
        self.assertIsNone(result)

    def test_deserialize_too_short(self):
        result = self.service.deserialize("topic", b'\x00\x01')
        self.assertIsNone(result)

    def test_deserialize_no_magic_byte(self):
        # Bytes that don't start with 0x00 — not Confluent wire format
        result = self.service.deserialize("topic", b'\x01\x00\x00\x00\x01payload')
        self.assertIsNone(result)

    def test_deserialize_magic_byte_bad_schema(self):
        # Valid wire format header but schema fetch fails
        data = struct.pack(">bI", 0, 999) + b'payload'
        self.mock_client.get_schema.side_effect = Exception("Schema not found")
        result = self.service.deserialize("topic", data)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
