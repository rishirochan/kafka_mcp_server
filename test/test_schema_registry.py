import unittest
from unittest.mock import MagicMock, patch
import struct
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.schema_registry import SchemaRegistryService


class TestSchemaRegistryService(unittest.TestCase):
    @patch("src.schema_registry.SchemaRegistryClient")
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
        mock_registered.schema.schema_str = (
            '{"type": "record", "name": "Test", "fields": []}'
        )
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
            "topic1-value", '{"type": "record", "name": "Test", "fields": []}', "AVRO"
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
        self.assertEqual(
            self.service._get_subject_name("my-topic", is_key=True), "my-topic-key"
        )

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
        result = self.service.deserialize("topic", b"\x00\x01")
        self.assertIsNone(result)

    def test_deserialize_no_magic_byte(self):
        # Bytes that don't start with 0x00 — not Confluent wire format
        result = self.service.deserialize("topic", b"\x01\x00\x00\x00\x01payload")
        self.assertIsNone(result)

    def test_deserialize_magic_byte_bad_schema(self):
        # Valid wire format header but schema fetch fails
        data = struct.pack(">bI", 0, 999) + b"payload"
        self.mock_client.get_schema.side_effect = Exception("Schema not found")
        result = self.service.deserialize("topic", data)
        self.assertIsNone(result)

    def test_has_subject_exception(self):
        """_has_subject returns False when get_subjects raises."""
        self.mock_client.get_subjects.side_effect = Exception("connection error")
        self.assertFalse(self.service._has_subject("any-subject"))

    def test_get_or_create_serializer_caches(self):
        """Serializer is created once and reused on subsequent calls."""
        mock_registered = MagicMock()
        mock_registered.schema.schema_str = '{"type":"string"}'
        self.mock_client.get_latest_version.return_value = mock_registered

        with patch("src.schema_registry.AvroSerializer") as MockSerializer:
            MockSerializer.return_value = MagicMock()
            s1 = self.service._get_or_create_serializer("subj1")
            s2 = self.service._get_or_create_serializer("subj1")
            self.assertEqual(s1, s2)
            MockSerializer.assert_called_once()

    def test_get_or_create_deserializer_caches(self):
        """Deserializer is created once per schema_id and reused."""
        mock_schema = MagicMock()
        mock_schema.schema_str = '{"type":"string"}'
        self.mock_client.get_schema.return_value = mock_schema

        with patch("src.schema_registry.AvroDeserializer") as MockDeser:
            MockDeser.return_value = MagicMock()
            d1 = self.service._get_or_create_deserializer(1)
            d2 = self.service._get_or_create_deserializer(1)
            self.assertEqual(d1, d2)
            MockDeser.assert_called_once()

    def test_serialize_success(self):
        """serialize() returns bytes when subject exists."""
        self.mock_client.get_subjects.return_value = ["t-value"]
        mock_registered = MagicMock()
        mock_registered.schema.schema_str = '{"type":"record","name":"T","fields":[]}'
        self.mock_client.get_latest_version.return_value = mock_registered

        with patch("src.schema_registry.AvroSerializer") as MockSerializer:
            mock_ser_instance = MagicMock(return_value=b"avro_encoded")
            MockSerializer.return_value = mock_ser_instance
            result = self.service.serialize("t", {"field": "value"})
            self.assertEqual(result, b"avro_encoded")

    def test_serialize_key(self):
        """serialize() with is_key=True uses key subject."""
        self.mock_client.get_subjects.return_value = ["t-key"]
        mock_registered = MagicMock()
        mock_registered.schema.schema_str = '{"type":"string"}'
        self.mock_client.get_latest_version.return_value = mock_registered

        with patch("src.schema_registry.AvroSerializer") as MockSerializer:
            mock_ser_instance = MagicMock(return_value=b"key_bytes")
            MockSerializer.return_value = mock_ser_instance
            result = self.service.serialize("t", {"k": "v"}, is_key=True)
            self.assertEqual(result, b"key_bytes")

    def test_deserialize_success(self):
        """deserialize() returns dict when wire format is valid."""
        data = struct.pack(">bI", 0, 1) + b"avro_payload"
        mock_schema = MagicMock()
        mock_schema.schema_str = '{"type":"string"}'
        self.mock_client.get_schema.return_value = mock_schema

        with patch("src.schema_registry.AvroDeserializer") as MockDeser:
            mock_deser_instance = MagicMock(return_value={"decoded": True})
            MockDeser.return_value = mock_deser_instance
            result = self.service.deserialize("t", data)
            self.assertEqual(result, {"decoded": True})

    def test_deserialize_key(self):
        """deserialize() with is_key=True uses KEY message field."""
        data = struct.pack(">bI", 0, 5) + b"avro_payload"
        mock_schema = MagicMock()
        mock_schema.schema_str = '{"type":"string"}'
        self.mock_client.get_schema.return_value = mock_schema

        with patch("src.schema_registry.AvroDeserializer") as MockDeser:
            mock_deser_instance = MagicMock(return_value={"key": "val"})
            MockDeser.return_value = mock_deser_instance
            result = self.service.deserialize("t", data, is_key=True)
            self.assertEqual(result, {"key": "val"})


if __name__ == "__main__":
    unittest.main()
