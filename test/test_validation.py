import unittest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.validation import (
    validate_topic_name,
    validate_message_value,
    validate_positive_int,
    validate_publish_schema_type,
    validate_schema_json,
)


class TestValidateTopicName(unittest.TestCase):
    def test_valid_names(self):
        for name in ["my-topic", "topic.v2", "topic_name", "T123", "a"]:
            validate_topic_name(name)  # should not raise

    def test_empty_name(self):
        with self.assertRaises(ValueError, msg="empty"):
            validate_topic_name("")

    def test_whitespace_only(self):
        with self.assertRaises(ValueError):
            validate_topic_name("   ")

    def test_too_long(self):
        with self.assertRaises(ValueError, msg="maximum length"):
            validate_topic_name("a" * 250)

    def test_exactly_max_length(self):
        validate_topic_name("a" * 249)  # should not raise

    def test_invalid_characters(self):
        for bad in ["topic name", "topic/name", "topic@name", "topic!!", "topic#1"]:
            with self.assertRaises(ValueError, msg=f"should reject '{bad}'"):
                validate_topic_name(bad)


class TestValidateMessageValue(unittest.TestCase):
    def test_small_message(self):
        validate_message_value("hello")  # should not raise

    def test_exactly_1mb(self):
        # 1 MB of single-byte chars
        validate_message_value("a" * 1_048_576)  # should not raise

    def test_over_1mb(self):
        with self.assertRaises(ValueError, msg="maximum size"):
            validate_message_value("a" * 1_048_577)


class TestValidateSchemaJson(unittest.TestCase):
    def test_valid_json(self):
        validate_schema_json('{"type": "string"}')  # should not raise

    def test_valid_json_array(self):
        validate_schema_json("[1, 2, 3]")  # should not raise

    def test_invalid_json(self):
        with self.assertRaises(ValueError, msg="not valid JSON"):
            validate_schema_json("not json at all")

    def test_empty_string(self):
        with self.assertRaises(ValueError):
            validate_schema_json("")


class TestValidatePositiveInt(unittest.TestCase):
    def test_accepts_positive_values(self):
        validate_positive_int(1, "num_partitions")
        validate_positive_int(3, "replication_factor")

    def test_rejects_zero(self):
        with self.assertRaises(ValueError):
            validate_positive_int(0, "num_partitions")

    def test_rejects_negative(self):
        with self.assertRaises(ValueError):
            validate_positive_int(-1, "replication_factor")


class TestValidatePublishSchemaType(unittest.TestCase):
    def test_none_is_allowed(self):
        self.assertIsNone(validate_publish_schema_type(None))

    def test_normalizes_avro(self):
        self.assertEqual(validate_publish_schema_type("avro"), "AVRO")

    def test_rejects_unsupported_types(self):
        with self.assertRaises(ValueError):
            validate_publish_schema_type("json")


if __name__ == "__main__":
    unittest.main()
