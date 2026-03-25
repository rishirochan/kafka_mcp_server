import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import sys
import os

# Ensure src is in path so we can import from src.service
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.service import KafkaConnector

class TestKafkaConnectorUnit(unittest.IsolatedAsyncioTestCase):
    @patch('src.service.KafkaAdminClient')
    def setUp(self, MockAdminClient):
        self.mock_admin_client = MockAdminClient.return_value
        self.connector = KafkaConnector("localhost:9092")
        self.connector.admin_client = self.mock_admin_client

    def test_get_topics_success(self):
        self.mock_admin_client.list_topics.return_value = ['topic1', 'topic2']
        topics = self.connector.get_topics()
        self.assertEqual(topics, ['topic1', 'topic2'])

    def test_get_topics_failure(self):
        self.mock_admin_client.list_topics.side_effect = Exception("Kafka Error")
        topics = self.connector.get_topics()
        self.assertIsNone(topics)

    def test_is_topic_exists(self):
        self.mock_admin_client.list_topics.return_value = ['topic1']
        self.assertTrue(self.connector.is_topic_exists('topic1'))
        self.assertFalse(self.connector.is_topic_exists('topic2'))

    def test_create_topic_success(self):
        self.mock_admin_client.list_topics.return_value = []
        result = self.connector.create_topic('new_topic')
        self.assertTrue(result)
        self.mock_admin_client.create_topics.assert_called_once()

    def test_create_topic_exists(self):
        self.mock_admin_client.list_topics.return_value = ['existing_topic']
        result = self.connector.create_topic('existing_topic')
        self.assertFalse(result)
        self.mock_admin_client.create_topics.assert_not_called()

    def test_delete_topic_success(self):
        self.mock_admin_client.list_topics.return_value = ['topic_to_delete']
        result = self.connector.delete_topic('topic_to_delete')
        self.assertTrue(result)
        self.mock_admin_client.delete_topics.assert_called_once()

    def test_delete_topic_not_found(self):
        self.mock_admin_client.list_topics.return_value = []
        result = self.connector.delete_topic('non_existent')
        self.assertFalse(result)
        self.mock_admin_client.delete_topics.assert_not_called()

    @patch('src.service.AIOKafkaProducer')
    async def test_publish_success(self, MockProducer):
        mock_producer_instance = AsyncMock()
        MockProducer.return_value = mock_producer_instance
        mock_producer_instance.start = AsyncMock()
        mock_producer_instance.stop = AsyncMock()
        mock_producer_instance.send_and_wait = AsyncMock(return_value="metadata")

        result = await self.connector.publish("test_topic", "value")

        self.assertEqual(result, "metadata")
        mock_producer_instance.start.assert_called()
        call_kwargs = mock_producer_instance.send_and_wait.call_args
        self.assertIsInstance(call_kwargs.kwargs['value'], bytes)
        self.assertIsInstance(call_kwargs.kwargs['key'], bytes)
        mock_producer_instance.stop.assert_called()

    @patch('src.service.AIOKafkaConsumer')
    async def test_consume_success(self, MockConsumer):
        mock_consumer_instance = AsyncMock()
        MockConsumer.return_value = mock_consumer_instance
        mock_consumer_instance.start = AsyncMock()
        mock_consumer_instance.stop = AsyncMock()

        # Consumer now receives raw bytes; _deserialize_value handles decoding
        mock_tp = MagicMock()
        mock_tp.topic = "test_topic"
        mock_tp.partition = 0
        mock_msg = MagicMock()
        mock_msg.value = b'test_message'
        mock_consumer_instance.getmany = AsyncMock(return_value={
            mock_tp: [mock_msg]
        })

        result = await self.connector.consume("test_topic")

        self.assertEqual(result, ["test_message"])
        mock_consumer_instance.start.assert_called()
        mock_consumer_instance.getmany.assert_called()
        mock_consumer_instance.stop.assert_called()

    # ---- Serde tests ----

    def test_deserialize_value_json(self):
        result = self.connector._deserialize_value("topic", b'{"key": "value"}')
        self.assertEqual(result, {"key": "value"})

    def test_deserialize_value_plain_string(self):
        result = self.connector._deserialize_value("topic", b'test_message')
        self.assertEqual(result, "test_message")

    def test_deserialize_value_none(self):
        self.assertIsNone(self.connector._deserialize_value("topic", None))

    def test_deserialize_value_invalid_utf8(self):
        result = self.connector._deserialize_value("topic", b'\x80\x81')
        # Falls back to decode with errors='replace'
        self.assertIsInstance(result, str)

    def test_serialize_value_dict(self):
        result = self.connector._serialize_value("topic", {"key": "value"})
        self.assertEqual(result, b'{"key": "value"}')

    def test_serialize_value_string(self):
        result = self.connector._serialize_value("topic", "hello")
        self.assertEqual(result, b'hello')

    def test_serialize_value_json_string(self):
        # A JSON string input should be parsed to dict then serialized
        result = self.connector._serialize_value("topic", '{"key": "value"}')
        self.assertEqual(result, b'{"key": "value"}')

    def test_serialize_key(self):
        self.assertEqual(self.connector._serialize_key("mykey"), b'mykey')

    def test_serialize_key_none(self):
        self.assertIsNone(self.connector._serialize_key(None))

    def test_deserialize_key(self):
        self.assertEqual(self.connector._deserialize_key(b'mykey'), "mykey")

    def test_deserialize_key_none(self):
        self.assertIsNone(self.connector._deserialize_key(None))


if __name__ == "__main__":
    unittest.main()
