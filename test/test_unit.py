import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import sys
import os
import asyncio

# Ensure src is in path so we can import from src.service
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.service import KafkaConnector

class TestKafkaConnectorUnit(unittest.IsolatedAsyncioTestCase):
    @patch('src.service.KafkaAdminClient')
    def setUp(self, MockAdminClient):
        self.mock_admin_client = MockAdminClient.return_value
        self.connector = KafkaConnector("localhost:9092")
        # Replace the real admin_client with our mock (safeguard, though patch should handle init)
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
        # Mock get_topics to return empty list first (topic doesn't exist)
        self.mock_admin_client.list_topics.return_value = []
        result = self.connector.create_topic('new_topic')
        self.assertTrue(result)
        self.mock_admin_client.create_topics.assert_called_once()

    def test_create_topic_exists(self):
        # Mock get_topics to return list containing the topic
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
        # Setup mock producer
        mock_producer_instance = AsyncMock()
        MockProducer.return_value = mock_producer_instance
        mock_producer_instance.start = AsyncMock()
        mock_producer_instance.stop = AsyncMock()
        mock_producer_instance.send_and_wait = AsyncMock(return_value="metadata")
        
        # We need to ensure the connector creates a new producer
        # Passing None as session_id to generate a new one
        result = await self.connector.publish("test_topic", "value")
        
        self.assertEqual(result, "metadata")
        mock_producer_instance.start.assert_called()
        mock_producer_instance.send_and_wait.assert_called()
        mock_producer_instance.stop.assert_called()

    @patch('src.service.AIOKafkaConsumer')
    async def test_consume_success(self, MockConsumer):
         # Setup mock consumer
        mock_consumer_instance = AsyncMock()
        MockConsumer.return_value = mock_consumer_instance
        mock_consumer_instance.start = AsyncMock()
        mock_consumer_instance.stop = AsyncMock()
        
        # Mock getmany to return a dict of messages
        mock_msg = MagicMock()
        mock_msg.value = "test_message"
        mock_consumer_instance.getmany = AsyncMock(return_value={
            "tp": [mock_msg]
        })

        result = await self.connector.consume("test_topic")
        
        self.assertEqual(result, ["test_message"])
        mock_consumer_instance.start.assert_called()
        mock_consumer_instance.getmany.assert_called()
        mock_consumer_instance.stop.assert_called()


    def test_deserialize_msg(self):
        # Test JSON message
        json_msg = b'{"key": "value"}'
        self.assertEqual(self.connector._deserialize_msg(json_msg), {"key": "value"})

        # Test String message
        str_msg = b'test_message'
        self.assertEqual(self.connector._deserialize_msg(str_msg), "test_message")

        # Test None
        self.assertIsNone(self.connector._deserialize_msg(None))
        
        # Test malformed bytes that are still valid utf-8 but not valid json
        # (covered by string message)
        
        # Test invalid utf-8 (should be returned as is because decode raises Exception and we catch it)
        # Note: In the optimized code:
        # try:
        #     decoded_value = msg.decode('utf-8') -> Raises UnicodeDecodeError
        #     return json.loads(decoded_value)
        # except json.JSONDecodeError:
        #     return decoded_value
        # except Exception as e: -> Catches UnicodeDecodeError
        #     return msg
        
        invalid_utf8 = b'\x80\x81' 
        self.assertEqual(self.connector._deserialize_msg(invalid_utf8), invalid_utf8)

if __name__ == "__main__":
    unittest.main()
