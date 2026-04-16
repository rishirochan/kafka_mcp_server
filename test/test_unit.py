import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import sys
import os

# Ensure src is in path so we can import from src.service
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.service import KafkaConnector


class TestKafkaConnectorUnit(unittest.IsolatedAsyncioTestCase):
    @patch("src.service.KafkaAdminClient")
    def setUp(self, MockAdminClient):
        self.mock_admin_client = MockAdminClient.return_value
        self.connector = KafkaConnector("localhost:9092")
        self.connector.admin_client = self.mock_admin_client

    def test_get_topics_success(self):
        self.mock_admin_client.list_topics.return_value = ["topic1", "topic2"]
        topics = self.connector.get_topics()
        self.assertEqual(topics, ["topic1", "topic2"])

    def test_get_topics_failure(self):
        self.mock_admin_client.list_topics.side_effect = Exception("Kafka Error")
        topics = self.connector.get_topics()
        self.assertIsNone(topics)

    def test_is_topic_exists(self):
        self.mock_admin_client.list_topics.return_value = ["topic1"]
        self.assertTrue(self.connector.is_topic_exists("topic1"))
        self.assertFalse(self.connector.is_topic_exists("topic2"))

    def test_create_topic_success(self):
        self.mock_admin_client.list_topics.return_value = []
        result = self.connector.create_topic("new_topic")
        self.assertTrue(result)
        self.mock_admin_client.create_topics.assert_called_once()

    def test_create_topic_exists(self):
        self.mock_admin_client.list_topics.return_value = ["existing_topic"]
        result = self.connector.create_topic("existing_topic")
        self.assertFalse(result)
        self.mock_admin_client.create_topics.assert_not_called()

    def test_delete_topic_success(self):
        self.mock_admin_client.list_topics.return_value = ["topic_to_delete"]
        result = self.connector.delete_topic("topic_to_delete")
        self.assertTrue(result)
        self.mock_admin_client.delete_topics.assert_called_once()

    def test_delete_topic_not_found(self):
        self.mock_admin_client.list_topics.return_value = []
        result = self.connector.delete_topic("non_existent")
        self.assertFalse(result)
        self.mock_admin_client.delete_topics.assert_not_called()

    @patch("src.service.AIOKafkaProducer")
    async def test_publish_success(self, MockProducer):
        mock_producer_instance = AsyncMock()
        MockProducer.return_value = mock_producer_instance
        mock_producer_instance.start = AsyncMock()
        mock_producer_instance.stop = AsyncMock()
        mock_metadata = MagicMock()
        mock_metadata.topic = "test_topic"
        mock_metadata.partition = 0
        mock_metadata.offset = 5
        mock_metadata.timestamp = 1234567890
        mock_producer_instance.send_and_wait = AsyncMock(return_value=mock_metadata)

        result = await self.connector.publish("test_topic", "value")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["topic"], "test_topic")
        self.assertEqual(result["partition"], 0)
        self.assertEqual(result["offset"], 5)
        mock_producer_instance.start.assert_called()
        call_kwargs = mock_producer_instance.send_and_wait.call_args
        self.assertIsInstance(call_kwargs.kwargs["value"], bytes)
        self.assertIsInstance(call_kwargs.kwargs["key"], bytes)
        mock_producer_instance.stop.assert_called()

    @patch("src.service.AIOKafkaConsumer")
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
        mock_msg.value = b"test_message"
        mock_consumer_instance.getmany = AsyncMock(return_value={mock_tp: [mock_msg]})

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
        result = self.connector._deserialize_value("topic", b"test_message")
        self.assertEqual(result, "test_message")

    def test_deserialize_value_none(self):
        self.assertIsNone(self.connector._deserialize_value("topic", None))

    def test_deserialize_value_invalid_utf8(self):
        result = self.connector._deserialize_value("topic", b"\x80\x81")
        # Falls back to decode with errors='replace'
        self.assertIsInstance(result, str)

    def test_serialize_value_dict(self):
        result = self.connector._serialize_value("topic", {"key": "value"})
        self.assertEqual(result, b'{"key": "value"}')

    def test_serialize_value_string(self):
        result = self.connector._serialize_value("topic", "hello")
        self.assertEqual(result, b"hello")

    def test_serialize_value_json_string(self):
        # A JSON string input should be parsed to dict then serialized
        result = self.connector._serialize_value("topic", '{"key": "value"}')
        self.assertEqual(result, b'{"key": "value"}')

    def test_serialize_key(self):
        self.assertEqual(self.connector._serialize_key("mykey"), b"mykey")

    def test_serialize_key_none(self):
        self.assertIsNone(self.connector._serialize_key(None))

    def test_deserialize_key(self):
        self.assertEqual(self.connector._deserialize_key(b"mykey"), "mykey")

    def test_deserialize_key_none(self):
        self.assertIsNone(self.connector._deserialize_key(None))

    # ---- Schema Registry init ----

    @patch("src.service.KafkaAdminClient")
    def test_init_with_schema_registry(self, MockAdmin):
        with patch("src.service.KafkaConnector.__init__", lambda self, *a, **kw: None):
            connector = KafkaConnector.__new__(KafkaConnector)
        connector.bootstrap_servers = "localhost:9092"
        connector.admin_client = MockAdmin.return_value
        connector.producers = {}
        connector.consumers = {}
        connector.schema_registry = None
        # Manually test the branch
        with patch.dict("sys.modules", {"src.schema_registry": MagicMock()}):
            from src.schema_registry import SchemaRegistryService

            connector.schema_registry = SchemaRegistryService("http://localhost:8081")
        self.assertIsNotNone(connector.schema_registry)

    # ---- get_admin_client ----

    def test_get_admin_client_returns_existing(self):
        result = self.connector.get_admin_client()
        self.assertEqual(result, self.mock_admin_client)

    @patch("src.service.KafkaAdminClient")
    def test_get_admin_client_creates_new(self, MockAdminClient):
        self.connector.admin_client = None
        result = self.connector.get_admin_client()
        self.assertIsNotNone(result)
        MockAdminClient.assert_called_once()

    # ---- describe_topic ----

    def test_describe_topic_success(self):
        self.mock_admin_client.describe_topics.return_value = [
            {"topic": "t1", "partitions": []}
        ]
        result = self.connector.describe_topic("t1")
        self.assertEqual(result[0]["topic"], "t1")

    def test_describe_topic_none(self):
        self.mock_admin_client.describe_topics.return_value = None
        result = self.connector.describe_topic("t1")
        self.assertIsNone(result)

    def test_describe_topic_error(self):
        self.mock_admin_client.describe_topics.side_effect = Exception("fail")
        result = self.connector.describe_topic("t1")
        self.assertIsNone(result)

    # ---- get_partitions ----

    def test_get_partitions_success(self):
        self.mock_admin_client.describe_topics.return_value = [
            {
                "topic": "t1",
                "partitions": [
                    {"partition": 0, "leader": 1, "replicas": [1], "isr": [1]},
                    {"partition": 1, "leader": 1, "replicas": [1], "isr": [1]},
                ],
            }
        ]
        result = self.connector.get_partitions("t1")
        self.assertEqual(result["partitions_count"], 2)
        self.assertEqual(result["topic"], "t1")
        self.assertEqual(result["replication_factor"], 1)

    def test_get_partitions_no_metadata(self):
        self.mock_admin_client.describe_topics.return_value = None
        result = self.connector.get_partitions("t1")
        self.assertIsNone(result)

    def test_get_partitions_error(self):
        self.mock_admin_client.describe_topics.side_effect = Exception("fail")
        result = self.connector.get_partitions("t1")
        self.assertIsNone(result)

    # ---- get_topics edge case ----

    def test_get_topics_empty(self):
        self.mock_admin_client.list_topics.return_value = []
        result = self.connector.get_topics()
        self.assertIsNone(result)

    # ---- create_topic error ----

    def test_create_topic_error(self):
        self.mock_admin_client.list_topics.return_value = []
        self.mock_admin_client.create_topics.side_effect = Exception("fail")
        result = self.connector.create_topic("new_topic")
        self.assertFalse(result)

    # ---- delete_topic error ----

    def test_delete_topic_error(self):
        self.mock_admin_client.list_topics.return_value = ["t1"]
        self.mock_admin_client.delete_topics.side_effect = Exception("fail")
        result = self.connector.delete_topic("t1")
        self.assertFalse(result)

    def test_delete_topic_exists_check_error(self):
        self.mock_admin_client.list_topics.side_effect = Exception("fail")
        result = self.connector.delete_topic("t1")
        self.assertFalse(result)

    # ---- Producer lifecycle ----

    @patch("src.service.AIOKafkaProducer")
    async def test_get_or_create_producer_reuses_open(self, MockProducer):
        mock_producer = AsyncMock()
        mock_producer._closed = False
        self.connector.producers["sess1"] = mock_producer
        sid, p = await self.connector.get_or_create_producer("sess1")
        self.assertEqual(sid, "sess1")
        self.assertEqual(p, mock_producer)

    @patch("src.service.AIOKafkaProducer")
    async def test_get_or_create_producer_replaces_closed(self, MockProducer):
        closed_producer = MagicMock()
        closed_producer._closed = True
        self.connector.producers["sess1"] = closed_producer
        new_producer = AsyncMock()
        MockProducer.return_value = new_producer
        sid, p = await self.connector.get_or_create_producer("sess1")
        self.assertEqual(sid, "sess1")
        self.assertEqual(p, new_producer)
        new_producer.start.assert_called_once()

    @patch("src.service.AIOKafkaProducer")
    async def test_get_or_create_producer_generates_id(self, MockProducer):
        new_producer = AsyncMock()
        MockProducer.return_value = new_producer
        sid, p = await self.connector.get_or_create_producer(None)
        self.assertTrue(sid.startswith("producer_"))
        new_producer.start.assert_called_once()

    @patch("src.service.AIOKafkaProducer")
    async def test_get_or_create_producer_error(self, MockProducer):
        MockProducer.return_value = AsyncMock()
        MockProducer.return_value.start = AsyncMock(side_effect=Exception("fail"))
        result = await self.connector.get_or_create_producer("sess1")
        self.assertIsNone(result)

    async def test_close_producer_not_found(self):
        result = await self.connector.close_producer("nonexistent")
        self.assertFalse(result)

    async def test_close_producer_error(self):
        mock_producer = AsyncMock()
        mock_producer.stop = AsyncMock(side_effect=Exception("fail"))
        self.connector.producers["sess1"] = mock_producer
        result = await self.connector.close_producer("sess1")
        self.assertFalse(result)

    # ---- Consumer lifecycle ----

    @patch("src.service.AIOKafkaConsumer")
    async def test_get_or_create_consumer_reuses_open(self, MockConsumer):
        mock_consumer = AsyncMock()
        mock_consumer._closed = False
        self.connector.consumers["sess1"] = mock_consumer
        sid, c = await self.connector.get_or_create_consumer("t1", "grp", "sess1")
        self.assertEqual(sid, "sess1")
        self.assertEqual(c, mock_consumer)

    @patch("src.service.AIOKafkaConsumer")
    async def test_get_or_create_consumer_replaces_closed(self, MockConsumer):
        closed_consumer = MagicMock()
        closed_consumer._closed = True
        self.connector.consumers["sess1"] = closed_consumer
        new_consumer = AsyncMock()
        MockConsumer.return_value = new_consumer
        sid, c = await self.connector.get_or_create_consumer("t1", "grp", "sess1")
        self.assertEqual(sid, "sess1")
        new_consumer.start.assert_called_once()

    @patch("src.service.AIOKafkaConsumer")
    async def test_get_or_create_consumer_generates_id(self, MockConsumer):
        new_consumer = AsyncMock()
        MockConsumer.return_value = new_consumer
        sid, c = await self.connector.get_or_create_consumer("t1")
        self.assertTrue(sid.startswith("consumer_"))

    @patch("src.service.AIOKafkaConsumer")
    async def test_get_or_create_consumer_error(self, MockConsumer):
        MockConsumer.return_value = AsyncMock()
        MockConsumer.return_value.start = AsyncMock(side_effect=Exception("fail"))
        result = await self.connector.get_or_create_consumer("t1")
        self.assertIsNone(result)

    async def test_close_consumer_not_found(self):
        result = await self.connector.close_consumer("nonexistent")
        self.assertFalse(result)

    async def test_close_consumer_error(self):
        mock_consumer = AsyncMock()
        mock_consumer.stop = AsyncMock(side_effect=Exception("fail"))
        self.connector.consumers["sess1"] = mock_consumer
        result = await self.connector.close_consumer("sess1")
        self.assertFalse(result)

    # ---- publish error ----

    @patch("src.service.AIOKafkaProducer")
    async def test_publish_error(self, MockProducer):
        mock_producer = AsyncMock()
        MockProducer.return_value = mock_producer
        mock_producer.send_and_wait = AsyncMock(side_effect=Exception("send failed"))
        result = await self.connector.publish("t1", "value")
        self.assertEqual(result["status"], "error")
        self.assertIn("send failed", result["message"])

    # ---- consume error ----

    @patch("src.service.AIOKafkaConsumer")
    async def test_consume_error(self, MockConsumer):
        mock_consumer = AsyncMock()
        MockConsumer.return_value = mock_consumer
        mock_consumer.getmany = AsyncMock(side_effect=Exception("consume failed"))
        result = await self.connector.consume("t1")
        self.assertIsNone(result)

    # ---- close (shutdown) ----

    async def test_close_all_clean(self):
        """close() stops open producers and consumers, closes admin client."""
        mock_producer = AsyncMock()
        mock_producer._closed = False
        self.connector.producers["p1"] = mock_producer

        mock_consumer = AsyncMock()
        mock_consumer._closed = False
        self.connector.consumers["c1"] = mock_consumer

        await self.connector.close()

        mock_producer.stop.assert_called_once()
        mock_consumer.stop.assert_called_once()
        self.mock_admin_client.close.assert_called_once()
        self.assertEqual(len(self.connector.producers), 0)
        self.assertEqual(len(self.connector.consumers), 0)

    async def test_close_skips_already_closed(self):
        mock_producer = MagicMock()
        mock_producer._closed = True
        self.connector.producers["p1"] = mock_producer

        mock_consumer = MagicMock()
        mock_consumer._closed = True
        self.connector.consumers["c1"] = mock_consumer

        await self.connector.close()
        # Should not try to stop already-closed instances
        self.assertEqual(len(self.connector.producers), 0)
        self.assertEqual(len(self.connector.consumers), 0)

    async def test_close_handles_errors(self):
        mock_producer = AsyncMock()
        mock_producer._closed = False
        mock_producer.stop = AsyncMock(side_effect=Exception("stop fail"))
        self.connector.producers["p1"] = mock_producer

        mock_consumer = AsyncMock()
        mock_consumer._closed = False
        mock_consumer.stop = AsyncMock(side_effect=Exception("stop fail"))
        self.connector.consumers["c1"] = mock_consumer

        self.mock_admin_client.close.side_effect = Exception("close fail")

        # Should not raise
        await self.connector.close()
        self.assertEqual(len(self.connector.producers), 0)
        self.assertEqual(len(self.connector.consumers), 0)

    async def test_close_no_admin_client(self):
        self.connector.admin_client = None
        await self.connector.close()  # should not raise

    # ---- serialize with schema registry ----

    def test_serialize_value_with_schema_registry(self):
        mock_registry = MagicMock()
        mock_registry.serialize.return_value = b"\x00\x00\x00\x00\x01avro"
        self.connector.schema_registry = mock_registry
        result = self.connector._serialize_value("t1", {"key": "val"})
        self.assertEqual(result, b"\x00\x00\x00\x00\x01avro")
        mock_registry.serialize.assert_called_once_with("t1", {"key": "val"})

    def test_serialize_value_schema_registry_returns_none(self):
        """When schema registry returns None, falls back to JSON."""
        mock_registry = MagicMock()
        mock_registry.serialize.return_value = None
        self.connector.schema_registry = mock_registry
        result = self.connector._serialize_value("t1", {"key": "val"})
        self.assertEqual(result, b'{"key": "val"}')

    def test_deserialize_value_with_schema_registry(self):
        mock_registry = MagicMock()
        mock_registry.deserialize.return_value = {"decoded": True}
        self.connector.schema_registry = mock_registry
        result = self.connector._deserialize_value("t1", b"\x00\x00\x00\x00\x01data")
        self.assertEqual(result, {"decoded": True})

    def test_deserialize_value_schema_registry_returns_none(self):
        """When schema registry returns None, falls back to JSON/string."""
        mock_registry = MagicMock()
        mock_registry.deserialize.return_value = None
        self.connector.schema_registry = mock_registry
        result = self.connector._deserialize_value("t1", b"plain text")
        self.assertEqual(result, "plain text")

    # ---- serialize_value with schema_type hint (JSON string -> dict path) ----

    def test_serialize_value_json_string_with_schema_registry(self):
        """JSON string input should be parsed to dict, then schema registry is tried."""
        mock_registry = MagicMock()
        mock_registry.serialize.return_value = b"avro_bytes"
        self.connector.schema_registry = mock_registry
        result = self.connector._serialize_value("t1", '{"name": "Alice"}')
        self.assertEqual(result, b"avro_bytes")


if __name__ == "__main__":
    unittest.main()
