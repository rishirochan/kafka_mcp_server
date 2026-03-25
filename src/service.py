import logging
import json
import uuid
from datetime import datetime
from typing import Optional, Dict


from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin.config_resource import ConfigResource
#from aiokafka.errors import ConsumerStoppedError, ProducerStoppedError, KafkaError
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaConnector:
    """
    Encapsulates the connection to a kafka server and all the methods to interact with it.
    defines message serializer and deserializer for kafka messages. self.admin_client is used to manage topics and cluster health.
    :param bootstrap_servers The URL of the kafka server.
    :param schema_registry_url Optional URL for the Confluent Schema Registry.
    """

    def __init__(self, bootstrap_servers: str = "localhost:9092", schema_registry_url: Optional[str] = None):
        """Initialize the KafkaConnector.
        Args:
            bootstrap_servers: The URL of the kafka server.
            schema_registry_url: Optional URL for the Confluent Schema Registry.
        """
        self.bootstrap_servers = bootstrap_servers
        self.admin_client: KafkaAdminClient = None
        self.producers: Dict[str, AIOKafkaProducer] = {}
        self.consumers: Dict[str, AIOKafkaConsumer] = {}

        # Schema Registry (optional)
        self.schema_registry = None
        if schema_registry_url:
            from src.schema_registry import SchemaRegistryService
            self.schema_registry = SchemaRegistryService(schema_registry_url)
            logger.info(f"Schema Registry configured at {schema_registry_url}")

        # Initialize admin client for topic management and cluster health
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers, client_id='kafka-admin-client')

    def _serialize_value(self, topic: str, value, schema_type: Optional[str] = None) -> bytes:
        """Serialize a value, using Schema Registry if available and applicable.
        Args:
            topic: Kafka topic name.
            value: Message value (dict or str).
            schema_type: Optional schema type to force ("AVRO"). If None, auto-detects.
        Returns:
            bytes: Serialized message bytes.
        """
        # If value is a string, try to parse as JSON dict for schema-aware encoding
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                pass

        # Attempt schema-aware serialization if registry is configured and value is a dict
        if self.schema_registry and isinstance(value, dict):
            result = self.schema_registry.serialize(topic, value)
            if result is not None:
                return result

        # Fallback: JSON for dicts, string for everything else
        if isinstance(value, dict):
            return json.dumps(value).encode('utf-8')
        return str(value).encode('utf-8')

    def _deserialize_value(self, topic: str, raw: bytes):
        """Deserialize a value, using Schema Registry if available.
        Args:
            topic: Kafka topic name.
            raw: Raw message bytes.
        Returns:
            Deserialized value (dict or str).
        """
        if raw is None:
            return None

        # Try schema-aware deserialization first
        if self.schema_registry:
            result = self.schema_registry.deserialize(topic, raw)
            if result is not None:
                return result

        # Fallback: try JSON, then plain string
        try:
            return json.loads(raw.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return raw.decode('utf-8', errors='replace')

    def _serialize_key(self, key) -> bytes:
        """Serialize a message key to bytes."""
        return key.encode('utf-8') if key else None

    def _deserialize_key(self, key: bytes) -> str:
        """Deserialize a message key from bytes."""
        return key.decode('utf-8') if key else None
    
    def get_admin_client(self) ->  KafkaAdminClient:
        """Get or create admin client.
        Returns:
            KafkaAdminClient: Admin client for topic management and cluster health
        """
        if self.admin_client is None:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id="kafka-admin-client"
            )
        return self.admin_client

# ============================================================================
# Topic Management Tools
# ============================================================================

    def get_topics(self):
        """Get a list of all topics in the cluster.
        
        Returns:
            List[str]: List of topic names
        """
        try:
            topics = self.admin_client.list_topics()
            if topics:
                return topics
            else:
                return None
        except Exception as e:
            logger.error(f"Error listing topics: {e}")
            return None

    def describe_topic(self, topic: str):
        """Describe a topic details. 
        Args:
            topic: Name of the topic to describe

        Returns:
            Dict[str, Any]: Topic details
        """
        try:
            topic_info =  self.admin_client.describe_topics([topic])
            if topic_info:
                return topic_info
            else:
                return None
        except Exception as e:
            logger.error(f"Describe_Topic:Error describing topic {topic}: {e}")
            return None

    def get_partitions(self, topic: str):
        """Get a list of all partitions for a topic.

        Args:
            topic: Name of the topic to get partitions for
        """
        try:
            metadata = self.describe_topic(topic)
            if metadata:
                logger.info(f"get_partitions: Metadata for topic {topic}: {metadata}")
                partitions_info = []
                for partition in metadata[0].get("partitions"):
                    partitions_info.append(
                        {
                            "partition_id": partition.get("partition"),
                            "leader": partition.get("leader"),
                            "replicas": partition.get("replicas"),
                            "isr": partition.get("isr"),
                        }
                    )
                
                topic_details = {
                    "topic": topic,
                    "partitions": partitions_info,
                    "partitions_count": len(partitions_info),
                    "replication_factor": partitions_info[0].get("replicas").__len__()
                }
                return  topic_details
            else:
                return None 
        except Exception as e:
            logger.error(f"get_partitions: Error listing partitions for topic {topic}: {e}")
            return None

    def is_topic_exists(self, topic: str):
        """Check if a topic exists.
        Args:
            topic: Name of the topic to check
        Returns:
            bool: True if the topic exists, False otherwise
        """
        try:
            topics = self.get_topics()
            if topics:
                return True if topic in topics else False
            else:
                return False
        except Exception as e:
            logger.error(f"is_topic_exists: Error checking if topic {topic} exists: {e}")
            return False    


    def create_topic(self, topic: str, num_partitions: int = 1, replication_factor: int = 1, configs: Optional[Dict[str, str]] = None):
        """Create a new topic.
        Args:
            topic: Name of the topic to create
            num_partitions: Number of partitions for the topic
            replication_factor: Replication factor for the topic
            configs: Optional configuration for the topic
        Returns:
            bool: True if the topic was created, False otherwise    
        """
        try:
            if not self.is_topic_exists(topic):
                self.admin_client.create_topics([NewTopic(topic, num_partitions, replication_factor, configs)])
            
                logger.info(f"create_topic: Topic {topic} created successfully")
                return True
            else:
                logger.info(f"create_topic: Topic {topic} already exists")
                return False
        except Exception as e:
            logger.error(f"create_topic: Error creating topic {topic}: {e}")
            return False
    
    def delete_topic(self, topic: str):
        """Delete a topic.
        Args:
            topic: Name of the topic to delete
        """
        try:
            if self.is_topic_exists(topic):
                try:
                    self.admin_client.delete_topics([topic])
                    logger.info(f"delete_topic: Topic {topic} deleted successfully") 
                    return True
                except Exception as e:
                    logger.error(f"delete_topic: Error deleting topic {topic}: {e}")
                    return False
            else:
                logger.info(f"delete_topic: Topic {topic} does not exist")
                return False
        except Exception as e:
            logger.error(f"delete_topic: Error deleting topic {topic}: {e}")
            return False

#======================================================
# Producer Management Tools
#======================================================
    # Producer Tools for publishing messages to Kafka topics       
    async def get_or_create_producer(self, session_id: Optional[str] = None) -> (str, AIOKafkaProducer):
        """Get or Create and start a Kafka producer.
        Args:
            session_id: Session ID for the producer (optional)

        Returns:
            str: Session ID
            AIOKafkaProducer: Kafka producer instance
        """
        if session_id in self.producers:
            if not self.producers[session_id]._closed:
                return session_id, self.producers[session_id]
            else:
                #return session_id, self.producers[session_id].start()
                # Producer is closed, we need to create a new one
                logger.info(f"get_or_create_producer: Producer with session_id {session_id} is closed. Creating a new one.")
                # We can reuse the session_id or let the caller handle it. 
                # Here we reuse it but must re-instantiate because AIOKafkaProducer cannot be restarted easily.
                pass 
        
        if session_id is None:
            session_id = f"producer_{uuid.uuid4().hex[:8]}"
        try:
            self.producers[session_id] = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
            await self.producers[session_id].start()
            logger.info(f"get_or_create_producer: Kafka producer started, connected to {self.bootstrap_servers}")
            return session_id, self.producers[session_id]
        except Exception as e:
            logger.error(f"get_or_create_producer: Error creating producer: {e}")
            return None
        
    async def close_producer(self, session_id: str) -> None:
        """Close the Kafka producer.
        Args:
            session_id: Session ID for the producer
        """
        try:
            if session_id not in self.producers:
                logger.info(f"close_producer: Kafka producer with session_id {session_id} not found")
                return False    
            await self.producers[session_id].stop()
            # method was attempting to restart a closed AIOKafkaProducer instance. aiokafka does not support restarting closed producers.
            del self.producers[session_id]
            logger.info(f"close_producer: Kafka producer with session_id {session_id} stopped")
            return True
        except Exception as e:
            logger.error(f"close_producer: Error closing producer: {e}")
            return False
    
    async def publish(self, topic: str, value: str, key: Optional[str] = None, session_id: Optional[str] = None, schema_type: Optional[str] = None):
        """Publish a message to the specified Kafka topic.
        Args:
            topic: Topic to publish to
            value: Message value
            key: Message key (optional)
            session_id: Session ID for the producer (optional)
            schema_type: Schema type for encoding (e.g. "AVRO"). Auto-detects if None.
        """
        try:
            # Send message
            session_id, producer = await self.get_or_create_producer(session_id)
            if key is None:
                key = f"msg_key_{uuid.uuid4().hex[:8]}"

            serialized_value = self._serialize_value(topic, value, schema_type)
            serialized_key = self._serialize_key(key)
            metadata = await producer.send_and_wait(topic, value=serialized_value, key=serialized_key)
            logger.info(f"publish: Published message with session_id {session_id} and key {key} to topic {topic}")
            return metadata
        except Exception as e:
            logger.error(f"publish: Error publishing message: {e}")
            return None
        finally:
            await self.close_producer(session_id)
            

#======================================================
# Consumer Management Tools
#====================================================== 
# Consumer Tools for consuming messages from Kafka topics   
#======================================================    

    async def get_or_create_consumer(self, topic:str, group_id:str = "default-group", session_id: Optional[str] = None) ->  (str,AIOKafkaConsumer):
        """Get or Create and start a Kafka consumer.
        Args:
            topic: Topic to consume from
            group_id: Consumer group ID (optional)
            session_id: Session ID for the consumer (optional)
        Returns:
            str: Session ID
            AIOKafkaConsumer: Kafka consumer instance
        """
        if session_id in self.consumers:
            if not self.consumers[session_id]._closed:
                return session_id, self.consumers[session_id]
            else:
                # Consumer is closed, we need to create a new one
                logger.info(f"get_or_create_consumer: Consumer with session_id {session_id} is closed. Creating a new one.")
                pass
        
        if session_id is None:
            session_id = f"consumer_{uuid.uuid4().hex[:8]}"
        
        # Convert single topic to list
        if isinstance(topic, str):
            topics = [topic]
        try:
            self.consumers[session_id] = AIOKafkaConsumer(*topics, bootstrap_servers=self.bootstrap_servers,
            group_id=group_id, enable_auto_commit=True)
            await self.consumers[session_id].start()
            logger.info(f"get_or_create_consumer: Kafka consumer started, subscribed to {topics}")
            
            return session_id, self.consumers[session_id]
        except Exception as e:
            logger.error(f"get_or_create_consumer: Error creating consumer: {e}")
            return None
       

    async def close_consumer(self, session_id: str):
        """Close the Kafka consumer.
        Args:
            session_id: Session ID for the consumer
        """
        try:
            if session_id not in self.consumers:
                logger.info(f"close_consumer: Kafka consumer with session_id {session_id} not found")
                return False    
            await self.consumers[session_id].stop()
            del self.consumers[session_id]
            logger.info("close_consumer: Kafka consumer with session_id {%s} stopped and deleted", session_id)
            return True
        except Exception as e:
            logger.error(f"close_consumer: Error closing consumer: {e}")
            return False


    async def consume(self, topic: str, group_id:str = "default-group", session_id: Optional[str] = None):
        """Consume messages from the specified Kafka topics.
        Args:
            topic: Topic to consume from   
        """
        session_id, consumer = await self.get_or_create_consumer(topic, group_id, session_id)

        messages = []

        try:
            # Get a batch of messages with timeout
            batch = await consumer.getmany(timeout_ms=5000)

            for tp, msgs in batch.items():
                for msg in msgs:
                    logger.info(f"consume: Raw message received from partition {tp.partition}")
                    deserialized = self._deserialize_value(tp.topic, msg.value)
                    messages.append(deserialized)

            return messages

        except Exception as e:
            logger.error(f"consume: Error consuming messages: {e}")
            return None

        finally:
            # Close consumer
            await self.close_consumer(session_id)

           
            