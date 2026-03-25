import logging
import struct
from typing import Optional

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

logger = logging.getLogger(__name__)

# Confluent wire format: magic byte (0x00) + 4-byte schema ID (big-endian)
_MAGIC_BYTE = 0x00
_HEADER_SIZE = 5


class SchemaRegistryService:
    """Wraps Confluent SchemaRegistryClient with convenience methods for
    schema management, Avro serialization, and deserialization."""

    def __init__(self, schema_registry_url: str):
        self.client = SchemaRegistryClient({"url": schema_registry_url})
        # Cache serializers/deserializers per (subject) to avoid re-creation
        self._serializers: dict[str, AvroSerializer] = {}
        self._deserializers: dict[int, AvroDeserializer] = {}

    # ========================================================================
    # Schema Management
    # ========================================================================

    def get_subjects(self) -> list[str]:
        """List all registered subjects in the Schema Registry."""
        return self.client.get_subjects()

    def get_schema(self, subject: str, version: str = "latest") -> dict:
        """Get schema details for a subject at a given version.

        Returns:
            dict with schema_id, schema_str, schema_type, version, subject.
        """
        if version == "latest":
            registered = self.client.get_latest_version(subject)
        else:
            registered = self.client.get_version(subject, int(version))
        return {
            "schema_id": registered.schema_id,
            "schema_str": registered.schema.schema_str,
            "schema_type": registered.schema.schema_type,
            "version": registered.version,
            "subject": registered.subject,
        }

    def register_schema(self, subject: str, schema_str: str, schema_type: str = "AVRO") -> int:
        """Register a new schema for a subject.

        Returns:
            int: The schema ID assigned by the registry.
        """
        schema = Schema(schema_str, schema_type)
        schema_id = self.client.register_schema(subject, schema)
        logger.info(f"Registered schema for subject '{subject}' with id {schema_id}")
        return schema_id

    def delete_subject(self, subject: str) -> list[int]:
        """Delete all versions of a subject.

        Returns:
            list[int]: List of deleted version numbers.
        """
        versions = self.client.delete_subject(subject)
        logger.info(f"Deleted subject '{subject}', versions: {versions}")
        return versions

    # ========================================================================
    # Serialization / Deserialization
    # ========================================================================

    def _get_subject_name(self, topic: str, is_key: bool = False) -> str:
        """Return the subject name using TopicNameStrategy."""
        suffix = "key" if is_key else "value"
        return f"{topic}-{suffix}"

    def _has_subject(self, subject: str) -> bool:
        """Check if a subject exists in the registry."""
        try:
            subjects = self.client.get_subjects()
            return subject in subjects
        except Exception:
            return False

    def _get_or_create_serializer(self, subject: str) -> AvroSerializer:
        """Get a cached AvroSerializer for the given subject, or create one."""
        if subject not in self._serializers:
            registered = self.client.get_latest_version(subject)
            self._serializers[subject] = AvroSerializer(
                self.client,
                registered.schema.schema_str,
            )
        return self._serializers[subject]

    def _get_or_create_deserializer(self, schema_id: int) -> AvroDeserializer:
        """Get a cached AvroDeserializer for the given schema ID, or create one."""
        if schema_id not in self._deserializers:
            schema = self.client.get_schema(schema_id)
            self._deserializers[schema_id] = AvroDeserializer(
                self.client,
                schema.schema_str,
            )
        return self._deserializers[schema_id]

    def serialize(self, topic: str, data: dict, is_key: bool = False) -> Optional[bytes]:
        """Serialize data using the Avro schema registered for the topic's subject.

        Args:
            topic: Kafka topic name (used for subject lookup via TopicNameStrategy).
            data: Dictionary to serialize.
            is_key: Whether this is a key (True) or value (False).

        Returns:
            bytes: Confluent wire format encoded bytes, or None if no schema found.
        """
        subject = self._get_subject_name(topic, is_key)
        if not self._has_subject(subject):
            return None

        serializer = self._get_or_create_serializer(subject)
        field = MessageField.KEY if is_key else MessageField.VALUE
        ctx = SerializationContext(topic, field)
        return serializer(data, ctx)

    def deserialize(self, topic: str, data: bytes, is_key: bool = False) -> Optional[dict]:
        """Deserialize Confluent wire-format Avro bytes.

        Inspects the first 5 bytes for the magic byte + schema ID header.
        If present, fetches the schema and deserializes. If absent, returns
        None to signal the caller should fall back to JSON/string decoding.

        Args:
            topic: Kafka topic name.
            data: Raw message bytes.
            is_key: Whether this is a key (True) or value (False).

        Returns:
            dict if successfully deserialized, None if not Confluent wire format.
        """
        if data is None or len(data) < _HEADER_SIZE:
            return None

        # Check for Confluent wire format magic byte
        if data[0] != _MAGIC_BYTE:
            return None

        schema_id = struct.unpack(">I", data[1:5])[0]

        try:
            deserializer = self._get_or_create_deserializer(schema_id)
            field = MessageField.KEY if is_key else MessageField.VALUE
            ctx = SerializationContext(topic, field)
            return deserializer(data, ctx)
        except Exception as e:
            logger.error(f"Failed to deserialize message with schema_id {schema_id}: {e}")
            return None
