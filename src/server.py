#!/usr/bin/env python3
"""Kafka MCP Server - MCP protocol integration for Kafka operations."""

import json
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Annotated, AsyncIterator, Dict, Optional

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from src.service import KafkaConnector
from src.validation import (
    validate_message_value,
    validate_schema_json,
    validate_topic_name,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

REQUIRE_BYOK = os.getenv("REQUIRE_BYOK", "false").lower() in ("true", "1", "yes")


@dataclass
class AppContext:
    """Application context with typed dependencies.

    Holds an optional global KafkaConnector (disabled when REQUIRE_BYOK=true)
    and a per-session registry for BYOK connectors keyed by session id.
    """

    global_connector: Optional[KafkaConnector] = None
    session_connectors: Dict[int, KafkaConnector] = field(default_factory=dict)


def _session_key(ctx: Context) -> int:
    """Derive a stable key for the current MCP session."""
    return id(ctx.session)


def _kafka(ctx: Context) -> KafkaConnector:
    """Resolve the KafkaConnector for the current request.

    Lookup order:
      1. Per-session connector (set via configure_kafka tool).
      2. Global connector (from env vars at startup).
      3. Raise if REQUIRE_BYOK is enabled and neither is available.
    """
    app: AppContext = ctx.request_context.lifespan_context
    key = _session_key(ctx)

    # 1. Per-session BYOK connector
    connector = app.session_connectors.get(key)
    if connector is not None:
        return connector

    # 2. Global connector (disabled when REQUIRE_BYOK=true)
    if app.global_connector is not None:
        return app.global_connector

    raise ValueError(
        "No Kafka connection configured for this session. "
        "Call the 'configure_kafka' tool first to provide your cluster credentials."
    )


@asynccontextmanager
async def server_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """
    Context manager to handle the lifespan of the server.
    All configuration is loaded from environment variables.
    """
    global_connector = None
    app = AppContext()
    try:
        if not REQUIRE_BYOK:
            bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
            schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL")
            global_connector = KafkaConnector(
                bootstrap_servers=bootstrap_servers,
                schema_registry_url=schema_registry_url,
            )
            app.global_connector = global_connector
            logger.info("Global Kafka connector initialized")
        else:
            logger.info("BYOK mode enabled — no global Kafka connector")
        yield app
    except Exception as e:
        logger.error(e)
        raise e
    finally:
        if global_connector is not None:
            await global_connector.close()
        # Close all BYOK session connectors
        for key, connector in list(app.session_connectors.items()):
            try:
                await connector.close()
            except Exception as e:
                logger.error(f"Error closing session connector {key}: {e}")
            app.session_connectors.pop(key, None)


# FastMCP is an alternative interface for declaring the capabilities
# of the server. Its API is based on FastAPI.
mcp_server = FastMCP("kafka-mcp-server", lifespan=server_lifespan)


# ============================================================================
# Elicitation schema for destructive operations
# ============================================================================


class ConfirmDelete(BaseModel):
    confirmed: bool


# ============================================================================
# BYOK Session Management Tools
# ============================================================================


@mcp_server.tool(
    "configure_kafka",
    description="Configure Kafka connection credentials for this session (BYOK). "
    "Must be called before any other Kafka tool when REQUIRE_BYOK is enabled. "
    "Replaces any existing session connection.",
    annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False),
)
async def configure_kafka(
    ctx: Context[ServerSession, AppContext],
    bootstrap_servers: Annotated[
        str,
        Field(
            description="Comma-separated Kafka broker addresses, e.g. 'broker1:9092,broker2:9092'"
        ),
    ],
    schema_registry_url: Annotated[
        Optional[str],
        Field(
            description="Optional Confluent Schema Registry URL, e.g. 'http://registry:8081'"
        ),
    ] = None,
):
    """Set up a per-session Kafka connection with the provided credentials.

    Args:
        bootstrap_servers: Kafka broker address(es).
        schema_registry_url: Optional Schema Registry URL.
    Returns:
        dict: Status confirmation with the configured endpoints.
    """
    app: AppContext = ctx.request_context.lifespan_context
    key = _session_key(ctx)

    # Close any existing session connector
    old = app.session_connectors.pop(key, None)
    if old is not None:
        try:
            await old.close()
        except Exception as e:
            logger.warning(f"Error closing previous session connector: {e}")

    connector = KafkaConnector(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=schema_registry_url,
    )
    app.session_connectors[key] = connector
    logger.info(f"BYOK session {key} configured: {bootstrap_servers}")
    return {
        "status": "ok",
        "bootstrap_servers": bootstrap_servers,
        "schema_registry_url": schema_registry_url,
        "message": "Kafka connection configured for this session.",
    }


@mcp_server.tool(
    "disconnect_kafka",
    description="Tear down the per-session Kafka connection established by configure_kafka. "
    "Closes all producers, consumers, and the admin client for this session.",
    annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True),
)
async def disconnect_kafka(ctx: Context[ServerSession, AppContext]):
    """Close and remove the BYOK Kafka connection for the current session.

    Returns:
        dict: Status confirmation.
    """
    app: AppContext = ctx.request_context.lifespan_context
    key = _session_key(ctx)
    connector = app.session_connectors.pop(key, None)
    if connector is None:
        return {"status": "ok", "message": "No session connection to close."}
    await connector.close()
    logger.info(f"BYOK session {key} disconnected")
    return {"status": "ok", "message": "Session Kafka connection closed."}


# ============================================================================
# Topic Management Tools
# ============================================================================


@mcp_server.tool(
    "get_topics",
    description="List all Kafka topic names in the cluster. Returns a list of strings.",
    annotations=ToolAnnotations(readOnlyHint=True),
)
def get_topics(ctx: Context[ServerSession, AppContext]):
    """Get a list of all topics in the cluster.

    Returns:
        List[str]: List of topics
    """
    return _kafka(ctx).get_topics()


@mcp_server.tool(
    "describe_topic",
    description="Return partition layout, leader IDs, replica sets, and ISR for a named Kafka topic.",
    annotations=ToolAnnotations(readOnlyHint=True),
)
def describe_topic(ctx: Context[ServerSession, AppContext], topic_name: str):
    """Describe a topic details.

    Args:
        topic_name: Name of the topic to describe
    Returns:
        Dict[str,Any]: Dict with status and topic details
    """
    validate_topic_name(topic_name)
    return _kafka(ctx).describe_topic(topic_name)


@mcp_server.tool(
    "get_partitions",
    description="Return partition count, replication factor, and per-partition leader/ISR details for a topic.",
    annotations=ToolAnnotations(readOnlyHint=True),
)
def get_partitions(ctx: Context[ServerSession, AppContext], topic_name: str):
    """Get a list of all partitions for a topic.

    Args:
        topic_name: Name of the topic to get partitions for
    Returns:
        Dict[str,Any]: Dict with status and list of partitions
    """
    validate_topic_name(topic_name)
    return _kafka(ctx).get_partitions(topic_name)


@mcp_server.tool(
    "is_topic_exists",
    description="Return true if the named topic exists in the cluster, false otherwise. Use before create or delete.",
    annotations=ToolAnnotations(readOnlyHint=True),
)
def is_topic_exists(ctx: Context[ServerSession, AppContext], topic_name: str):
    """Check if a topic exists.

    Args:
        topic_name: Name of the topic to check
    Returns:
        bool: True if the topic exists, False otherwise
    """
    validate_topic_name(topic_name)
    return _kafka(ctx).is_topic_exists(topic_name)


@mcp_server.tool(
    "create_topic",
    description="Create a Kafka topic. No-ops if the topic already exists. Returns true on creation, false if already present.",
    annotations=ToolAnnotations(
        readOnlyHint=False, destructiveHint=False, idempotentHint=True
    ),
)
def create_topic(
    ctx: Context[ServerSession, AppContext],
    topic_name: str,
    num_partitions: int = 1,
    replication_factor: int = 1,
    configs: Annotated[
        Optional[Dict[str, str]],
        Field(
            description="Optional Kafka topic configs, e.g. {'retention.ms': '86400000', 'cleanup.policy': 'delete'}. Keys must be valid Kafka topic-level configuration names."
        ),
    ] = None,
):
    """Create a new topic.
    Args:
        topic_name: Name of the topic to create
        num_partitions: Number of partitions for the topic
        replication_factor: Replication factor for the topic
        configs: Optional configuration for the topic
    Returns:
        bool: True if the topic was created, False otherwise
    """
    validate_topic_name(topic_name)
    return _kafka(ctx).create_topic(
        topic_name, num_partitions, replication_factor, configs
    )


@mcp_server.tool(
    "delete_topic",
    description="Permanently delete a Kafka topic and all its data. Irreversible. Returns true if deleted, false if topic not found.",
    annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True),
)
async def delete_topic(ctx: Context[ServerSession, AppContext], topic_name: str):
    """Delete a topic.
    Args:
        topic_name: Name of the topic to delete
    Returns:
        bool: True if the topic was deleted, False otherwise
    """
    validate_topic_name(topic_name)
    try:
        result = await ctx.elicit(
            message=f"This will permanently delete topic '{topic_name}' and all its data. This cannot be undone. Confirm?",
            schema=ConfirmDelete,
        )
        if result.action != "accept" or not result.content.get("confirmed"):
            return {
                "status": "cancelled",
                "topic": topic_name,
                "message": "Deletion cancelled.",
            }
    except Exception:
        # Client does not support elicitation — proceed without confirmation
        pass
    return _kafka(ctx).delete_topic(topic_name)


@mcp_server.tool(
    "publish",
    description="Publish one message to a Kafka topic. If Schema Registry is configured and a subject exists for this topic, the value is Avro-encoded automatically. Pass value as a JSON string for Avro topics.",
    annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False),
)
async def publish(
    ctx: Context[ServerSession, AppContext],
    topic_name: str,
    value: str,
    key: Optional[str] = None,
    session_id: Optional[str] = None,
    schema_type: Optional[str] = None,
):
    """Publish a message to the specified Kafka topic.
    Args:
        topic_name: Name of the topic to publish to
        value: Message to publish (JSON string for Avro-encoded topics)
        key: Optional key for the message
        session_id: Optional session ID for the producer
        schema_type: Optional schema type for encoding ("AVRO"). Auto-detects if not specified.
    Returns:
        dict: Delivery confirmation with topic, partition, offset, and timestamp
    """
    validate_topic_name(topic_name)
    validate_message_value(value)
    await ctx.log("info", f"Publishing to topic '{topic_name}'")
    return await _kafka(ctx).publish(topic_name, value, key, session_id, schema_type)


@mcp_server.tool(
    "consume",
    description="Poll a Kafka topic for up to max_messages messages within a 5-second window. Returns a list of deserialized message values. Returns an empty list if no messages are available.",
    annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True),
)
async def consume(
    ctx: Context[ServerSession, AppContext],
    topic_name: str,
    group_id: str = "default-group",
    session_id: Optional[str] = None,
    max_messages: Annotated[
        int,
        Field(
            description="Maximum number of messages to return. Defaults to 10. Use lower values for inspection, higher for bulk reads."
        ),
    ] = 10,
):
    """Consume messages from the specified Kafka topic.
    Args:
        topic_name: Name of the topic to consume from
        group_id: Consumer group ID
        session_id: Optional session ID for the consumer
        max_messages: Maximum number of messages to return
    Returns:
        List[str]: List of messages consumed
    """
    validate_topic_name(topic_name)
    await ctx.log(
        "info", f"Consuming from '{topic_name}' (group={group_id}, max={max_messages})"
    )
    return await _kafka(ctx).consume(topic_name, group_id, session_id, max_messages)


# ============================================================================
# Schema Registry Tools
# ============================================================================


def _require_schema_registry(kafka_connector: KafkaConnector):
    """Raise ValueError if Schema Registry is not configured."""
    if kafka_connector.schema_registry is None:
        raise ValueError(
            "Schema Registry is not configured. Set SCHEMA_REGISTRY_URL environment variable."
        )


@mcp_server.tool(
    "list_schemas",
    description="List all subject names registered in the Schema Registry. Returns an error dict if Schema Registry is not configured.",
    annotations=ToolAnnotations(readOnlyHint=True),
)
def list_schemas(ctx: Context[ServerSession, AppContext]):
    """List all schema subjects in the Schema Registry.

    Returns:
        dict: status and list of subject names
    """
    kafka_connector = _kafka(ctx)
    try:
        _require_schema_registry(kafka_connector)
        return {
            "status": "ok",
            "subjects": kafka_connector.schema_registry.get_subjects(),
        }
    except ValueError as e:
        return {"status": "error", "message": str(e)}


@mcp_server.tool(
    "get_schema",
    description="Fetch the Avro schema string, schema ID, and version for a subject. Use 'latest' or an integer version number.",
    annotations=ToolAnnotations(readOnlyHint=True),
)
def get_schema(
    ctx: Context[ServerSession, AppContext],
    subject: str,
    version: Optional[str] = "latest",
):
    """Get schema details for a subject.

    Args:
        subject: Schema subject name (e.g. "my-topic-value")
        version: Schema version ("latest" or a version number)
    Returns:
        dict: Schema details including schema_id, schema_str, schema_type, version, subject
    """
    kafka_connector = _kafka(ctx)
    try:
        _require_schema_registry(kafka_connector)
        result = kafka_connector.schema_registry.get_schema(subject, version)
        return {"status": "ok", **result}
    except ValueError as e:
        return {"status": "error", "message": str(e)}


@mcp_server.tool(
    "register_schema",
    description="Register an Avro schema string for a subject. Returns the assigned schema_id. Reuses an existing schema_id if the schema is identical.",
    annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False),
)
def register_schema(
    ctx: Context[ServerSession, AppContext],
    subject: str,
    schema_str: str,
    schema_type: str = "AVRO",
):
    """Register a new schema for a subject.

    Args:
        subject: Schema subject name (e.g. "my-topic-value")
        schema_str: The schema definition as a JSON string
        schema_type: Schema type ("AVRO" by default)
    Returns:
        dict: The schema_id assigned by the registry
    """
    validate_schema_json(schema_str)
    kafka_connector = _kafka(ctx)
    try:
        _require_schema_registry(kafka_connector)
        schema_id = kafka_connector.schema_registry.register_schema(
            subject, schema_str, schema_type
        )
        return {
            "status": "ok",
            "schema_id": schema_id,
            "subject": subject,
            "schema_type": schema_type,
        }
    except ValueError as e:
        return {"status": "error", "message": str(e)}


@mcp_server.tool(
    "delete_schema",
    description="Permanently delete all versions of a schema subject from the Schema Registry. Returns deleted version numbers.",
    annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True),
)
async def delete_schema(ctx: Context[ServerSession, AppContext], subject: str):
    """Delete all versions of a schema subject.

    Args:
        subject: Schema subject name to delete
    Returns:
        dict: List of deleted version numbers
    """
    kafka_connector = _kafka(ctx)
    try:
        _require_schema_registry(kafka_connector)
    except ValueError as e:
        return {"status": "error", "message": str(e)}

    try:
        result = await ctx.elicit(
            message=f"This will permanently delete all versions of schema subject '{subject}'. Any producers encoding against this subject will break. Confirm?",
            schema=ConfirmDelete,
        )
        if result.action != "accept" or not result.content.get("confirmed"):
            return {
                "status": "cancelled",
                "subject": subject,
                "message": "Deletion cancelled.",
            }
    except Exception:
        # Client does not support elicitation — proceed without confirmation
        pass

    deleted = kafka_connector.schema_registry.delete_subject(subject)
    return {"status": "ok", "subject": subject, "deleted_versions": deleted}


# ============================================================================
# Resources — browsable Kafka cluster state
# ============================================================================


@mcp_server.resource(
    "kafka://topics",
    name="kafka-topics",
    description="All Kafka topic names in the cluster",
    mime_type="application/json",
)
async def resource_topics(ctx: Context) -> str:
    """Return all topic names as a JSON array."""
    topics = _kafka(ctx).get_topics() or []
    return json.dumps(topics)


@mcp_server.resource(
    "kafka://topics/{topic_name}",
    name="kafka-topic-detail",
    description="Partition layout, replication factor, and ISR for a specific topic",
    mime_type="application/json",
)
async def resource_topic_detail(topic_name: str, ctx: Context) -> str:
    """Return partition details for a specific topic."""
    detail = _kafka(ctx).get_partitions(topic_name)
    return json.dumps(detail)


@mcp_server.resource(
    "kafka://schemas",
    name="kafka-schemas",
    description="All subjects registered in the Schema Registry. Returns empty list if Schema Registry is not configured.",
    mime_type="application/json",
)
async def resource_schemas(ctx: Context) -> str:
    """Return all schema subjects as a JSON array."""
    kafka_connector = _kafka(ctx)
    if kafka_connector.schema_registry is None:
        return json.dumps([])
    subjects = kafka_connector.schema_registry.get_subjects() or []
    return json.dumps(subjects)


# ============================================================================
# Prompts — canned workflow shortcuts
# ============================================================================


@mcp_server.prompt(
    name="inspect-topic",
    description="Walk through a complete topic inspection: existence check, partition layout, and sample of recent messages.",
)
def prompt_inspect_topic(topic_name: str) -> list[dict]:
    return [
        {
            "role": "user",
            "content": (
                f"Inspect Kafka topic '{topic_name}': "
                "check if it exists, describe its partition layout, "
                "then consume up to 5 recent messages and summarize what you find."
            ),
        }
    ]


@mcp_server.prompt(
    name="register-and-publish",
    description="Register an Avro schema for a topic, then publish a sample message encoded with that schema.",
)
def prompt_register_and_publish(
    topic_name: str, schema_json: str, sample_message: str
) -> list[dict]:
    return [
        {
            "role": "user",
            "content": (
                f"Register the following Avro schema for topic '{topic_name}' "
                f"(subject: '{topic_name}-value'), then publish this sample message "
                f"using that schema.\n\nSchema:\n{schema_json}\n\nSample:\n{sample_message}"
            ),
        }
    ]
