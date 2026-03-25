#!/usr/bin/env python3
"""Kafka MCP Server - MCP protocol integration for Kafka operations."""

import logging
import os
from typing import AsyncIterator, Optional, Dict
from dataclasses import dataclass
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession
from contextlib import asynccontextmanager
from src.service import KafkaConnector

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class AppContext:
    """Application context with typed dependencies."""

    kafka_connector: KafkaConnector


@asynccontextmanager
async def server_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """
    Context manager to handle the lifespan of the server.
    This is used to configure the kafka connector.
    All the configuration is now loaded from the environment variables.
    Settings handle that for us.
    """
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL")
    try:
        kafka_connector = KafkaConnector(
            bootstrap_servers=bootstrap_servers, schema_registry_url=schema_registry_url
        )
        logger.info("Kafka connector initialized")
        yield AppContext(kafka_connector=kafka_connector)
    except Exception as e:
        logger.error(e)
        raise e
    finally:
        pass
        # await kafka_connector.close()


# FastMCP is an alternative interface for declaring the capabilities
# of the server. Its API is based on FastAPI.
mcp_server = FastMCP("kafka-mcp-server", lifespan=server_lifespan)


@mcp_server.tool(
    "get_topics",
    description="Use this tool when you need to get a list of all topics in the cluster",
)
def get_topics(ctx: Context[ServerSession, AppContext]):
    """Get a list of all topics in the cluster.

    Returns:
        List[str]: List of topics
    """
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    return kafka_connector.get_topics()


@mcp_server.tool(
    "describe_topic",
    description="Use this tool when you need to describe a topic details",
)
def describe_topic(ctx: Context[ServerSession, AppContext], topic_name: str):
    """Describe a topic details.

    Args:
        topic_name: Name of the topic to describe
    Returns:
        Dict[str,Any]: Dict with status and topic details
    """
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    return kafka_connector.describe_topic(topic_name)


@mcp_server.tool(
    "get_partitions",
    description="Use this tool when you need to get a list of all partitions for a topic",
)
def get_partitions(ctx: Context[ServerSession, AppContext], topic_name: str):
    """Get a list of all partitions for a topic.

    Args:
        topic_name: Name of the topic to get partitions for
    Returns:
        Dict[str,Any]: Dict with status and list of partitions
    """
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    return kafka_connector.get_partitions(topic_name)


@mcp_server.tool(
    "is_topic_exists",
    description="Use this tool when you need to check if a topic exists",
)
def is_topic_exists(ctx: Context[ServerSession, AppContext], topic_name: str):
    """Check if a topic exists.

    Args:
        topic_name: Name of the topic to check
    Returns:
        bool: True if the topic exists, False otherwise
    """
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    return kafka_connector.is_topic_exists(topic_name)


@mcp_server.tool(
    "create_topic", description="Use this tool when you need to create a new topic"
)
def create_topic(
    ctx: Context[ServerSession, AppContext],
    topic_name: str,
    num_partitions: int = 1,
    replication_factor: int = 1,
    configs: Optional[Dict[str, str]] = None,
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
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    return kafka_connector.create_topic(
        topic_name, num_partitions, replication_factor, configs
    )


@mcp_server.tool(
    "delete_topic", description="Use this tool when you need to delete a topic"
)
def delete_topic(ctx: Context[ServerSession, AppContext], topic_name: str):
    """Delete a topic.
    Args:
        topic_name: Name of the topic to delete

    Returns:
        bool: True if the topic was deleted, False otherwise
    """
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    return kafka_connector.delete_topic(topic_name)


@mcp_server.tool(
    "publish",
    description="Use this tool when you need to publish a message to a topic. If a Schema Registry is configured and a schema exists for the topic, the value will be Avro-encoded automatically.",
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
        str: Confirmation message
    """
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    return await kafka_connector.publish(
        topic_name, value, key, session_id, schema_type
    )


@mcp_server.tool(
    "consume",
    description="Use this tool when you need to consume messages from a topic",
)
async def consume(
    ctx: Context[ServerSession, AppContext],
    topic_name: str,
    group_id: str = "default-group",
    session_id: Optional[str] = None,
):
    """Consume messages from the specified Kafka topic.
    Args:
        topic_name: Name of the topic to consume from
        session_id: Optional session ID for the consumer
        group_id: Optional group ID for the consumer
    Returns:
        List[str]: List of messages consumed
    """
    # await ctx.debug(f"Consuming messages from topic {topic_name}")
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    result = await kafka_connector.consume(topic_name, group_id, session_id)
    return result


# ============================================================================
# Schema Registry Tools
# ============================================================================


@mcp_server.tool(
    "list_schemas",
    description="Use this tool to list all schema subjects registered in the Schema Registry",
)
def list_schemas(ctx: Context[ServerSession, AppContext]):
    """List all schema subjects in the Schema Registry.

    Returns:
        list[str]: List of subject names
    """
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    if kafka_connector.schema_registry is None:
        return {
            "error": "Schema Registry is not configured. Set SCHEMA_REGISTRY_URL environment variable."
        }
    return kafka_connector.schema_registry.get_subjects()


@mcp_server.tool(
    "get_schema",
    description="Use this tool to get a schema by subject name and version from the Schema Registry",
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
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    if kafka_connector.schema_registry is None:
        return {
            "error": "Schema Registry is not configured. Set SCHEMA_REGISTRY_URL environment variable."
        }
    return kafka_connector.schema_registry.get_schema(subject, version)


@mcp_server.tool(
    "register_schema",
    description="Use this tool to register a new Avro schema for a subject in the Schema Registry",
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
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    if kafka_connector.schema_registry is None:
        return {
            "error": "Schema Registry is not configured. Set SCHEMA_REGISTRY_URL environment variable."
        }
    schema_id = kafka_connector.schema_registry.register_schema(
        subject, schema_str, schema_type
    )
    return {"schema_id": schema_id, "subject": subject, "schema_type": schema_type}


@mcp_server.tool(
    "delete_schema",
    description="Use this tool to delete all versions of a schema subject from the Schema Registry",
)
def delete_schema(ctx: Context[ServerSession, AppContext], subject: str):
    """Delete all versions of a schema subject.

    Args:
        subject: Schema subject name to delete
    Returns:
        dict: List of deleted version numbers
    """
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    if kafka_connector.schema_registry is None:
        return {
            "error": "Schema Registry is not configured. Set SCHEMA_REGISTRY_URL environment variable."
        }
    deleted = kafka_connector.schema_registry.delete_subject(subject)
    return {"subject": subject, "deleted_versions": deleted}
