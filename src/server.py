#!/usr/bin/env python3
"""Kafka MCP Server - MCP protocol integration for Kafka operations."""
import asyncio
import sys
import logging
import os
from typing import List, AsyncIterator, Optional, Dict  
from dataclasses import dataclass
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession
from contextlib import asynccontextmanager
from src.service import KafkaConnector

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
    try:
        kafka_connector = KafkaConnector(bootstrap_servers=bootstrap_servers)
        logger.info("Kafka connector initialized")
        yield AppContext(kafka_connector=kafka_connector)
    except Exception as e:
        logger.error(e)
        raise e
    finally:
        pass
        #await kafka_connector.close()

# FastMCP is an alternative interface for declaring the capabilities
# of the server. Its API is based on FastAPI.
mcp_server = FastMCP("kafka-mcp-server", 
    lifespan=server_lifespan
)   

@mcp_server.tool("get_topics", description="Use this tool when you need to get a list of all topics in the cluster")
def get_topics(ctx: Context[ServerSession, AppContext]):
    """Get a list of all topics in the cluster.
    
    Returns:
        List[str]: List of topics
    """
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    return kafka_connector.get_topics()

@mcp_server.tool("describe_topic", description="Use this tool when you need to describe a topic details")
def describe_topic(ctx: Context[ServerSession, AppContext], topic_name: str):
    """Describe a topic details.
    
    Args:
        topic_name: Name of the topic to describe
    Returns:
        Dict[str,Any]: Dict with status and topic details
    """
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    return kafka_connector.describe_topic(topic_name)

@mcp_server.tool("get_partitions", description="Use this tool when you need to get a list of all partitions for a topic")
def get_partitions(ctx: Context[ServerSession, AppContext], topic_name: str):
    """Get a list of all partitions for a topic.
    
    Args:
        topic_name: Name of the topic to get partitions for
    Returns:
        Dict[str,Any]: Dict with status and list of partitions
    """
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    return kafka_connector.get_partitions(topic_name)

@mcp_server.tool("is_topic_exists", description="Use this tool when you need to check if a topic exists")
def is_topic_exists(ctx: Context[ServerSession, AppContext], topic_name: str):
    """Check if a topic exists.
    
    Args:
        topic_name: Name of the topic to check
    Returns:
        bool: True if the topic exists, False otherwise
    """
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    return kafka_connector.is_topic_exists(topic_name)  

@mcp_server.tool("create_topic", description="Use this tool when you need to create a new topic")
def create_topic(ctx: Context[ServerSession, AppContext], topic_name: str, num_partitions: int = 1, replication_factor: int = 1, configs: Optional[Dict[str, str]] = None):
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
    return kafka_connector.create_topic(topic_name, num_partitions, replication_factor, configs)
     

@mcp_server.tool("delete_topic", description="Use this tool when you need to delete a topic")
def delete_topic(ctx: Context[ServerSession, AppContext], topic_name: str):
    """Delete a topic.
    Args:
        topic_name: Name of the topic to delete

    Returns:
        bool: True if the topic was deleted, False otherwise
    """
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    return kafka_connector.delete_topic(topic_name)

@mcp_server.tool("publish", description="Use this tool when you need to publish a message to a topic")
async def publish(ctx: Context[ServerSession, AppContext], topic_name: str, value:str, key: Optional[str] = None, session_id: Optional[str] = None ):
    """Publish a message to the specified Kafka topic.
    Args:
        topic_name: Name of the topic to publish to
        value: Message to publish
        key: Optional key for the message
        session_id: Optional session ID for the producer
    Returns:
        str: Confirmation message
    """
    #ctx.debug(f"Publishing message to topic {topic_name} and message {value}")
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    return await kafka_connector.publish(topic_name, value, key, session_id)

@mcp_server.tool("consume", description="Use this tool when you need to consume messages from a topic")
async def consume(ctx: Context[ServerSession, AppContext], topic_name: str, group_id:str = "default-group", session_id: Optional[str] = None):
    """Consume messages from the specified Kafka topic.
    Args:
        topic_name: Name of the topic to consume from
        session_id: Optional session ID for the consumer
        group_id: Optional group ID for the consumer
    Returns:
        List[str]: List of messages consumed
    """
    #await ctx.debug(f"Consuming messages from topic {topic_name}")
    kafka_connector = ctx.request_context.lifespan_context.kafka_connector
    result = await kafka_connector.consume(topic_name, group_id, session_id)
    return result







    