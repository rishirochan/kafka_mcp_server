"""Tests for individual MCP tools via the LangChain agent.

These are integration tests that require:
- Docker Kafka running
- OPENAI_API_KEY set (for the LLM agent that invokes tools)
"""

import json
import time

import pytest
from langchain_core.messages import ToolMessage


pytestmark = [pytest.mark.integration, pytest.mark.llm]


# ---------------------------------------------------------------------------
# Topic management
# ---------------------------------------------------------------------------


class TestTopicTools:
    async def test_get_topics(self, mcp_agent):
        """Agent can list Kafka topics."""
        result = await mcp_agent.ainvoke(
            {"messages": [{"role": "user", "content": "list all kafka topics"}]}
        )
        result["messages"][-1]
        # The agent should have invoked get_topics and returned a response
        assert any(isinstance(m, ToolMessage) for m in result["messages"]), (
            "Expected at least one ToolMessage from get_topics"
        )

    async def test_create_and_delete_topic(self, mcp_agent, unique_topic_name):
        """Agent can create a topic, verify it exists, then delete it."""
        topic = unique_topic_name()

        # Create
        result = await mcp_agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": f"create a kafka topic named '{topic}' with 1 partition and replication factor 1",
                    }
                ]
            }
        )
        assert any(isinstance(m, ToolMessage) for m in result["messages"])

        # Verify exists
        result = await mcp_agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": f"check if kafka topic '{topic}' exists",
                    }
                ]
            }
        )
        tool_msgs = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        assert len(tool_msgs) > 0

        # Delete
        result = await mcp_agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": f"delete kafka topic '{topic}'",
                    }
                ]
            }
        )
        assert any(isinstance(m, ToolMessage) for m in result["messages"])

    async def test_describe_topic(self, mcp_agent, unique_topic_name):
        """Agent can describe a topic and return partition info."""
        topic = unique_topic_name()

        await mcp_agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": f"create kafka topic '{topic}' with 2 partitions",
                    }
                ]
            }
        )

        result = await mcp_agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": f"describe kafka topic '{topic}'",
                    }
                ]
            }
        )
        tool_msgs = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        assert len(tool_msgs) > 0

    async def test_is_topic_exists_false(self, mcp_agent):
        """Agent reports False for a non-existent topic."""
        result = await mcp_agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": "check if kafka topic 'nonexistent_topic_xyz_999' exists. Reply only true or false.",
                    }
                ]
            }
        )
        last_content = result["messages"][-1].content.lower()
        assert (
            "false" in last_content or "not" in last_content or "doesn" in last_content
        )


# ---------------------------------------------------------------------------
# Publish / Consume
# ---------------------------------------------------------------------------


class TestPubSubTools:
    async def test_publish_and_consume(self, mcp_agent, unique_topic_name):
        """Agent can publish a message and then consume it back."""
        topic = unique_topic_name()

        # Create topic
        await mcp_agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": f"create kafka topic '{topic}' with 1 partition",
                    }
                ]
            }
        )
        # Small delay for topic propagation
        time.sleep(2)

        # Publish
        test_msg = '{"test_key": "hello_world"}'
        await mcp_agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": f"publish this exact message to kafka topic '{topic}': {test_msg}",
                    }
                ]
            }
        )

        # Consume
        result = await mcp_agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": f"consume messages from kafka topic '{topic}' with group id 'test-group-{topic}'",
                    }
                ]
            }
        )
        tool_msgs = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        assert len(tool_msgs) > 0, "Expected ToolMessage from consume"
        # At least one tool message should reference our test data
        all_tool_text = " ".join(
            m.content if isinstance(m.content, str) else json.dumps(m.content)
            for m in tool_msgs
        )
        assert "hello_world" in all_tool_text or "test_key" in all_tool_text


# ---------------------------------------------------------------------------
# Schema Registry
# ---------------------------------------------------------------------------


@pytest.mark.schema_registry
class TestSchemaTools:
    async def test_list_schemas(self, mcp_agent, require_schema_registry):
        """Agent can list schema registry subjects."""
        result = await mcp_agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": "list all schema subjects in the schema registry",
                    }
                ]
            }
        )
        assert any(isinstance(m, ToolMessage) for m in result["messages"])

    async def test_register_and_get_schema(self, mcp_agent, require_schema_registry):
        """Agent can register an Avro schema and retrieve it."""
        subject = f"test-schema-{__import__('uuid').uuid4().hex[:8]}-value"
        schema_str = json.dumps(
            {
                "type": "record",
                "name": "TestRecord",
                "fields": [{"name": "id", "type": "int"}],
            }
        )

        # Register
        await mcp_agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": f"register an Avro schema for subject '{subject}' with this schema: {schema_str}",
                    }
                ]
            }
        )

        # Get
        result = await mcp_agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": f"get the latest schema for subject '{subject}'",
                    }
                ]
            }
        )
        tool_msgs = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        assert len(tool_msgs) > 0

        # Cleanup
        await mcp_agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": f"delete schema subject '{subject}'",
                    }
                ]
            }
        )
