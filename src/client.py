"""LangChain MCP client for the Kafka MCP server.

Exposes one method per server capability (tool, resource, prompt) so every
surface of the server is reachable from tests and demos. Tool invocations go
through a LangChain agent; resources and prompts are read directly from the
MCP session for exact-payload access.
"""

import argparse
import asyncio
import os
import sys
from typing import Any, Optional

from dotenv import load_dotenv
from langchain.agents import create_agent
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools

try:
    from src.prompt_template import (
        get_configure_kafka_message,
        get_consume_message,
        get_create_topic_message,
        get_delete_schema_message,
        get_delete_topic_message,
        get_describe_topic_message,
        get_disconnect_kafka_message,
        get_get_schema_message,
        get_list_schemas_message,
        get_list_topics_message,
        get_partitions_message,
        get_publish_message,
        get_register_schema_message,
        get_robust_publish_message,
        get_topic_exists_message,
    )
except ImportError:
    from prompt_template import (  # type: ignore
        get_configure_kafka_message,
        get_consume_message,
        get_create_topic_message,
        get_delete_schema_message,
        get_delete_topic_message,
        get_describe_topic_message,
        get_disconnect_kafka_message,
        get_get_schema_message,
        get_list_schemas_message,
        get_list_topics_message,
        get_partitions_message,
        get_publish_message,
        get_register_schema_message,
        get_robust_publish_message,
        get_topic_exists_message,
    )

load_dotenv()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


DEFAULT_MODEL = os.getenv("CLIENT_MODEL", "openai:gpt-4.1")
DEFAULT_SERVER_NAME = "kafka"
SAMPLE_AVRO_SCHEMA = (
    '{"type":"record","name":"Msg","fields":[{"name":"msg","type":"string"}]}'
)


def _extract_tool_messages(agent_response: dict) -> list[str]:
    """Return every tool message payload as plain strings."""
    out: list[str] = []
    for msg in agent_response.get("messages", []):
        if not isinstance(msg, ToolMessage):
            continue
        content = msg.content
        if isinstance(content, list):
            out.extend(
                item["text"] if isinstance(item, dict) and "text" in item else str(item)
                for item in content
            )
        elif isinstance(content, dict) and "text" in content:
            out.append(content["text"])
        else:
            out.append(str(content))
    return out


def _extract_messages(agent_response: dict, message_type: str) -> list[str]:
    """Return every message of the given type from an agent response."""
    type_map = {
        "human": HumanMessage,
        "ai": AIMessage,
        "system": SystemMessage,
        "tool": ToolMessage,
    }
    if message_type == "tool":
        return _extract_tool_messages(agent_response)

    cls = type_map.get(message_type)
    if cls is None:
        return [str(m.content) for m in agent_response.get("messages", [])]
    return [
        str(m.content) for m in agent_response.get("messages", []) if isinstance(m, cls)
    ]


class KafkaClient:
    """Agent-driven MCP client with one method per server capability."""

    def __init__(self, model: str = DEFAULT_MODEL):
        self.model = model
        self.mcp_client = MultiServerMCPClient(
            {
                DEFAULT_SERVER_NAME: {
                    "command": "uv",
                    "args": ["run", "src/main.py"],
                    "transport": "stdio",
                },
            }
        )
        self._session_cm = None
        self._session = None
        self._agent = None

    async def __aenter__(self) -> "KafkaClient":
        self._session_cm = self.mcp_client.session(DEFAULT_SERVER_NAME)
        self._session = await self._session_cm.__aenter__()
        tools = await load_mcp_tools(self._session)
        self._agent = create_agent(self.model, tools)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session_cm is not None:
            await self._session_cm.__aexit__(exc_type, exc, tb)
        self._session_cm = None
        self._session = None
        self._agent = None

    async def _invoke(self, prompt: dict) -> list[str]:
        if self._agent is None:
            raise RuntimeError("KafkaClient must be used as an async context manager.")
        response = await self._agent.ainvoke(prompt)
        return _extract_tool_messages(response)

    # ---- BYOK session lifecycle -------------------------------------------------

    async def configure(
        self, bootstrap_servers: str, schema_registry_url: Optional[str] = None
    ) -> list[str]:
        return await self._invoke(
            get_configure_kafka_message(bootstrap_servers, schema_registry_url)
        )

    async def disconnect(self) -> list[str]:
        return await self._invoke(get_disconnect_kafka_message())

    # ---- Topic admin ------------------------------------------------------------

    async def list_topics(self) -> list[str]:
        return await self._invoke(get_list_topics_message())

    async def describe_topic(self, topic_name: str) -> list[str]:
        return await self._invoke(get_describe_topic_message(topic_name))

    async def get_partitions(self, topic_name: str) -> list[str]:
        return await self._invoke(get_partitions_message(topic_name))

    async def topic_exists(self, topic_name: str) -> list[str]:
        return await self._invoke(get_topic_exists_message(topic_name))

    async def create_topic(
        self, topic_name: str, partitions: int = 1, replication_factor: int = 1
    ) -> list[str]:
        return await self._invoke(
            get_create_topic_message(topic_name, partitions, replication_factor)
        )

    async def delete_topic(self, topic_name: str) -> list[str]:
        return await self._invoke(get_delete_topic_message(topic_name))

    # ---- Producer / consumer ----------------------------------------------------

    async def publish(
        self, session_id: str, topic_name: str, message: str
    ) -> list[str]:
        return await self._invoke(get_publish_message(session_id, topic_name, message))

    async def robust_publish(
        self,
        session_id: str,
        topic_name: str,
        message: str,
        tool_name: str = "get_or_create_producer",
    ) -> list[str]:
        return await self._invoke(
            get_robust_publish_message(session_id, topic_name, message, tool_name)
        )

    async def consume(self, session_id: str, topic_name: str) -> list[str]:
        return await self._invoke(get_consume_message(session_id, topic_name))

    # ---- Schema Registry --------------------------------------------------------

    async def list_schemas(self) -> list[str]:
        return await self._invoke(get_list_schemas_message())

    async def get_schema(self, subject: str, version: str = "latest") -> list[str]:
        return await self._invoke(get_get_schema_message(subject, version))

    async def register_schema(
        self, subject: str, schema_str: str, schema_type: str = "AVRO"
    ) -> list[str]:
        return await self._invoke(
            get_register_schema_message(subject, schema_str, schema_type)
        )

    async def delete_schema(self, subject: str) -> list[str]:
        return await self._invoke(get_delete_schema_message(subject))

    # ---- Raw MCP surfaces (resources + prompts) ---------------------------------

    def _require_session(self):
        if self._session is None:
            raise RuntimeError("KafkaClient must be used as an async context manager.")
        return self._session

    async def list_resources(self) -> Any:
        return await self._require_session().list_resources()

    async def read_resource(self, uri: str) -> Any:
        return await self._require_session().read_resource(uri)

    async def list_prompts(self) -> Any:
        return await self._require_session().list_prompts()

    async def get_prompt(self, name: str, arguments: Optional[dict] = None) -> Any:
        return await self._require_session().get_prompt(name, arguments or {})

    async def list_tools(self) -> Any:
        return await self._require_session().list_tools()


# ---- Demos --------------------------------------------------------------------


async def run_legacy_demo() -> None:
    """Original create/publish/consume walkthrough."""
    async with KafkaClient() as client:
        print("create:", await client.create_topic("my_topic1", 2, 1))
        print(
            "publish:",
            await client.publish("producer_01", "my_topic1", '{"msg":"hello world24"}'),
        )
        print(
            "robust_publish:",
            await client.robust_publish(
                "producer_01", "my_topic1", '{"msg":"hello world25"}'
            ),
        )
        print("consume:", await client.consume("consumer_01", "my_topic1"))


async def run_smoke_test(topic: str = "smoke_topic") -> None:
    """Exercise every server surface end-to-end."""
    async with KafkaClient() as client:
        print("tools:", await client.list_tools())
        print("resources:", await client.list_resources())
        print("prompts:", await client.list_prompts())

        bootstrap = os.getenv("BOOTSTRAP_SERVERS")
        if bootstrap:
            print(
                "configure:",
                await client.configure(bootstrap, os.getenv("SCHEMA_REGISTRY_URL")),
            )

        print("exists(before):", await client.topic_exists(topic))
        print("create:", await client.create_topic(topic, 2, 1))
        print("describe:", await client.describe_topic(topic))
        print("partitions:", await client.get_partitions(topic))
        print("list_topics:", await client.list_topics())

        if os.getenv("SCHEMA_REGISTRY_URL"):
            subject = f"{topic}-value"
            print(
                "register_schema:",
                await client.register_schema(subject, SAMPLE_AVRO_SCHEMA),
            )
            print("list_schemas:", await client.list_schemas())
            print("get_schema:", await client.get_schema(subject))

        print(
            "publish:",
            await client.publish("producer_smoke", topic, '{"msg":"hello"}'),
        )
        print("consume:", await client.consume("consumer_smoke", topic))

        print("resource_topics:", await client.read_resource("kafka://topics"))
        print(
            "resource_topic_detail:",
            await client.read_resource(f"kafka://topics/{topic}"),
        )
        print("resource_schemas:", await client.read_resource("kafka://schemas"))

        print(
            "prompt_inspect_topic:",
            await client.get_prompt("inspect-topic", {"topic_name": topic}),
        )

        if os.getenv("SCHEMA_REGISTRY_URL"):
            print("delete_schema:", await client.delete_schema(f"{topic}-value"))
        print("delete_topic:", await client.delete_topic(topic))
        print("disconnect:", await client.disconnect())


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka MCP client runner")
    parser.add_argument(
        "command",
        nargs="?",
        default="demo",
        choices=["demo", "smoke"],
        help="demo: original create/publish/consume flow; smoke: exercise every capability",
    )
    parser.add_argument(
        "--topic", default="smoke_topic", help="Topic name for the smoke test"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.command == "smoke":
        asyncio.run(run_smoke_test(args.topic))
    else:
        asyncio.run(run_legacy_demo())
