"""Shared fixtures for tests."""

import os
import sys
import uuid

# Ensure src is in path so we can import from src.*
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def env_loaded():
    """Load .env file once per test session and disable LangSmith tracing."""
    load_dotenv()
    os.environ.setdefault("LANGCHAIN_TRACING_V2", "false")


@pytest.fixture(scope="session")
def require_openai_key():
    """Skip the test if OPENAI_API_KEY is not set."""
    if not os.getenv("OPENAI_API_KEY"):
        pytest.skip("OPENAI_API_KEY not set")


@pytest.fixture(scope="session")
def require_kafka():
    """Skip the test if Kafka is not reachable."""
    from kafka.admin import KafkaAdminClient

    bootstrap = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap, request_timeout_ms=5000
        )
        admin.close()
    except Exception:
        pytest.skip(f"Kafka not reachable at {bootstrap}")


@pytest.fixture(scope="session")
def require_schema_registry():
    """Skip the test if Schema Registry is not configured."""
    url = os.getenv("SCHEMA_REGISTRY_URL")
    if not url:
        pytest.skip("SCHEMA_REGISTRY_URL not set")
    return url


# ---------------------------------------------------------------------------
# Kafka connector (direct, no MCP)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_connector(require_kafka):
    """Create a KafkaConnector for direct service-level tests."""
    from src.service import KafkaConnector

    bootstrap = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    schema_url = os.getenv("SCHEMA_REGISTRY_URL")
    return KafkaConnector(
        bootstrap_servers=bootstrap, schema_registry_url=schema_url
    )


# ---------------------------------------------------------------------------
# MCP client / tools / agent  (requires Docker Kafka + MCP server subprocess)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
async def mcp_client(require_kafka):
    """Start the Kafka MCP server via stdio and yield an MCP client session."""
    from langchain_mcp_adapters.client import MultiServerMCPClient

    client = MultiServerMCPClient(
        {
            "kafka": {
                "command": "uv",
                "args": ["run", "src/main.py"],
                "transport": "stdio",
            },
        }
    )
    async with client as c:
        yield c


@pytest.fixture(scope="session")
async def mcp_tools(mcp_client):
    """Load MCP tools from the running Kafka MCP server."""
    tools = await mcp_client.get_tools()
    assert len(tools) > 0, "No MCP tools loaded"
    return tools


@pytest.fixture
async def mcp_agent(mcp_tools, require_openai_key):
    """Create a LangChain agent backed by MCP tools (fresh per test)."""
    from langchain.agents import create_agent

    agent = create_agent("openai:gpt-4.1", mcp_tools)
    return agent


# ---------------------------------------------------------------------------
# Topic helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def unique_topic_name(kafka_connector):
    """Factory that generates unique topic names and cleans them up after the test."""
    created = []

    def _make():
        name = f"test_{uuid.uuid4().hex[:8]}"
        created.append(name)
        return name

    yield _make

    # Cleanup
    for name in created:
        try:
            kafka_connector.delete_topic(name)
        except Exception:
            pass
