# Kafka MCP Server

<!-- mcp-name: io.github.rishirochan/kafka-mcp-server -->

![CI](https://github.com/rishirochan/kafka_mcp_server/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.13-blue)

A Model Context Protocol (MCP) server that provides a comprehensive suite of tools for interacting with a Kafka cluster. This server enables LLMs and other MCP clients to manage topics, partitions, and message publishing/consumption directly.

Registry-ready metadata for the official MCP Registry is included in [server.json](/Users/rishirochan/Projects/kafka_mcp_server/server.json).

## Features

- **Session Configuration**: Supports global environment-based Kafka access and per-session BYOK configuration.
- **Topic Management**: Create, delete, list, and check the existence of Kafka topics.
- **Topic Inspection**: Describe topic details and retrieve partition information.
- **Messaging**: Publish messages to topics and consume messages from topics (supports group IDs and session management).
- **Schema Registry**: List, inspect, register, and delete schema subjects when Confluent Schema Registry is configured.
- **MCP Resources and Prompts**: Exposes browsable cluster resources and reusable prompt shortcuts for common workflows.
- **Local Infrastructure**: Includes Docker Compose configuration for a local Kafka cluster and a Kafka UI for easy monitoring.

## MCP Tools

The following tools are exposed by this server:

### Session Tools

| Tool | Description |
| --- | --- |
| `configure_kafka` | Configure Kafka connection settings for the current MCP session in BYOK mode. |
| `disconnect_kafka` | Close and remove the current session-scoped Kafka connection. |

### Topic Tools

| Tool | Description |
| --- | --- |
| `get_topics` | List all topics in the Kafka cluster. |
| `describe_topic` | Get detailed information about a specific topic. |
| `get_partitions` | List all partitions for a given topic. |
| `is_topic_exists` | Check if a specific topic exists in the cluster. |
| `create_topic` | Create a new topic with specified partitions and replication factor. |
| `delete_topic` | Remove a topic from the cluster. |

### Messaging Tools

| Tool | Description |
| --- | --- |
| `publish` | Send a message to a Kafka topic (supports optional keys and session IDs). |
| `consume` | Read messages from a topic (supports consumer groups and session IDs). |

### Schema Registry Tools

| Tool | Description |
| --- | --- |
| `list_schemas` | List all schema subjects registered in Schema Registry. |
| `get_schema` | Fetch schema details for a subject and version. |
| `register_schema` | Register a new schema for a subject. |
| `delete_schema` | Delete all versions of a schema subject. |

## MCP Resources

| Resource | Description |
| --- | --- |
| `kafka://topics` | JSON array of all topic names in the cluster. |
| `kafka://topics/{topic_name}` | JSON topic detail payload for one topic. |
| `kafka://schemas` | JSON array of schema subjects, or an empty list when Schema Registry is not configured. |

## MCP Prompts

| Prompt | Description |
| --- | --- |
| `inspect-topic` | Checks topic existence, inspects partitions, and consumes a small sample of messages. |
| `register-and-publish` | Registers an Avro schema for a topic and publishes a sample message with it. |

## Prerequisites

- **Python 3.13+**
- **[uv](https://docs.astral.sh/uv/)** package manager (or pip)
- **Docker Desktop** installed and running only if you want to use the optional local Kafka auto-start
- A running **Kafka broker** (local Docker or remote) at `BOOTSTRAP_SERVERS` (default: `localhost:9092`)
- _Optional_: Confluent **Schema Registry** at `SCHEMA_REGISTRY_URL`

## Setup

### 1. Python Environment

```bash
# Using uv (recommended)
uv sync

# Using pip
pip install .
```

To install with the optional agent/LangGraph demo dependencies:

```bash
uv sync --extra agent
# or
pip install ".[agent]"
```

### 2. Infrastructure

A local Kafka cluster can be started using the provided Docker Compose files in the `infra/` directory.

```bash
# Start the Kafka cluster and Kafka UI
./infra/kafka_docker_start.sh
```

If you want the MCP server to start the local Docker stack for you when it launches, set:

```bash
export AUTO_START_LOCAL_KAFKA=true
```

- **Kafka Broker**: `localhost:9092`
- **Schema Registry**: `localhost:8081`
- **Kafka UI**: [http://localhost:8080](http://localhost:8080)

## Usage

Run the MCP server using the following command:

```bash
python src/main.py
```

For SSE transport:

```bash
python src/main.py --transport sse
```

SSE is currently provided as an alternate local transport with optional API-key protection.
Kafka connection setup still uses the same supported paths as `stdio` mode:

- global environment variables such as `BOOTSTRAP_SERVERS` and `SCHEMA_REGISTRY_URL`
- the `configure_kafka` tool for per-session BYOK setup

If you run with `REQUIRE_BYOK=true`, clients must call `configure_kafka` before using Kafka tools.

### Claude Desktop Configuration

Add the following to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "kafka": {
      "command": "uv",
      "args": ["--directory", "/path/to/kafka_mcp_server", "run", "src/main.py"],
      "env": {
        "BOOTSTRAP_SERVERS": "localhost:9092"
      }
    }
  }
}
```

### Configuration

Environment variables can be configured in a `.env` file:

| Variable | Description | Default |
| --- | --- | --- |
| `BOOTSTRAP_SERVERS` | Comma-separated list of Kafka brokers | `localhost:9092` |
| `SCHEMA_REGISTRY_URL` | Confluent Schema Registry URL | _(none)_ |
| `REQUIRE_BYOK` | Disable the global connector and require per-session `configure_kafka` first | `false` |
| `AUTO_START_LOCAL_KAFKA` | Auto-start the bundled local Kafka Docker stack on server launch | `false` |
| `MCP_API_KEY` | API key for SSE transport auth (optional) | _(none — no auth)_ |
| `MCP_SSE_PORT` | Port for SSE transport | `8000` |

### SSE API Key Authentication

When running with `--transport sse`, set `MCP_API_KEY` to require authentication. Clients must provide the key via either:

- `X-API-Key: <key>` header, or
- `Authorization: Bearer <key>` header

Richer SSE-specific request-header Kafka configuration is deferred for a later release and is not part of the currently supported runtime behavior.

## Development

- **Source Code**: All logic is located in the `src/` directory.
- **Infrastructure**: Docker and configuration files are in `infra/`.
- **Tests**: Unit tests can be found in `test/`.

---
