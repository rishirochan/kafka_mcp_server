# Kafka MCP Server

![CI](https://github.com/rishirochan/kafka_mcp_server/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.13-blue)

A Model Context Protocol (MCP) server that provides a comprehensive suite of tools for interacting with a Kafka cluster. This server enables LLMs and other MCP clients to manage topics, partitions, and message publishing/consumption directly.

## Features

- **Topic Management**: Create, delete, list, and check the existence of Kafka topics.
- **Topic Inspection**: Describe topic details and retrieve partition information.
- **Messaging**: Publish messages to topics and consume messages from topics (supports group IDs and session management).
- **Local Infrastructure**: Includes Docker Compose configuration for a local Kafka cluster and a Kafka UI for easy monitoring.

## MCP Tools

The following tools are exposed by this server:

| Tool | Description |
| --- | --- |
| `get_topics` | List all topics in the Kafka cluster. |
| `describe_topic` | Get detailed information about a specific topic. |
| `get_partitions` | List all partitions for a given topic. |
| `is_topic_exists` | Check if a specific topic exists in the cluster. |
| `create_topic` | Create a new topic with specified partitions and replication factor. |
| `delete_topic` | Remove a topic from the cluster. |
| `publish` | Send a message to a Kafka topic (supports optional keys and session IDs). |
| `consume` | Read messages from a topic (supports consumer groups and session IDs). |

## Setup

### 1. Python Environment

Ensure you have Python 3.10+ installed. This project uses `uv` for dependency management, but you can also use `pip`.

```bash
# Using uv (recommended)
uv sync

# Using pip
pip install -r requirements.txt
```

### 2. Infrastructure

A local Kafka cluster can be started using the provided Docker Compose files in the `infra/` directory.

```bash
# Start the Kafka cluster and Kafka UI
./infra/kafka_docker_start.sh
```

- **Kafka Broker**: `localhost:9092`
- **Kafka UI**: [http://localhost:8080](http://localhost:8080)

## Usage

Run the MCP server using the following command:

```bash
python src/main.py
```

### Configuration

Environment variables can be configured in a `.env` file (see `.env.example` if available, or create one with your Kafka connection details).

- `KAFKA_BOOTSTRAP_SERVERS`: Comma-separated list of Kafka brokers (default: `localhost:9092`).

## Development

- **Source Code**: All logic is located in the `src/` directory.
- **Infrastructure**: Docker and configuration files are in `infra/`.
- **Tests**: Unit tests can be found in `test/`.

---