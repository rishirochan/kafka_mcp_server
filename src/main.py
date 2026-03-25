import argparse
import subprocess
import sys
import os
import time
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# Ensure src is in path so we can import from src.service
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

COMPOSE_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', 'infra', 'kafka_docker_compose.yml')
)
KAFKA_CONTAINER = "kafka"
KAFKA_READY_TIMEOUT = 30  # seconds


def _is_kafka_running() -> bool:
    """Return True if the kafka container is running."""
    result = subprocess.run(
        ["docker", "ps", "--filter", f"name=^{KAFKA_CONTAINER}$", "--filter", "status=running", "--format", "{{.Names}}"],
        capture_output=True, text=True
    )
    return KAFKA_CONTAINER in result.stdout.splitlines()


def _start_kafka():
    """Start the Kafka Docker stack via docker compose and wait until ready."""
    print(f"Kafka container '{KAFKA_CONTAINER}' is not running. Starting via Docker Compose...")
    subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "up", "-d"],
        check=True
    )
    print("Waiting for Kafka to become ready...", end="", flush=True)
    deadline = time.time() + KAFKA_READY_TIMEOUT
    while time.time() < deadline:
        if _is_kafka_running():
            print(" ready.")
            return
        print(".", end="", flush=True)
        time.sleep(2)
    print()
    raise RuntimeError(
        f"Kafka did not become ready within {KAFKA_READY_TIMEOUT}s. "
        "Check 'docker compose logs' for details."
    )


def ensure_kafka_running():
    """Check if Kafka Docker container is running; start it if not."""
    try:
        if _is_kafka_running():
            print(f"Kafka container '{KAFKA_CONTAINER}' is already running.")
        else:
            _start_kafka()
    except FileNotFoundError:
        print("Warning: 'docker' not found on PATH. Skipping infrastructure check.", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to start Kafka via Docker Compose: {e}") from e


def main():
    """
    Main entry point for the mcp-server-kafka script.
    It runs the MCP server with a specific transport protocol.
    """

    # Parse the command-line arguments to determine the transport protocol.
    parser = argparse.ArgumentParser(description="kafka-mcp-server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
    )
    args = parser.parse_args()

    # Import is done here to make sure environment variables are loaded
    # only after we make the changes.
    from server import mcp_server

    # Check if the Kafka Docker container is running; start it if not.
    ensure_kafka_running()

    # run the mcp servers
    mcp_server.run(transport=args.transport)

if __name__ == "__main__":
    main()