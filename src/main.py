import argparse
import subprocess
import sys
import os
import time
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# Ensure the repo root is on the path so package imports work when
# this file is executed as `python src/main.py`.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

COMPOSE_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "infra", "kafka_docker_compose.yml")
)
KAFKA_CONTAINER = "kafka"
KAFKA_READY_TIMEOUT = 30  # seconds
AUTO_START_KAFKA_ENV_VAR = "AUTO_START_LOCAL_KAFKA"


def _is_kafka_running() -> bool:
    """Return True if the kafka container is running."""
    result = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            f"name=^{KAFKA_CONTAINER}$",
            "--filter",
            "status=running",
            "--format",
            "{{.Names}}",
        ],
        capture_output=True,
        text=True,
    )
    return KAFKA_CONTAINER in result.stdout.splitlines()


def _start_kafka():
    """Start the Kafka Docker stack via docker compose and wait until ready."""
    print(
        f"Kafka container '{KAFKA_CONTAINER}' is not running. Starting via Docker Compose...",
        file=sys.stderr,
    )
    subprocess.run(["docker", "compose", "-f", COMPOSE_FILE, "up", "-d"], check=True)
    print("Waiting for Kafka to become ready...", end="", flush=True, file=sys.stderr)
    deadline = time.time() + KAFKA_READY_TIMEOUT
    while time.time() < deadline:
        if _is_kafka_running():
            print(" ready.", file=sys.stderr)
            return
        print(".", end="", flush=True, file=sys.stderr)
        time.sleep(2)
    print(file=sys.stderr)
    raise RuntimeError(
        f"Kafka did not become ready within {KAFKA_READY_TIMEOUT}s. "
        "Check 'docker compose logs' for details."
    )


def ensure_kafka_running():
    """Check if Kafka Docker container is running; start it if not."""
    try:
        if _is_kafka_running():
            print(
                f"Kafka container '{KAFKA_CONTAINER}' is already running.",
                file=sys.stderr,
            )
        else:
            _start_kafka()
    except FileNotFoundError:
        print(
            "Warning: 'docker' not found on PATH. Skipping infrastructure check.",
            file=sys.stderr,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to start Kafka via Docker Compose: {e}") from e


def should_auto_start_kafka() -> bool:
    """Return True when local Kafka auto-start is explicitly enabled."""
    value = os.getenv(AUTO_START_KAFKA_ENV_VAR, "false").strip().lower()
    return value in {"1", "true", "yes", "on"}


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
    from src.server import mcp_server

    if should_auto_start_kafka():
        ensure_kafka_running()

    # For SSE transport, add middleware and run with uvicorn
    if args.transport == "sse":
        import uvicorn
        from src.auth import APIKeyMiddleware

        app = mcp_server.sse_app()
        if os.getenv("MCP_API_KEY"):
            app.add_middleware(APIKeyMiddleware)
        uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("MCP_SSE_PORT", "8000")))
    else:
        mcp_server.run(transport=args.transport)


if __name__ == "__main__":
    main()
