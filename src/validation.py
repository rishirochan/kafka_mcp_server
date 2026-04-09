"""Input validation for Kafka MCP Server tool parameters."""

import json
import re

# Kafka limits
MAX_TOPIC_NAME_LENGTH = 249
MAX_MESSAGE_SIZE_BYTES = 1_048_576  # 1 MB
TOPIC_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._-]+$")


def validate_topic_name(topic_name: str) -> None:
    """Validate a Kafka topic name.

    Raises:
        ValueError: If the topic name is invalid.
    """
    if not topic_name or not topic_name.strip():
        raise ValueError("Topic name must not be empty.")
    if len(topic_name) > MAX_TOPIC_NAME_LENGTH:
        raise ValueError(
            f"Topic name exceeds maximum length of {MAX_TOPIC_NAME_LENGTH} characters."
        )
    if not TOPIC_NAME_PATTERN.match(topic_name):
        raise ValueError(
            "Topic name contains invalid characters. "
            "Only alphanumeric characters, dots, underscores, and hyphens are allowed."
        )


def validate_message_value(value: str) -> None:
    """Validate a message value size.

    Raises:
        ValueError: If the message exceeds 1 MB.
    """
    if len(value.encode("utf-8")) > MAX_MESSAGE_SIZE_BYTES:
        raise ValueError(
            f"Message value exceeds maximum size of {MAX_MESSAGE_SIZE_BYTES // 1_048_576} MB."
        )


def validate_schema_json(schema_str: str) -> None:
    """Validate that a schema string is parseable JSON.

    Raises:
        ValueError: If the string is not valid JSON.
    """
    try:
        json.loads(schema_str)
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError(f"Schema string is not valid JSON: {e}")
