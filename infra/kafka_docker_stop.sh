#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ ! -f "$SCRIPT_DIR/kafka_docker_compose.yml" ]; then
    echo "Error: kafka_docker_compose.yml not found in $SCRIPT_DIR"
    exit 1
fi


docker compose -f "$SCRIPT_DIR/kafka_docker_compose.yml" down