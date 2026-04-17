"""SSE API key guard and deferred header-extraction middleware."""

import os

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Validate X-API-Key or Authorization: Bearer header against MCP_API_KEY env var.

    If MCP_API_KEY is not set, all requests are allowed (no-op).
    """

    async def dispatch(self, request: Request, call_next):
        api_key = os.getenv("MCP_API_KEY")
        if not api_key:
            return await call_next(request)

        # Check X-API-Key header
        provided = request.headers.get("x-api-key")
        if not provided:
            # Check Authorization: Bearer <token>
            auth_header = request.headers.get("authorization", "")
            if auth_header.startswith("Bearer "):
                provided = auth_header[7:]

        if provided != api_key:
            return JSONResponse(
                {"error": "Invalid or missing API key"},
                status_code=401,
            )

        return await call_next(request)


class KafkaHeaderMiddleware(BaseHTTPMiddleware):
    """Extract Kafka-related headers for future SSE session configuration.

    Supported headers:
      - X-Kafka-Bootstrap-Servers
      - X-Kafka-API-Key / X-Kafka-API-Secret (reserved for SASL)
      - X-Schema-Registry-URL

    The extracted values are stored on request.state.kafka_config so
    future SSE session wiring can consume them. This middleware is
    intentionally not attached in the current runtime path.
    """

    async def dispatch(self, request: Request, call_next):
        kafka_config = {}
        bootstrap = request.headers.get("x-kafka-bootstrap-servers")
        if bootstrap:
            kafka_config["bootstrap_servers"] = bootstrap

        api_key = request.headers.get("x-kafka-api-key")
        if api_key:
            kafka_config["sasl_api_key"] = api_key

        api_secret = request.headers.get("x-kafka-api-secret")
        if api_secret:
            kafka_config["sasl_api_secret"] = api_secret

        registry_url = request.headers.get("x-schema-registry-url")
        if registry_url:
            kafka_config["schema_registry_url"] = registry_url

        if kafka_config:
            request.state.kafka_config = kafka_config

        return await call_next(request)
