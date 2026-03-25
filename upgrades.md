# Security Review & Upgrade Plan

## Security Audit Findings

### Critical

| Finding | Details |
|---------|---------|
| API keys in `.env` | OpenAI, Groq, and LangSmith keys are present. `.gitignore` covers `.env`, but if these were ever committed to git history, they should be rotated immediately. |

### High

| Finding | Details |
|---------|---------|
| No MCP server authentication | Any client connecting via stdio/SSE can call all tools, including destructive ones (`delete_topic`, `delete_schema`). |
| Kafka connections are plaintext | No SASL or TLS configured. All listeners use `PLAINTEXT` in `kafka_docker_compose.yml`. |
| Schema Registry has no auth | Exposed on HTTP with no access controls at `0.0.0.0:8081`. |
| Docker ports bound to `0.0.0.0` | Kafka (9092), Schema Registry (8081), and Kafka UI (8080) are accessible from any network interface. |

### Medium

| Finding | Details |
|---------|---------|
| No input validation | Topic names, schema strings, and message values are not sanitized or size-limited. |
| Error messages leak infra details | Logger outputs include bootstrap server IPs and full stack traces. |
| No rate limiting | Unlimited `publish` / `create_topic` calls possible, risking DoS or disk exhaustion. |

### Low

| Finding | Details |
|---------|---------|
| Session IDs use weak entropy | Only 8 hex chars (32 bits) via `uuid.uuid4().hex[:8]`. |
| No access control distinction | Read-only tools (e.g. `get_topics`) have the same access level as destructive tools (e.g. `delete_topic`). |
| No dependency vulnerability scanning | No `pip audit` or equivalent configured. |

---

## Upgrade Plan

### 1. Kafka SASL + TLS (Broker-Level Auth)

**What:** Enable `SASL_SSL` on the Kafka listener instead of `PLAINTEXT`.

**Changes needed:**
- `infra/kafka_docker_compose.yml` — switch listener protocol map to `SASL_SSL`, configure JAAS credentials, mount TLS certs
- `src/service.py` — pass `sasl_mechanism`, `sasl_plain_username`, `sasl_plain_password`, and `ssl_context` to `AIOKafkaProducer`, `AIOKafkaConsumer`, and `KafkaAdminClient`
- `.env` — add `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_SSL_CAFILE`

### 2. Schema Registry Auth

**What:** Enable HTTP Basic Auth on the Confluent Schema Registry.

**Changes needed:**
- `infra/kafka_docker_compose.yml` — configure Schema Registry authentication settings
- `src/schema_registry.py` — pass credentials to `SchemaRegistryClient`:
  ```python
  SchemaRegistryClient({
      "url": url,
      "basic.auth.user.info": f"{username}:{password}"
  })
  ```
- `.env` — add `SCHEMA_REGISTRY_USERNAME`, `SCHEMA_REGISTRY_PASSWORD`

### 3. MCP Server Auth (SSE Transport)

**What:** Add API key or Bearer token validation for SSE connections.

**Changes needed:**
- `src/server.py` — add auth middleware that checks an `Authorization` header before accepting SSE connections
- `.env` — add `MCP_API_KEY`
- Note: stdio transport auth is typically handled by the host process (e.g. Claude Desktop), so no additional auth needed there

### 4. Tool-Level Access Control (RBAC)

**What:** Separate tools into permission tiers so not every client can perform destructive operations.

**Proposed roles:**
- `read` — `get_topics`, `describe_topic`, `get_partitions`, `is_topic_exists`, `consume`, `list_schemas`, `get_schema`
- `write` — `create_topic`, `publish`, `register_schema`
- `admin` — `delete_topic`, `delete_schema`

**Changes needed:**
- `src/server.py` — add a role check before executing each tool, based on a `MCP_ROLE` env var or per-session permission level

### 5. Input Validation

**What:** Sanitize and constrain all user-provided inputs.

**Changes needed:**
- `src/service.py` — validate topic names against Kafka's allowed pattern (`[a-zA-Z0-9._-]`, max 249 chars), cap message value size (e.g. 1MB)
- `src/schema_registry.py` — validate schema strings are valid JSON before sending to registry
- `src/server.py` — validate `schema_type` is one of the allowed values

### 6. Network Hardening (Docker)

**What:** Restrict exposed ports to localhost only.

**Changes needed:**
- `infra/kafka_docker_compose.yml` — bind ports to `127.0.0.1` (e.g. `"127.0.0.1:9092:9092"`) instead of `0.0.0.0`
- Remove or restrict Kafka UI exposure in production environments

### 7. Sanitize Logging

**What:** Prevent infrastructure details from leaking in logs.

**Changes needed:**
- `src/service.py` — remove bootstrap server addresses from `logger.info` calls, sanitize exception messages before logging
- All files — avoid logging full stack traces in production; use a log level gate

### 8. Rate Limiting

**What:** Prevent abuse via unbounded tool calls.

**Changes needed:**
- `src/server.py` — add a simple in-memory rate limiter (e.g. token bucket) per tool or per session, configurable via env vars like `RATE_LIMIT_PUBLISH_PER_MIN`
