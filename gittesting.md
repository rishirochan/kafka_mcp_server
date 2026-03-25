# GitHub Testing & CI Pipeline Plan

## Current State

- **Unit tests:** `test/test_unit.py` — 9 tests using `unittest` + mocks (no Kafka needed)
- **Integration tests:** `test/test.py` — requires a running Kafka broker (gitignored)
- **CI pipeline:** None (no `.github/workflows/` directory)
- **Coverage:** Not configured
- **Broken tests:** `test_deserialize_msg` references `_deserialize_msg` which was renamed to `_deserialize_value` in the schema registry refactor. The `test_consume_success` test also needs updating since consumers no longer use `value_deserializer` kwarg.

---

## Step 1: Fix Existing Unit Tests

**File:** `test/test_unit.py`

Tests that need updating after the schema registry refactor:

| Test | Issue | Fix |
|------|-------|-----|
| `test_deserialize_msg` | References removed `_deserialize_msg` method | Update to test `_deserialize_value(topic, raw_bytes)` instead |
| `test_consume_success` | Mock consumer returns `msg.value = "test_message"` (string), but consumer now receives raw bytes and calls `_deserialize_value` | Mock should return `b'test_message'` and assert deserialized output |
| `test_publish_success` | Producer no longer receives `value_serializer` kwarg, and `publish()` now calls `_serialize_value` internally | Verify `send_and_wait` receives bytes, not strings |

---

## Step 2: Add New Unit Tests

### Schema Registry tests (`test/test_schema_registry.py`)

| Test | What it covers |
|------|---------------|
| `test_get_subjects` | Mock `SchemaRegistryClient.get_subjects()` returns list |
| `test_get_schema_latest` | Mock `get_latest_version()`, verify returned dict structure |
| `test_get_schema_specific_version` | Mock `get_version()` with numeric version |
| `test_register_schema` | Mock `register_schema()`, verify schema_id returned |
| `test_delete_subject` | Mock `delete_subject()`, verify versions list returned |
| `test_serialize_with_schema` | Mock subject lookup + AvroSerializer, verify Confluent wire format bytes returned |
| `test_serialize_no_schema` | No subject registered, verify returns `None` (triggers fallback) |
| `test_deserialize_avro` | Provide bytes with magic byte `0x00` + schema ID header, verify dict returned |
| `test_deserialize_not_avro` | Provide plain JSON bytes (no magic byte), verify returns `None` (triggers fallback) |
| `test_deserialize_empty` | `None` or too-short bytes, verify returns `None` |

### Serde integration tests (`test/test_unit.py` additions)

| Test | What it covers |
|------|---------------|
| `test_serialize_value_json_fallback` | No schema registry configured, dict value serialized as JSON bytes |
| `test_serialize_value_string_fallback` | No schema registry configured, string value serialized as UTF-8 bytes |
| `test_serialize_value_json_string_input` | String input that is valid JSON gets parsed to dict before serialization |
| `test_deserialize_value_json` | Raw JSON bytes deserialized to dict |
| `test_deserialize_value_plain_string` | Non-JSON bytes deserialized to string |
| `test_deserialize_value_none` | `None` input returns `None` |
| `test_serialize_key` | Key string encoded to UTF-8 bytes |
| `test_serialize_key_none` | `None` key returns `None` |

### Server tool tests (`test/test_server.py`)

| Test | What it covers |
|------|---------------|
| `test_list_schemas_no_registry` | Returns error dict when schema registry not configured |
| `test_get_schema_no_registry` | Returns error dict when schema registry not configured |
| `test_register_schema_no_registry` | Returns error dict when schema registry not configured |
| `test_delete_schema_no_registry` | Returns error dict when schema registry not configured |

---

## Step 3: Switch to pytest

The project already has `pytest` as a dependency. Migrate from `unittest.main()` to pytest for better output, fixtures, and plugin support.

**Changes:**
- Add `conftest.py` in `test/` with shared fixtures (mock `KafkaConnector`, mock `SchemaRegistryService`)
- Add `pytest.ini` or `[tool.pytest.ini_options]` in `pyproject.toml`:
  ```toml
  [tool.pytest.ini_options]
  testpaths = ["test"]
  python_files = ["test_unit.py", "test_schema_registry.py", "test_server.py"]
  asyncio_mode = "auto"
  ```
- Add `pytest-asyncio` and `pytest-cov` to dev dependencies

---

## Step 4: Add Coverage

**Dependencies:** `pytest-cov`

**Command:** `pytest --cov=src --cov-report=xml --cov-report=term-missing`

**pyproject.toml addition:**
```toml
[tool.coverage.run]
source = ["src"]
omit = ["test/*", ".venv/*"]
```

---

## Step 5: GitHub Actions CI Workflow

Create `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [master]
  pull_request:
    branches: [master]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.13"]

    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v4

      - name: Set up Python ${{ matrix.python-version }}
        run: uv python install ${{ matrix.python-version }}

      - name: Install dependencies
        run: uv sync --dev

      - name: Run unit tests with coverage
        run: uv run pytest test/test_unit.py test/test_schema_registry.py test/test_server.py --cov=src --cov-report=xml --cov-report=term-missing -v

      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v4
        with:
          file: ./coverage.xml
          token: ${{ secrets.CODECOV_TOKEN }}

  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v4

      - name: Set up Python
        run: uv python install 3.13

      - name: Install dependencies
        run: uv sync --dev

      - name: Run ruff linter
        run: uv run ruff check src/

      - name: Run ruff formatter check
        run: uv run ruff format --check src/
```

---

## Step 6: GitHub Badges

### Badge setup

Add these to the top of `README.md` once CI is live:

| Badge | Source | Setup |
|-------|--------|-------|
| CI status | GitHub Actions | Automatic — `![CI](https://github.com/<user>/<repo>/actions/workflows/ci.yml/badge.svg)` |
| Coverage | Codecov | Sign up at codecov.io, link repo, add `CODECOV_TOKEN` to repo secrets |
| Python version | shields.io | Static — `![Python](https://img.shields.io/badge/python-3.13-blue)` |
| License | shields.io | Add a LICENSE file, then `![License](https://img.shields.io/github/license/<user>/<repo>)` |
| PyPI version | shields.io | Only if published to PyPI |

### Codecov setup
1. Go to [codecov.io](https://codecov.io) and sign in with GitHub
2. Add the repository
3. Copy the upload token
4. Add it as `CODECOV_TOKEN` in GitHub repo Settings > Secrets and variables > Actions

---

## Step 7: Dev Dependencies to Add

Add to `pyproject.toml` under a `[project.optional-dependencies]` or `[dependency-groups]` section:

```toml
[dependency-groups]
dev = [
    "pytest>=9.0.2",
    "pytest-asyncio>=0.24.0",
    "pytest-cov>=6.0.0",
    "ruff>=0.8.0",
]
```

Move `pytest` from main dependencies to the dev group (it shouldn't be a runtime dependency).

---

## Summary: Files to Create/Modify

| File | Action |
|------|--------|
| `test/test_unit.py` | Fix broken tests, add serde tests |
| `test/test_schema_registry.py` | Create — SchemaRegistryService unit tests |
| `test/test_server.py` | Create — MCP tool guard tests |
| `test/conftest.py` | Create — shared pytest fixtures |
| `.github/workflows/ci.yml` | Create — CI pipeline |
| `pyproject.toml` | Add pytest config, coverage config, dev dependencies, move pytest out of main deps |
| `README.md` | Add badges once CI is green |

## Order of Operations

1. Fix broken tests in `test_unit.py`
2. Add new test files (`test_schema_registry.py`, `test_server.py`, `conftest.py`)
3. Add pytest + coverage config to `pyproject.toml`
4. Run tests locally to verify all pass
5. Create `.github/workflows/ci.yml`
6. Commit and push
7. Set up Codecov and add badge to README
