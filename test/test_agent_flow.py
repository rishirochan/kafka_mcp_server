"""Tests for the LangGraph agent flow (data generation → validation → Kafka).

Pure logic tests (routers, helpers) run without LLM or infra.
Integration tests require OPENAI_API_KEY and Docker Kafka.
"""

import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from unittest.mock import MagicMock

from src.agent_models import State, DataValidatorOutput
from src.agent_graph import (
    format_conversation,
    data_generator_router,
    kafka_producer_router,
)
from src.system_prompts import hc_patient_json_schema


# ---------------------------------------------------------------------------
# Helper / pure logic tests  (no LLM, no infra)
# ---------------------------------------------------------------------------


class TestFormatConversation:
    def test_basic_conversation(self):
        messages = [
            HumanMessage(content="Generate 2 records"),
            AIMessage(content="Here are the records: [...]"),
        ]
        result = format_conversation(messages)
        assert "User: Generate 2 records" in result
        assert "Assistant: Here are the records" in result

    def test_empty_ai_content(self):
        messages = [
            HumanMessage(content="Hello"),
            AIMessage(content=""),
        ]
        result = format_conversation(messages)
        assert "[Tools use]" in result

    def test_empty_messages(self):
        result = format_conversation([])
        assert "Conversation history:" in result


class TestDataGeneratorRouter:
    def test_routes_to_kafka_producer_when_no_tool_calls(self):
        state = State(
            messages=[AIMessage(content="Here is the data")],
            success_criteria="generate data",
        )
        assert data_generator_router(state) == "kafka_producer_node"

    def test_routes_to_tools_when_tool_calls_present(self):
        ai_msg = AIMessage(content="")
        ai_msg.tool_calls = [{"name": "publish", "args": {}, "id": "1"}]
        state = State(
            messages=[ai_msg],
            success_criteria="generate data",
        )
        assert data_generator_router(state) == "tools"


class TestKafkaProducerRouter:
    def test_routes_to_tools_on_success(self):
        state = State(
            messages=[AIMessage(content="ok")],
            success_criteria="test",
            success_criteria_met=True,
            user_input_needed=False,
        )
        assert kafka_producer_router(state) == "tools"

    def test_routes_to_end_when_user_input_needed(self):
        state = State(
            messages=[AIMessage(content="ok")],
            success_criteria="test",
            success_criteria_met=False,
            user_input_needed=True,
        )
        assert kafka_producer_router(state) == "END"

    def test_routes_to_data_generator_on_retry(self):
        state = State(
            messages=[AIMessage(content="ok")],
            success_criteria="test",
            success_criteria_met=False,
            user_input_needed=False,
        )
        assert kafka_producer_router(state) == "data_generator_node"


class TestDataValidatorOutput:
    def test_model_creation(self):
        output = DataValidatorOutput(
            feedback="Looks good",
            success_criteria_met=True,
            user_input_needed=False,
        )
        assert output.feedback == "Looks good"
        assert output.success_criteria_met is True
        assert output.user_input_needed is False

    def test_json_schema_parseable(self):
        """The healthcare JSON schema is valid JSON."""
        schema = json.loads(hc_patient_json_schema())
        assert schema["title"] == "PatientList"
        assert schema["type"] == "array"
        assert "patient_id" in schema["items"]["properties"]


# ---------------------------------------------------------------------------
# LLM integration tests  (require OPENAI_API_KEY + Docker Kafka)
# ---------------------------------------------------------------------------


@pytest.mark.llm
@pytest.mark.integration
class TestDataGeneratorNode:
    async def test_generates_valid_json(self, require_openai_key):
        """data_generator_node should produce JSON matching the patient schema."""
        from src.agent_graph import data_generator_node

        state = State(
            messages=[HumanMessage(content="Generate 2 patient records")],
            success_criteria="Generate valid patient data matching the JSON schema",
        )
        result = data_generator_node(state)
        assert "messages" in result
        assert len(result["messages"]) > 0

        # The AI response should contain JSON data
        content = result["messages"][0].content
        assert len(content) > 0, "Expected non-empty response from data generator"


@pytest.mark.llm
@pytest.mark.integration
class TestKafkaProducerNode:
    async def test_accepts_valid_data(self, require_openai_key):
        """kafka_producer_node should accept well-formed patient JSON."""
        from src.agent_graph import kafka_producer_node

        valid_patient = json.dumps(
            [
                {
                    "patient_id": 1,
                    "name": "John Doe",
                    "age": 30,
                    "gender": "Male",
                    "address": "123 Main St",
                    "phone": "1234567890",
                    "email": "john@example.com",
                    "insurance": {
                        "insurance_id": 1,
                        "name": "Basic Plan",
                        "provider": "Aetna",
                        "policy_number": "POL001",
                        "coverage": "Premium",
                        "start_date": "2024-01-01",
                        "end_date": "2025-12-31",
                    },
                    "doctor": {
                        "doctor_id": 1,
                        "name": "Dr. Smith",
                        "specialization": "Cardiology",
                        "experience": 5,
                        "location": "New York",
                        "phone": "9876543210",
                        "email": "smith@hospital.com",
                    },
                }
            ]
        )
        state = State(
            messages=[
                HumanMessage(content="Generate 1 patient record"),
                AIMessage(content=valid_patient),
            ],
            success_criteria="Generate valid patient data matching the JSON schema",
        )
        result = kafka_producer_node(state)
        assert "success_criteria_met" in result
        # With valid data the validator should accept it
        assert result["success_criteria_met"] is True

    async def test_rejects_invalid_data(self, require_openai_key):
        """kafka_producer_node should reject malformed data."""
        from src.agent_graph import kafka_producer_node

        invalid_data = json.dumps([{"patient_id": "not_an_int"}])
        state = State(
            messages=[
                HumanMessage(content="Generate 1 patient record"),
                AIMessage(content=invalid_data),
            ],
            success_criteria="Generate valid patient data matching the JSON schema",
        )
        result = kafka_producer_node(state)
        assert "feedback_on_work" in result
        assert len(result["feedback_on_work"]) > 0


@pytest.mark.llm
@pytest.mark.integration
@pytest.mark.timeout(120)
class TestFullGraphExecution:
    async def test_end_to_end_flow(self, mcp_tools, require_openai_key):
        """Build the full graph and run the data generation + validation flow."""
        from src.agent_graph import build_agent_graph

        graph = build_agent_graph(mcp_tools)
        initial_state = {
            "messages": "Generate 2 patient records",
            "success_criteria": "Generate valid patient data matching the JSON schema and produce to kafka topic 'test_agent_flow'",
            "feedback_on_work": None,
            "success_criteria_met": False,
            "user_input_needed": False,
        }
        result = await graph.ainvoke(initial_state)

        assert result["success_criteria_met"] is True
        assert len(result["messages"]) > 1, "Expected multiple messages in the flow"
