"""LangGraph agent for healthcare data generation, validation, and Kafka production."""

from typing import Dict, Any, List, Optional

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.language_models import BaseChatModel
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import ToolNode

from src.agent_models import State, DataValidatorOutput
from src.system_prompts import data_generator_instructions, kafka_producer_instructions


# LLM instances are created lazily to allow importing this module without
# an OPENAI_API_KEY (needed for pure-logic tests on routers/helpers).
_data_generator_llm = None
_kafka_producer_llm_with_output = None


def _get_data_generator_llm():
    global _data_generator_llm
    if _data_generator_llm is None:
        _data_generator_llm = ChatOpenAI(model="gpt-4o-mini")
    return _data_generator_llm


def _get_kafka_producer_llm():
    global _kafka_producer_llm_with_output
    if _kafka_producer_llm_with_output is None:
        llm = ChatOpenAI(model="gpt-4o-mini")
        _kafka_producer_llm_with_output = llm.with_structured_output(
            DataValidatorOutput
        )
    return _kafka_producer_llm_with_output


def format_conversation(messages: List[Any]) -> str:
    """Format a list of messages into a human-readable conversation string."""
    conversation = "Conversation history:\n\n"
    for message in messages:
        if isinstance(message, HumanMessage):
            conversation += f"User: {message.content}\n"
        elif isinstance(message, AIMessage):
            text = message.content or "[Tool call]"
            conversation += f"Assistant: {text}\n"
        elif isinstance(message, ToolMessage):
            conversation += f"Tool result ({message.name}): {message.content}\n"
    return conversation


def data_generator_node(
    state: State, llm: Optional[BaseChatModel] = None
) -> Dict[str, Any]:
    """Generate healthcare data based on success criteria and feedback."""
    system_message = data_generator_instructions(state.success_criteria)
    if state.feedback_on_work:
        system_message += f"""
        Previously you thought you completed the assignment, but your reply was rejected because the success criteria was not met.
        Here is the feedback on why this was rejected:{state.feedback_on_work}
With this feedback, please continue the assignment, ensuring that you meet the success criteria or have a question for the user.
        """
    # Add or update the system message
    is_system_message = False
    messages = state.messages
    for message in messages:
        if isinstance(message, SystemMessage):
            message.content = system_message
            is_system_message = True

    if not is_system_message:
        messages = [SystemMessage(content=system_message)] + messages

    active_llm = llm if llm is not None else _get_data_generator_llm()
    response = active_llm.invoke(messages)
    return {
        "messages": [response],
    }


def kafka_producer_node(state: State) -> Dict[str, Any]:
    """Validate generated data and decide whether to produce to Kafka."""
    last_response = state.messages[-1].content
    system_message = kafka_producer_instructions(state.success_criteria)
    user_message = f"""You are a data validator for a conversation between the User and a data generator agent.
The full conversation, including any tool calls and their results, is:
{format_conversation(state.messages)}

The success criteria for this assignment is:
{state.success_criteria}

The last entry in the conversation above is what you are evaluating:
{last_response}

If the last entry is a tool result (e.g. a Kafka publish confirmation), use the full conversation to determine whether the data was valid and the tool call succeeded.
Respond with your feedback, and decide if the success criteria is met.
Also, decide if more user input is required, either because the data generator agent has a question, needs clarification, or seems to be stuck and unable to answer without help.
"""
    if state.feedback_on_work:
        user_message += f"Also, note that in a prior attempt from the data generator agent, you provided this feedback: {state.feedback_on_work}\n"
        user_message += "If you're seeing the data generator agent repeating the same mistakes, then consider responding that user input is required."
    data_validator_messages = [
        SystemMessage(content=system_message),
        HumanMessage(content=user_message),
    ]
    data_validator_response = _get_kafka_producer_llm().invoke(data_validator_messages)

    return {
        "messages": [
            {
                "role": "assistant",
                "content": f"Data validator agent feedback on data generator agent response: {data_validator_response.feedback}",
            }
        ],
        "feedback_on_work": data_validator_response.feedback,
        "success_criteria_met": data_validator_response.success_criteria_met,
        "user_input_needed": data_validator_response.user_input_needed,
    }


def data_generator_router(state: State) -> str:
    """Route after data generation: to tools if tool calls present, else to validator."""
    last_message = state.messages[-1]
    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "tools"
    else:
        return "kafka_producer_node"


def kafka_producer_router(state: State) -> str:
    """Route after validation: END on success or user input needed, else retry."""
    if state.success_criteria_met:
        return "END"
    elif state.user_input_needed:
        return "END"
    else:
        return "data_generator_node"


def build_agent_graph(tools):
    """Build and compile the LangGraph agent graph.

    Args:
        tools: List of MCP tools to use in the ToolNode.

    Returns:
        Compiled StateGraph ready for invocation.
    """
    llm_with_tools = ChatOpenAI(model="gpt-4o-mini").bind_tools(tools)

    def _data_generator_node(state: State) -> Dict[str, Any]:
        return data_generator_node(state, llm=llm_with_tools)

    graph_builder = StateGraph(State)
    graph_builder.add_node("data_generator_node", _data_generator_node)
    graph_builder.add_node("kafka_producer_node", kafka_producer_node)
    tool_node = ToolNode(tools)
    graph_builder.add_node("tools", tool_node)

    graph_builder.add_conditional_edges(
        "data_generator_node",
        data_generator_router,
        {
            "kafka_producer_node": "kafka_producer_node",
            "tools": "tools",
        },
    )
    graph_builder.add_conditional_edges(
        "kafka_producer_node",
        kafka_producer_router,
        {
            "data_generator_node": "data_generator_node",
            "END": END,
        },
    )
    graph_builder.add_edge("tools", "data_generator_node")
    graph_builder.add_edge(START, "data_generator_node")

    return graph_builder.compile()
