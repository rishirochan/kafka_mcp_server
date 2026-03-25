from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain.agents import create_agent
from langchain_mcp_adapters.tools import load_mcp_tools
import asyncio
from dotenv import load_dotenv
import os
import sys
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage

try:
    from src.prompt_template import (
        get_create_topic_message,
        get_publish_message,
        get_robust_publish_message,
        get_consume_message,
    )
except ImportError:
    from prompt_template import (
        get_create_topic_message,
        get_publish_message,
        get_robust_publish_message,
        get_consume_message,
    )

load_dotenv()

# Ensure src is in path so we can import from src.service
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class KafkaClient:
    def __init__(self):
        self.client = MultiServerMCPClient(
            {
                "kafka": {
                    "command": "uv",
                    # Make sure to update to the full absolute path to your math_server.py file
                    "args": ["run", "src/main.py"],
                    "transport": "stdio",
                },
            }
        )
        # self.llm = ChatGroq(model="llama-3.1-70b-versatile", temperature=0.1)

    def get_messages(self, agent_response, message_type: str):
        """Get the messages from the agent response.

        Args:
            agent_response: The agent response.
            message_type: The type of message to get.

        Returns:
            The message content.
        """
        match message_type:
            case "human":
                for msg in agent_response["messages"]:
                    if isinstance(msg, HumanMessage):
                        print("human_message ", msg.content)
                        return msg.content
            case "ai":
                for msg in agent_response["messages"]:
                    if isinstance(msg, AIMessage):
                        print("ai_message ", msg.content)
                        return msg.content
            case "system":
                for msg in agent_response["messages"]:
                    if isinstance(msg, SystemMessage):
                        print("system_message ", msg.content)
                        return msg.content
            case "tool":
                tool_content = []
                for msg in agent_response["messages"]:
                    if isinstance(msg, ToolMessage):
                        if isinstance(msg.content, list):
                            for item in msg.content:
                                tool_content.append(item["text"])
                                # print("tool_message ", item["text"])
                        else:
                            tool_content.append(msg.content["text"])
                            # print("tool_message ", tool_content)
                return tool_content
            case _:
                for msg in agent_response["messages"]:
                    print("unknown_message ", msg.content)

    async def kafka_agent(self):
        """Run the Kafka agent."""
        async with self.client.session("kafka") as session:
            tools = await load_mcp_tools(session)
            # agent = create_agent(llm, tools)
            agent = create_agent("openai:gpt-4.1", tools)

            # Create Topic
            kafka_create_status = await agent.ainvoke(
                get_create_topic_message(
                    topic_name="my_topic1", partitions=2, replication_factor=1
                )
            )
            print("kafka_create_status ", kafka_create_status["messages"][-1].content)

            # Publish Message 1
            kafka_publish_status = await agent.ainvoke(
                get_publish_message(
                    session_id="producer_01",
                    topic_name="my_topic1",
                    message='{"msg":"hello world24"}',
                )
            )
            print(
                "kafka_publish_status ", self.get_messages(kafka_publish_status, "tool")
            )

            # Publish Message 2 (Robust)
            kafka_publish_status = await agent.ainvoke(
                get_robust_publish_message(
                    session_id="producer_01",
                    topic_name="my_topic1",
                    message='{"msg":"hello world25"}',
                    tool_name="get_or_create_producer",
                )
            )
            print(
                "kafka_publish_status ", self.get_messages(kafka_publish_status, "tool")
            )

            # Consume Messages
            kafka_consume_status = await agent.ainvoke(
                get_consume_message(session_id="consumer_01", topic_name="my_topic1")
            )
            print(
                "kafka_consume_status ", self.get_messages(kafka_consume_status, "tool")
            )


if __name__ == "__main__":
    asyncio.run(KafkaClient().kafka_agent())
