from langchain_core.prompts import PromptTemplate

TOPIC_CREATION_PROMPT = PromptTemplate(
    input_variables=["topic_name", "partitions", "replication_factor"],
    template="create a topic with name {topic_name} with {partitions} partitions and {replication_factor} replication factor",
)

PRODUCER_PUBLISH_PROMPT = PromptTemplate(
    input_variables=["session_id", "topic_name", "message"],
    template="create a producer with session id {session_id} and publish a message to topic {topic_name} with message {message}",
)

PRODUCER_ROBUST_PUBLISH_PROMPT = PromptTemplate(
    input_variables=["session_id", "topic_name", "message", "tool_name"],
    template="use producer session id {session_id} and publish a message to topic {topic_name} with message {message} if {session_id} closed then start the {session_id} by calling {tool_name} tool",
)

CONSUMER_CONSUME_PROMPT = PromptTemplate(
    input_variables=["session_id", "topic_name"],
    template="create consumer with session id {session_id} and consume messages from topic {topic_name}",
)


def get_create_topic_message(topic_name, partitions, replication_factor):
    content = TOPIC_CREATION_PROMPT.format(
        topic_name=topic_name,
        partitions=partitions,
        replication_factor=replication_factor,
    )
    return {"messages": [{"role": "user", "content": content}]}


def get_publish_message(session_id, topic_name, message):
    content = PRODUCER_PUBLISH_PROMPT.format(
        session_id=session_id, topic_name=topic_name, message=message
    )
    return {"messages": [{"role": "user", "content": content}]}


def get_robust_publish_message(session_id, topic_name, message, tool_name):
    content = PRODUCER_ROBUST_PUBLISH_PROMPT.format(
        session_id=session_id,
        topic_name=topic_name,
        message=message,
        tool_name=tool_name,
    )
    return {"messages": [{"role": "user", "content": content}]}


def get_consume_message(session_id, topic_name):
    content = CONSUMER_CONSUME_PROMPT.format(
        session_id=session_id, topic_name=topic_name
    )
    return {"messages": [{"role": "user", "content": content}]}
