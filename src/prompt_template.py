"""Natural-language prompts for driving the Kafka MCP agent.

Each helper returns a dict in the LangChain agent input shape:
    {"messages": [{"role": "user", "content": "..."}]}
"""

from langchain_core.prompts import PromptTemplate


def _user_message(content: str) -> dict:
    return {"messages": [{"role": "user", "content": content}]}


CONFIGURE_KAFKA_PROMPT = PromptTemplate(
    input_variables=["bootstrap_servers", "schema_registry_url"],
    template=(
        "Configure the Kafka connection for this session using bootstrap_servers "
        "'{bootstrap_servers}' and schema_registry_url '{schema_registry_url}'."
    ),
)

DISCONNECT_KAFKA_PROMPT = PromptTemplate(
    input_variables=[],
    template="Disconnect the current Kafka session.",
)

LIST_TOPICS_PROMPT = PromptTemplate(
    input_variables=[],
    template="List all Kafka topics in the cluster.",
)

DESCRIBE_TOPIC_PROMPT = PromptTemplate(
    input_variables=["topic_name"],
    template="Describe the topic named '{topic_name}'.",
)

GET_PARTITIONS_PROMPT = PromptTemplate(
    input_variables=["topic_name"],
    template="Get the partition layout for topic '{topic_name}'.",
)

TOPIC_EXISTS_PROMPT = PromptTemplate(
    input_variables=["topic_name"],
    template="Check whether topic '{topic_name}' exists.",
)

TOPIC_CREATION_PROMPT = PromptTemplate(
    input_variables=["topic_name", "partitions", "replication_factor"],
    template=(
        "Create a topic with name {topic_name} with {partitions} partitions "
        "and {replication_factor} replication factor."
    ),
)

DELETE_TOPIC_PROMPT = PromptTemplate(
    input_variables=["topic_name"],
    template="Delete the topic named '{topic_name}'.",
)

PRODUCER_PUBLISH_PROMPT = PromptTemplate(
    input_variables=["session_id", "topic_name", "message"],
    template=(
        "Create a producer with session id {session_id} and publish a message "
        "to topic {topic_name} with message {message}."
    ),
)

PRODUCER_ROBUST_PUBLISH_PROMPT = PromptTemplate(
    input_variables=["session_id", "topic_name", "message", "tool_name"],
    template=(
        "Use producer session id {session_id} and publish a message to topic "
        "{topic_name} with message {message}. If {session_id} is closed then "
        "start it by calling the {tool_name} tool."
    ),
)

CONSUMER_CONSUME_PROMPT = PromptTemplate(
    input_variables=["session_id", "topic_name"],
    template=(
        "Create a consumer with session id {session_id} and consume messages "
        "from topic {topic_name}."
    ),
)

LIST_SCHEMAS_PROMPT = PromptTemplate(
    input_variables=[],
    template="List all schema subjects in the Schema Registry.",
)

GET_SCHEMA_PROMPT = PromptTemplate(
    input_variables=["subject", "version"],
    template="Get schema for subject '{subject}' at version '{version}'.",
)

REGISTER_SCHEMA_PROMPT = PromptTemplate(
    input_variables=["subject", "schema_str", "schema_type"],
    template=(
        "Register schema for subject '{subject}' of type {schema_type} with "
        "definition: {schema_str}"
    ),
)

DELETE_SCHEMA_PROMPT = PromptTemplate(
    input_variables=["subject"],
    template="Delete all versions of schema subject '{subject}'.",
)


def get_configure_kafka_message(bootstrap_servers, schema_registry_url=None):
    return _user_message(
        CONFIGURE_KAFKA_PROMPT.format(
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url or "none",
        )
    )


def get_disconnect_kafka_message():
    return _user_message(DISCONNECT_KAFKA_PROMPT.format())


def get_list_topics_message():
    return _user_message(LIST_TOPICS_PROMPT.format())


def get_describe_topic_message(topic_name):
    return _user_message(DESCRIBE_TOPIC_PROMPT.format(topic_name=topic_name))


def get_partitions_message(topic_name):
    return _user_message(GET_PARTITIONS_PROMPT.format(topic_name=topic_name))


def get_topic_exists_message(topic_name):
    return _user_message(TOPIC_EXISTS_PROMPT.format(topic_name=topic_name))


def get_create_topic_message(topic_name, partitions, replication_factor):
    return _user_message(
        TOPIC_CREATION_PROMPT.format(
            topic_name=topic_name,
            partitions=partitions,
            replication_factor=replication_factor,
        )
    )


def get_delete_topic_message(topic_name):
    return _user_message(DELETE_TOPIC_PROMPT.format(topic_name=topic_name))


def get_publish_message(session_id, topic_name, message):
    return _user_message(
        PRODUCER_PUBLISH_PROMPT.format(
            session_id=session_id, topic_name=topic_name, message=message
        )
    )


def get_robust_publish_message(session_id, topic_name, message, tool_name):
    return _user_message(
        PRODUCER_ROBUST_PUBLISH_PROMPT.format(
            session_id=session_id,
            topic_name=topic_name,
            message=message,
            tool_name=tool_name,
        )
    )


def get_consume_message(session_id, topic_name):
    return _user_message(
        CONSUMER_CONSUME_PROMPT.format(session_id=session_id, topic_name=topic_name)
    )


def get_list_schemas_message():
    return _user_message(LIST_SCHEMAS_PROMPT.format())


def get_get_schema_message(subject, version="latest"):
    return _user_message(GET_SCHEMA_PROMPT.format(subject=subject, version=version))


def get_register_schema_message(subject, schema_str, schema_type="AVRO"):
    return _user_message(
        REGISTER_SCHEMA_PROMPT.format(
            subject=subject, schema_str=schema_str, schema_type=schema_type
        )
    )


def get_delete_schema_message(subject):
    return _user_message(DELETE_SCHEMA_PROMPT.format(subject=subject))
