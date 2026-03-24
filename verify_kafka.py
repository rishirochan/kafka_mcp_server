from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable

try:
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092", 
        client_id='test'
    )
    print("Successfully connected to Kafka Broker at localhost:9092")
    print("Cluster Metadata:", admin_client.describe_cluster())
    admin_client.close()
except NoBrokersAvailable:
    print("Failed to connect to Kafka Broker at localhost:9092")
except Exception as e:
    print(f"An error occurred: {e}")
