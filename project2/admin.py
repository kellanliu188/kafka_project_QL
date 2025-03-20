from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError, TopicAlreadyExistsError

# Kafka Configuration
KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "employee_changes"

# Initialize Kafka Admin Client
admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)

# Step 1: Delete the topic if it exists
try:
    admin_client.delete_topics([TOPIC_NAME])
    print(f"Kafka topic '{TOPIC_NAME}' deleted successfully.")
except UnknownTopicOrPartitionError:
    print(f"Kafka topic '{TOPIC_NAME}' does not exist, skipping deletion.")

# Step 2: Recreate the topic
topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)]
try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print(f"Kafka topic '{TOPIC_NAME}' created successfully.")
except TopicAlreadyExistsError:
    print(f"Kafka topic '{TOPIC_NAME}' already exists.")
except Exception as e:
    print(f"Error creating topic: {e}")

# Close the admin client
admin_client.close()