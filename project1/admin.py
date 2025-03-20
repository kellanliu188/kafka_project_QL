from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "employee_salaries"

admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)]

try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print(f"Kafka topic '{TOPIC_NAME}' created.")
except TopicAlreadyExistsError:
    print(f"Kafka topic '{TOPIC_NAME}' already exists.")