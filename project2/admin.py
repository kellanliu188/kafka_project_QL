from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError, TopicAlreadyExistsError

class KafkaAdminManager:
    """
    Kafka Admin Manager for handling topic creation, deletion, and management.
    """

    def __init__(self, kafka_broker="localhost:29092", topic_name="employee_changes", num_partitions=1, replication_factor=1):
        """Initialize the Kafka Admin Client."""
        self.kafka_broker = kafka_broker
        self.topic_name = topic_name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor

        # Initialize Kafka Admin Client
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.kafka_broker)

    def delete_topic(self):
        """Deletes the Kafka topic if it exists."""
        try:
            self.admin_client.delete_topics([self.topic_name])
            print(f"Kafka topic '{self.topic_name}' deleted successfully.")
        except UnknownTopicOrPartitionError:
            print(f"Kafka topic '{self.topic_name}' does not exist, skipping deletion.")
        except Exception as e:
            print(f"Error deleting topic '{self.topic_name}': {e}")

    def create_topic(self):
        """Creates a Kafka topic with the specified configuration."""
        topic_list = [NewTopic(name=self.topic_name, num_partitions=self.num_partitions, replication_factor=self.replication_factor)]

        try:
            self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"Kafka topic '{self.topic_name}' created successfully.")
        except TopicAlreadyExistsError:
            print(f"Kafka topic '{self.topic_name}' already exists.")
        except Exception as e:
            print(f"Error creating topic '{self.topic_name}': {e}")

    def reset_topic(self):
        """Deletes and recreates the Kafka topic."""
        self.delete_topic()
        self.create_topic()
        self.admin_client.close()
        print("Kafka topic reset completed.")

if __name__ == "__main__":
    kafka_admin = KafkaAdminManager()
    kafka_admin.reset_topic()