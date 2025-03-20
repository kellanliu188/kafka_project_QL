from confluent_kafka.admin import AdminClient, NewTopic

class CDCClient(AdminClient):
    """
    AdminClient that manages Kafka topics, including creation, deletion, and checking existence.
    """
    def __init__(self, bootstrap_servers="localhost:29092"):
        config = {"bootstrap.servers": bootstrap_servers}
        super().__init__(config)

    def topic_exists(self, topic):
        """Check if a topic exists in Kafka."""
        metadata = self.list_topics(timeout=5)
        return topic in metadata.topics

    def create_topic(self, topic, num_partitions=1):
        """Create a Kafka topic with the specified number of partitions."""
        new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=1)  # Only 1 broker in YML
        result_dict = self.create_topics([new_topic])

        for topic, future in result_dict.items():
            try:
                future.result()  # The result itself is None
                print(f"Topic '{topic}' created with {num_partitions} partitions.")
            except Exception as e:
                print(f"Failed to create topic '{topic}': {e}")

    def delete_topic(self, topics):
        """Delete the specified Kafka topic(s)."""
        fs = self.delete_topics(topics, operation_timeout=5)

        for topic, future in fs.items():
            try:
                future.result()  # The result itself is None
                print(f"Topic '{topic}' deleted successfully.")
            except Exception as e:
                print(f"Failed to delete topic '{topic}': {e}")

if __name__ == "__main__":
    client = CDCClient()
    employee_topic_name = "employee_salaries"
    num_partitions = 1

    if client.topic_exists(employee_topic_name):
        client.delete_topic([employee_topic_name])
    else:
        client.create_topic(employee_topic_name, num_partitions)