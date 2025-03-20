import json
import psycopg2
from kafka import KafkaConsumer

class KafkaCDCConsumer:
    """
    Kafka CDC Consumer that listens for employee changes from Kafka
    and updates the PostgreSQL destination database accordingly.
    """

    def __init__(self, kafka_broker="localhost:29092", topic_name="employee_changes", db_params=None):
        """Initialize Kafka Consumer and PostgreSQL connection."""
        if db_params is None:
            db_params = {
                "dbname": "postgres",
                "user": "postgres",
                "password": "postgres",
                "host": "localhost",
                "port": "5435"
            }

        self.kafka_broker = kafka_broker
        self.topic_name = topic_name
        self.db_params = db_params

        # Connect to PostgreSQL
        self.conn = psycopg2.connect(**self.db_params)
        self.cur = self.conn.cursor()

        # Initialize Kafka Consumer
        self.consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest"
        )

    def process_message(self, data):
        """Processes each Kafka message and updates the database."""
        print("Received:", data)

        if data["action"] == "INSERT":
            self.cur.execute("""
                INSERT INTO employees_b (emp_id, first_name, last_name, dob, city)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (emp_id) DO NOTHING
            """, (data["emp_id"], data["first_name"], data["last_name"], data["dob"], data["city"]))

        elif data["action"] == "UPDATE":
            self.cur.execute("""
                UPDATE employees_b
                SET first_name = %s, last_name = %s, dob = %s, city = %s
                WHERE emp_id = %s
            """, (data["first_name"], data["last_name"], data["dob"], data["city"], data["emp_id"]))

        elif data["action"] == "DELETE":
            self.cur.execute("DELETE FROM employees_b WHERE emp_id = %s", (data["emp_id"],))

        self.conn.commit()

    def run(self):
        """Continuously listens for messages and updates the database."""
        print(f"Listening for changes on topic '{self.topic_name}'...")

        for message in self.consumer:
            self.process_message(message.value)

        print("Finished consuming messages.")
        self.cur.close()
        self.conn.close()

if __name__ == "__main__":
    consumer = KafkaCDCConsumer()
    consumer.run()