import json
import psycopg2
from kafka import KafkaConsumer
import time

class KafkaDatabaseConsumer:
    """
    Kafka Consumer Service that listens for employee salary messages
    and updates the PostgreSQL database accordingly.
    """

    def __init__(self, kafka_broker="localhost:29092", topic_name="employee_salaries", db_params=None):
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
        print("Message received:", data)

        # Insert into department_employee table
        self.cur.execute("""
            INSERT INTO department_employee (department, department_division, position_title, hire_date, salary)
            VALUES (%s, %s, %s, %s, %s)
        """, (data["department"], data["department_division"], data["position_title"], data["hire_date"], data["salary"]))

        # Update total salary per department
        self.cur.execute("""
            INSERT INTO department_employee_salary (department, total_salary)
            VALUES (%s, %s)
            ON CONFLICT (department)
            DO UPDATE SET total_salary = department_employee_salary.total_salary + EXCLUDED.total_salary
        """, (data["department"], data["salary"]))

        self.conn.commit()

    def run(self, timeout=5):
        """Continuously listens for messages and updates the database."""
        print(f"Listening for changes on topic '{self.topic_name}'...")
        last_message_time = time.time()

        for message in self.consumer:
            self.process_message(message.value)
            last_message_time = time.time()

            # Check if no messages received within the timeout
            if time.time() - last_message_time > timeout:
                print("⏳ No messages received in the last 5 seconds.")

        print("Finished consuming messages.")
        self.cur.close()
        self.conn.close()

if __name__ == "__main__":
    consumer = KafkaDatabaseConsumer()
    consumer.run()