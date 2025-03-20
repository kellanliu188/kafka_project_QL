import json
import psycopg2
from kafka import KafkaProducer
import time

class KafkaCDCProducer:
    """
    Kafka CDC Producer that fetches changes from PostgreSQL 'emp_cdc' table
    and sends them to a Kafka topic.
    """

    def __init__(self, kafka_broker="localhost:29092", topic_name="employee_changes", db_params=None, poll_interval=5):
        """Initialize Kafka Producer and PostgreSQL connection."""
        if db_params is None:
            db_params = {
                "dbname": "postgres",
                "user": "postgres",
                "password": "postgres",
                "host": "localhost",
                "port": "5434"
            }

        self.kafka_broker = kafka_broker
        self.topic_name = topic_name
        self.db_params = db_params
        self.poll_interval = poll_interval

        # Initialize Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def fetch_changes(self):
        """Fetches new rows from emp_cdc and sends them to Kafka."""
        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()

            cur.execute("SELECT * FROM emp_cdc")
            rows = cur.fetchall()

            for row in rows:
                emp_data = {
                    "emp_id": row[0],
                    "first_name": row[1],
                    "last_name": row[2],
                    "dob": str(row[3]),
                    "city": row[4],
                    "action": row[5]
                }

                self.producer.send(self.topic_name, emp_data)

                # Mark row as processed by deleting it
                cur.execute("DELETE FROM emp_cdc WHERE emp_id = %s", (row[0],))

            conn.commit()
            cur.close()
            conn.close()

        except psycopg2.DatabaseError as e:
            print(f"Database error: {e}")

    def run(self):
        """Continuously fetch changes and send them to Kafka."""
        print(f"Kafka Producer for topic '{self.topic_name}' started...")
        while True:
            self.fetch_changes()
            time.sleep(self.poll_interval)

if __name__ == "__main__":
    producer = KafkaCDCProducer()
    producer.run()