import json
import psycopg2
from kafka import KafkaProducer
import time

# Kafka Configuration
KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "employee_changes"

# PostgreSQL Connection
DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5434"
}

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_changes():
    """ Fetches new rows from emp_cdc and sends them to Kafka """
    conn = psycopg2.connect(**DB_PARAMS)
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

        producer.send(TOPIC_NAME, emp_data)

        # Mark row as processed
        cur.execute("DELETE FROM emp_cdc WHERE emp_id = %s", (row[0],))

    conn.commit()
    cur.close()
    conn.close()

while True:
    fetch_changes()
    time.sleep(5)  # Poll every 5 seconds