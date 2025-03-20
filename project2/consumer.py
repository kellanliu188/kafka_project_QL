import json
import psycopg2
from kafka import KafkaConsumer

# Kafka Configuration
KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "employee_changes"

# PostgreSQL Connection
DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5435"  # Destination database port
}

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

print("Listening for changes...")

for message in consumer:
    data = message.value
    print("Received:", data)

    if data["action"] == "INSERT":
        cur.execute("""
            INSERT INTO employees_b (emp_id, first_name, last_name, dob, city)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (emp_id) DO NOTHING
        """, (data["emp_id"], data["first_name"], data["last_name"], data["dob"], data["city"]))

    elif data["action"] == "UPDATE":
        cur.execute("""
            UPDATE employees_b
            SET first_name = %s, last_name = %s, dob = %s, city = %s
            WHERE emp_id = %s
        """, (data["first_name"], data["last_name"], data["dob"], data["city"], data["emp_id"]))

    elif data["action"] == "DELETE":
        cur.execute("DELETE FROM employees_b WHERE emp_id = %s", (data["emp_id"],))

    conn.commit()

cur.close()
conn.close()