import json
import psycopg2
from kafka import KafkaConsumer
import time

# Kafka configuration
KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "employee_salaries"

# PostgreSQL configuration
DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5435"
}

# Connect to PostgreSQL
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

# Poll for messages every 5 seconds
timeout = 5
last_message_time = time.time()

for message in consumer:
    data = message.value
    print("Message received:", data)

    # Insert into department_employee table
    cur.execute("""
        INSERT INTO department_employee (department, department_division, position_title, hire_date, salary)
        VALUES (%s, %s, %s, %s, %s)
    """, (data["department"], data["department_division"], data["position_title"], data["hire_date"], data["salary"]))

    # Update total salary per department
    cur.execute("""
        INSERT INTO department_employee_salary (department, total_salary)
        VALUES (%s, %s)
        ON CONFLICT (department)
        DO UPDATE SET total_salary = department_employee_salary.total_salary + EXCLUDED.total_salary
    """, (data["department"], data["salary"]))

    conn.commit()
    last_message_time = time.time()  # Reset timeout tracker

    # Check if no messages received within the timeout
    if time.time() - last_message_time > timeout:
        print("No messages received in the last 5 seconds.")

print("Finished consuming messages.")
cur.close()
conn.close()