import csv
import json
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = "localhost:29092"  # Adjust if needed
TOPIC_NAME = "employee_salaries"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read and process CSV file
with open("Employee_Salaries.csv", mode="r") as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        salary_str = row.get("Salary", "").strip()
        if not salary_str:  # Skip rows with empty salary
            print("Skipping row with empty salary:", row)
            continue
        
        try:
            salary = int(float(salary_str))  # Convert to int (round down)
        except ValueError:
            print("Skipping row with invalid salary:", row)
            continue

        department = row.get("Department", "").strip()
        hire_date_str = row.get("Initial Hire Date", "").strip()
        if not hire_date_str:
            print("Skipping row with empty hire date:", row)
            continue
        try:
            # Assuming the year is the last part of the date string after a dash.
            hire_year = int(hire_date_str.split("-")[-1])
        except Exception:
            print("Error parsing hire date for row:", row)
            continue

        # Filter only ECC, CIT, EMS and employees hired after 2010
        if department in ["ECC", "CIT", "EMS"] and hire_year > 2010:
            message = {
                "department": department,
                "department_division": row.get("Department-Division", "").strip(),
                "position_title": row.get("Position Title", "").strip(),
                "hire_date": hire_date_str,
                "salary": salary
            }
            producer.send(TOPIC_NAME, message)

print("Finished producing messages.")
producer.flush()
producer.close()