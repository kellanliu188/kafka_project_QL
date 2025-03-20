import csv
import json
from kafka import KafkaProducer

class CSVKafkaProducer:
    """
    Kafka Producer Service that reads a CSV file and sends relevant employee salary data to a Kafka topic.
    """

    def __init__(self, kafka_broker="localhost:29092", topic_name="employee_salaries", csv_file="Employee_Salaries.csv"):
        """Initialize Kafka Producer and CSV file details."""
        self.kafka_broker = kafka_broker
        self.topic_name = topic_name
        self.csv_file = csv_file

        # Initialize Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def process_csv(self):
        """Reads the CSV file, filters relevant data, and sends it to Kafka."""
        with open(self.csv_file, mode="r") as file:
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
                    self.producer.send(self.topic_name, message)

    def run(self):
        """Executes the Kafka producer pipeline."""
        print(f"Processing CSV file '{self.csv_file}' and sending messages to Kafka topic '{self.topic_name}'...")
        self.process_csv()
        self.producer.flush()
        self.producer.close()
        print("Finished producing messages.")

if __name__ == "__main__":
    producer = CSVKafkaProducer()
    producer.run()