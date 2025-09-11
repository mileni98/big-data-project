import os
import time
import json
import hdfs
import kafka.errors

from kafka import KafkaProducer
from hdfs import InsecureClient

HDFS_NAMENODE = os.environ["HDFS_NAMENODE"]
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]
TOPIC = "earthquakes"

# Connect to Kafka Producer
while True:
    try:
        # Define the Kafka producer based on the broker addresses
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print("No brokers available:", e)
        time.sleep(3)
    except Exception as e:
        print("Error connecting to Kafka:", e)
        time.sleep(3)

# Connect to HDFS
client = InsecureClient(HDFS_NAMENODE)
file_path = "/user/root/data-lake/raw/batch_data.csv"

# Verify HDFS file exists
while True:
    try:
        client.status(file_path)
        break
    except Exception as e:
        print("Invalid location")
        time.sleep(3)

# Read the CSV file from HDFS and send each row to Kafka
print(f"Reading file: {file_path}")
with client.read(file_path, encoding="utf-8", delimiter="\n") as reader:
    print("File successfully opened.")

    # Read the header line to get column names and skip the first column (id)
    first_line = next(reader)
    column_names = [c.strip() for c in first_line.strip().split(",")[1:]]
    print("Headers:", column_names) 

    # Loop through each line in the file
    for line in reader:
        try:
            # Split line into columns
            parts = line.rstrip("\n").split(",")
            record_id = parts[0].strip()           # key
            data = [x.strip() for x in parts[1:]]  # payload

            # Pad with empty strings if data is shorter than headers
            if len(data) < len(column_names):
                data += [""] * (len(column_names) - len(data))

            # Create a row dictionary mapping column names to data
            row = dict(zip(column_names, data))

            print(f"sending comment to kafka topic {TOPIC}")
            print(f"key={record_id} row={row}")

            # Send the record to Kafka 
            producer.send(
                TOPIC,
                key=record_id.encode("utf-8"),
                value=json.dumps(row).encode("utf-8"), # Serialize to string, then encode
            )
            time.sleep(1)

        except Exception as e:
            print(f"Error processing line: {line}. Error: {e}")

