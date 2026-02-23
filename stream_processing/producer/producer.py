import os
import time
import json
import kafka.errors

from kafka import KafkaProducer
from hdfs import InsecureClient


HDFS_NAMENODE = os.environ["HDFS_NAMENODE"]
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]

SEND_DELAY_MS = 1
TOPIC = "earthquakes"


def create_kafka_producer() -> KafkaProducer:
    """Create and connect to a Kafka producer"""
    while True:
        try:
            # Define the Kafka producer based on the broker addresses
            producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS.split(","))
            print(">> Connected to Kafka.")
            return producer
        except kafka.errors.NoBrokersAvailable as e:
            print(">> No brokers available:", e)
            time.sleep(3)
        except Exception as e:
            print(">> Error connecting to Kafka:", e)
            time.sleep(3)
            
            
def verify_hdfs_file_exists(client: InsecureClient, file_path: str) -> None:
    """Verify that a file exists in HDFS."""
    while True:
        try:
            client.status(file_path)
            print(f">> Found file in HDFS: {file_path}")
            return
        except Exception as e:
            print(f">> Waiting for file to appear in HDFS...")
            time.sleep(3)


def read_csv_and_send_to_kafka(client: InsecureClient, producer: KafkaProducer, file_path: str, send_delay_ms: int = SEND_DELAY_MS) -> None:
    
    with client.read(file_path, encoding="utf-8", delimiter="\n") as reader:
        print(">> File successfully opened.")

        # Read the header line to get column names and skip the first column (id)
        first_line = next(reader)
        column_names = [c.strip() for c in first_line.strip().split(",")[1:]]
        print(">> Headers:", column_names) 

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

                print(f">> Sending comment to kafka topic {TOPIC}")
                print(f">> key={record_id} row={row}")

                # Send the record to Kafka 
                producer.send(
                    TOPIC,
                    key=record_id.encode("utf-8"),
                    value=json.dumps(row).encode("utf-8"), # Serialize to string, then encode
                )
                time.sleep(send_delay_ms)

            except Exception as e:
                print(f">> Error processing line: {line}. Error: {e}")


def main() -> None:
    """Main entrypoint for the producer."""
    
    # Create Kafka producer
    print(">> Connecting to Kafka...")
    producer = create_kafka_producer()

    # Connect to HDFS
    print(">> Connecting to HDFS...")
    client = InsecureClient(HDFS_NAMENODE)
    
    # Verify the CSV file exists in HDFS
    print(">> Verifying CSV file exists in HDFS...")
    file_path = "/user/root/data-lake/raw/batch_data.csv"
    verify_hdfs_file_exists(client, file_path)
    
    # Read the CSV file and send records to Kafka
    print(">> Starting the to send records to Kafka...")
    read_csv_and_send_to_kafka(client, producer, file_path, send_time_interval=1)
    

if __name__ == "__main__":
    main()