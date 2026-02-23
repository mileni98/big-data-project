import os
import csv
import time
import json
import kafka.errors

from kafka import KafkaProducer
from hdfs import InsecureClient


HDFS_NAMENODE = os.environ["HDFS_NAMENODE"]
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]

TOPIC = "tsunamis"
SEND_DELAY_MS = 50


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

        # Read the csv file
        csv_reader = csv.DictReader(reader)
        print(">> Headers:", csv_reader.fieldnames)

        # Loop through each line in the file
        for row in csv_reader:
            try:
                # Split line into columns
                record_id = row[csv_reader.fieldnames[0]] # Use the first column as the record ID

                print(f">> Sending comment to kafka topic {TOPIC}")
                print(f">> key={record_id} row={row}")

                # Send the record to Kafka 
                producer.send(
                    TOPIC,
                    key=str(record_id).encode("utf-8"),
                    value=json.dumps(row).encode("utf-8"), # Serialize to string, then encode
                )
                time.sleep(send_delay_ms / 1000.0)

            except Exception as e:
                print(f">> Error processing line: {row}. Error: {e}")


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
    file_path = "/user/root/data-lake/raw/tsunami_dataset.csv"
    verify_hdfs_file_exists(client, file_path)
    
    # Read the CSV file and send records to Kafka
    print(">> Starting the to send records to Kafka...")
    read_csv_and_send_to_kafka(client, producer, file_path, send_delay_ms=SEND_DELAY_MS)
    

if __name__ == "__main__":
    main()