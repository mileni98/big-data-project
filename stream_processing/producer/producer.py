import os
import time
import hdfs
import kafka.errors
from kafka import KafkaProducer
from hdfs import InsecureClient

HDFS_NAMENODE = os.environ["HDFS_NAMENODE"]
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]
TOPIC = "earthquakes"

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

client = InsecureClient(HDFS_NAMENODE)
file_path = "/user/root/data-lake/raw/batch_data.csv"

while True:
    try:
        client.status(file_path)
        break
    except Exception as e:
        print("Invalid location")
        time.sleep(3)

print(f"Reading file: {file_path}")
with client.read(file_path, encoding="utf-8", delimiter='\n') as reader:
    print("File successfully opened.")
    first_line = next(reader)  # Read the first line
    column_names = first_line.strip().split(",")

    for line in reader:

        id = line.split(",")[0]

        try:
            data = line.strip().split(",")
            row = dict(zip(column_names, data))

            print(f'sending comment to kafka topic {TOPIC}')
            print(row)

            producer.send(TOPIC, key=bytes(id, 'utf-8'),
                          value=bytes(row, 'utf-8'))
            time.sleep(10)

        except Exception as e:
            print('Exception')
