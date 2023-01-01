import csv
import json
import time

from kafka import KafkaProducer

brokers = [':9091', ':9092', ':9093']
producer = KafkaProducer(bootstrap_servers=brokers)

topic_name = 'trips'

with open('/Users/auto/github/data-engineering-fastcampus/data/trips/yellow_tripdata_2021-01.csv', 'r') as file:
    reader = csv.reader(file)
    header = next(reader)

    for row in reader:
        producer.send(topic_name, json.dumps(row).encode('utf-8'))
        print(row)
        time.sleep(1)
