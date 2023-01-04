import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=[':9092'])
TAXI_TRIPS_TOPIC = 'taxi-trips'

# trips 파일
with open('./trips/yellow_tripdata_2021-01.csv', 'r') as f:
    next(f)  # header 제거
    for row in f:
        print(row)
        producer.send(TAXI_TRIPS_TOPIC, row.encode('utf-8'))
        time.sleep(1)

    producer.flush()
