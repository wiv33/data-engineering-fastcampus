from kafka import KafkaConsumer

consumer = KafkaConsumer('first-topic', bootstrap_servers=['localhost:9092'])

for msg in consumer:
    print(msg)
