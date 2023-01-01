from kafka import KafkaConsumer

brokers = [":9091", ":9092", ":9093"]
consumer = KafkaConsumer('first-cluster-topic', bootstrap_servers=brokers)

for msg in consumer:
    print(msg)
