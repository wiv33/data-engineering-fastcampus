from kafka import KafkaProducer

brokers = [':9091', ":9092", ":9093"]
topic_name = 'first-cluster-topic'

producer = KafkaProducer(bootstrap_servers=brokers)

for _ in range(7):
    producer.send(topic_name, b'hello world cluster')
    
producer.flush()
