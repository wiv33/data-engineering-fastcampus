if __name__ == '__main__':
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    for _ in range(7):
        producer.send('first-topic', b'hello world from python')

    producer.flush()


