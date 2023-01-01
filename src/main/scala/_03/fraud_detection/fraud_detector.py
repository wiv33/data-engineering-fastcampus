PAYMENT_NAME = 'payments'

FRAUD_TOPIC = 'fraud_payments'
LEGIT_TOPIC = 'legit_payments'
if __name__ == '__main__':
    from kafka import KafkaConsumer, KafkaProducer

    import json

    brokers = [':9091', ':9092', ':9093']
    consumer = KafkaConsumer(PAYMENT_NAME, bootstrap_servers=brokers)

    producer = KafkaProducer()


    def is_suspicious(transactions) -> bool:
        # 수상한지 체크
        return transactions['PAYMENT_TYPE'] == 'BITCOIN'


    for msg in consumer:
        # {'DATE': '01/01/2023', 'TIME': '16:18:20', 'PAYMENT_TYPE': 'VISA', 'AMOUNT': 17, 'TO': 'friend'}
        msg = json.loads(msg.value.decode())
        topic = FRAUD_TOPIC if is_suspicious(msg) else LEGIT_TOPIC

        producer.send(topic, json.dumps(msg).encode('utf-8'))
        print(topic, is_suspicious(msg), msg['PAYMENT_TYPE'])
