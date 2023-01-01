if __name__ == '__main__':
    from kafka import KafkaConsumer

    import json
    from _03.fraud_detection.fraud_detector import FRAUD_TOPIC

    brokers = [':9091', ':9092', ':9093']
    consumer = KafkaConsumer(FRAUD_TOPIC, bootstrap_servers=brokers)

    for msg in consumer:
        m = json.loads(msg.value.decode())
        to = m['TO']
        amount = m["AMOUNT"]
        if to == 'stranger':
            print(f'[ALERT] fraud detect payment to : {to} - {amount}')
        else:
            print(f'[PROCESSING BITCOIN] payment to :{to} - {amount}')
