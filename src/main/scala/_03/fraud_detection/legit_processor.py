from _03.fraud_detection.fraud_detector import LEGIT_TOPIC
if __name__ == '__main__':
    from kafka import KafkaConsumer

    import json

    broker = [':9091', ':9092', ':9093']
    consumer = KafkaConsumer(LEGIT_TOPIC, bootstrap_servers=broker)

    for msg in consumer:
        m = json.loads(msg.value.decode())
        to = m['TO']
        amount = m["AMOUNT"]

        # payment_type에 따라 처리가 달라짐
        if m['PAYMENT_TYPE'] == 'VISA':
            print(f'[VISA] payment to : {to} - {amount}')
        elif m['PAYMENT_TYPE'] == 'MASTERCARD':
            print(f'[MASTERCARD] payment to :{to} - {amount}')
        else:
            print('[ALERT] unable to process payments')
