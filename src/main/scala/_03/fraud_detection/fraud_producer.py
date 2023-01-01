TOPIC_NAME = 'payments'
if __name__ == '__main__':
    import datetime
    import random
    import json
    import time

    import pytz

    from kafka import KafkaProducer

    brokers = [':9091', ':9092', ':9093']
    producer = KafkaProducer(bootstrap_servers=brokers)


    def get_time_date():
        utc_now = pytz.utc.localize(datetime.datetime.utcnow())
        kst_now = utc_now.astimezone(pytz.timezone('Asia/Seoul'))
        _d = kst_now.strftime('%m/%d/%Y')
        _t = kst_now.strftime("%H:%M:%S")
        return _d, _t


    def generate_payment_data():
        _payment_type = random.choice(['VISA', 'MASTERCARD', 'BITCOIN'])
        _amount = random.randint(0, 100)
        _to = random.choice(['me', 'mom', 'dad', 'friend', 'stranger'])
        return _payment_type, _amount, _to


    while True:
        d, t, = get_time_date()
        payment_type, amount, to = generate_payment_data()
        new_data = {
            'DATE': d,
            'TIME': t,
            'PAYMENT_TYPE': payment_type,
            'AMOUNT': amount,
            'TO': to,
        }

        producer.send(TOPIC_NAME, json.dumps(new_data).encode('utf-8'))
        print(new_data)
        time.sleep(1)

