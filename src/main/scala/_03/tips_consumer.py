if __name__ == '__main__':
    from kafka import KafkaConsumer
    import json

    brokers = [':9091', ':9092', ':9093']
    topic_name = 'trips'

    consumer = KafkaConsumer(topic_name, bootstrap_servers=brokers)

    for msg in consumer:
        # json.loads() == string to python
        row = json.loads(msg.value.decode())
        # ,VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge,airport_fee
        # 10 == payment_type
        # 11 == fare_amount
        if float(row[11]) > 10:
            print('--over 10--')
            print(f'{row[10]} - {row[11]}')
