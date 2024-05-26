import json
import time
from kafka import KafkaProducer
from generate_data import (stream_customer_data,
                           stream_credit_card_data,
                           stream_company_data,
                           stream_shop_activity_data)

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for _ in range(100):
    print('Produced to consumer')
    producer.send('company_details', stream_company_data())
    producer.send('credit_card_details', stream_credit_card_data())
    producer.send('customer_details', stream_customer_data())
    producer.send('shop_activity', stream_shop_activity_data())
    time.sleep(60)
