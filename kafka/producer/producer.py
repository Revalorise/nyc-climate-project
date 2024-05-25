import json
import time
from kafka import KafkaProducer
from fake_data_generator.generate_data import (generate_customer_data,
                                               generate_credit_card_data,
                                               generate_company_data,
                                               generate_shop_activity_data)


producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for _ in range(100):
    print('Produced to consumer')
    producer.send('company_details', generate_company_data(1))
    producer.send('credit_card_details', generate_credit_card_data(1))
    producer.send('customer_details', generate_customer_data(1))
    producer.send('shop_activity', generate_shop_activity_data(1))
    time.sleep(10)
