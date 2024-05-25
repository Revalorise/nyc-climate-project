import json
import time
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for _ in range(100):
    print('Produced to consumer')
    producer.send('company_details', )
    producer.send('credit_card_details', )
    producer.send('customer_details',)
    producer.send('shop_activity',)
    time.sleep(10)
