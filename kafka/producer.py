import json
import time
from kafka import KafkaProducer
from faker import Faker


fake = Faker()

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for _ in range(100):
    print('Produced to consumer')
    producer.send('shopper', generate_data_point())
    time.sleep(10)
