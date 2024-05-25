import json
import time
from kafka import KafkaProducer
from faker import Faker


fake = Faker()

fields = ['timestamp', 'user_id', 'event_type', 'product_id', 'price']


def generate_data_point():
    data = {
        'timestamp': fake.date_time_this_year().isoformat(),
        'user_id': fake.uuid4(),
        'event_type': fake.random_element(['purchase', 'view', 'add_to_cart']),
        'product_id': fake.uuid4(),
        'price': fake.pricetag()
    }
    return data


producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for _ in range(100):
    print('Produced to consumer')
    producer.send('shopper', generate_data_point())
    time.sleep(10)
