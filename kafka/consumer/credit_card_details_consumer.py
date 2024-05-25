import json
import csv
from kafka import KafkaConsumer


consumer = KafkaConsumer('company_details',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

header = ['company_name', 'company_address', 'company_phone']


for msg in consumer:
    print('--------------------------------')
    print('Received message from topic: shopper, writing to customer_data file...')

    row = [msg.value['company_name'],
           msg.value['user_id'],
           msg.value['event_type'],
           msg.value['product_id'],
           msg.value['price']]

    with open('produced_data/shop_activity.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(row)
        print('Appended row to shop_activity.csv')

    print('--------------------------------')
