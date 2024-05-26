import json
import csv
from kafka import KafkaConsumer


consumer = KafkaConsumer('shop_activity',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

header = ['timestamp', 'user_id', 'event_type', 'product_id', 'price']


for msg in consumer:
    print('--------------------------------')
    print('Received message from topic: shop_activity, writing to customer_data file...')

    row = [msg.value['timestamp'],
           msg.value['user_id'],
           msg.value['event_type'],
           msg.value['product_id'],
           msg.value['price']]

    with open('../../stream_data/shop_activity.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(row)
        print('Appended row to shop_activity.csv')

    print('--------------------------------')
