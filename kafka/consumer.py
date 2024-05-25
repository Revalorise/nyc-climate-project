import json
import csv
import os
from pprint import pprint
from kafka import KafkaConsumer


consumer = KafkaConsumer('shopper',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

header = ['timestamp', 'user_id', 'event_type', 'product_id', 'price']


def create_csv_file_with_header():
    with open('produced_data/shop_activity.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)


def test_if_csv_file_exists():
    if not os.path.exists('produced_data/shop_activity.csv'):
        create_csv_file_with_header()
    else:
        pass


def check_csv_header():
    with open('produced_data/shop_activity.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)

        if reader.fieldnames == header:
            print('Header is correct')
        else:
            print('Header is incorrect.. fixing the issue...')
            create_csv_file_with_header()
            test_if_csv_file_exists()


for msg in consumer:
    test_if_csv_file_exists()
    print('--------------------------------')
    print('Received message from topic: shopper, writing to customer_data file...')

    row = [msg.value['timestamp'],
           msg.value['user_id'],
           msg.value['event_type'],
           msg.value['product_id'],
           msg.value['price']]

    with open('produced_data/shop_activity.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(row)
        print('Appended row to shop_activity.csv')

    print('--------------------------------')
