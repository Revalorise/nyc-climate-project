import json
import csv
from kafka import KafkaConsumer


consumer = KafkaConsumer('customer_details',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

header = ['first_name', 'last_name', 'address', 'email', 'username', 'password']


for msg in consumer:
    print('--------------------------------')
    print('Received message from topic: customer_details, writing to customer_data file...')

    row = [msg.value['first_name'],
           msg.value['last_name'],
           msg.value['address'],
           msg.value['email'],
           msg.value['username'],
           msg.value['password']]

    with open('../data/customer_details.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(row)
        print('Appended row to customer_details.csv')

    print('--------------------------------')
