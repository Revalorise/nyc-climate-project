import json
import csv
from kafka import KafkaConsumer


consumer = KafkaConsumer('company_details',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

header = ['company_name', 'company_address', 'company_phone']


for msg in consumer:
    print('--------------------------------')
    print('Received message from topic: company_details, writing to customer_data file...')

    row = [msg.value['company_name'],
           msg.value['company_address'],
           msg.value['company_phone']]

    with open('../data/company_details.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(row)
        print('Appended row to company_details.csv')

    print('--------------------------------')
