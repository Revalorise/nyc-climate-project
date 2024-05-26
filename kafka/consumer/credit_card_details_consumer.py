import json
import csv
from kafka import KafkaConsumer


consumer = KafkaConsumer('credit_card_details',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

header = ['card_number', 'cc_expire_date', 'cvv', 'provider']


for msg in consumer:
    print('--------------------------------')
    print('Received message from topic: credit_card_details, writing to customer_data file...')

    row = [msg.value['card_number'],
           msg.value['cc_expire_date'],
           msg.value['cvv'],
           msg.value['provider']]

    with open('../data/credit_card_details.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(row)
        print('Appended row to credit_card_details.csv')

    print('--------------------------------')
