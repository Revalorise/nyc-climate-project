#!/bin/bash
python company_details_consumer.py &
python credit_card_details_consumer.py &
python customer_details_consumer.py &
python shop_activity_consumer.py

wait
