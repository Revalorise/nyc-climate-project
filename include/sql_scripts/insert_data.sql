COPY orders (event_time, order_id, product_id, category_id, category_code, brand, price, user_id)
FROM '/opt/airflow/data/ecommerce-electronics-purchase-history.csv'
DELIMITER ','
CSV HEADER;