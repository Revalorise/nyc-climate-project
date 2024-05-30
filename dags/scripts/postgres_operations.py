import psycopg2
import os


pg_user = os.environ.get('POSTGRES_USER')
pg_password = os.environ.get('POSTGRES_PASSWORD')
db_name = os.environ.get('POSTGRES_DB')
table_name = os.environ.get('POSTGRES_TABLE')


def connect():
    conn = psycopg2.connect(f"dbname=test user=postgres")
    cur = conn.cursor()
    return cur, conn


def create_table():
    conn, cur = connect()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS electronic_purchase (
            event_time TIMESTAMP,
            order_id BIGINT PRIMARY KEY,
            product_id BIGINT,
            category_id BIGINT,
            category_code VARCHAR(255),
            brand VARCHAR(255),
            price DECIMAL(10, 2),
            user_id BIGINT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


def insert_data():
    conn, cur = connect()
    cur.execute(f"""
        COPY electronic_purchase 
        FROM '/opt/airflow/data/ecommerce-electronics-purchase-history.csv' 
        DELIMITER ',' CSV HEADER;
    """)
    conn.commit()
    cur.close()
    conn.close()
    