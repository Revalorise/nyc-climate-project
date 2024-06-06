import os
import pandas as pd
from psycopg2 import sql
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

pg_user = os.getenv("PG_USER")
pg_password = os.getenv("PG_PASSWORD")
pg_host = os.getenv("PG_HOST")
pg_database = os.getenv("PG_DATABASE")


class PostgresOperations:
    @staticmethod
    def create_engine():
        try:
            engine = create_engine(
                f'postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}')
            return engine
        except Exception as error:
            print("Error connecting to PostgreSQL database", error)
            exit()

    @staticmethod
    def create_table():
        try:
            engine = PostgresOperations.create_engine()

            with engine.connect() as connection:
                connection.execute(text("""
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
                    """))
                connection.close()
        except Exception as error:
            print("Error while creating PostgreSQL table ", error)
            raise

    @staticmethod
    def load_data():
        try:
            engine = PostgresOperations.create_engine()
            df = pd.read_csv('/opt/airflow/data/ecommerce-electronics-purchase-history.csv')
            df.to_sql('electronic_purchase', con=engine, if_exists='replace', index=False)
        except Exception as error:
            print("Error inserting data: ", error)
            raise
