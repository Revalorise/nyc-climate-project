import datetime

from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from fake_data_generator.generate_data import (generate_customer_data,
                                               generate_product_data,
                                               generate_order_data)


MY_GX_DATA_CONTEXT = "include/great_expectations"


@dag(
    start_date=datetime.datetime(2024, 8, 1),
    schedule="@daily",
    default_args={"retries": 1}
)
def e_commerce():
    @task_group(group_id='generate_data')
    def generate_data():
        customer_data = PythonOperator(
            task_id="generate_customer_data",
            python_callable=generate_customer_data,
            op_kwargs={"num_rows": 1000}
        )
        product_data = PythonOperator(
            task_id="generate_product_data",
            python_callable=generate_product_data,
            op_kwargs={"num_rows": 1000}
        )
        order_data = PythonOperator(
            task_id="generate_order_data",
            python_callable=generate_order_data,
            op_kwargs={"num_rows": 1000}
        )

    @task_group(group_id='upload_to_raw_bucket')
    def upload_to_raw_bucket():
        upload_customer_data = LocalFilesystemToS3Operator(
            task_id="upload_customer_data",
            filename="/opt/airflow/data/customer_data.csv",
            dest_key="customer_data.csv",
            dest_bucket="bon-raw-data-17835146",
            aws_conn_id="aws_default"
        )
        upload_product_data = LocalFilesystemToS3Operator(
            task_id="upload_product_data",
            filename="/opt/airflow/data/product_data.csv",
            dest_key="product_data.csv",
            dest_bucket="bon-raw-data-17835146",
            aws_conn_id="aws_default"
        )
        upload_order_data = LocalFilesystemToS3Operator(
            task_id="upload_order_data",
            filename="/opt/airflow/data/order_data.csv",
            dest_key="order_data.csv",
            dest_bucket="bon-raw-data-17835146",
            aws_conn_id="aws_default"
        )




    generate_data() >> upload_to_raw_bucket()


dag = e_commerce()
