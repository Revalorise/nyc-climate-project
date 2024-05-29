import datetime
import zipfile
import os

from dotenv import load_dotenv
from airflow.decorators import dag, task, task_group
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.postgres.operators.postgres import PostgresOperator

load_dotenv()
MY_GX_DATA_CONTEXT = "include/great_expectations"

KAGGLE_USERNAME = os.getenv('KAGGLE_USERNAME')
KAGGLE_KEY = os.getenv('KAGGLE_KEY')

zip_path = "/opt/airflow/data/ecommerce-purchase-history-from-electronics-store.zip"
csv_path = "/opt/airflow/data/kz.csv"
data_dest_path = "/opt/airflow/data/"


@dag(
    start_date=datetime.datetime(2024, 5, 1),
    schedule="@daily",
    default_args={"retries": 1}
)
def e_commerce():
    @task.bash
    def download_ecommerce_data():
        return f"""
            export KAGGLE_USERNAME={KAGGLE_USERNAME} && \
            export KAGGLE_KEY={KAGGLE_KEY} && \
            kaggle datasets download -d mkechinov/ecommerce-purchase-history-from-electronics-store -p $AIRFLOW_HOME/data && \
            kaggle datasets metadata -d mkechinov/ecommerce-purchase-history-from-electronics-store -p $AIRFLOW_HOME/data/metadata
        """

    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename="/opt/airflow/data/ecommerce-purchase-history-from-electronics-store.zip",
        dest_key="ecommerce-purchase-history-from-electronics-store.zip",
        dest_bucket='bon-raw-data-17835146',
        aws_conn_id="aws_default",
        replace=True
    )

    @task_group(group_id="first_data_processing")
    def first_data_processing():

        @task(task_id="unzip_data")
        def unzip_data(zip_path, dest_path):
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(dest_path)

        unzip_data = unzip_data(zip_path, data_dest_path)

        @task(task_id="remove_zip_file")
        def remove_zip_file(zip_path):
            os.remove(zip_path)

        remove_zip_file = remove_zip_file(zip_path=zip_path)

        @task.bash(task_id="rename_file")
        def rename_file(file_path, csv_file):
            return f"mv {file_path} {csv_file}"

        rename_file = rename_file(
            file_path="/opt/airflow/data/kz.csv",
            csv_file="/opt/airflow/data/ecommerce-electronics-purchase-history.csv"
        )

        return unzip_data >> remove_zip_file >> rename_file

    @task_group(group_id="postgres_operations")
    def upload_to_postgres():

        create_table = PostgresOperator(
            task_id="create_table",
            postgres_conn_id="postgres_localhost",
            sql="/opt/airflow/dags/sql/create_table.sql"
        )

        insert_data = PostgresOperator(
            task_id="insert_data",
            postgres_conn_id="postgres_localhost",
            sql="/opt/airflow/dags/sql/insert_data.sql"
        )

        return create_table >> insert_data

    download_data = download_ecommerce_data()
    first_processing = first_data_processing()
    postgres_operations = upload_to_postgres()

    download_data >> upload_to_s3 >> first_processing >> postgres_operations


dag = e_commerce()
