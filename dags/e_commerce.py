import datetime
import os

from dotenv import load_dotenv
from airflow.decorators import dag, task, task_group
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

from scripts.first_data_processing import DataProcessor
from scripts.postgres_operations import PostgresOperations

load_dotenv()

kaggle_username = os.environ.get("kaggle_config", "username")
kaggle_key = os.environ.get("kaggle_config", "kaggle_key")


@dag(
    start_date=datetime.datetime(2024, 5, 1),
    schedule="@daily",
    default_args={"retries": 1}
)
def e_commerce():
    @task.bash
    def download_ecommerce_data():
        return f"""
            export KAGGLE_USERNAME={kaggle_username} && \
            export KAGGLE_KEY={kaggle_key} && \
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
    def first_data_processing(zip_source: str,
                              csv_destination: str,
                              csv_path: str,
                              new_csv_name: str):

        @task(task_id="unzip_data")
        def unzip_data(zip_source: str, csv_destination: str) -> None:
            return DataProcessor.unzip_data(zip_source, csv_destination)

        @task(task_id="remove_zip_file")
        def remove_zip_file(zip_source: str) -> None:
            return DataProcessor.remove_file(zip_source)

        @task.bash(task_id="rename_file")
        def rename_file(csv_path: str, new_csv_name: str) -> None:
            return DataProcessor.rename_file(csv_path, new_csv_name)

        unzip_data = unzip_data(zip_source=zip_source,
                                     csv_destination=csv_destination)
        remove_zip_file = remove_zip_file(zip_source=zip_source)
        rename_file = rename_file(csv_path=csv_path,
                                       new_csv_name=new_csv_name)

        return unzip_data >> remove_zip_file >> rename_file

    @task_group(group_id="postgres_operations")
    def postgres_operations():

        @task(task_id="create_table")
        def create_table():
            return PostgresOperations.create_table()

        @task(task_id="load_data")
        def load_data():
            return PostgresOperations.load_data()

        create_table = create_table()
        load_data = load_data()

        return create_table >> load_data

    download_data = download_ecommerce_data()

    first_processing = first_data_processing(
        zip_source="/opt/airflow/data/ecommerce-purchase-history-from-electronics-store.zip",
        csv_destination="/opt/airflow/data/",
        csv_path="/opt/airflow/data/kz.csv",
        new_csv_name="/opt/airflow/data/ecommerce-electronics-purchase-history.csv"
    )

    postgres_operations = postgres_operations()

    download_data >> upload_to_s3 >> first_processing >> postgres_operations


dag = e_commerce()
