import datetime
import os

from dotenv import load_dotenv
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

load_dotenv()
MY_GX_DATA_CONTEXT = "include/great_expectations"

RAW_BUCKET_NAME = os.getenv('RAW_BUCKET_NAME')
TRANSFROMED_BUCKET_NAME = os.getenv('TRANSFROMED_BUCKET_NAME')
SERVING_BUCKET_NAME = os.getenv('SERVING_BUCKET_NAME')

KAGGLE_USERNAME = os.getenv('KAGGLE_USERNAME')
KAGGLE_KEY = os.getenv('KAGGLE_KEY')


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

    download_ecommerce_data() >> upload_to_s3


dag = e_commerce()
