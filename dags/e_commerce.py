import datetime

from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    CheckpointConfig
)
from fake_data_generator.generate_data import (generate_customer_data,
                                               generate_product_data,
                                               generate_order_data)


@dag(
    start_date=datetime.datetime(2024, 1, 1),
    schedule="@daily",
    default_args={"retries": 1}
)
def e_commerce():
    @task_group(group_id='generate_data')
    def generate_data():
        create_data = PythonOperator(
            task_id="generate_customer_data",
            python_callable=generate_customer_data,
            op_kwargs={"num_rows": 1000}
        )
        create_data2 = PythonOperator(
            task_id="generate_product_data",
            python_callable=generate_product_data,
            op_kwargs={"num_rows": 1000}
        )
        create_data3 = PythonOperator(
            task_id="generate_order_data",
            python_callable=generate_order_data,
            op_kwargs={"num_rows": 1000}
        )

    @task_group(group_id='data_validator')
    def validate_data():
        ge_operator = GreatExpectationsOperator(
            task_id="ge_operator",
            checkpoint_name="my_checkpoint",
            batch_request=BatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_inferred_data_connector_name",
                data_asset_name="my_data_asset",
            ),
        )

    generate_data()


dag = e_commerce()
