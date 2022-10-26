from airflow import DAG, Dataset
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

dataset_table_1 = Dataset('snowflake://table_1')
dataset_table_2 = Dataset('snowflake://table_2')

with DAG(
    dag_id="create_table_2_dag",
    start_date=datetime(2022, 9, 27),
    schedule=[dataset_table_1],
    catchup=False,
    template_searchpath="include"
):

    test = SnowflakeOperator(
        task_id="test",
        snowflake_conn_id="snowflake_conn",
        sql="create_table_2.sql",
        outlets=[dataset_table_2]
    )
