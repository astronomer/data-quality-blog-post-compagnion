from airflow import DAG, Dataset
from airflow.utils.task_group import TaskGroup
from datetime import datetime, date
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator, SQLTableCheckOperator
)

dataset_table_1 = Dataset('snowflake://table_1')
dataset_table_2 = Dataset('snowflake://table_2')

with DAG(
    dag_id="data_quality_checks_dag",
    start_date=datetime(2022, 9, 27),
    schedule=[dataset_table_1, dataset_table_2],
    catchup=False,
    default_args={
        "conn_id" : "snowflake_conn"
    }
):

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup(
        group_id="checking_table_1"
    ) as checking_table_1:

        column_checks = SQLColumnCheckOperator(
            task_id="columns_checks_table_1",
            table="table_1",
            column_mapping={
                "customer_date_id": {
                    "null_check": {"equal_to": 0},
                    "unique_check": {"equal_to": 0}
                },
                "customer_id": {
                    "null_check": {"equal_to": 0}
                },
                "date": {
                    "null_check": {"equal_to": 0}
                },
                "dag_runs": {
                    "null_check": {"equal_to": 0},
                    "min": {"geq_to": 0},
                    "max": {"leq_to": 100}
                },
                "successful_tasks": {
                    "null_check": {"equal_to": 0},
                    "min": {"geq_to": 0},
                    "max": {"leq_to": 100_000}
                },
                "failed_tasks": {
                    "null_check": {"equal_to": 0},
                    "min": {"geq_to": 0},
                    "max": {"leq_to": 100}
                },
                "total_tasks": {
                    "null_check": {"equal_to": 0},
                    "min": {"geq_to": 0},
                    "max": {"leq_to": 100_100}
                },
                "success_rate": {
                    "null_check": {"equal_to": 0},
                    "min": {"geq_to": 0},
                    "max": {"leq_to": 1}
                }
            }
        )

        table_checks = SQLTableCheckOperator(
            task_id="table_checks_table_1",
            table="table_1",
            checks={
                "row_count_check": {
                    "check_statement": "COUNT(*) > 0"
                },
                "task_total_greater_dag_total_check": {
                    "check_statement": "TOTAL_TASKS > DAG_RUNS"
                },
                "task_total_success_plus_failed_check": {
                    "check_statement": "SUCCESSFUL_TASKS + FAILED_TASKS = \
                        TOTAL_TASKS"
                }, 
                "success_rate_check": {
                    "check_statement": "SUCCESSFUL_TASKS/TOTAL_TASKS = \
                        SUCCESS_RATE"
                },
                "dagrun_sum_check": {
                    "check_statement": "SUM(DAG_RUNS) > 0"
                },
                "date_in_bounds_check": {
                    "check_statement": "DATE BETWEEN '2022-05-01' AND \
                        SYSDATE()"
                }
            }
        )

        column_checks >> table_checks

    with TaskGroup(
        group_id="checking_table_2"
    ) as checking_table_2:

        column_checks = SQLColumnCheckOperator(
            task_id="columns_checks_table_2",
            table="table_2",
            column_mapping={
                "customer_id": {
                    "null_check": {"equal_to": 0},
                    "unique_check": {"equal_to": 0}
                },
                "is_active": {
                    "null_check": {"equal_to": 0}
                },
                "active_deployments": {
                    "null_check": {"equal_to": 0},
                    "min": {"geq_to": 0},
                    "max": {"leq_to": 100}
                }
            }
        )

        table_checks_general = SQLTableCheckOperator(
            task_id="table_checks_table_2_general",
            table="table_2",
            checks={
                "row_count_check": {
                    "check_statement": "COUNT(*) > 0"
                },
                "active_before_churn": {
                    "check_statement": "date_churn IS NULL OR \
                        (date_activated < date_churn)"
                },
                "total_deployments_check": {
                    "check_statement": "SUM(active_deployments) < 1000"
                },
                "date_activated_in_bounds_check": {
                    "check_statement": "date_activated is null OR \
                        date_activated BETWEEN '2022-05-01' AND SYSDATE()"
                },
                "date_churn_in_bounds_check": {
                    "check_statement": "date_churn is null OR date_churn \
                        BETWEEN '2022-05-01' AND SYSDATE()"
                }   
            }
        )

        column_checks >> [
            table_checks_general
        ]

    start >> [checking_table_1, checking_table_2] >> end