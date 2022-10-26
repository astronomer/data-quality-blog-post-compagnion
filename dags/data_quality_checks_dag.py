from airflow import DAG, Dataset
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator, SQLTableCheckOperator
)
from airflow.operators.sql import SQLCheckOperator
from include.checks import schemas, TABLE_SCHEMA_CHECK

dataset_table_1 = Dataset('snowflake://table_1')
dataset_table_2 = Dataset('snowflake://table_2')

db_to_query = "SANDBOX"


def set_dependencies(table_objects, definition_of_dependencies):
    for up, down in definition_of_dependencies.items():
        if down:
            for downstream_table in down:
                table_objects[up] >> table_objects[downstream_table]


with DAG(
    dag_id="data_quality_checks_dag",
    start_date=datetime(2022, 9, 27),
    schedule=[dataset_table_1, dataset_table_2],
    catchup=False
):

    for schema in schemas:

        with TaskGroup(
            group_id=schema
        ) as schema_tg:

            tables = schemas[schema]["tables"]
            table_objects = {}

            for table in tables:

                with TaskGroup(
                    group_id=table,
                    default_args={
                        "conn_id": "snowflake_conn",
                        "trigger_rule": "all_done"
                    }
                ) as table_tg:

                    # check if there were any changes to the table schema
                    schema_change_check = SQLCheckOperator(
                        task_id="schema_change_check",
                        sql=TABLE_SCHEMA_CHECK,
                        params={
                            "db_to_query": db_to_query,
                            "schema": schema,
                            "table": table,
                            "col_list": tables[table]["col_list"]
                        }
                    )

                    # run a list of checks on individual columns
                    column_checks = SQLColumnCheckOperator(
                        task_id="column_checks",
                        table=f"{db_to_query}.{schema}.{table}",
                        column_mapping=tables[table]["column_mapping"]
                    )

                    # run a list of checks on the whole SQL table
                    table_checks = SQLTableCheckOperator(
                        task_id="table_checks",
                        table=f"{db_to_query}.{schema}.{table}",
                        checks=tables[table]["table_checks"]
                    )

                    # set dependencies of the checks
                    schema_change_check >> [column_checks, table_checks]

                    # if defined create custom checks
                    if tables[table]["custom_checks"]:
                        custom_checks = tables[table]["custom_checks"]
                        for custom_check in custom_checks:
                            custom_check_task = SQLCheckOperator(
                                task_id="custom_check_task",
                                sql=custom_checks[custom_check]["sql"],
                                params=custom_checks[custom_check]["params"]
                            )

                            schema_change_check >> custom_check_task

                    table_objects[table] = table_tg

        # set dependencies between table-level task groups
        set_dependencies(
            table_objects,
            schemas[schema]["definition_of_dependencies"]
        )
