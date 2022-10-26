Overview
========

> If you are unfamiliar with Airflow or the Astro CLI start with [this tutorial](https://docs.astronomer.io/tutorials/get-started-with-airflow).

Welcome! This repository contains an examples for data quality checks running on two tables created by two different DAGs.

To run the repository you will need to configure a Snowflake connection. With at least the following:

- conn_id: snowflake_conn
- conn_type: Snowflake
- account: your Snowflake account (e.g. xy12345)
- warehouse: your Snowflake warehouse type (e.g. HUMANS)
- database: your Snowflake database
- schema: your Snowflake schema
- login: your Snowflake login
- password: your Snowflake password
- location: your Snowflake location (e.g. us-east-01)

The repository contains 3 dags in the dags folder. `create_table_1_dag` will create `table_1` in Snowflake, `create_table_2_dag` will create `table_2`. `create_table_2_dag` is triggered once `create_table_1_dag` has completed and `data_quality_checks_dag` is triggered once the both two dags have been completed (see: [Datasets and Data-Aware Scheduling in Airflow](https://www.astronomer.io/guides/airflow-datasets/)) and will run a set of data quality checks on both tables. All checks are set up to pass.

You can find more information on how to implement data quality checks in Airflow in these resources:

- [Data Quality and Airflow](https://docs.astronomer.io/learn/data-quality) concept guide.
- [Airflow data quality checks with SQL operators](https://docs.astronomer.io/learn/airflow-sql-data-quality) concept guide.
- [Implementing Data Quality checks in Airflow](https://www.astronomer.io/events/webinars/implementing-data-quality-checks-in-airflow/) webinar.

