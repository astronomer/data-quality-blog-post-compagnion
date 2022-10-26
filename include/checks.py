MY_SCHEMA = "TAMARAFINGERLIN"

schemas = {
    f"{MY_SCHEMA}": {
        "tables": {
            "TABLE_1": {
                "col_list": """[
                    "CUSTOMER_DATE_ID", "CUSTOMER_ID", "DATE", "DAG_RUNS", 
                    "SUCCESSFUL_TASKS", "FAILED_TASKS", "TOTAL_TASKS", 
                    "SUCCESS_RATE"
                    ]""",
                "column_mapping": {
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
                    },
                "table_checks": {
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
                },
                "custom_checks": []
            },
            "TABLE_2": {
                "col_list": """[
                    "CUSTOMER_ID", "IS_ACTIVE", "DATE_ACTIVATED", 
                    "DATE_CHURN", "ACTIVE_DEPLOYMENTS"
                    ]""",
                "column_mapping": {
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
                },
                "table_checks": {
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
                },
                "custom_checks": []
            }
        },
        "definition_of_dependencies" : {
            "TABLE_1": ["TABLE_2"],
            "TABLE_2": []
        }
    }
}

TABLE_SCHEMA_CHECK = """

    WITH

    curr_col_names AS (

    SELECT COLUMN_NAME
    FROM "{{ params.db_to_query }}"."INFORMATION_SCHEMA"."COLUMNS"
    WHERE TABLE_SCHEMA = '{{ params.schema }}' AND TABLE_NAME = \
        '{{ params.table }}'),

    prev_col_names AS (
    select value from table(flatten ( input => \
        parse_json('{{ params.col_list }}')))

    ),

    null_count_join_one AS (

    SELECT COUNT(*) AS null_count_one
    FROM curr_col_names t1
    LEFT JOIN prev_col_names t2 ON CAST(t1.COLUMN_NAME AS TEXT) = \
        CAST(t2.value AS TEXT)
    WHERE value IS NULL

    ),

    null_count_join_two AS (

    SELECT COUNT(*) AS null_count_two
    FROM prev_col_names t1
    LEFT JOIN curr_col_names t2 ON CAST(t1.value AS TEXT) = \
        CAST(t2.COLUMN_NAME AS TEXT)
    WHERE COLUMN_NAME IS NULL

    )

    SELECT
        CASE
            WHEN null_count_one = 0 AND null_count_two = 0 THEN 1
            ELSE 0
        END AS testresult
    FROM null_count_join_one
    OUTER JOIN null_count_join_two

"""
