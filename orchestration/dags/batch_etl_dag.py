"""Hourly batch ETL: bronze -> silver -> gold, with a data quality gate after
every stage. Each task submits a Spark application INTO the spark-master
container (processing component) via docker exec — no Spark code runs on the
Airflow containers.

Trigger with config {"snapshot": "day2"} to load the second customer snapshot
and demonstrate the SCD2 dimension picking up the changes.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from alerts import task_failure_alert

SPARK_SUBMIT = "docker exec spark-master spark-submit --master 'local[*]' /opt/spark/jobs"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="batch_etl",
    description="Batch ETL bronze->silver->gold with DQ gates",
    schedule="@hourly",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={"snapshot": Param("day1", enum=["day1", "day2"],
                              description="customer snapshot to load")},
    tags=["batch", "iceberg"],
) as dag:

    load_batch_sources = BashOperator(
        task_id="load_batch_sources",
        bash_command=f"{SPARK_SUBMIT}/seed_bronze.py {{{{ params.snapshot }}}}",
    )

    dq_bronze = BashOperator(
        task_id="dq_bronze",
        bash_command=f"{SPARK_SUBMIT}/data_quality.py bronze",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"{SPARK_SUBMIT}/bronze_to_silver.py",
    )

    dq_silver = BashOperator(
        task_id="dq_silver",
        bash_command=f"{SPARK_SUBMIT}/data_quality.py silver",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"{SPARK_SUBMIT}/silver_to_gold.py",
    )

    dq_gold = BashOperator(
        task_id="dq_gold",
        bash_command=f"{SPARK_SUBMIT}/data_quality.py gold",
    )

    ge_validation = BashOperator(
        task_id="great_expectations_validation",
        bash_command=f"{SPARK_SUBMIT}/ge_quality.py",
    )

    (load_batch_sources >> dq_bronze >> bronze_to_silver >> dq_silver
     >> silver_to_gold >> dq_gold >> ge_validation)
