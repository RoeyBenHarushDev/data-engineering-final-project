"""Keeps the Spark Structured Streaming ingestion alive.

Every 10 minutes: verify the processing container is up, then start the
streaming job inside spark-master if (and only if) it is not already running.
The Spark driver runs inside the processing component; Airflow only supervises.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from alerts import task_failure_alert

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": task_failure_alert,
}

# /proc scan instead of pgrep so no extra packages are needed in the Spark image.
# The bracketed pattern streaming_ingest[.]py cannot match the literal text of the
# checking command itself, so the check never self-matches.
ENSURE_RUNNING = r"""
set -e
RUNNING=$(docker exec spark-master bash -c \
  "grep -sl 'streaming_ingest[.]py' /proc/[0-9]*/cmdline | head -n 1" || true)
if [ -n "$RUNNING" ]; then
    echo "streaming_ingest already running ($RUNNING)"
else
    echo "starting streaming_ingest in spark-master"
    docker exec -d spark-master bash -c \
      "nohup spark-submit --master 'local[*]' /opt/spark/jobs/streaming_ingest.py \
       >> /tmp/streaming_ingest.log 2>&1 &"
    sleep 20
    docker exec spark-master bash -c \
      "grep -sl 'streaming_ingest[.]py' /proc/[0-9]*/cmdline | head -n 1" > /dev/null \
      || { echo 'streaming job failed to start, log tail:'; \
           docker exec spark-master tail -n 50 /tmp/streaming_ingest.log; exit 1; }
    echo "streaming job started"
fi
"""

with DAG(
    dag_id="streaming_supervisor",
    description="Ensures the Kafka->bronze Spark streaming job is running",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["streaming", "kafka"],
) as dag:

    check_processing_up = BashOperator(
        task_id="check_processing_container",
        bash_command="docker exec spark-master true && echo 'spark-master is up'",
    )

    ensure_streaming_running = BashOperator(
        task_id="ensure_streaming_job_running",
        bash_command=ENSURE_RUNNING,
    )

    check_processing_up >> ensure_streaming_running
