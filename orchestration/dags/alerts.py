"""Failure alerting for all DAGs.

Logs a structured alert line on any task failure (picked up by `docker logs` /
Airflow task logs). To enable email alerts, configure the AIRFLOW__SMTP__*
environment variables in orchestration/docker-compose.yml and set
`email` + `email_on_failure` in the DAG default_args.
"""

import logging

log = logging.getLogger("dag-alerts")


def task_failure_alert(context):
    ti = context["task_instance"]
    log.error(
        "ALERT | dag=%s task=%s run=%s try=%s | %s",
        ti.dag_id, ti.task_id, context.get("run_id"),
        ti.try_number, context.get("exception"),
    )
