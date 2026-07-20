FROM apache/airflow:2.9.3-python3.11

# Docker CLI only (no daemon): Airflow triggers Spark jobs with `docker exec` against
# the spark-master container, so every Spark application runs inside the processing
# component, never inside Airflow's containers.
USER root
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-26.1.4.tgz \
    | tar -xz --strip-components=1 -C /usr/local/bin docker/docker \
    && chmod 755 /usr/local/bin/docker
USER airflow
