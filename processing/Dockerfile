# Spark image with Iceberg support (tabulario/spark-iceberg ships Spark 3.5 + Iceberg runtime,
# matching the environment used in the course quickstart guide).
FROM tabulario/spark-iceberg:latest

# Kafka connector jars for Spark Structured Streaming (baked in so no internet is needed at runtime)
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
              /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
              /opt/spark/jars/kafka-clients-3.4.1.jar \
              /opt/spark/jars/commons-pool2-2.11.1.jar

# Great Expectations for the bonus data quality suite
RUN pip install --no-cache-dir great_expectations==0.18.19

WORKDIR /opt/spark
