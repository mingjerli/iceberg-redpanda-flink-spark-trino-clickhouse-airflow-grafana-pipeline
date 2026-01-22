# =============================================================================
# Apache Flink with Iceberg and Kafka Connectors
# =============================================================================
# Custom Flink image with pre-installed connectors for:
# - Apache Iceberg (table format)
# - Apache Kafka (Redpanda compatible)
# - AWS S3 (MinIO compatible)
#
# Based on the working Decodable example:
# https://github.com/decodableco/examples/blob/main/catalogs/flink-iceberg-hive/flink/Dockerfile
# =============================================================================

ARG FLINK_VERSION=1.18.1
ARG FLINK_MAJOR_MINOR=1.18
ARG SCALA_VERSION=2.12
ARG ICEBERG_VERSION=1.5.0
ARG HADOOP_VERSION=3.3.4
ARG KAFKA_CONNECTOR_VERSION=3.1.0-1.18

FROM flink:${FLINK_VERSION}-scala_${SCALA_VERSION}-java11

# Build arguments for connector versions
ARG FLINK_VERSION
ARG FLINK_MAJOR_MINOR
ARG SCALA_VERSION
ARG ICEBERG_VERSION
ARG HADOOP_VERSION
ARG KAFKA_CONNECTOR_VERSION

# Labels
LABEL maintainer="iceberg-incremental-demo"
LABEL description="Flink with Iceberg and Kafka connectors"

WORKDIR /opt/flink

# Install required dependencies
USER root
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Enable Flink S3 Plugin
RUN mkdir -p ./plugins/s3-fs-hadoop && \
    cp ./opt/flink-s3-fs-hadoop-${FLINK_VERSION}.jar ./plugins/s3-fs-hadoop/

# Download Iceberg Flink runtime
RUN echo "-> Install: Iceberg Flink runtime" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_MAJOR_MINOR}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_MAJOR_MINOR}-${ICEBERG_VERSION}.jar"

# Download Flink Kafka connector
RUN echo "-> Install: Flink Kafka connector" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"

# Download AWS / Hadoop S3 dependencies
RUN echo "-> Install: AWS / Hadoop S3" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.648/aws-java-sdk-bundle-1.12.648.jar"

# Download Iceberg AWS bundle (includes AWS SDK v2 for S3FileIO)
RUN echo "-> Install: Iceberg AWS bundle" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# Download Hadoop dependencies (all required for Iceberg)
RUN echo "-> Install: Hadoop core dependencies" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.1.1/commons-configuration2-2.1.1.jar" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/${HADOOP_VERSION}/hadoop-auth-${HADOOP_VERSION}.jar" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/5.3.0/woodstox-core-5.3.0.jar" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/${HADOOP_VERSION}/hadoop-hdfs-client-${HADOOP_VERSION}.jar" && \
    wget -q -P ./lib/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/${HADOOP_VERSION}/hadoop-mapreduce-client-core-${HADOOP_VERSION}.jar"

# Set proper permissions
RUN chown -R flink:flink ./lib ./plugins

# Switch back to flink user
USER flink

# Environment variables for S3/MinIO access (these are overridden at runtime)
ENV AWS_ACCESS_KEY_ID=admin
ENV AWS_SECRET_ACCESS_KEY=admin123456
ENV AWS_REGION=us-east-1

# Default command
CMD ["help"]
