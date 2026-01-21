# =============================================================================
# Apache Flink with Iceberg and Kafka Connectors
# =============================================================================
# Custom Flink image with pre-installed connectors for:
# - Apache Iceberg (table format)
# - Apache Kafka (Redpanda compatible)
# - AWS S3 (MinIO compatible)
# =============================================================================

ARG FLINK_VERSION=1.18.1
ARG SCALA_VERSION=2.12
ARG ICEBERG_VERSION=1.5.0
ARG HADOOP_VERSION=3.3.4

FROM flink:${FLINK_VERSION}-scala_${SCALA_VERSION}-java11

# Build arguments for connector versions
ARG FLINK_VERSION
ARG SCALA_VERSION
ARG ICEBERG_VERSION
ARG HADOOP_VERSION

# Labels
LABEL maintainer="iceberg-incremental-demo"
LABEL description="Flink with Iceberg and Kafka connectors"

# Install required dependencies
USER root
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Create directories for connectors
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
    && mkdir -p /opt/flink/lib

# Download Flink S3 filesystem plugin
RUN cp /opt/flink/opt/flink-s3-fs-hadoop-${FLINK_VERSION}.jar \
    /opt/flink/plugins/s3-fs-hadoop/ || \
    wget -P /opt/flink/plugins/s3-fs-hadoop/ \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}/flink-s3-fs-hadoop-${FLINK_VERSION}.jar"

# Download Iceberg Flink runtime
RUN wget -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/${ICEBERG_VERSION}/iceberg-flink-runtime-1.18-${ICEBERG_VERSION}.jar"

# Download Flink Kafka connector
RUN wget -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_VERSION}/flink-sql-connector-kafka-${FLINK_VERSION}.jar"

# Download AWS SDK bundle (for S3/MinIO)
RUN wget -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.131/bundle-2.20.131.jar"

# Download Hadoop AWS (for S3A filesystem)
RUN wget -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"

# Set proper permissions
RUN chown -R flink:flink /opt/flink/lib /opt/flink/plugins

# Switch back to flink user
USER flink

# Environment variables for S3/MinIO access
ENV AWS_ACCESS_KEY_ID=${MINIO_ROOT_USER:-admin}
ENV AWS_SECRET_ACCESS_KEY=${MINIO_ROOT_PASSWORD:-admin123456}
ENV AWS_REGION=us-east-1

# Default command
CMD ["help"]
