# =============================================================================
# Apache Spark with Iceberg Support
# =============================================================================
# Custom Spark image with pre-installed libraries for:
# - Apache Iceberg (table format)
# - AWS S3 (MinIO compatible)
# =============================================================================

ARG SPARK_VERSION=3.5.3
ARG ICEBERG_VERSION=1.5.0

FROM apache/spark:${SPARK_VERSION}

# Build arguments
ARG ICEBERG_VERSION

# Labels
LABEL maintainer="iceberg-incremental-demo"
LABEL description="Spark with Iceberg support"

# Switch to root for installations
USER root

# Install additional tools
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set Spark jars directory
ENV SPARK_HOME=/opt/spark
ENV SPARK_JARS_DIR=${SPARK_HOME}/jars

# Download Iceberg Spark runtime
RUN wget -P ${SPARK_JARS_DIR}/ \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"

# Download AWS SDK bundle for S3
RUN wget -P ${SPARK_JARS_DIR}/ \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.131/bundle-2.20.131.jar"

# Download Hadoop AWS for S3A
RUN wget -P ${SPARK_JARS_DIR}/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"

# Download AWS Java SDK (required by hadoop-aws)
RUN wget -P ${SPARK_JARS_DIR}/ \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

# Download Iceberg AWS module
RUN wget -P ${SPARK_JARS_DIR}/ \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar"

# Copy custom configuration
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# Environment variables
ENV AWS_ACCESS_KEY_ID=admin
ENV AWS_SECRET_ACCESS_KEY=admin123
ENV AWS_REGION=us-east-1

# Switch back to spark user
USER spark
