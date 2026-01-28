# =============================================================================
# Apache Iceberg REST Catalog
# =============================================================================
# REST catalog server that provides a unified catalog interface for
# Spark, Flink, and Trino to access Iceberg tables.
#
# Extends the official Apache Iceberg REST fixture image with:
# - PostgreSQL JDBC driver for catalog backend
# - Iceberg AWS bundle for S3 file I/O
# =============================================================================

FROM apache/iceberg-rest-fixture:latest

USER root

# Download PostgreSQL JDBC driver
RUN curl -fSL -o /usr/lib/iceberg-rest/postgresql.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# Download Iceberg AWS bundle for S3FileIO
RUN curl -fSL -o /usr/lib/iceberg-rest/iceberg-aws-bundle.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.0/iceberg-aws-bundle-1.5.0.jar

# Add JARs to classpath - update the entrypoint to include them
ENV CLASSPATH="/usr/lib/iceberg-rest/postgresql.jar:/usr/lib/iceberg-rest/iceberg-aws-bundle.jar"

USER iceberg

# Override entrypoint to include additional JARs in classpath
ENTRYPOINT ["java", "-cp", "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/usr/lib/iceberg-rest/postgresql.jar:/usr/lib/iceberg-rest/iceberg-aws-bundle.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
