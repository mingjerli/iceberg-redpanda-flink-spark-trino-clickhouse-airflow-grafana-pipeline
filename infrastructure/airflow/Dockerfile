# =============================================================================
# Apache Airflow with Docker CLI
# =============================================================================
# Custom Airflow image that includes Docker CLI for executing spark-submit
# commands on the Spark container via docker exec.
# =============================================================================

FROM apache/airflow:2.8.1-python3.11

USER root

# Install Docker CLI (not Docker daemon, just the client)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
        tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create docker group matching typical macOS Docker Desktop GID
# On macOS, Docker Desktop uses GID 0 (root/wheel) for the socket
RUN usermod -aG root airflow 2>/dev/null || true

# Switch back to airflow user
USER airflow

# Install clgraph for DAG generation from SQL lineage
# Using pip as the airflow user
RUN pip install --no-cache-dir clgraph
