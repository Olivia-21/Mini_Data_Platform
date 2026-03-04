FROM apache/airflow:2.9.1-python3.11

USER root

# Install system-level dependencies (if any needed in the future)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy and install Python requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy source code into the image so it's available on PYTHONPATH
COPY src/ /opt/airflow/src/
COPY airflow/dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
