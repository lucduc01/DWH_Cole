FROM apache/airflow:2.10.0-python3.10

USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Trở lại user airflow
USER airflow

# Copy requirements.txt và cài đặt package Python
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir --default-timeout=100 -r requirements.txt

# Copy DAGs
COPY dags/ /opt/airflow/dags/

# Copy src code
COPY src/ /opt/airflow/src/
COPY Connect_DB.py /opt/airflow/src/

# Nếu muốn container chạy có thể import trực tiếp từ src/
ENV PYTHONPATH=/opt/airflow/src
