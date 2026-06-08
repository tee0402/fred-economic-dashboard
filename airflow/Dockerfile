FROM apache/airflow:3.2.2

# Install pipeline and dbt dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt