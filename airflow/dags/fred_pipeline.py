from airflow.sdk import dag, task
from datetime import datetime, timedelta
import logging
import os
import subprocess

log = logging.getLogger(__name__)

default_args = {
    "owner": "tony",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False
}


@dag(
    dag_id="fred_economics_pipeline",
    description="Weekly FRED economic indicators - ingest, transform, test",
    schedule="0 8 * * 1",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fred", "economics", "snowflake", "dbt"]
)
def fred_pipeline():

    @task
    def ingest() -> dict:
        """Extract FRED observations and load to Snowflake raw layer."""
        log.info("Starting FRED ingestion")
        result = subprocess.run(
            ["python", "/opt/airflow/ingestion/fred_ingest.py"],
            capture_output=True,
            text=True,
            env={**os.environ}
        )
        log.info(result.stdout)
        if result.returncode != 0:
            log.error(result.stderr)
            raise RuntimeError(f"Ingestion failed:\n{result.stderr}")
        return {"status": "success", "timestamp": str(datetime.now())}
    

    @task
    def deps(ingest_result: dict) -> dict:
        """Run dbt deps - install dependencies."""
        log.info(f"Starting dbt deps - ingestion at {ingest_result['timestamp']}")
        result = subprocess.run(
            [
                "dbt", "deps",
                "--project-dir", "/opt/airflow/dbt",
                "--profiles-dir", "/opt/airflow/dbt"
            ],
            capture_output=True,
            text=True,
            env={**os.environ}
        )
        log.info(result.stdout)
        if result.returncode != 0:
            log.error(result.stderr)
            raise RuntimeError(f"dbt deps failed:\n{result.stderr}")
        return {"status": "success", "timestamp": str(datetime.now())}
    
    @task
    def freshness(deps_result: dict) -> dict:
        """Check source freshness."""
        log.info(f"Checking freshness - deps at {deps_result['timestamp']}")
        result = subprocess.run(
            [
                "dbt", "source", "freshness",
                "--project-dir", "/opt/airflow/dbt",
                "--profiles-dir", "/opt/airflow/dbt"
            ],
            capture_output=True,
            text=True,
            env={**os.environ}
        )
        log.info(result.stdout)
        if result.returncode != 0:
            log.error(result.stderr)
            raise RuntimeError(f"Freshness check failed:\n{result.stderr}")
        return {"status": "success", "timestamp": str(datetime.now())}


    @task
    def build(freshness_result: dict) -> dict:
        """Run dbt build - data transformation and testing."""
        log.info(f"Starting dbt build - freshness check at {freshness_result['timestamp']}")
        result = subprocess.run(
            [
                "dbt", "build",
                "--project-dir", "/opt/airflow/dbt",
                "--profiles-dir", "/opt/airflow/dbt"
            ],
            capture_output=True,
            text=True,
            env={**os.environ}
        )
        log.info(result.stdout)
        if result.returncode != 0:
            log.error(result.stderr)
            raise RuntimeError(f"dbt build failed:\n{result.stderr}")
        return {"status": "success", "timestamp": str(datetime.now())}


    ingest_result = ingest()
    deps_result = deps(ingest_result)
    freshness_result = freshness(deps_result)
    build(freshness_result)


fred_pipeline()