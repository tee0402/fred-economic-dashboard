from dotenv import load_dotenv
import logging
import os
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception, before_sleep_log
import pandas as pd
import time
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ── Setup ──────────────────────────────────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H-%M-%S"
)
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────────

FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}

# Series to ingest: series_id
SERIES = [
    "UNRATE",
    "CIVPART",
    "CPIAUCSL",
    "A191RL1Q225SBEA",
    "FEDFUNDS",
    "MEHOINUSA672N"
]

TARGET_TABLE = "FRED_OBSERVATIONS"

# ── FRED API ──────────────────────────────────────────────────────────────────────

def is_retryable(exception: Exception) -> bool:
    """Return True only for 500 server errors."""
    return isinstance(exception, requests.exceptions.HTTPError) and exception.response.status_code == 500


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception(is_retryable),
    before_sleep=before_sleep_log(log, logging.WARNING)
)
def fetch_series(series_id: str) -> pd.DataFrame:
    """Fetch all observations for a single FRED series with 3 retries using exponential backoff for retryable errors."""
    log.info(f"Fetching {series_id}")

    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }

    response = requests.get(FRED_BASE_URL, params=params, timeout=10)
    response.raise_for_status()
    
    data = response.json()

    if "observations" not in data:
        log.warning(f"No observations returned for {series_id}")
        return pd.DataFrame()

    df = pd.DataFrame(data["observations"])

    # Keep only the columns we need
    df = df[["date", "value"]].copy()

    # Cast types
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Add metadata column
    df["series_id"] = series_id

    # Drop rows where date parsing failed
    before = len(df)
    df = df.dropna(subset=["date"])
    after = len(df)

    if before != after:
        log.warning(f"{series_id}: dropped {before - after} rows with unparseable dates")

    log.info(f"{series_id}: {len(df)} observations fetched")
    return df


def fetch_all_series() -> pd.DataFrame:
    """Fetch all configured FRED series and combine into one DataFrame."""
    frames = []

    for series_id in SERIES:
        try:
            df = fetch_series(series_id)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            log.error(f"{series_id}: skipping after all retries failed - {e}")
        time.sleep(0.5) # Wait 0.5s for politeness

    if not frames:
        log.info("No data fetched - aborting")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Reorder columns cleanly
    combined = combined[["series_id", "date", "value"]]

    log.info(f"Total rows fetched across all series: {len(combined)}")
    return combined


# ── Snowflake ──────────────────────────────────────────────────────────────────────

def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Create and return a Snowflake connection."""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        log.info("Snowflake connection established")
        return conn
    except Exception as e:
        log.error(f"Failed to connect to Snowflake: {e}")
        raise


def create_table_if_not_exists(conn: snowflake.connector.SnowflakeConnection):
    """Create the raw observations table if it does not already exist."""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            SERIES_ID VARCHAR(50) NOT NULL,
            DATE DATE NOT NULL,
            VALUE FLOAT,
            LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """
    try:
        conn.cursor().execute(ddl)
        log.info(f"Table {TARGET_TABLE} ready")
    except Exception as e:
        log.error(f"Failed to create table: {e}")
        raise


def truncate_table(conn: snowflake.connector.SnowflakeConnection):
    """Truncate the raw table before a full reload."""
    try:
        conn.cursor().execute(f"TRUNCATE TABLE {TARGET_TABLE}")
        log.info(f"Truncated {TARGET_TABLE}")
    except Exception as e:
        log.error(f"Failed to truncate table: {e}")
        raise


def load_to_snowflake(conn: snowflake.connector.SnowflakeConnection, df: pd.DataFrame):
    """Write dataframe to Snowflake using write_pandas."""

    # write_pandas requires uppercase column names to match Snowflake table
    df.columns = [column.upper() for column in df.columns]

    # Convert date column to string in YYYY-MM-DD format to ensure write_pandas compatibility
    df["DATE"] = df["DATE"].dt.strftime("%Y-%m-%d")

    try:
        success, num_chunks, num_rows, output = write_pandas(conn, df, table_name=TARGET_TABLE)
        if success:
            log.info(f"Loaded {num_rows} rows into {TARGET_TABLE} in {num_chunks} chunk(s)")
        else:
            log.error(f"write_pandas failed: {output}")
    except Exception as e:
        log.error(f"Failed to load data to Snowflake: {e}")
        raise


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("Starting FRED ingestion")

    # Validate environment
    if not FRED_API_KEY:
        raise ValueError("FRED_API_KEY is not set in .env")
    if not all(SNOWFLAKE_CONFIG.values()):
        raise ValueError("One or more Snowflake credentials are missing from .env")

    # Fetch all FRED data
    df = fetch_all_series()
    if df.empty:
        log.error("No data to load - exiting")
        return
    
    # Load to Snowflake - full reload every run
    conn = get_snowflake_connection()
    try:
        create_table_if_not_exists(conn)
        truncate_table(conn)
        load_to_snowflake(conn, df)
    finally:
        conn.close()
        log.info("Snowflake connection closed")

    log.info("FRED ingestion complete")


if __name__ == "__main__":
    main()