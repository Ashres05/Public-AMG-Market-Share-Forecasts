import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import os
from cryptography.hazmat.primitives import serialization
import textwrap
from update_table import load_sql

class Snowflake:
    """
    Snowflake connection class.
    """
    def __init__(self, creds):
        """
        Initialize the Snowflake connection using provided credentials.
        """
        self._creds = creds
        self.conn = None
        
    def __enter__(self):
        """
        Returns self to build context manager.
        """
        try:
            self.conn = snowflake.connector.connect(**self._creds)
            return self
        except Exception as e:
            raise SnowflakeConnectionError(f"Failed to connect to Snowflake: {e}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Ensure the Snowflake connection is closed when the object exits a context manager.
        """
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
 
    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query and return the results as a pandas DataFrame.
        """
        if not self.conn:
            raise SnowflakeConnectionError("Snowflake connection is not established.")
 
        try:
            return pd.read_sql(sql, con=self.conn)
        except snowflake.connector.errors.ProgrammingError as e:
            raise SnowflakeConnectionError(f"An error occurred while executing the query: {e}")
        except Exception as e:
            raise SnowflakeConnectionError(f"Unexpected error: {e}")


class SnowflakeConnectionError(Exception):
    """
    Custom Snowflake Connection Error.
    """
    pass


def get_snowflake_connection():
    """
    Returns connection to Snowflake based off data from secrets file.
    """
    # For the EC2, comment out the env_path and uncomment out the single load_dotenv()
    # env_path = Path(os.getcwd()) / 'snowflake_secrets.env'
    # load_dotenv(env_path, override=True)
    load_dotenv()

    private_key_str = os.getenv("SNOWFLAKE_PRIVATE_KEY")

    # Extract the header and footer
    header = "-----BEGIN PRIVATE KEY-----"
    footer = "-----END PRIVATE KEY-----"
    key_body = private_key_str.replace(header, "").replace(footer, "").strip()

    # Insert line breaks every 64 characters
    key_body_wrapped = "\n".join(textwrap.wrap(key_body, 64))
    private_key_pem = f"{header}\n{key_body_wrapped}\n{footer}".encode()

    private_key = serialization.load_pem_private_key(
        private_key_pem,
        password=None
    )

    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    creds = {
        "user": os.environ.get("SNOWFLAKE_USER"),
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "private_key": private_key_der,
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "account":os.environ.get("SNOWFLAKE_ACCOUNT"),
        # "password":os.environ.get("PASSWORD")
    }
    
    return Snowflake(creds)


def query_amg_totals(_sf, *, final_week=None) -> pd.DataFrame:
    # Get max week if no week limit was specified
    if final_week is None:
        final_week = 'max(week)'
    sql = load_sql('query_amg_totals.sql').replace('{final_week}', str(final_week))
    return _sf.query(sql)


def query_market_totals(_sf, *, final_week=None) -> pd.DataFrame:
    # Get max week if no week limit was specified
    if final_week is None:
        final_week = 'max(week)'
    sql = load_sql('query_market_totals.sql').replace('{final_week}', str(final_week))
    return _sf.query(sql)


def query_latest_week(_sf, year) -> pd.DataFrame:
    sql = f"""
    
    """
    return _sf.query(sql)
