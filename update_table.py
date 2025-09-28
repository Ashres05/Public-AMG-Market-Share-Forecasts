from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path
import os


def load_sql(file_name: str):
    current_dir = Path(os.getcwd())
    path = current_dir / 'queries' / file_name
    return path.read_text(encoding='utf-8')


def populate_table_market_share_weekly_historicals(cs):
    cs.execute(load_sql('populate_market_share_weekly_historicals.sql'))


def populate_market_share_building_ytd(cs):
    cs.execute(load_sql('populate_market_share_building_ytd.sql'))


def update_market_share_forecasts_ytd(_sf, df):
    # Set database and schema context first
    cs = _sf.conn.cursor()
    cs.execute("USE DATABASE CURRENT_DEV")
    cs.execute("USE SCHEMA DATA")
    
    # Convert DataFrame column names to uppercase to match Snowflake table
    df_upper = df.copy()
    df_upper.columns = df_upper.columns.str.upper()
    
    # First truncate table to remove all old projections
    cs.execute(f'TRUNCATE TABLE MARKET_SHARE_FORECASTS_YTD')

    success, nchunks, nrows, _ = write_pandas(_sf.conn, df_upper, 'MARKET_SHARE_FORECASTS_YTD')

    if success:
        print('Success!')
        print(f"Loaded {nrows} rows into Snowflake table")
    else:
        print('Error')
