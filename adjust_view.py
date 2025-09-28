from snowflake.connector.pandas_tools import write_pandas
from query_market_share import DATABASE, SCHEMA

def drop_table(_sf, table_name):
    # Set database and schema context first
    cs = _sf.conn.cursor()
    cs.execute(f"USE DATABASE {DATABASE}")
    cs.execute(f"USE SCHEMA {SCHEMA}")
    
    cs.execute(f'''drop table {table_name}''')


def create_market_share_weekly_historicals(_sf):
    # Set database and schema context first
    cs = _sf.conn.cursor()
    cs.execute(f"USE DATABASE {DATABASE}")
    cs.execute(f"USE SCHEMA {SCHEMA}")
    
    cs.execute(f"""

    """)


def create_market_share_building_ytd(_sf):
    # Set database and schema context first
    cs = _sf.conn.cursor()
    cs.execute(f"USE DATABASE {DATABASE}")
    cs.execute(f"USE SCHEMA {SCHEMA}")
    
    cs.execute(f"""

    """)


def create_market_share_forecasts_ytd_table(_sf):
    # Set database and schema context first
    cs = _sf.conn.cursor()
    cs.execute(f"USE DATABASE {DATABASE}")
    cs.execute(f"USE SCHEMA {SCHEMA}")
    
    cs.execute(f"""

    """)
