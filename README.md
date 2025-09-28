# Public AMG Market Share Forecast Tool

### This is a public version of the Atlantic Music Group market share forecast tool,
### which trains a Random Forest Regressor model on queried KPIs from Warner Music Group's
### Snowflake database and utilizes it to build EOY market share forecasts and populate
### Snowflake tables for easier querying for Atlantic Music Group.
### Queries and schema-related information was removed for privacy purposes; they need to be
### replaced with alternate data and queries to run.

## Setup Instructions

### 1. Install requirements.txt

### 2. Create snowflake_secrets.env file with credentials to login
### into Snowflake, generally in the format of:
### USER=USER
### ACCOUNT=ACCOUNT
### PRIVATE_KEY="-----BEGIN PRIVATE KEY-----PRIVATEKEY-----END PRIVATE KEY-----"
### WAREHOUSE=WAREHOUSE
### ROLE=ROLE
### PASSWORD=PASSWORD

### 3. Enter Database and Schema data into their respective global variables in
### query_market_share.py

### 4. Fill in empty SQL strings in adjust_view.py and .sql files within the queries folder.

### 5. Run main on market_share_main.py.
