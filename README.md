# go-playground

## Overview

This project demonstrates how to interact with Snowflake using Go, specifically focusing on the following objectives:

1. Creating asynchronous queries in Snowflake.
2. Confirming that if a session is closed, the query isn't killed (using the `ABORT_DETACHED_QUERY` parameter).
3. Finding a specific query ID and outputting the results.

## Steps

1. **Creating Asynchronous Queries**:
    - We use the `gosnowflake` package to create an asynchronous query in Snowflake.
    - The query ID is retrieved to track the query status.

2. **Handling Session Closure**:
    - The `ABORT_DETACHED_QUERY` parameter is set to `FALSE` to ensure that in-progress queries are not aborted if the session is closed.
    - The session is deliberately closed to simulate a connection loss.

3. **Query ID Tracking and Result Retrieval**:
    - A new session is created to monitor the status of the query using the query ID.
    - The `information_schema.query_history()` function is used to check the query status.
    - Once the query is successful, the results are fetched and output to the console.

## Usage

1. Set the required environment variables:
    ```sh
    export SNOWFLAKE_TEST_ACCOUNT="your_account"
    export SNOWFLAKE_TEST_USER="your_user"
    export SNOWFLAKE_TEST_PASSWORD="your_password"
    export SNOWFLAKE_TEST_HOST="your_host"
    export SNOWFLAKE_TEST_DATABASE="your_database"
    export SNOWFLAKE_TEST_WAREHOUSE="your_warehouse"
    export SNOWFLAKE_TEST_SCHEMA="your_schema"
    ```

2. Run the Go script:
    ```sh
    go run main.go
    ```

## Example Output

```sh
2025/03/22 12:59:48 Query ID: 01bb2e9b-020c-af05-0000-000cad472f11
2025/03/22 12:59:49 query status: RUNNING, waiting...
2025/03/22 12:59:51 query status: RUNNING, waiting...
2025/03/22 12:59:53 query status: RUNNING, waiting...
2025/03/22 12:59:56 query status: RUNNING, waiting...
2025/03/22 12:59:58 query completed successfully
waited 10 seconds
```
