package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"time"

	sf "github.com/snowflakedb/gosnowflake"
)

func main() {
	cfg, err := sf.GetConfigFromEnv([]*sf.ConfigParam{
		{Name: "Account", EnvName: "SNOWFLAKE_TEST_ACCOUNT", FailOnMissing: true},
		{Name: "User", EnvName: "SNOWFLAKE_TEST_USER", FailOnMissing: true},
		{Name: "Password", EnvName: "SNOWFLAKE_TEST_PASSWORD", FailOnMissing: true},
		{Name: "Host", EnvName: "SNOWFLAKE_TEST_HOST", FailOnMissing: false},
		{Name: "Database", EnvName: "SNOWFLAKE_TEST_DATABASE", FailOnMissing: false},
		{Name: "Warehouse", EnvName: "SNOWFLAKE_TEST_WAREHOUSE", FailOnMissing: false},
		{Name: "Schema", EnvName: "SNOWFLAKE_TEST_SCHEMA", FailOnMissing: false},
	})
	if err != nil {
		log.Fatalf("failed to create Config, err: %v", err)
	}
	dsn, err := sf.DSN(cfg)
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	// New code to test ABORT_DETACHED_QUERY=false
	var db *sql.DB
	db, err = sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to open connection: %v", err)
	}
	defer db.Close()

	// Set the ABORT_DETACHED_QUERY parameter to false
	_, err = db.Exec("ALTER SESSION SET ABORT_DETACHED_QUERY = FALSE")
	if err != nil {
		log.Fatalf("failed to set ABORT_DETACHED_QUERY: %v", err)
	}

	// Create an asynchronous query and get the query ID
	var queryID string
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatalf("failed to get connection: %v", err)
	}
	defer conn.Close()

	err = conn.Raw(func(x any) error {
		rows, err := x.(driver.QueryerContext).QueryContext(ctx, "CALL SYSTEM$WAIT(10, 'SECONDS')", nil)
		if err != nil {
			return err
		}
		queryID = rows.(sf.SnowflakeRows).GetQueryID()
		return nil
	})
	if err != nil {
		log.Fatalf("failed to execute query: %v", err)
	}
	log.Printf("Query ID: %s", queryID)

	// Simulate connection loss by closing the session
	db.Close()

	// Create a new session
	db, err = sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to open new connection: %v", err)
	}
	defer db.Close()

	// Monitor the query status
	for {
		var status string
		err = db.QueryRow(fmt.Sprintf("SELECT EXECUTION_STATUS FROM table(snowflake.information_schema.query_history()) WHERE QUERY_ID = '%s'", queryID)).Scan(&status)
		if err != nil {
			if err == sql.ErrNoRows {
				// No rows returned, skip and wait for 2 seconds
				time.Sleep(2 * time.Second)
				continue
			}
			log.Fatalf("failed to query status: %v", err)
		}
		if status == "SUCCESS" {
			break
		}
		time.Sleep(2 * time.Second)
	}

	// Pull the results
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM TABLE(RESULT_SCAN('%s'))", queryID))
	if err != nil {
		log.Fatalf("failed to retrieve results: %v", err)
	}
	defer rows.Close()

	// Output the results
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			log.Fatalf("failed to scan result: %v", err)
		}
		fmt.Println(result)
	}
}
