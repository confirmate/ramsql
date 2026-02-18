package ramsql

import (
	"database/sql"
	"testing"
)

func TestSimpleCTE(t *testing.T) {
	db, err := sql.Open("ramsql", "TestSimpleCTE")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	if err != nil {
		t.Fatalf("CREATE TABLE error: %s", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatalf("INSERT error: %s", err)
	}
	_, err = db.Exec(`INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	if err != nil {
		t.Fatalf("INSERT error: %s", err)
	}

	// Test simple CTE
	rows, err := db.Query(`WITH cte AS (SELECT id, name FROM users) SELECT * FROM cte`)
	if err != nil {
		t.Fatalf("CTE query error: %s", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan error: %s", err)
		}
		count++
	}

	if count != 2 {
		t.Fatalf("Expected 2 rows, got %d", count)
	}
}

func TestCTEWithFilter(t *testing.T) {
	db, err := sql.Open("ramsql", "TestCTEWithFilter")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`CREATE TABLE users (id INT, name TEXT, active INT)`)
	if err != nil {
		t.Fatalf("CREATE TABLE error: %s", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO users (id, name, active) VALUES (1, 'Alice', 1)`)
	if err != nil {
		t.Fatalf("INSERT error: %s", err)
	}
	_, err = db.Exec(`INSERT INTO users (id, name, active) VALUES (2, 'Bob', 0)`)
	if err != nil {
		t.Fatalf("INSERT error: %s", err)
	}
	_, err = db.Exec(`INSERT INTO users (id, name, active) VALUES (3, 'Charlie', 1)`)
	if err != nil {
		t.Fatalf("INSERT error: %s", err)
	}

	// Test CTE with WHERE clause
	rows, err := db.Query(`WITH active_users AS (
		SELECT id, name FROM users WHERE active = 1
	) SELECT * FROM active_users`)
	if err != nil {
		t.Fatalf("CTE query error: %s", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan error: %s", err)
		}
		count++
	}

	if count != 2 {
		t.Fatalf("Expected 2 rows (active users), got %d", count)
	}
}

func TestMultipleCTEs(t *testing.T) {
	db, err := sql.Open("ramsql", "TestMultipleCTEs")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(`CREATE TABLE table1 (id INT)`)
	if err != nil {
		t.Fatalf("CREATE TABLE error: %s", err)
	}
	_, err = db.Exec(`CREATE TABLE table2 (name TEXT)`)
	if err != nil {
		t.Fatalf("CREATE TABLE error: %s", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO table1 (id) VALUES (1), (2)`)
	if err != nil {
		t.Fatalf("INSERT error: %s", err)
	}
	_, err = db.Exec(`INSERT INTO table2 (name) VALUES ('Alice'), ('Bob')`)
	if err != nil {
		t.Fatalf("INSERT error: %s", err)
	}

	// Test multiple CTEs (Note: we can't test joins without ON clause support, so just select from one)
	rows, err := db.Query(`WITH 
		cte1 AS (SELECT id FROM table1),
		cte2 AS (SELECT name FROM table2)
	SELECT * FROM cte1`)
	if err != nil {
		t.Fatalf("Multiple CTEs query error: %s", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("Scan error: %s", err)
		}
		count++
	}

	if count != 2 {
		t.Fatalf("Expected 2 rows, got %d", count)
	}
}
