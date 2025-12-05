package ramsql

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestLimitReturnsMultipleRows(t *testing.T) {
	db, err := sql.Open("ramsql", "TestLimitReturnsMultipleRows")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE items (id TEXT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("sql.Exec: %s", err)
	}

	// Insert 5 rows using parameterized queries
	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO items (id) VALUES ($1)", fmt.Sprintf("id%d", i))
		if err != nil {
			t.Fatalf("Cannot insert into table items: %s", err)
		}
	}

	// Test LIMIT 2
	rows, err := db.Query("SELECT * FROM items LIMIT 2")
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}
	defer rows.Close()

	var count int
	var ids []string
	
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("rows.Scan: %s", err)
		}
		ids = append(ids, id)
		count++
	}

	if count != 2 {
		t.Fatalf("Expected 2 rows, got %d. IDs: %v", count, ids)
	}

	t.Logf("Successfully retrieved %d rows with IDs: %v", count, ids)
}

func TestLimitWithOffset(t *testing.T) {
	db, err := sql.Open("ramsql", "TestLimitWithOffset")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE items (id TEXT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("sql.Exec: %s", err)
	}

	// Insert 5 rows using parameterized queries
	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO items (id) VALUES ($1)", fmt.Sprintf("id%d", i))
		if err != nil {
			t.Fatalf("Cannot insert into table items: %s", err)
		}
	}

	// Test LIMIT 3 OFFSET 1
	rows, err := db.Query("SELECT * FROM items LIMIT 3 OFFSET 1")
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}
	defer rows.Close()

	var count int
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("rows.Scan: %s", err)
		}
		ids = append(ids, id)
		count++
	}

	if count != 3 {
		t.Fatalf("Expected 3 rows, got %d. IDs: %v", count, ids)
	}

	t.Logf("Successfully retrieved %d rows with IDs: %v", count, ids)
}
