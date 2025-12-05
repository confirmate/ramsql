package ramsql

import (
	"database/sql"
	"fmt"
	"testing"
)

// TestLimitWithParameter tests LIMIT using parameterized queries ($1 style)
func TestLimitWithParameter(t *testing.T) {
	db, err := sql.Open("ramsql", "TestLimitWithParameter")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE items (id TEXT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("sql.Exec: %s", err)
	}

	// Insert 5 rows
	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO items (id) VALUES ($1)", fmt.Sprintf("id%d", i))
		if err != nil {
			t.Fatalf("Cannot insert: %s", err)
		}
	}

	// Test LIMIT with parameter
	rows, err := db.Query("SELECT * FROM items LIMIT $1", 3)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("rows.Scan: %s", err)
		}
		count++
	}

	if count != 3 {
		t.Fatalf("Expected 3 rows with LIMIT $1, got %d", count)
	}

	t.Logf("Successfully retrieved %d rows with parameterized LIMIT", count)
}

// TestOffsetWithParameter tests OFFSET using parameterized queries ($1 style)
func TestOffsetWithParameter(t *testing.T) {
	db, err := sql.Open("ramsql", "TestOffsetWithParameter")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE items (id TEXT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("sql.Exec: %s", err)
	}

	// Insert 5 rows
	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO items (id) VALUES ($1)", fmt.Sprintf("id%d", i))
		if err != nil {
			t.Fatalf("Cannot insert: %s", err)
		}
	}

	// Test OFFSET with parameter
	rows, err := db.Query("SELECT * FROM items OFFSET $1", 2)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("rows.Scan: %s", err)
		}
		count++
	}

	if count != 3 {
		t.Fatalf("Expected 3 rows with OFFSET $1 (skipping 2), got %d", count)
	}

	t.Logf("Successfully retrieved %d rows with parameterized OFFSET", count)
}

// TestLimitOffsetWithParameters tests both LIMIT and OFFSET with parameters
func TestLimitOffsetWithParameters(t *testing.T) {
	db, err := sql.Open("ramsql", "TestLimitOffsetWithParameters")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE items (id TEXT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("sql.Exec: %s", err)
	}

	// Insert 10 rows
	for i := 1; i <= 10; i++ {
		_, err = db.Exec("INSERT INTO items (id) VALUES ($1)", fmt.Sprintf("id%d", i))
		if err != nil {
			t.Fatalf("Cannot insert: %s", err)
		}
	}

	// Test LIMIT and OFFSET with parameters: skip 2, take 3
	rows, err := db.Query("SELECT * FROM items LIMIT $1 OFFSET $2", 3, 2)
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
		t.Fatalf("Expected 3 rows with LIMIT $1 OFFSET $2, got %d. IDs: %v", count, ids)
	}

	// Should get id3, id4, id5 (skipping id1, id2)
	expected := []string{"id3", "id4", "id5"}
	for i, id := range ids {
		if id != expected[i] {
			t.Errorf("Row %d: expected %s, got %s", i, expected[i], id)
		}
	}

	t.Logf("Successfully retrieved %d rows with parameterized LIMIT and OFFSET: %v", count, ids)
}
