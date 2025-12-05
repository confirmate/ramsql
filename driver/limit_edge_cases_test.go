package ramsql

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
)

func TestLimitZero(t *testing.T) {
	db, err := sql.Open("ramsql", "TestLimitZero")
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

	// Query with LIMIT 0
	rows, err := db.Query("SELECT * FROM items LIMIT 0")
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

	if count != 0 {
		t.Fatalf("LIMIT 0: Expected 0 rows, got %d", count)
	}

	t.Logf("LIMIT 0 correctly returned 0 rows")
}

func TestLimitWithNullValues(t *testing.T) {
	db, err := sql.Open("ramsql", "TestLimitWithNullValues")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: %s", err)
	}

	// Insert rows with some NULL values
	for i := 1; i <= 5; i++ {
		if i%2 == 0 {
			_, err = db.Exec("INSERT INTO items (id, value) VALUES ($1, $2)", fmt.Sprintf("id%d", i), nil)
		} else {
			_, err = db.Exec("INSERT INTO items (id, value) VALUES ($1, $2)", fmt.Sprintf("id%d", i), fmt.Sprintf("value%d", i))
		}
		if err != nil {
			t.Fatalf("Cannot insert: %s", err)
		}
	}

	// Query with LIMIT 3
	rows, err := db.Query("SELECT * FROM items LIMIT 3")
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var id string
		var value sql.NullString
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("rows.Scan: %s", err)
		}
		count++
	}

	if count != 3 {
		t.Fatalf("LIMIT 3 with NULL values: Expected 3 rows, got %d", count)
	}

	t.Logf("LIMIT 3 with NULL values correctly returned 3 rows")
}

func TestLimitConcurrent(t *testing.T) {
	db, err := sql.Open("ramsql", "TestLimitConcurrent")
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

	// Run multiple concurrent queries with LIMIT
	var wg sync.WaitGroup
	errors := make(chan error, 10)
	
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(limit int) {
			defer wg.Done()
			
			rows, err := db.Query(fmt.Sprintf("SELECT * FROM items LIMIT %d", limit))
			if err != nil {
				errors <- fmt.Errorf("query failed: %w", err)
				return
			}
			defer rows.Close()

			var count int
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					errors <- fmt.Errorf("scan failed: %w", err)
					return
				}
				count++
			}

			if count != limit {
				errors <- fmt.Errorf("LIMIT %d: expected %d rows, got %d", limit, limit, count)
			}
		}(i + 1)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent test error: %v", err)
	}

	t.Logf("Concurrent LIMIT queries all succeeded")
}
