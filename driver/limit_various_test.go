package ramsql

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestLimitVariousValues(t *testing.T) {
	tests := []struct {
		limit    int
		expected int
	}{
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
		{10, 5}, // Should return all 5 rows since we only have 5
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("LIMIT_%d", tc.limit), func(t *testing.T) {
			db, err := sql.Open("ramsql", fmt.Sprintf("TestLimit%d", tc.limit))
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

			// Query with specified LIMIT
			rows, err := db.Query(fmt.Sprintf("SELECT * FROM items LIMIT %d", tc.limit))
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

			if count != tc.expected {
				t.Fatalf("LIMIT %d: Expected %d rows, got %d", tc.limit, tc.expected, count)
			}
		})
	}
}
