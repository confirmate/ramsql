package ramsql

import (
	"database/sql"
	"testing"
)

// TestCTEIssue29 tests the specific use case from issue #29
// This is a simplified version of the query mentioned in the issue
func TestCTEIssue29(t *testing.T) {
	db, err := sql.Open("ramsql", "TestCTEIssue29")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	// Create evaluation_results table similar to the issue
	_, err = db.Exec(`CREATE TABLE evaluation_results (
		id INT,
		control_id TEXT,
		control_catalog_id TEXT,
		row_number INT,
		status TEXT
	)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %s", err)
	}

	// Insert test data with multiple results per control_id
	_, err = db.Exec("INSERT INTO evaluation_results (id, control_id, control_catalog_id, row_number, status) VALUES (1, 'C1', 'CAT1', 2, 'pass')")
	if err != nil {
		t.Fatalf("INSERT 1: %s", err)
	}

	_, err = db.Exec("INSERT INTO evaluation_results (id, control_id, control_catalog_id, row_number, status) VALUES (2, 'C1', 'CAT1', 1, 'fail')")
	if err != nil {
		t.Fatalf("INSERT 2: %s", err)
	}

	_, err = db.Exec("INSERT INTO evaluation_results (id, control_id, control_catalog_id, row_number, status) VALUES (3, 'C2', 'CAT1', 1, 'pass')")
	if err != nil {
		t.Fatalf("INSERT 3: %s", err)
	}

	// Test CTE query - get latest results per control_id
	// Simplified version without ROW_NUMBER() OVER which isn't supported yet
	query := `WITH sorted_results AS (
		SELECT * FROM evaluation_results WHERE id > 0
	)
	SELECT * FROM sorted_results WHERE row_number = 1 ORDER BY control_catalog_id`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("CTE query failed: %s", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, rowNumber int
		var controlID, controlCatalogID, status string
		err = rows.Scan(&id, &controlID, &controlCatalogID, &rowNumber, &status)
		if err != nil {
			t.Fatalf("Scan: %s", err)
		}
		count++
		if rowNumber != 1 {
			t.Fatalf("Expected row_number = 1, got %d", rowNumber)
		}
	}

	if count != 2 {
		t.Fatalf("Expected 2 rows (latest per control_id), got %d", count)
	}
}
