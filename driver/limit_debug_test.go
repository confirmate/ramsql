package ramsql

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"
)

func TestLimitDebugRowsStruct(t *testing.T) {
	db, err := sql.Open("ramsql", "TestLimitDebugRowsStruct")
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
			t.Fatalf("Cannot insert into table items: %s", err)
		}
	}

	// Query with LIMIT 2
	rows, err := db.Query("SELECT * FROM items LIMIT 2")
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}
	defer rows.Close()

	// Use reflection to inspect the Rows object internals
	rowsValue := reflect.ValueOf(rows).Elem()
	t.Logf("Rows type: %v", rowsValue.Type())
	
	// Try to find the underlying implementation
	siField := rowsValue.FieldByName("rowsi")
	if siField.IsValid() {
		t.Logf("rowsi field found: %v", siField.Type())
		
		// Get the actual Rows implementation (it's a pointer)
		if siField.Elem().IsValid() {
			actualRows := siField.Elem().Elem()
			t.Logf("Actual rows type: %v", actualRows.Type())
			
			// Try to access the internal fields
			tuplesField := actualRows.FieldByName("tuples")
			if tuplesField.IsValid() {
				t.Logf("tuples field length: %d", tuplesField.Len())
			}
			
			endField := actualRows.FieldByName("end")
			if endField.IsValid() {
				t.Logf("end field value: %d", endField.Int())
			}
			
			idxField := actualRows.FieldByName("idx")
			if idxField.IsValid() {
				t.Logf("idx field value: %d", idxField.Int())
			}
		}
	}

	// Now scan normally
	var count int
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("rows.Scan: %s", err)
		}
		count++
	}

	if count != 2 {
		t.Fatalf("Expected 2 rows, got %d", count)
	}

	t.Logf("Successfully retrieved %d rows", count)
}
