package ramsql

import (
	"database/sql"
	"testing"
)

func TestCurrentDatabase(t *testing.T) {

	db, err := sql.Open("ramsql", "TestCurrentDatabase///")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	// Test CURRENT_DATABASE() without FROM clause
	var dbName string
	err = db.QueryRow("SELECT CURRENT_DATABASE()").Scan(&dbName)
	if err != nil {
		t.Fatalf("SELECT CURRENT_DATABASE(): %s", err)
	}

	// Should return the database name specified in Open
	expected := "TestCurrentDatabase"
	if dbName != expected {
		t.Errorf("expected database '%s', got '%s'", expected, dbName)
	}
}
