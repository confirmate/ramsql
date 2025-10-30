package ramsql

import (
	"database/sql"
	"testing"
)

func TestSchemaQualifiedSelects(t *testing.T) {
	db, err := sql.Open("ramsql", "TestSchemaQualifiedSelects")
	if err != nil {
		t.Fatalf("cannot open db: %s", err)
	}
	defer db.Close()

	// Create schema and table
	if _, err := db.Exec(`CREATE SCHEMA foo`); err != nil {
		t.Fatalf("cannot create schema: %s", err)
	}
	if _, err := db.Exec(`CREATE TABLE foo.products (id BIGSERIAL PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	// Insert a row
	if _, err := db.Exec(`INSERT INTO foo.products (name) VALUES ('Widget')`); err != nil {
		t.Fatalf("cannot insert: %s", err)
	}

	// Basic select using schema-qualified name
	var name string
	if err := db.QueryRow(`SELECT name FROM foo.products WHERE id = 1`).Scan(&name); err != nil {
		t.Fatalf("cannot select: %s", err)
	}
	if name != "Widget" {
		t.Fatalf("expected 'Widget', got %q", name)
	}

	// Select with table alias on schema-qualified table
	name = ""
	if err := db.QueryRow(`SELECT p.name FROM foo.products AS p WHERE p.id = 1`).Scan(&name); err != nil {
		t.Fatalf("cannot select with alias: %s", err)
	}
	if name != "Widget" {
		t.Fatalf("expected 'Widget' with alias, got %q", name)
	}
}
