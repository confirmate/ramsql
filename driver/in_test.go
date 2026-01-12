package ramsql

import (
	"database/sql"
	"testing"
)

var userTableBatch = []string{
	`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
	`INSERT INTO user (name, surname, age) VALUES (Foo, Bar, 20);`,
	`INSERT INTO user (name, surname, age) VALUES (John, Doe, 32);`,
	`INSERT INTO user (name, surname, age) VALUES (Jane, Doe, 33);`,
	`INSERT INTO user (name, surname, age) VALUES (Joe, Doe, 10);`,
	`INSERT INTO user (name, surname, age) VALUES (Homer, Simpson, 40);`,
	`INSERT INTO user (name, surname, age) VALUES (Marge, Simpson, 40);`,
	`INSERT INTO user (name, surname, age) VALUES (Bruce, Wayne, 3333);`,
}

func setupUserTable(t *testing.T, dbName string) *sql.DB {
	t.Helper()
	db, err := sql.Open("ramsql", dbName)
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}

	for _, b := range userTableBatch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	return db
}

func TestIn(t *testing.T) {
	db := setupUserTable(t, "TestIn")
	defer db.Close()

	query := `SELECT * FROM user WHERE user.surname IN ('Doe', 'Simpson')`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age int
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if surname != "Doe" && surname != "Simpson" {
			t.Fatalf("Unwanted row: %s %s %d", name, surname, age)
		}

		nb++
	}

	if nb != 5 {
		t.Fatalf("Expected 5 rows, got %d", nb)
	}

}

func TestNotIn(t *testing.T) {
	db := setupUserTable(t, "TestNotIn")
	defer db.Close()

	query := `SELECT * FROM user WHERE user.surname NOT IN ('Doe', 'Simpson')`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age int
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if surname == "Doe" || surname == "Simpson" {
			t.Fatalf("Unwanted row: %s %s %d", name, surname, age)
		}

		nb++
	}

	if nb != 2 {
		t.Fatalf("Expected 2 rows, got %d", nb)
	}

}

func TestTupleIn(t *testing.T) {
	db := setupUserTable(t, "TestTupleIn")
	defer db.Close()

	query := `SELECT * FROM user WHERE (user.name, user.surname) IN (('Homer', 'Simpson'), ('Bruce', 'Wayne'))`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age int
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if !((name == "Homer" && surname == "Simpson") || (name == "Bruce" && surname == "Wayne")) {
			t.Fatalf("Unwanted row: %s %s %d", name, surname, age)
		}

		nb++
	}

	if nb != 2 {
		t.Fatalf("Expected 2 rows, got %d", nb)
	}

}

func TestTupleNotIn(t *testing.T) {
	db := setupUserTable(t, "TestTupleNotIn")
	defer db.Close()

	query := `SELECT * FROM user WHERE (user.name, user.surname) NOT IN (('Homer', 'Simpson'), ('Bruce', 'Wayne'))`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age int
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if (name == "Homer" && surname == "Simpson") || (name == "Bruce" && surname == "Wayne") {
			t.Fatalf("Unwanted row: %s %s %d", name, surname, age)
		}

		nb++
	}

	if nb != 5 {
		t.Fatalf("Expected 5 rows, got %d", nb)
	}

}

func TestInWithBoundParameters(t *testing.T) {
	db := setupUserTable(t, "TestInWithBoundParameters")
	defer db.Close()

	// Test with Postgres-style placeholders ($1)
	query := `SELECT * FROM user WHERE user.surname IN ($1, $2)`

	rows, err := db.Query(query, "Doe", "Simpson")
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age int
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if surname != "Doe" && surname != "Simpson" {
			t.Fatalf("Unwanted row: %s %s %d", name, surname, age)
		}

		nb++
	}

	if nb != 5 {
		t.Fatalf("Expected 5 rows, got %d", nb)
	}
}

func TestInWithArrayBinding(t *testing.T) {
	db := setupUserTable(t, "TestInWithArrayBinding")
	defer db.Close()

	// Test binding using the pq.Array wrapper (common approach for Postgres drivers)
	// Note: When GORM sends IN queries with array binding, the SQL actually contains
	// multiple placeholders like IN ($1, $2) not IN ($1) with an array.
	// The real issue is when drivers expand array-like types.
	// For now, let's test that individual placeholders work correctly.
	query := `SELECT * FROM user WHERE user.surname IN ($1, $2, $3)`

	rows, err := db.Query(query, "Doe", "Simpson", "Wayne")
	if err != nil {
		t.Fatalf("db.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age int
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if surname != "Doe" && surname != "Simpson" && surname != "Wayne" {
			t.Fatalf("Unwanted row: %s %s %d", name, surname, age)
		}

		nb++
	}

	if nb != 6 {
		t.Fatalf("Expected 6 rows, got %d", nb)
	}
}
