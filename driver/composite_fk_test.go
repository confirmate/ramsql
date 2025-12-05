package ramsql

import (
	"database/sql"
	"testing"
)

// TestCompositeForeignKey_DifferentColumnNames tests that composite foreign keys
// work correctly when the column names in the child table differ from those in
// the parent table.
func TestCompositeForeignKey_DifferentColumnNames(t *testing.T) {
	db, err := sql.Open("ramsql", "TestCompositeForeignKey_DifferentColumnNames")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	// Create parent table with composite primary key
	_, err = db.Exec(`
		CREATE TABLE categories (
			name TEXT,
			catalog_id TEXT,
			PRIMARY KEY (name, catalog_id)
		)
	`)
	if err != nil {
		t.Fatalf("CREATE TABLE categories: %s", err)
	}

	// Create child table with composite foreign key using different column names
	_, err = db.Exec(`
		CREATE TABLE controls (
			id TEXT PRIMARY KEY,
			category_name TEXT,
			category_catalog_id TEXT,
			FOREIGN KEY (category_name, category_catalog_id) REFERENCES categories(name, catalog_id)
		)
	`)
	if err != nil {
		t.Fatalf("CREATE TABLE controls: %s", err)
	}

	// Insert a row into the parent table
	_, err = db.Exec(`INSERT INTO categories (name, catalog_id) VALUES ('category-1', 'catalog-1')`)
	if err != nil {
		t.Fatalf("INSERT INTO categories: %s", err)
	}

	// Insert a row into the child table with matching values - this should succeed
	_, err = db.Exec(`INSERT INTO controls (id, category_name, category_catalog_id) VALUES ('control-1', 'category-1', 'catalog-1')`)
	if err != nil {
		t.Fatalf("INSERT INTO controls with valid FK: %s", err)
	}

	// Try to insert a row with invalid FK - first column matches but second doesn't
	_, err = db.Exec(`INSERT INTO controls (id, category_name, category_catalog_id) VALUES ('control-2', 'category-1', 'wrong-catalog')`)
	if err == nil {
		t.Fatalf("expected FK violation error for invalid catalog_id, got nil")
	}

	// Try to insert a row with invalid FK - second column matches but first doesn't
	_, err = db.Exec(`INSERT INTO controls (id, category_name, category_catalog_id) VALUES ('control-3', 'wrong-category', 'catalog-1')`)
	if err == nil {
		t.Fatalf("expected FK violation error for invalid category_name, got nil")
	}

	// Try to insert a row with completely invalid FK
	_, err = db.Exec(`INSERT INTO controls (id, category_name, category_catalog_id) VALUES ('control-4', 'wrong-category', 'wrong-catalog')`)
	if err == nil {
		t.Fatalf("expected FK violation error for invalid FK, got nil")
	}

	// Verify that the valid row was inserted
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM controls WHERE id = 'control-1'`).Scan(&count)
	if err != nil {
		t.Fatalf("SELECT COUNT: %s", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row in controls, got %d", count)
	}
}

// TestCompositeForeignKey_SameColumnNames tests composite foreign keys when
// the column names are the same in both parent and child tables.
func TestCompositeForeignKey_SameColumnNames(t *testing.T) {
	db, err := sql.Open("ramsql", "TestCompositeForeignKey_SameColumnNames")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	// Create parent table
	_, err = db.Exec(`
		CREATE TABLE parent (
			key1 TEXT,
			key2 TEXT,
			PRIMARY KEY (key1, key2)
		)
	`)
	if err != nil {
		t.Fatalf("CREATE TABLE parent: %s", err)
	}

	// Create child table with same column names
	_, err = db.Exec(`
		CREATE TABLE child (
			id TEXT PRIMARY KEY,
			key1 TEXT,
			key2 TEXT,
			FOREIGN KEY (key1, key2) REFERENCES parent(key1, key2)
		)
	`)
	if err != nil {
		t.Fatalf("CREATE TABLE child: %s", err)
	}

	// Insert parent row
	_, err = db.Exec(`INSERT INTO parent (key1, key2) VALUES ('a', 'b')`)
	if err != nil {
		t.Fatalf("INSERT INTO parent: %s", err)
	}

	// Insert child row - should succeed
	_, err = db.Exec(`INSERT INTO child (id, key1, key2) VALUES ('1', 'a', 'b')`)
	if err != nil {
		t.Fatalf("INSERT INTO child with valid FK: %s", err)
	}

	// Insert child row with invalid FK - should fail
	_, err = db.Exec(`INSERT INTO child (id, key1, key2) VALUES ('2', 'a', 'c')`)
	if err == nil {
		t.Fatalf("expected FK violation error, got nil")
	}
}

// TestCompositeForeignKey_Update tests UPDATE operations with composite FKs
func TestCompositeForeignKey_Update(t *testing.T) {
	db, err := sql.Open("ramsql", "TestCompositeForeignKey_Update")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	// Create parent and child tables
	_, err = db.Exec(`
		CREATE TABLE categories (
			name TEXT,
			catalog_id TEXT,
			PRIMARY KEY (name, catalog_id)
		)
	`)
	if err != nil {
		t.Fatalf("CREATE TABLE categories: %s", err)
	}

	_, err = db.Exec(`
		CREATE TABLE controls (
			id TEXT PRIMARY KEY,
			category_name TEXT,
			category_catalog_id TEXT,
			FOREIGN KEY (category_name, category_catalog_id) REFERENCES categories(name, catalog_id)
		)
	`)
	if err != nil {
		t.Fatalf("CREATE TABLE controls: %s", err)
	}

	// Insert parent rows
	_, err = db.Exec(`INSERT INTO categories (name, catalog_id) VALUES ('cat1', 'catalog1')`)
	if err != nil {
		t.Fatalf("INSERT INTO categories: %s", err)
	}
	_, err = db.Exec(`INSERT INTO categories (name, catalog_id) VALUES ('cat2', 'catalog2')`)
	if err != nil {
		t.Fatalf("INSERT INTO categories: %s", err)
	}

	// Insert child row
	_, err = db.Exec(`INSERT INTO controls (id, category_name, category_catalog_id) VALUES ('c1', 'cat1', 'catalog1')`)
	if err != nil {
		t.Fatalf("INSERT INTO controls: %s", err)
	}

	// Update child to reference a different valid parent - should succeed
	_, err = db.Exec(`UPDATE controls SET category_name = 'cat2', category_catalog_id = 'catalog2' WHERE id = 'c1'`)
	if err != nil {
		t.Fatalf("UPDATE controls with valid FK: %s", err)
	}

	// Update child to reference invalid parent - should fail
	_, err = db.Exec(`UPDATE controls SET category_name = 'invalid', category_catalog_id = 'invalid' WHERE id = 'c1'`)
	if err == nil {
		t.Fatalf("expected FK violation error for invalid update, got nil")
	}

	// Update parent row that is referenced by child - should fail
	_, err = db.Exec(`UPDATE categories SET name = 'cat2_modified' WHERE name = 'cat2' AND catalog_id = 'catalog2'`)
	if err == nil {
		t.Fatalf("expected FK restrict error for parent update, got nil")
	}
}

// TestCompositeForeignKey_Delete tests DELETE operations with composite FKs
func TestCompositeForeignKey_Delete(t *testing.T) {
	db, err := sql.Open("ramsql", "TestCompositeForeignKey_Delete")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	// Create parent and child tables
	_, err = db.Exec(`
		CREATE TABLE categories (
			name TEXT,
			catalog_id TEXT,
			PRIMARY KEY (name, catalog_id)
		)
	`)
	if err != nil {
		t.Fatalf("CREATE TABLE categories: %s", err)
	}

	_, err = db.Exec(`
		CREATE TABLE controls (
			id TEXT PRIMARY KEY,
			category_name TEXT,
			category_catalog_id TEXT,
			FOREIGN KEY (category_name, category_catalog_id) REFERENCES categories(name, catalog_id)
		)
	`)
	if err != nil {
		t.Fatalf("CREATE TABLE controls: %s", err)
	}

	// Insert parent rows
	_, err = db.Exec(`INSERT INTO categories (name, catalog_id) VALUES ('cat1', 'catalog1')`)
	if err != nil {
		t.Fatalf("INSERT INTO categories: %s", err)
	}
	_, err = db.Exec(`INSERT INTO categories (name, catalog_id) VALUES ('cat2', 'catalog2')`)
	if err != nil {
		t.Fatalf("INSERT INTO categories: %s", err)
	}

	// Insert child row
	_, err = db.Exec(`INSERT INTO controls (id, category_name, category_catalog_id) VALUES ('c1', 'cat1', 'catalog1')`)
	if err != nil {
		t.Fatalf("INSERT INTO controls: %s", err)
	}

	// Try to delete parent row that is referenced - should fail
	_, err = db.Exec(`DELETE FROM categories WHERE name = 'cat1' AND catalog_id = 'catalog1'`)
	if err == nil {
		t.Fatalf("expected FK restrict error for parent delete, got nil")
	}

	// Delete parent row that is NOT referenced - should succeed
	_, err = db.Exec(`DELETE FROM categories WHERE name = 'cat2' AND catalog_id = 'catalog2'`)
	if err != nil {
		t.Fatalf("DELETE unreferenced parent: %s", err)
	}

	// Delete child row - should succeed
	_, err = db.Exec(`DELETE FROM controls WHERE id = 'c1'`)
	if err != nil {
		t.Fatalf("DELETE child: %s", err)
	}

	// Now delete the parent - should succeed since child is gone
	_, err = db.Exec(`DELETE FROM categories WHERE name = 'cat1' AND catalog_id = 'catalog1'`)
	if err != nil {
		t.Fatalf("DELETE parent after child removed: %s", err)
	}
}
