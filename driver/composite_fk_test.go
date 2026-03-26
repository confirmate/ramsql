package ramsql

import (
	"database/sql"
	"testing"
)

// TestOnDeleteCascade tests that ON DELETE CASCADE deletes child rows when a parent is deleted.
func TestOnDeleteCascade(t *testing.T) {
	db, err := sql.Open("ramsql", "TestOnDeleteCascade")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	setup := []string{
		`CREATE TABLE catalogs (id TEXT PRIMARY KEY)`,
		`CREATE TABLE categories (
			id TEXT PRIMARY KEY,
			catalog_id TEXT,
			FOREIGN KEY (catalog_id) REFERENCES catalogs(id) ON DELETE CASCADE
		)`,
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup failed: %s (query: %s)", err, q)
		}
	}

	// Insert a catalog and two categories that reference it
	if _, err := db.Exec(`INSERT INTO catalogs (id) VALUES ('catalog-1')`); err != nil {
		t.Fatalf("insert catalog: %s", err)
	}
	if _, err := db.Exec(`INSERT INTO categories (id, catalog_id) VALUES ('cat-1', 'catalog-1')`); err != nil {
		t.Fatalf("insert category 1: %s", err)
	}
	if _, err := db.Exec(`INSERT INTO categories (id, catalog_id) VALUES ('cat-2', 'catalog-1')`); err != nil {
		t.Fatalf("insert category 2: %s", err)
	}

	// Deleting the catalog should cascade to its categories
	if _, err := db.Exec(`DELETE FROM catalogs WHERE id = 'catalog-1'`); err != nil {
		t.Fatalf("delete catalog (expected cascade): %s", err)
	}

	// Verify catalog is gone
	var catalogCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM catalogs`).Scan(&catalogCount); err != nil {
		t.Fatalf("count catalogs: %s", err)
	}
	if catalogCount != 0 {
		t.Fatalf("expected 0 catalogs, got %d", catalogCount)
	}

	// Verify categories were also deleted by cascade
	var categoryCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM categories`).Scan(&categoryCount); err != nil {
		t.Fatalf("count categories: %s", err)
	}
	if categoryCount != 0 {
		t.Fatalf("expected 0 categories after cascade, got %d", categoryCount)
	}
}

// TestOnDeleteCascadeDeep tests ON DELETE CASCADE across multiple levels of nesting.
func TestOnDeleteCascadeDeep(t *testing.T) {
	db, err := sql.Open("ramsql", "TestOnDeleteCascadeDeep")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	setup := []string{
		`CREATE TABLE catalogs (id TEXT PRIMARY KEY)`,
		`CREATE TABLE categories (
			id TEXT PRIMARY KEY,
			catalog_id TEXT,
			FOREIGN KEY (catalog_id) REFERENCES catalogs(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE controls (
			id TEXT PRIMARY KEY,
			category_id TEXT,
			FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
		)`,
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup failed: %s (query: %s)", err, q)
		}
	}

	if _, err := db.Exec(`INSERT INTO catalogs (id) VALUES ('c1')`); err != nil {
		t.Fatalf("insert catalog: %s", err)
	}
	if _, err := db.Exec(`INSERT INTO categories (id, catalog_id) VALUES ('cat1', 'c1')`); err != nil {
		t.Fatalf("insert category: %s", err)
	}
	if _, err := db.Exec(`INSERT INTO controls (id, category_id) VALUES ('ctrl1', 'cat1')`); err != nil {
		t.Fatalf("insert control: %s", err)
	}

	// Deleting the catalog should cascade through categories to controls
	if _, err := db.Exec(`DELETE FROM catalogs WHERE id = 'c1'`); err != nil {
		t.Fatalf("delete catalog (expected deep cascade): %s", err)
	}

	var count int
	for _, tbl := range []string{"catalogs", "categories", "controls"} {
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + tbl).Scan(&count); err != nil {
			t.Fatalf("count %s: %s", tbl, err)
		}
		if count != 0 {
			t.Fatalf("expected 0 rows in %s after cascade, got %d", tbl, count)
		}
	}
}

// TestOnDeleteCascadeColumnLevel tests ON DELETE CASCADE on a column-level REFERENCES clause.
func TestOnDeleteCascadeColumnLevel(t *testing.T) {
	db, err := sql.Open("ramsql", "TestOnDeleteCascadeColumnLevel")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	setup := []string{
		`CREATE TABLE parent (id TEXT PRIMARY KEY)`,
		`CREATE TABLE child (
			id TEXT PRIMARY KEY,
			parent_id TEXT REFERENCES parent(id) ON DELETE CASCADE
		)`,
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup failed: %s (query: %s)", err, q)
		}
	}

	if _, err := db.Exec(`INSERT INTO parent (id) VALUES ('p1')`); err != nil {
		t.Fatalf("insert parent: %s", err)
	}
	if _, err := db.Exec(`INSERT INTO child (id, parent_id) VALUES ('c1', 'p1')`); err != nil {
		t.Fatalf("insert child: %s", err)
	}

	if _, err := db.Exec(`DELETE FROM parent WHERE id = 'p1'`); err != nil {
		t.Fatalf("delete parent with CASCADE: %s", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM child`).Scan(&count); err != nil {
		t.Fatalf("count child: %s", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 child rows after cascade delete, got %d", count)
	}
}

// TestOnDeleteRestrictStillWorks verifies that RESTRICT (or no action) still prevents deletes.
func TestOnDeleteRestrictStillWorks(t *testing.T) {
	db, err := sql.Open("ramsql", "TestOnDeleteRestrictStillWorks")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	setup := []string{
		`CREATE TABLE parent (id TEXT PRIMARY KEY)`,
		`CREATE TABLE child (
			id TEXT PRIMARY KEY,
			parent_id TEXT,
			FOREIGN KEY (parent_id) REFERENCES parent(id)
		)`,
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup failed: %s (query: %s)", err, q)
		}
	}

	if _, err := db.Exec(`INSERT INTO parent (id) VALUES ('p1')`); err != nil {
		t.Fatalf("insert parent: %s", err)
	}
	if _, err := db.Exec(`INSERT INTO child (id, parent_id) VALUES ('c1', 'p1')`); err != nil {
		t.Fatalf("insert child: %s", err)
	}

	// Delete should fail because child references parent and no CASCADE
	if _, err := db.Exec(`DELETE FROM parent WHERE id = 'p1'`); err == nil {
		t.Fatal("expected FK restrict error on parent delete, got nil")
	}
}


// work correctly when the column names in the child table differ from those in
// the parent table.
func TestCompositeForeignKey_DifferentColumnNames(t *testing.T) {
	db, err := sql.Open("ramsql", "TestCompositeForeignKey_DifferentColumnNames")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	setup := []string{
		`CREATE TABLE categories (
			name TEXT,
			catalog_id TEXT,
			PRIMARY KEY (name, catalog_id)
		)`,
		`CREATE TABLE controls (
			id TEXT PRIMARY KEY,
			category_name TEXT,
			category_catalog_id TEXT,
			FOREIGN KEY (category_name, category_catalog_id) REFERENCES categories(name, catalog_id)
		)`,
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup failed: %s (query: %s)", err, q)
		}
	}

	// insert parent row
	if _, err := db.Exec(`INSERT INTO categories (name, catalog_id) VALUES ('category-1', 'catalog-1')`); err != nil {
		t.Fatalf("insert parent: %s", err)
	}

	// insert child with matching values - should succeed
	if _, err := db.Exec(`INSERT INTO controls (id, category_name, category_catalog_id) VALUES ('control-1', 'category-1', 'catalog-1')`); err != nil {
		t.Fatalf("insert child with valid FK: %s", err)
	}

	// try insert with invalid FK - first column matches but second doesn't
	if _, err := db.Exec(`INSERT INTO controls (id, category_name, category_catalog_id) VALUES ('control-2', 'category-1', 'wrong-catalog')`); err == nil {
		t.Fatalf("expected FK violation error for invalid catalog_id, got nil")
	}

	// try insert with invalid FK - second column matches but first doesn't
	if _, err := db.Exec(`INSERT INTO controls (id, category_name, category_catalog_id) VALUES ('control-3', 'wrong-category', 'catalog-1')`); err == nil {
		t.Fatalf("expected FK violation error for invalid category_name, got nil")
	}

	// try insert with completely invalid FK
	if _, err := db.Exec(`INSERT INTO controls (id, category_name, category_catalog_id) VALUES ('control-4', 'wrong-category', 'wrong-catalog')`); err == nil {
		t.Fatalf("expected FK violation error for invalid FK, got nil")
	}

	// verify valid row was inserted
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM controls WHERE id = 'control-1'`).Scan(&count); err != nil {
		t.Fatalf("query count: %s", err)
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
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	setup := []string{
		`CREATE TABLE parent (
			key1 TEXT,
			key2 TEXT,
			PRIMARY KEY (key1, key2)
		)`,
		`CREATE TABLE child (
			id TEXT PRIMARY KEY,
			key1 TEXT,
			key2 TEXT,
			FOREIGN KEY (key1, key2) REFERENCES parent(key1, key2)
		)`,
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup failed: %s (query: %s)", err, q)
		}
	}

	// insert parent row
	if _, err := db.Exec(`INSERT INTO parent (key1, key2) VALUES ('a', 'b')`); err != nil {
		t.Fatalf("insert parent: %s", err)
	}

	// insert child row - should succeed
	if _, err := db.Exec(`INSERT INTO child (id, key1, key2) VALUES ('1', 'a', 'b')`); err != nil {
		t.Fatalf("insert child with valid FK: %s", err)
	}

	// insert child row with invalid FK - should fail
	if _, err := db.Exec(`INSERT INTO child (id, key1, key2) VALUES ('2', 'a', 'c')`); err == nil {
		t.Fatalf("expected FK violation error, got nil")
	}
}

// TestCompositeForeignKey_Update tests UPDATE operations with composite FKs
func TestCompositeForeignKey_Update(t *testing.T) {
	db, err := sql.Open("ramsql", "TestCompositeForeignKey_Update")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	setup := []string{
		`CREATE TABLE categories (
			name TEXT,
			catalog_id TEXT,
			PRIMARY KEY (name, catalog_id)
		)`,
		`CREATE TABLE controls (
			id TEXT PRIMARY KEY,
			category_name TEXT,
			category_catalog_id TEXT,
			FOREIGN KEY (category_name, category_catalog_id) REFERENCES categories(name, catalog_id)
		)`,
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup failed: %s (query: %s)", err, q)
		}
	}

	// insert parent rows
	if _, err := db.Exec(`INSERT INTO categories (name, catalog_id) VALUES ('cat1', 'catalog1')`); err != nil {
		t.Fatalf("insert parent: %s", err)
	}
	if _, err := db.Exec(`INSERT INTO categories (name, catalog_id) VALUES ('cat2', 'catalog2')`); err != nil {
		t.Fatalf("insert parent: %s", err)
	}

	// insert child row
	if _, err := db.Exec(`INSERT INTO controls (id, category_name, category_catalog_id) VALUES ('c1', 'cat1', 'catalog1')`); err != nil {
		t.Fatalf("insert child: %s", err)
	}

	// update child to reference different valid parent - should succeed
	if _, err := db.Exec(`UPDATE controls SET category_name = 'cat2', category_catalog_id = 'catalog2' WHERE id = 'c1'`); err != nil {
		t.Fatalf("update child with valid FK: %s", err)
	}

	// update child to reference invalid parent - should fail
	if _, err := db.Exec(`UPDATE controls SET category_name = 'invalid', category_catalog_id = 'invalid' WHERE id = 'c1'`); err == nil {
		t.Fatalf("expected FK violation error for invalid update, got nil")
	}

	// update parent row referenced by child - should fail
	if _, err := db.Exec(`UPDATE categories SET name = 'cat2_modified' WHERE name = 'cat2' AND catalog_id = 'catalog2'`); err == nil {
		t.Fatalf("expected FK restrict error for parent update, got nil")
	}
}

// TestCompositeForeignKey_Delete tests DELETE operations with composite FKs
func TestCompositeForeignKey_Delete(t *testing.T) {
	db, err := sql.Open("ramsql", "TestCompositeForeignKey_Delete")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	setup := []string{
		`CREATE TABLE categories (
			name TEXT,
			catalog_id TEXT,
			PRIMARY KEY (name, catalog_id)
		)`,
		`CREATE TABLE controls (
			id TEXT PRIMARY KEY,
			category_name TEXT,
			category_catalog_id TEXT,
			FOREIGN KEY (category_name, category_catalog_id) REFERENCES categories(name, catalog_id)
		)`,
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup failed: %s (query: %s)", err, q)
		}
	}

	// insert parent rows
	if _, err := db.Exec(`INSERT INTO categories (name, catalog_id) VALUES ('cat1', 'catalog1')`); err != nil {
		t.Fatalf("insert parent: %s", err)
	}
	if _, err := db.Exec(`INSERT INTO categories (name, catalog_id) VALUES ('cat2', 'catalog2')`); err != nil {
		t.Fatalf("insert parent: %s", err)
	}

	// insert child row
	if _, err := db.Exec(`INSERT INTO controls (id, category_name, category_catalog_id) VALUES ('c1', 'cat1', 'catalog1')`); err != nil {
		t.Fatalf("insert child: %s", err)
	}

	// attempt to delete parent row referenced by child - should fail
	if _, err := db.Exec(`DELETE FROM categories WHERE name = 'cat1' AND catalog_id = 'catalog1'`); err == nil {
		t.Fatalf("expected FK restrict error on parent delete, got nil")
	}

	// delete parent row NOT referenced - should succeed
	if _, err := db.Exec(`DELETE FROM categories WHERE name = 'cat2' AND catalog_id = 'catalog2'`); err != nil {
		t.Fatalf("delete unreferenced parent: %s", err)
	}

	// delete child row - should succeed
	if _, err := db.Exec(`DELETE FROM controls WHERE id = 'c1'`); err != nil {
		t.Fatalf("delete child: %s", err)
	}

	// now delete parent - should succeed since child is gone
	if _, err := db.Exec(`DELETE FROM categories WHERE name = 'cat1' AND catalog_id = 'catalog1'`); err != nil {
		t.Fatalf("delete parent after child removed: %s", err)
	}
}

// TestCompositeForeignKey_DeletePartialMatch tests that DELETE RESTRICT properly
// checks ALL columns of a composite FK, not just the first one.
func TestCompositeForeignKey_DeletePartialMatch(t *testing.T) {
	db, err := sql.Open("ramsql", "TestCompositeForeignKey_DeletePartialMatch")
	if err != nil {
		t.Fatalf("sql.Open : %s", err)
	}
	defer db.Close()

	setup := []string{
		`CREATE TABLE categories (
			name TEXT,
			catalog_id TEXT,
			PRIMARY KEY (name, catalog_id)
		)`,
		`CREATE TABLE controls (
			id TEXT PRIMARY KEY,
			category_name TEXT,
			category_catalog_id TEXT,
			FOREIGN KEY (category_name, category_catalog_id) REFERENCES categories(name, catalog_id)
		)`,
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup failed: %s (query: %s)", err, q)
		}
	}

	// Insert two parent rows with same name but different catalog_id
	if _, err := db.Exec(`INSERT INTO categories (name, catalog_id) VALUES ('cat', 'catalog1')`); err != nil {
		t.Fatalf("insert parent 1: %s", err)
	}
	if _, err := db.Exec(`INSERT INTO categories (name, catalog_id) VALUES ('cat', 'catalog2')`); err != nil {
		t.Fatalf("insert parent 2: %s", err)
	}

	// Insert child referencing the first parent
	if _, err := db.Exec(`INSERT INTO controls (id, category_name, category_catalog_id) VALUES ('c1', 'cat', 'catalog1')`); err != nil {
		t.Fatalf("insert child: %s", err)
	}

	// Delete the SECOND parent (not referenced) - should succeed
	// This tests that we check ALL columns, not just the first one
	if _, err := db.Exec(`DELETE FROM categories WHERE name = 'cat' AND catalog_id = 'catalog2'`); err != nil {
		t.Fatalf("delete unreferenced parent (cat, catalog2): %s", err)
	}

	// Delete the FIRST parent (referenced) - should fail
	if _, err := db.Exec(`DELETE FROM categories WHERE name = 'cat' AND catalog_id = 'catalog1'`); err == nil {
		t.Fatalf("expected FK restrict error for referenced parent (cat, catalog1), got nil")
	}

	// Verify that only the second parent was deleted
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM categories`).Scan(&count); err != nil {
		t.Fatalf("query count: %s", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 parent row remaining, got %d", count)
	}
}
