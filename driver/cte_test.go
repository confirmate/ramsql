package ramsql

import (
	"database/sql"
	"testing"
)

func TestSimpleCTE(t *testing.T) {
	db, err := sql.Open("ramsql", "TestSimpleCTE")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE users (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %s", err)
	}

	// Insert test data
	_, err = db.Exec("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("INSERT: %s", err)
	}

	_, err = db.Exec("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
	if err != nil {
		t.Fatalf("INSERT: %s", err)
	}

	// Test simple CTE
	rows, err := db.Query("WITH user_cte AS (SELECT * FROM users WHERE age > 20) SELECT * FROM user_cte")
	if err != nil {
		t.Fatalf("CTE query failed: %s", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, age int
		var name string
		err = rows.Scan(&id, &name, &age)
		if err != nil {
			t.Fatalf("Scan: %s", err)
		}
		count++
	}

	if count != 2 {
		t.Fatalf("Expected 2 rows, got %d", count)
	}
}

func TestCTEWithFilter(t *testing.T) {
	db, err := sql.Open("ramsql", "TestCTEWithFilter")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE products (id INT, name TEXT, price INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %s", err)
	}

	// Insert test data
	_, err = db.Exec("INSERT INTO products (id, name, price) VALUES (1, 'Widget', 100)")
	if err != nil {
		t.Fatalf("INSERT: %s", err)
	}

	_, err = db.Exec("INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 200)")
	if err != nil {
		t.Fatalf("INSERT: %s", err)
	}

	_, err = db.Exec("INSERT INTO products (id, name, price) VALUES (3, 'Tool', 50)")
	if err != nil {
		t.Fatalf("INSERT: %s", err)
	}

	// Test CTE with filter in main query
	rows, err := db.Query("WITH expensive AS (SELECT * FROM products WHERE price > 75) SELECT * FROM expensive WHERE price < 150")
	if err != nil {
		t.Fatalf("CTE query failed: %s", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, price int
		var name string
		err = rows.Scan(&id, &name, &price)
		if err != nil {
			t.Fatalf("Scan: %s", err)
		}
		count++
		if price <= 75 || price >= 150 {
			t.Fatalf("Row should have price > 75 and < 150, got %d", price)
		}
	}

	if count != 1 {
		t.Fatalf("Expected 1 row, got %d", count)
	}
}

func TestMultipleCTEs(t *testing.T) {
	db, err := sql.Open("ramsql", "TestMultipleCTEs")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec("CREATE TABLE orders (id INT, customer_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders: %s", err)
	}

	_, err = db.Exec("CREATE TABLE customers (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE customers: %s", err)
	}

	// Insert test data
	_, err = db.Exec("INSERT INTO customers (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT customers: %s", err)
	}

	_, err = db.Exec("INSERT INTO orders (id, customer_id, amount) VALUES (1, 1, 100)")
	if err != nil {
		t.Fatalf("INSERT orders: %s", err)
	}

	// Test multiple CTEs
	query := `WITH 
		big_orders AS (SELECT * FROM orders WHERE amount > 50),
		alice AS (SELECT * FROM customers WHERE name = 'Alice')
	SELECT * FROM big_orders`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Multiple CTE query failed: %s", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, customer_id, amount int
		err = rows.Scan(&id, &customer_id, &amount)
		if err != nil {
			t.Fatalf("Scan: %s", err)
		}
		count++
	}

	if count != 1 {
		t.Fatalf("Expected 1 row, got %d", count)
	}
}
