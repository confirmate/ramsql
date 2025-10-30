package ramsql

import (
	"database/sql"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Product struct {
	gorm.Model
	Code       string
	Price      int32
	TestBigint uint64 `gorm:"test_bigint;type:BIGINT UNSIGNED AUTO_INCREMENT"`
}

// From https://gorm.io/docs/connecting_to_the_database.html
// and  https://gorm.io/docs/
func TestGormQuickStart(t *testing.T) {

	ramdb, err := sql.Open("ramsql", "TestGormQuickStart")
	if err != nil {
		t.Fatalf("cannot open db: %s", err)
	}

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn: ramdb,
	}),
		&gorm.Config{})
	if err != nil {
		t.Fatalf("cannot setup gorm: %s", err)
	}

	// Migrate the schema
	err = db.AutoMigrate(&Product{})
	if err != nil {
		t.Fatalf("cannot automigrate: %s", err)
	}

	// Create
	err = db.Create(&Product{Code: "D42", Price: 100}).Error
	if err != nil {
		t.Fatalf("cannot create: %s", err)
	}

	var id uint
	err = ramdb.QueryRow(`SELECT id FROM products WHERE id = 1 AND deleted_at IS NULL`).Scan(&id)
	if err != nil {
		t.Fatalf("cannot select manually: %s", err)
	}
	if id == 0 {
		t.Fatalf("unexpected 0 value for id")
	}

	// Read
	var product Product
	err = db.First(&product, 1).Error // find product with integer primary key
	if err != nil {
		t.Fatalf("cannot read with primary key: %s", err)
	}
	err = db.First(&product, "code = ?", "D42").Error // find product with code D42
	if err != nil {
		t.Fatalf("cannot read with code: %s", err)
	}
	err = db.First(&product, "Code = ?", "D42").Error // find product with code D42
	if err != nil {
		t.Fatalf("cannot read with Code: %s", err)
	}

	// Update - update product's price to 200
	err = db.Model(&product).Update("Price", 200).Error
	if err != nil {
		t.Fatalf("cannot update: %s", err)
	}
	// Update - update multiple fields
	err = db.Model(&product).Updates(Product{Price: 200, Code: "F42"}).Error // non-zero fields
	if err != nil {
		t.Fatalf("cannot update multiple fields 1: %s", err)
	}
	err = db.Model(&product).Updates(map[string]interface{}{"Price": 200, "Code": "F42"}).Error
	if err != nil {
		t.Fatalf("cannot update multiple fields 2: %s", err)
	}

	// Delete - delete product
	err = db.Delete(&product, 1).Error
	if err != nil {
		t.Fatalf("cannot delete: %s", err)
	}
}

// Test many-to-many relationship with join table
func TestGormManyToManyWithJoinTable(t *testing.T) {
	type Order struct {
		ID      uint
		OrderNo string
	}

	type OrderDetail struct {
		OrderID   uint
		ProductID uint
		Quantity  int
	}

	type Product struct {
		ID    uint
		Name  string
		Price float64
	}

	ramdb, err := sql.Open("ramsql", "TestGormManyToManyWithJoinTable")
	if err != nil {
		t.Fatalf("cannot open db: %s", err)
	}

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn: ramdb,
	}), &gorm.Config{})
	if err != nil {
		t.Fatalf("cannot setup gorm: %s", err)
	}

	// Create tables manually (GORM AutoMigrate may not work for join tables)
	err = db.Exec(`CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, order_no TEXT)`).Error
	if err != nil {
		t.Fatalf("cannot create orders table: %s", err)
	}

	err = db.Exec(`CREATE TABLE products (id BIGSERIAL PRIMARY KEY, name TEXT, price DECIMAL)`).Error
	if err != nil {
		t.Fatalf("cannot create products table: %s", err)
	}

	err = db.Exec(`CREATE TABLE order_details (
		order_id BIGINT,
		product_id BIGINT,
		quantity INT
	)`).Error
	if err != nil {
		t.Fatalf("cannot create order_details table: %s", err)
	}

	// Create test data
	product := Product{ID: 1, Name: "Widget", Price: 99.99}
	err = db.Create(&product).Error
	if err != nil {
		t.Fatalf("cannot create product: %s", err)
	}

	order := Order{ID: 1, OrderNo: "ORD-001"}
	err = db.Create(&order).Error
	if err != nil {
		t.Fatalf("cannot create order: %s", err)
	}

	// Create join table entry
	orderDetail := OrderDetail{OrderID: 1, ProductID: 1, Quantity: 5}
	err = db.Create(&orderDetail).Error
	if err != nil {
		t.Fatalf("cannot create order detail: %s", err)
	}

	// Test join query with aliases
	type Result struct {
		OrderNo  string
		Name     string
		Quantity int
		Price    float64
	}

	var results []Result
	err = db.Raw(`
		SELECT o.order_no, p.name, od.quantity, p.price
		FROM orders o
		JOIN order_details od ON o.id = od.order_id
		JOIN products p ON od.product_id = p.id
	`).Scan(&results).Error
	if err != nil {
		t.Fatalf("cannot execute join query with aliases: %s", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].OrderNo != "ORD-001" {
		t.Errorf("expected OrderNo 'ORD-001', got '%s'", results[0].OrderNo)
	}
	if results[0].Name != "Widget" {
		t.Errorf("expected Name 'Widget', got '%s'", results[0].Name)
	}
	if results[0].Quantity != 5 {
		t.Errorf("expected Quantity 5, got %d", results[0].Quantity)
	}
	if results[0].Price != 99.99 {
		t.Errorf("expected Price 99.99, got %f", results[0].Price)
	}
}
