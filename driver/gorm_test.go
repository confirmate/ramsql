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
	Price      uint
	TestBigint uint64 `gorm:"test_bigint;type:BIGINT UNSIGNED AUTO_INCREMENT"`
}

type Order struct {
	gorm.Model
	OrderNumber string
	Status      string
	Products    []Product `gorm:"many2many:order_details;"`
}

type OrderDetail struct {
	OrderID   uint `gorm:"primaryKey"`
	ProductID uint `gorm:"primaryKey"`
	Quantity  int
	Price     uint
	CreatedAt int64 `gorm:"autoCreateTime"`
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

	// Create initial product
	product := Product{Code: "D42", Price: 100}
	err = db.Create(&product).Error
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

	// Test custom join table with Orders and OrderDetails
	// Set up the many-to-many relationship with custom join table
	err = db.SetupJoinTable(&Order{}, "Products", &OrderDetail{})
	if err != nil {
		t.Fatalf("cannot setup join table: %s", err)
	}

	// Migrate the schema for orders
	err = db.AutoMigrate(&Order{}, &OrderDetail{})
	if err != nil {
		t.Fatalf("cannot automigrate orders: %s", err)
	}

	// Create another product for the order (we already have product with ID 1)
	product2 := Product{Code: "GADGET-002", Price: 250}
	err = db.Create(&product2).Error
	if err != nil {
		t.Fatalf("cannot create product2: %s", err)
	}

	// Create an order
	order := Order{
		OrderNumber: "ORD-2025-001",
		Status:      "pending",
	}
	err = db.Create(&order).Error
	if err != nil {
		t.Fatalf("cannot create order: %s", err)
	}

	// Manually insert into the join table with custom fields (quantity, price)
	// Use the existing product (ID 1) and the newly created product2
	orderDetail1 := OrderDetail{
		OrderID:   order.ID,
		ProductID: product.ID, // reuse the existing product
		Quantity:  2,
		Price:     product.Price, // 200 from the update above
	}
	err = db.Create(&orderDetail1).Error
	if err != nil {
		t.Fatalf("cannot create order detail 1: %s", err)
	}

	orderDetail2 := OrderDetail{
		OrderID:   order.ID,
		ProductID: product2.ID,
		Quantity:  1,
		Price:     product2.Price,
	}
	err = db.Create(&orderDetail2).Error
	if err != nil {
		t.Fatalf("cannot create order detail 2: %s", err)
	}

	// Verify the order details exist with correct quantities and prices
	var details []OrderDetail
	err = db.Where("order_id = ?", order.ID).Find(&details).Error
	if err != nil {
		t.Fatalf("cannot find order details: %s", err)
	}
	if len(details) != 2 {
		t.Fatalf("expected 2 order details, got %d", len(details))
	}

	// Verify custom join table fields
	if details[0].Quantity != 2 || details[0].Price != 200 {
		t.Fatalf("unexpected detail 1: quantity=%d, price=%d", details[0].Quantity, details[0].Price)
	}
	if details[1].Quantity != 1 || details[1].Price != 250 {
		t.Fatalf("unexpected detail 2: quantity=%d, price=%d", details[1].Quantity, details[1].Price)
	}

	// Test preloading products through the join table
	var orderWithProducts Order
	err = db.Preload("Products").First(&orderWithProducts, order.ID).Error
	if err != nil {
		t.Fatalf("cannot preload products: %s", err)
	}

	if len(orderWithProducts.Products) != 2 {
		t.Fatalf("expected 2 products in order, got %d", len(orderWithProducts.Products))
	}

	// Test updating order status
	err = db.Model(&order).Update("Status", "shipped").Error
	if err != nil {
		t.Fatalf("cannot update order status: %s", err)
	}

	var updatedOrder Order
	err = db.First(&updatedOrder, order.ID).Error
	if err != nil {
		t.Fatalf("cannot fetch updated order: %s", err)
	}
	if updatedOrder.Status != "shipped" {
		t.Fatalf("expected status 'shipped', got '%s'", updatedOrder.Status)
	}

	// Delete - delete product (moved to end to allow order testing)
	err = db.Delete(&product, 1).Error
	if err != nil {
		t.Fatalf("cannot delete: %s", err)
	}
}
