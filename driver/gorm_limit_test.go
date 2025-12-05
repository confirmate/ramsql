package ramsql

import (
	"database/sql"
	"fmt"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Item struct {
	ID string `gorm:"primaryKey"`
}

// Test case that replicates the exact issue from the GitHub issue
// Note: Using postgres driver as specified in the issue report to test
// ramsql with the same GORM dialect configuration the user was using
func TestGormLimitReturnsMultipleRows(t *testing.T) {
	db, err := sql.Open("ramsql", "TestGormLimitReturnsMultipleRows")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	g, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		t.Fatalf("gorm.Open: %s", err)
	}

	err = g.AutoMigrate(&Item{})
	if err != nil {
		t.Fatalf("AutoMigrate: %s", err)
	}

	// Insert 5 rows
	for i := 1; i <= 5; i++ {
		err = g.Create(&Item{ID: fmt.Sprintf("%d", i)}).Error
		if err != nil {
			t.Fatalf("Create: %s", err)
		}
	}

	// Query with LIMIT 2
	var items []Item
	result := g.Limit(2).Offset(0).Find(&items)

	if result.Error != nil {
		t.Fatalf("Query error: %v", result.Error)
	}

	if len(items) != 2 {
		t.Fatalf("Expected 2 items, got %d. Items: %+v", len(items), items)
	}

	t.Logf("Successfully retrieved %d items: %+v", len(items), items)
}
