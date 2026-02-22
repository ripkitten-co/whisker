//go:build integration

package hooks

import (
	"context"
	"testing"

	whisker "github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/internal/testutil"
	"gorm.io/gorm"
)

type GormUser struct {
	ID      string `gorm:"primaryKey"`
	Name    string
	Email   string
	Version int
}

func (GormUser) TableName() string { return "users" }

func TestGORM_CreateAndFind(t *testing.T) {
	connStr := testutil.SetupPostgres(t)
	ctx := context.Background()

	store, err := whisker.New(ctx, connStr)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	pool := NewPool(store)
	Register[GormUser](pool, "users")

	db, err := OpenGORM(pool)
	if err != nil {
		t.Fatalf("open gorm: %v", err)
	}

	// AutoMigrate should create whisker table
	if err := db.AutoMigrate(&GormUser{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Create
	user := GormUser{ID: "u1", Name: "Alice", Email: "alice@test.com"}
	if err := db.WithContext(ctx).Create(&user).Error; err != nil {
		t.Fatalf("create: %v", err)
	}

	// Find
	var found GormUser
	if err := db.WithContext(ctx).First(&found, "id = ?", "u1").Error; err != nil {
		t.Fatalf("find: %v", err)
	}
	if found.Name != "Alice" {
		t.Errorf("name = %q, want Alice", found.Name)
	}
	if found.Version != 1 {
		t.Errorf("version = %d, want 1", found.Version)
	}
}

// verifyGormUnused prevents the import from being removed.
var _ *gorm.DB
