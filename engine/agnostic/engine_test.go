package agnostic

import "testing"

func TestRelationStoresForeignKeysMetadata(t *testing.T) {
	// Build a simple schema with parent and child
	e := NewEngine()
	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	parentAttrs := []Attribute{
		NewAttribute("id", "bigserial").WithAutoIncrement(),
		NewAttribute("name", "varchar"),
	}
	if err := tx.CreateRelation("", "users", parentAttrs, []string{"id"}); err != nil {
		t.Fatalf("create parent: %v", err)
	}

	childAttrs := []Attribute{
		NewAttribute("user_id", "bigint"),
	}
	fkDef := NewForeignKey("child_user_fk").
		WithLocalColumn("user_id").
		WithRefRelation("users").
		WithRefColumn("id")
	// attach fk to attribute (column-level in this test)
	childAttrs[0] = childAttrs[0].WithForeignKeyStruct(fkDef)
	if err := tx.CreateRelation("", "memberships", childAttrs, nil); err != nil {
		t.Fatalf("create child: %v", err)
	}

	s, err := e.schema(DefaultSchema)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	r, err := s.Relation("memberships")
	if err != nil {
		t.Fatalf("relation: %v", err)
	}
	// verify via attribute metadata instead of relation-level accessor
	_, attr, err := r.Attribute("user_id")
	if err != nil {
		t.Fatalf("attribute: %v", err)
	}
	afk := attr.ForeignKey()
	if afk == nil {
		t.Fatalf("expected attribute FK to be set")
	}
	if afk.Name() != "child_user_fk" {
		t.Fatalf("unexpected fk name: %s", afk.Name())
	}
	if afk.RefRelation() != "users" {
		t.Fatalf("unexpected fk ref relation: %s", afk.RefRelation())
	}
	if len(afk.LocalColumns()) != 1 || afk.LocalColumns()[0] != "user_id" {
		t.Fatalf("unexpected local columns: %v", afk.LocalColumns())
	}
	if len(afk.RefColumns()) != 1 || afk.RefColumns()[0] != "id" {
		t.Fatalf("unexpected ref columns: %v", afk.RefColumns())
	}
}
