package parser

import (
	"testing"
)

func TestSimpleCTE(t *testing.T) {
	query := `WITH cte_name AS (SELECT id, name FROM users) SELECT * FROM cte_name`
	instructions, err := ParseInstruction(query)
	if err != nil {
		t.Fatalf("ParseInstruction error: %s", err)
	}
	if len(instructions) != 1 {
		t.Fatalf("Expected 1 instruction, got %d", len(instructions))
	}
	if len(instructions[0].Decls) != 1 {
		t.Fatalf("Expected 1 declaration, got %d", len(instructions[0].Decls))
	}
	if instructions[0].Decls[0].Token != WithToken {
		t.Fatalf("Expected WITH token, got %d", instructions[0].Decls[0].Token)
	}
}

func TestCTEWithWhereClause(t *testing.T) {
	query := `WITH filtered_users AS (
		SELECT id, name FROM users WHERE active = true
	) SELECT * FROM filtered_users`
	instructions, err := ParseInstruction(query)
	if err != nil {
		t.Fatalf("ParseInstruction error: %s", err)
	}
	if len(instructions) != 1 {
		t.Fatalf("Expected 1 instruction, got %d", len(instructions))
	}
}

func TestMultipleCTEs(t *testing.T) {
	query := `WITH 
		cte1 AS (SELECT id FROM table1),
		cte2 AS (SELECT name FROM table2)
	SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id`
	instructions, err := ParseInstruction(query)
	if err != nil {
		t.Fatalf("ParseInstruction error: %s", err)
	}
	if len(instructions) != 1 {
		t.Fatalf("Expected 1 instruction, got %d", len(instructions))
	}
}
