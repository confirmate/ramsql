package parser

import (
	"testing"
)

func TestSimpleCTE(t *testing.T) {
	query := `WITH cte AS (SELECT * FROM table1) SELECT * FROM cte`
	instructions, err := ParseInstruction(query)
	if err != nil {
		t.Fatalf("ParseInstruction failed: %v", err)
	}
	if len(instructions) != 1 {
		t.Fatalf("Expected 1 instruction, got %d", len(instructions))
	}
	// Check that we have a WITH decl
	if len(instructions[0].Decls) < 2 {
		t.Fatalf("Expected at least 2 decls (WITH and SELECT), got %d", len(instructions[0].Decls))
	}
	if instructions[0].Decls[0].Token != WithToken {
		t.Fatalf("Expected first decl to be WithToken, got %d", instructions[0].Decls[0].Token)
	}
	if instructions[0].Decls[1].Token != SelectToken {
		t.Fatalf("Expected second decl to be SelectToken, got %d", instructions[0].Decls[1].Token)
	}
}

func TestCTEWithWhere(t *testing.T) {
	query := `WITH sorted_results AS (
		SELECT * FROM evaluation_results WHERE id > 5
	)
	SELECT * FROM sorted_results WHERE row_number = 1`
	instructions, err := ParseInstruction(query)
	if err != nil {
		t.Fatalf("ParseInstruction failed: %v", err)
	}
	if len(instructions) != 1 {
		t.Fatalf("Expected 1 instruction, got %d", len(instructions))
	}
}

func TestMultipleCTEs(t *testing.T) {
	query := `WITH cte1 AS (SELECT * FROM table1), cte2 AS (SELECT * FROM table2) SELECT * FROM cte1`
	instructions, err := ParseInstruction(query)
	if err != nil {
		t.Fatalf("ParseInstruction failed: %v", err)
	}
	if len(instructions) != 1 {
		t.Fatalf("Expected 1 instruction, got %d", len(instructions))
	}
}
