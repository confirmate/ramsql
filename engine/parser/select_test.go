package parser

import (
	"testing"
)

func TestSelectWithSimpleTableAlias(t *testing.T) {
	query := `SELECT col1 FROM table1 AS t`
	parse(query, 1, t)
}

func TestSelectWithArithmeticAndAlias(t *testing.T) {
	query := `SELECT 8 * col1 FROM table1 AS t`
	parse(query, 1, t)
}

func TestSelectWithTableAlias(t *testing.T) {
	query := `SELECT c.column_name, 8 * c.typlen FROM columns AS c`
	parse(query, 1, t)
}
