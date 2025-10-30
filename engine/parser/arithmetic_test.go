package parser

import (
	"testing"
)

func TestArithmeticInSelect(t *testing.T) {
	query := `SELECT 8 * typlen FROM test`
	parse(query, 1, t)
}

func TestComparisonInSelect(t *testing.T) {
	query := `SELECT col = 'YES' FROM test`
	parse(query, 1, t)
}

func TestMultipleExpressionsInSelect(t *testing.T) {
	query := `SELECT col1, col2 = 'YES', 8 * col3 FROM test`
	parse(query, 1, t)
}
