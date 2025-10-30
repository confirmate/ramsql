package parser

import "testing"

func TestWhereArithmeticExpressions(t *testing.T) {
	queries := []string{
		`SELECT * FROM products WHERE price * quantity > 1000`,
		`SELECT * FROM people WHERE age + 5 >= 18`,
		`SELECT * FROM orders WHERE total - discount < 100`,
		`SELECT * FROM conversions WHERE amount / rate = 10`,
	}

	for _, q := range queries {
		parse(q, 1, t)
	}
}
