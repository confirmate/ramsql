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

func TestWhereLikeExpressions(t *testing.T) {
	queries := []string{
		`SELECT * FROM resources WHERE name LIKE 'some%'`,
		`SELECT * FROM resources WHERE name LIKE 'some_thing%'`,
		`SELECT * FROM resources WHERE name LIKE 'some%' OR name LIKE 'other%' LIMIT 50`,
		`SELECT * FROM resources WHERE (name LIKE 'some%' OR name LIKE 'other%') LIMIT 50`,
	}

	for _, q := range queries {
		parse(q, 1, t)
	}
}
