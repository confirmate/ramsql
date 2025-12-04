package parser

import (
	"fmt"
)

func (p *parser) parseWhere(selectDecl *Decl) error {

	// May be WHERE  here
	// Can be ORDER BY if WHERE cause if implicit
	whereDecl, err := p.consumeToken(WhereToken)
	if err != nil {
		return err
	}
	selectDecl.Add(whereDecl)

	// Now should be a list of: Attribute and Operator and Value
	gotClause := false
	for {
		if !p.hasNext() && gotClause {
			break
		}

		if p.is(OrderToken, LimitToken, ForToken) {
			break
		}

		attributeDecl, err := p.parseCondition()
		if err != nil {
			return err
		}
		whereDecl.Add(attributeDecl)

		if p.is(AndToken, OrToken) {
			linkDecl, err := p.consumeToken(p.cur().Token)
			if err != nil {
				return err
			}
			whereDecl.Add(linkDecl)
		}

		// Got at least one clause
		gotClause = true
	}

	return nil
}

func (p *parser) parseCondition() (*Decl, error) {
	// Optionnaly, brackets

	// We may have the WHERE 1 condition
	if t := p.cur(); t.Token == NumberToken && t.Lexeme == "1" {
		attributeDecl := NewDecl(t)

		// WHERE 1
		if !p.hasNext() {
			return attributeDecl, nil
		}
		err := p.next()
		if err != nil {
			return nil, err
		}

		// WHERE 1 = 1
		if p.cur().Token == EqualityToken {
			t, err := p.isNext(NumberToken)
			if err == nil && t.Lexeme == "1" {
				_, err = p.consumeToken(EqualityToken)
				if err != nil {
					return nil, err
				}
				_, err = p.consumeToken(NumberToken)
				if err != nil {
					return nil, err
				}
			}
		}
		return attributeDecl, nil
	}

	// do we have brackets ?
	hasBracket := false
	if p.is(BracketOpeningToken) {
		_, err := p.consumeToken(BracketOpeningToken)
		if err != nil {
			return nil, err
		}

		// Parse first attribute
		firstAttr, err := p.parseAttribute()
		if err != nil {
			return nil, err
		}

		// Check if this is a tuple (comma follows) for tuple IN expressions
		if p.is(CommaToken) {
			// This is a tuple expression like (col1, col2) IN (...)
			tupleDecl := &Decl{Token: BracketOpeningToken, Lexeme: "("}
			tupleDecl.Add(firstAttr)

			// Parse remaining tuple elements
			for p.is(CommaToken) {
				_, err := p.consumeToken(CommaToken)
				if err != nil {
					return nil, err
				}
				attr, err := p.parseAttribute()
				if err != nil {
					return nil, err
				}
				tupleDecl.Add(attr)
			}

			// Consume closing bracket
			if _, err = p.consumeToken(BracketClosingToken); err != nil {
				return nil, err
			}

			// Now expect IN or NOT IN
			if p.is(InToken) {
				inDecl, err := p.parseTupleIn(len(tupleDecl.Decl))
				if err != nil {
					return nil, err
				}
				tupleDecl.Add(inDecl)
				return tupleDecl, nil
			} else if p.is(NotToken) {
				notDecl, err := p.consumeToken(NotToken)
				if err != nil {
					return nil, err
				}
				if !p.is(InToken) {
					return nil, fmt.Errorf("expected IN after NOT")
				}
				inDecl, err := p.parseTupleIn(len(tupleDecl.Decl))
				if err != nil {
					return nil, err
				}
				notDecl.Add(inDecl)
				tupleDecl.Add(notDecl)
				return tupleDecl, nil
			}
			return nil, fmt.Errorf("expected IN after tuple expression")
		}

		// Not a tuple, continue with normal bracket handling
		hasBracket = true
		// We already parsed the first attribute, so use it
		attributeDecl := firstAttr

		// Check for arithmetic operators first (*, +, -, /)
		// This allows expressions like: WHERE (price * quantity) > 1000
		if p.is(StarToken, PlusToken, MinusToken, DivideToken) {
			operatorDecl, err := p.consumeToken(p.cur().Token)
			if err != nil {
				return nil, err
			}
			attributeDecl.Add(operatorDecl)

			// Parse right side of arithmetic expression
			var rightDecl *Decl
			if p.is(NumberToken) {
				rightDecl, err = p.consumeToken(NumberToken)
				if err != nil {
					return nil, err
				}
			} else {
				rightDecl, err = p.parseAttribute()
				if err != nil {
					return nil, err
				}
			}
			attributeDecl.Add(rightDecl)
		}

		// Now check for comparison operators
		switch p.cur().Token {
		case EqualityToken, DistinctnessToken, LeftDipleToken, RightDipleToken, LessOrEqualToken, GreaterOrEqualToken:
			decl, err := p.consumeToken(p.cur().Token)
			if err != nil {
				return nil, err
			}
			attributeDecl.Add(decl)
		case InToken:
			inDecl, err := p.parseIn()
			if err != nil {
				return nil, err
			}
			attributeDecl.Add(inDecl)
			if hasBracket {
				if _, err = p.consumeToken(BracketClosingToken); err != nil {
					return nil, err
				}
			}
			return attributeDecl, nil
		case NotToken:
			notDecl, err := p.consumeToken(p.cur().Token)
			if err != nil {
				return nil, err
			}
			if p.cur().Token != InToken {
				return nil, fmt.Errorf("expected IN after NOT")
			}
			inDecl, err := p.parseIn()
			if err != nil {
				return nil, err
			}
			notDecl.Add(inDecl)
			attributeDecl.Add(notDecl)
			if hasBracket {
				if _, err = p.consumeToken(BracketClosingToken); err != nil {
					return nil, err
				}
			}
			return attributeDecl, nil
		case IsToken:
			decl, err := p.consumeToken(IsToken)
			if err != nil {
				return nil, err
			}
			attributeDecl.Add(decl)
			if p.cur().Token == NotToken {
				notDecl, err := p.consumeToken(NotToken)
				if err != nil {
					return nil, err
				}
				decl.Add(notDecl)
			}
			if p.cur().Token == NullToken {
				nullDecl, err := p.consumeToken(NullToken)
				if err != nil {
					return nil, err
				}
				decl.Add(nullDecl)
			}
			if hasBracket {
				if _, err = p.consumeToken(BracketClosingToken); err != nil {
					return nil, err
				}
			}
			return attributeDecl, nil
		}

		// Value
		valueDecl, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		attributeDecl.Add(valueDecl)

		if hasBracket {
			if _, err = p.consumeToken(BracketClosingToken); err != nil {
				return nil, err
			}
		}

		return attributeDecl, nil
	}

	// Attribute
	attributeDecl, err := p.parseAttribute()
	if err != nil {
		return nil, err
	}

	// Check for arithmetic operators first (*, +, -, /)
	// This allows expressions like: WHERE price * quantity > 1000
	if p.is(StarToken, PlusToken, MinusToken, DivideToken) {
		operatorDecl, err := p.consumeToken(p.cur().Token)
		if err != nil {
			return nil, err
		}
		attributeDecl.Add(operatorDecl)

		// Parse right side of arithmetic expression
		var rightDecl *Decl
		if p.is(NumberToken) {
			rightDecl, err = p.consumeToken(NumberToken)
			if err != nil {
				return nil, err
			}
		} else {
			rightDecl, err = p.parseAttribute()
			if err != nil {
				return nil, err
			}
		}
		attributeDecl.Add(rightDecl)
	}

	// Now check for comparison and special WHERE operators
	switch p.cur().Token {
	case EqualityToken, DistinctnessToken, LeftDipleToken, RightDipleToken, LessOrEqualToken, GreaterOrEqualToken:
		decl, err := p.consumeToken(p.cur().Token)
		if err != nil {
			return nil, err
		}
		attributeDecl.Add(decl)
	case InToken:
		inDecl, err := p.parseIn()
		if err != nil {
			return nil, err
		}
		attributeDecl.Add(inDecl)
		return attributeDecl, nil
	case NotToken:
		notDecl, err := p.consumeToken(p.cur().Token)
		if err != nil {
			return nil, err
		}

		if p.cur().Token != InToken {
			return nil, fmt.Errorf("expected IN after NOT")
		}

		inDecl, err := p.parseIn()
		if err != nil {
			return nil, err
		}
		notDecl.Add(inDecl)

		attributeDecl.Add(notDecl)
		return attributeDecl, nil
	case IsToken:
		decl, err := p.consumeToken(IsToken)
		if err != nil {
			return nil, err
		}
		attributeDecl.Add(decl)
		if p.cur().Token == NotToken {
			notDecl, err := p.consumeToken(NotToken)
			if err != nil {
				return nil, err
			}
			decl.Add(notDecl)
		}
		if p.cur().Token == NullToken {
			nullDecl, err := p.consumeToken(NullToken)
			if err != nil {
				return nil, err
			}
			decl.Add(nullDecl)
		}
		return attributeDecl, nil
	}

	// Value
	valueDecl, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	attributeDecl.Add(valueDecl)

	return attributeDecl, nil
}
