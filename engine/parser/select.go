package parser

import (
	"fmt"
)

func (p *parser) parseSelect(tokens []Token) (*Instruction, error) {
	i := &Instruction{}
	var err error

	// Create select decl
	selectDecl := NewDecl(tokens[p.index])
	i.Decls = append(i.Decls, selectDecl)

	// After select token, should be either
	// a StarToken
	// a list of table names + (StarToken Or Attribute)
	// a builtin func (COUNT, MAX, ...)
	if err = p.next(); err != nil {
		return nil, fmt.Errorf("SELECT token must be followed by attributes to select")
	}

	var (
		distinctDecl *Decl
		distinctOpen bool
	)
	if p.is(DistinctToken) {
		distinctDecl, err = p.consumeToken(DistinctToken)
		if err != nil {
			return nil, err
		}

		if p.is(OnToken) {
			if err := p.next(); err != nil {
				return nil, err
			}
			if !p.is(BracketOpeningToken) {
				return nil, fmt.Errorf("Syntax error near %v, opening bracket expected\n", tokens[p.index])
			}
			if err := p.next(); err != nil {
				return nil, err
			}
			distinctOpen = true
		}

		selectDecl.Add(distinctDecl)
	}

	for {
		var needsNext bool = false
		switch {
		case p.is(CountToken):
			attrDecl, err := p.parseBuiltinFunc()
			if err != nil {
				return nil, err
			}
			selectDecl.Add(attrDecl)
		case p.is(CurrentSchemaToken):
			// Handle CURRENT_SCHEMA() function
			attrDecl := NewDecl(p.cur())
			selectDecl.Add(attrDecl)
			needsNext = true
		case p.is(CurrentDatabaseToken):
			// Handle CURRENT_DATABASE() function
			attrDecl := NewDecl(p.cur())
			selectDecl.Add(attrDecl)
			needsNext = true
		case p.is(NumberToken):
			// Handle literal numbers in SELECT clause (may be part of arithmetic expression)
			attrDecl := NewDecl(p.cur())
			if err := p.next(); err != nil {
				selectDecl.Add(attrDecl)
				break
			}
			// Check for arithmetic operators
			attrDecl, err = p.parseExpression(attrDecl)
			if err != nil {
				return nil, err
			}
			selectDecl.Add(attrDecl)
			needsNext = false
		case p.is(SimpleQuoteToken):
			// Handle quoted string literals in SELECT clause
			if err := p.next(); err != nil {
				return nil, err
			}
			if !p.is(StringToken) {
				return nil, fmt.Errorf("expected string after quote")
			}
			// Use SimpleQuoteToken to mark this as a literal string value
			attrDecl := &Decl{
				Token:  SimpleQuoteToken,
				Lexeme: p.cur().Lexeme,
			}
			selectDecl.Add(attrDecl)
			if err := p.next(); err != nil {
				return nil, err
			}
			if !p.is(SimpleQuoteToken) {
				return nil, fmt.Errorf("expected closing quote")
			}
			needsNext = true
		case p.is(TrueToken), p.is(FalseToken):
			// Handle boolean literals in SELECT clause
			attrDecl := NewDecl(p.cur())
			selectDecl.Add(attrDecl)
			needsNext = true
		default:
			attrDecl, err := p.parseAttribute()
			if err != nil {
				return nil, err
			}

			// Check if this attribute is part of an expression
			attrDecl, err = p.parseExpression(attrDecl)
			if err != nil {
				return nil, err
			}

			if distinctOpen {
				distinctDecl.Add(attrDecl)
			} else {
				selectDecl.Add(attrDecl)
			}
			// parseAttribute already advanced the parser
		}

		// Advance parser if we handled a simple token (like NumberToken)
		if needsNext {
			if err := p.next(); err != nil {
				// End of tokens is ok - might be SELECT 1 without FROM
				break
			}
		}

		switch {
		case distinctOpen && p.is(BracketClosingToken):
			if err := p.next(); err != nil {
				return nil, err
			}
			distinctOpen = false
			continue
		case p.is(CommaToken):
			if err := p.next(); err != nil {
				return nil, err
			}
			continue
		}

		break
	}

	// FROM clause is optional (e.g., SELECT 1, SELECT CURRENT_SCHEMA())
	if !p.hasNext() {
		// No FROM clause, just return
		return i, nil
	}

	if tokens[p.index].Token != FromToken {
		// No FROM clause, continue to parse other clauses if any
		return i, nil
	}

	fromDecl := NewDecl(tokens[p.index])
	selectDecl.Add(fromDecl)

	// Now must be a list of table
	for {
		// string
		if err = p.next(); err != nil {
			return nil, fmt.Errorf("Unexpected end. Syntax error near %v\n", tokens[p.index])
		}
		tableNameDecl, err := p.parseTableName()
		if err != nil {
			return nil, err
		}
		fromDecl.Add(tableNameDecl)

		// If no next, then it's implicit where
		if !p.hasNext() {
			addImplicitWhereAll(selectDecl)
			return i, nil
		}
		// if not comma, break
		if tokens[p.index].Token != CommaToken {
			break // No more table
		}
	}

	// JOIN OR ...?
	for p.is(JoinToken) {
		joinDecl, err := p.parseJoin()
		if err != nil {
			return nil, err
		}
		selectDecl.Add(joinDecl)
	}

	hazWhereClause := false
	for {
		switch p.cur().Token {
		case WhereToken:
			err := p.parseWhere(selectDecl)
			if err != nil {
				return nil, err
			}
			hazWhereClause = true
		case OrderToken:
			if !hazWhereClause {
				// WHERE clause is implicit
				addImplicitWhereAll(selectDecl)
			}
			err := p.parseOrderBy(selectDecl)
			if err != nil {
				return nil, err
			}
		case LimitToken:
			limitDecl, err := p.consumeToken(LimitToken)
			if err != nil {
				return nil, err
			}
			selectDecl.Add(limitDecl)
			numDecl, err := p.consumeToken(NumberToken)
			if err != nil {
				return nil, err
			}
			limitDecl.Add(numDecl)
		case OffsetToken:
			offsetDecl, err := p.consumeToken(OffsetToken)
			if err != nil {
				return nil, err
			}
			selectDecl.Add(offsetDecl)
			offsetValue, err := p.consumeToken(NumberToken)
			if err != nil {
				return nil, err
			}
			offsetDecl.Add(offsetValue)
		case ForToken:
			err := p.parseForUpdate(selectDecl)
			if err != nil {
				return nil, err
			}
		default:
			return i, nil
		}
	}
}

func addImplicitWhereAll(decl *Decl) {

	whereDecl := &Decl{
		Token:  WhereToken,
		Lexeme: "where",
	}
	whereDecl.Add(&Decl{
		Token:  NumberToken,
		Lexeme: "1",
	})

	decl.Add(whereDecl)
}

func (p *parser) parseForUpdate(decl *Decl) error {
	// Optionnal
	if !p.is(ForToken) {
		return nil
	}

	d, err := p.consumeToken(ForToken)
	if err != nil {
		return err
	}

	u, err := p.consumeToken(UpdateToken)
	if err != nil {
		return err
	}

	d.Add(u)
	decl.Add(d)
	return nil
}
