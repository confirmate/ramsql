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

		// Check for optional alias (with or without AS keyword)
		// Supports: FROM table1 AS t1, FROM table1 t1
		if !p.hasNext() {
			addImplicitWhereAll(selectDecl)
			return i, nil
		}

		// Check for explicit AS keyword
		if p.is(AsToken) {
			if _, err := p.consumeToken(AsToken); err != nil {
				return nil, err
			}
			// Next token should be the alias name
			if !p.hasNext() {
				return nil, fmt.Errorf("Expected alias name after AS")
			}
			aliasDecl, err := p.consumeToken(StringToken)
			if err != nil {
				return nil, fmt.Errorf("Expected alias name after AS: %v", err)
			}
			tableNameDecl.Add(aliasDecl)
		} else if p.is(StringToken) {
			// Implicit alias (no AS keyword): FROM table1 t1
			// Only consume if it's not a keyword like WHERE, JOIN, etc.
			if !p.is(WhereToken, JoinToken, OrderToken, LimitToken, OffsetToken, ForToken, CommaToken) {
				aliasDecl, err := p.consumeToken(StringToken)
				if err != nil {
					return nil, err
				}
				tableNameDecl.Add(aliasDecl)
			}
		}

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
			// LIMIT can be a number or a parameter ($1, ?, :name)
			numDecl, err := p.consumeToken(NumberToken, ArgToken, NamedArgToken)
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
			// OFFSET can be a number or a parameter ($1, ?, :name)
			offsetValue, err := p.consumeToken(NumberToken, ArgToken, NamedArgToken)
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

func (p *parser) parseWith(tokens []Token) (*Instruction, error) {
	i := &Instruction{}

	// Create WITH decl
	withDecl := NewDecl(tokens[p.index])
	i.Decls = append(i.Decls, withDecl)

	if err := p.next(); err != nil {
		return nil, fmt.Errorf("WITH token must be followed by CTE name")
	}

	// Parse CTE definitions (can be multiple, comma-separated)
	for {
		// CTE name
		cteNameDecl, err := p.consumeToken(StringToken)
		if err != nil {
			return nil, fmt.Errorf("expected CTE name after WITH: %v", err)
		}
		withDecl.Add(cteNameDecl)

		// AS keyword
		if _, err := p.consumeToken(AsToken); err != nil {
			return nil, fmt.Errorf("expected AS after CTE name: %v", err)
		}

		// Opening parenthesis
		if _, err := p.consumeToken(BracketOpeningToken); err != nil {
			return nil, fmt.Errorf("expected '(' after AS: %v", err)
		}

		// Now should be a SELECT statement
		if !p.is(SelectToken) {
			return nil, fmt.Errorf("expected SELECT statement in CTE, got %v", p.cur().Lexeme)
		}

		// Parse the SELECT statement for this CTE
		// We need to find the matching closing parenthesis
		selectStartIndex := p.index
		parenCount := 1
		selectEndIndex := selectStartIndex + 1

		// Find the matching closing parenthesis
		for selectEndIndex < p.tokenLen && parenCount > 0 {
			if p.tokens[selectEndIndex].Token == BracketOpeningToken {
				parenCount++
			} else if p.tokens[selectEndIndex].Token == BracketClosingToken {
				parenCount--
			}
			if parenCount > 0 {
				selectEndIndex++
			}
		}

		if parenCount != 0 {
			return nil, fmt.Errorf("unmatched parenthesis in CTE")
		}

		// Create a sub-parser for the CTE SELECT
		cteTokens := p.tokens[selectStartIndex:selectEndIndex]
		subParser := &parser{
			tokens:   cteTokens,
			tokenLen: len(cteTokens),
			index:    0,
		}

		selectInst, err := subParser.parseSelect(cteTokens)
		if err != nil {
			return nil, fmt.Errorf("error parsing CTE SELECT: %v", err)
		}

		// Add the SELECT declaration to the CTE name
		if len(selectInst.Decls) > 0 {
			cteNameDecl.Add(selectInst.Decls[0])
		}

		// Move parser position past the CTE
		p.index = selectEndIndex + 1 // +1 to skip the closing paren

		// Check for comma (multiple CTEs) or SELECT (main query)
		if p.index < p.tokenLen && p.is(CommaToken) {
			if _, err := p.consumeToken(CommaToken); err != nil {
				return nil, err
			}
			// Continue to parse next CTE
			continue
		}

		// Must be followed by SELECT
		if p.index >= p.tokenLen || !p.is(SelectToken) {
			return nil, fmt.Errorf("expected SELECT after CTE definition")
		}
		break
	}

	// Parse the main SELECT statement
	selectInst, err := p.parseSelect(tokens)
	if err != nil {
		return nil, fmt.Errorf("error parsing main SELECT after WITH: %v", err)
	}

	// Add the main SELECT to the WITH instruction
	if len(selectInst.Decls) > 0 {
		withDecl.Add(selectInst.Decls[0])
	}

	return i, nil
}
