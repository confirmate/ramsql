package parser

import (
	"fmt"
)

// parseWithSelect parses a WITH clause followed by a SELECT statement
// Supports: WITH cte_name AS (SELECT ...) SELECT ...
// Also supports multiple CTEs: WITH cte1 AS (...), cte2 AS (...) SELECT ...
func (p *parser) parseWithSelect(tokens []Token) (*Instruction, error) {
	i := &Instruction{}

	// Create WITH decl
	withDecl := NewDecl(tokens[p.index])
	i.Decls = append(i.Decls, withDecl)

	// Parse one or more CTEs
	for {
		// Move past WITH or comma
		if err := p.next(); err != nil {
			return nil, fmt.Errorf("WITH must be followed by CTE name")
		}

		// Get CTE name
		if !p.is(StringToken) {
			return nil, fmt.Errorf("Expected CTE name after WITH, got %v", tokens[p.index])
		}
		cteNameDecl := NewDecl(tokens[p.index])
		withDecl.Add(cteNameDecl)

		// Expect AS
		if err := p.next(); err != nil {
			return nil, fmt.Errorf("Expected AS after CTE name")
		}
		if !p.is(AsToken) {
			return nil, fmt.Errorf("Expected AS after CTE name, got %v", tokens[p.index])
		}
		asDecl := NewDecl(tokens[p.index])
		cteNameDecl.Add(asDecl)

		// Expect opening parenthesis
		if err := p.next(); err != nil {
			return nil, fmt.Errorf("Expected opening parenthesis after AS")
		}
		if !p.is(BracketOpeningToken) {
			return nil, fmt.Errorf("Expected opening parenthesis after AS, got %v", tokens[p.index])
		}

		// Find matching closing parenthesis
		if err := p.next(); err != nil {
			return nil, fmt.Errorf("Expected SELECT in CTE")
		}

		// Parse the CTE subquery (should be a SELECT)
		if !p.is(SelectToken) {
			return nil, fmt.Errorf("Expected SELECT in CTE, got %v", tokens[p.index])
		}

		// Find the end of this CTE (matching closing parenthesis)
		startIndex := p.index
		depth := 1
		endIndex := startIndex

		for i := p.index + 1; i < p.tokenLen; i++ {
			if tokens[i].Token == BracketOpeningToken {
				depth++
			} else if tokens[i].Token == BracketClosingToken {
				depth--
				if depth == 0 {
					endIndex = i
					break
				}
			}
		}

		if depth != 0 {
			return nil, fmt.Errorf("Unmatched parenthesis in CTE")
		}

		// Extract tokens for the CTE subquery
		cteTokens := tokens[startIndex:endIndex]

		// Create a new parser for the CTE subquery
		cteParser := &parser{
			tokens:   cteTokens,
			tokenLen: len(cteTokens),
			index:    0,
		}

		// Parse the CTE subquery
		cteInst, err := cteParser.parseSelect(cteTokens)
		if err != nil {
			return nil, fmt.Errorf("Error parsing CTE subquery: %v", err)
		}

		// Add the CTE subquery to the AS decl
		if len(cteInst.Decls) > 0 {
			asDecl.Add(cteInst.Decls[0])
		}

		// Move past the closing parenthesis
		p.index = endIndex
		if err := p.next(); err != nil {
			return nil, fmt.Errorf("Expected SELECT after CTE")
		}

		// Check if there's another CTE (comma) or if we're done (SELECT)
		if p.is(CommaToken) {
			// Another CTE follows
			continue
		} else if p.is(SelectToken) {
			// Main SELECT follows
			break
		} else {
			return nil, fmt.Errorf("Expected comma or SELECT after CTE, got %v", tokens[p.index])
		}
	}

	// Now parse the main SELECT statement
	mainSelect, err := p.parseSelect(tokens)
	if err != nil {
		return nil, fmt.Errorf("Error parsing main SELECT: %v", err)
	}

	// Add main SELECT to the instruction
	if len(mainSelect.Decls) > 0 {
		i.Decls = append(i.Decls, mainSelect.Decls[0])
	}

	return i, nil
}
