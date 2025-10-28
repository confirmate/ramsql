package executor

import (
	"fmt"
	"strings"
	"time"

	"github.com/proullon/ramsql/engine/agnostic"
	"github.com/proullon/ramsql/engine/parser"
)

func parseAttribute(decl *parser.Decl) (attr agnostic.Attribute, isPk bool, err error) {
	var name, typeName string

	// Attribute name
	if decl.Token != parser.StringToken {
		return agnostic.Attribute{}, false, fmt.Errorf("engine: expected attribute name, got %v", decl.Token)
	}
	name = strings.ToLower(decl.Lexeme)

	// Attribute type
	if len(decl.Decl) < 1 {
		return attr, false, fmt.Errorf("Attribute %s has no type", decl.Lexeme)
	}
	switch decl.Decl[0].Token {
	case parser.DecimalToken:
		typeName = "float"
	case parser.NumberToken:
		typeName = "int"
	case parser.DateToken:
		typeName = "date"
	case parser.StringToken:
		typeName = decl.Decl[0].Lexeme
	default:
		return agnostic.Attribute{}, false, fmt.Errorf("engine: expected attribute type, got %v:%v", decl.Decl[0].Token, decl.Decl[0].Lexeme)
	}

	attr = agnostic.NewAttribute(name, typeName)

	// Maybe domain and special thing like primary key
	typeDecl := decl.Decl[1:]
	for i := range typeDecl {
		if typeDecl[i].Token == parser.AutoincrementToken {
			attr = attr.WithAutoIncrement()
		}

		if typeDecl[i].Token == parser.DefaultToken {
			switch typeDecl[i].Decl[0].Token {
			case parser.LocalTimestampToken, parser.NowToken:
				attr = attr.WithDefault(func() any { return time.Now() })
			default:
				v, err := agnostic.ToInstance(typeDecl[i].Decl[0].Lexeme, typeName)
				if err != nil {
					return agnostic.Attribute{}, false, err
				}
				attr = attr.WithDefaultConst(v)
			}
		}

		// Check if attribute is unique
		if typeDecl[i].Token == parser.UniqueToken {
			attr = attr.WithUnique()
		}
		if typeDecl[i].Token == parser.PrimaryToken {
			if len(typeDecl[i].Decl) > 0 && typeDecl[i].Decl[0].Token == parser.KeyToken {
				isPk = true
			}
		}

		// Check for column-level REFERENCES
		if typeDecl[i].Token == parser.ReferencesToken {
			refSchema, refTable, refCol, err := parseReferencesDecl(typeDecl[i])
			if err != nil {
				return agnostic.Attribute{}, false, err
			}
			attr = attr.WithForeignKey(refSchema, refTable, refCol)
		}

		// Check for column-level CONSTRAINT ... REFERENCES
		if typeDecl[i].Token == parser.ConstraintToken {
			// Skip constraint name (typeDecl[i].Decl[0])
			// Look for REFERENCES child
			if len(typeDecl[i].Decl) > 1 && typeDecl[i].Decl[1].Token == parser.ReferencesToken {
				refSchema, refTable, refCol, err := parseReferencesDecl(typeDecl[i].Decl[1])
				if err != nil {
					return agnostic.Attribute{}, false, err
				}
				attr = attr.WithForeignKey(refSchema, refTable, refCol)
			}
		}

	}

	if strings.ToLower(typeName) == "bigserial" {
		attr = attr.WithAutoIncrement()
	}

	return attr, isPk, nil
}

// parseReferencesDecl extracts schema, table, and column from a REFERENCES decl node.
// Returns (refSchema, refTable, refCol, error).
// If schema is not specified, refSchema is empty (meaning same schema as referencing table).
// If column list is omitted, refCol is empty (meaning reference the PK).
func parseReferencesDecl(refDecl *parser.Decl) (refSchema, refTable, refCol string, err error) {
	if len(refDecl.Decl) == 0 {
		return "", "", "", fmt.Errorf("REFERENCES clause has no children")
	}

	// First child is the table name (may be schema-qualified)
	tblDecl := refDecl.Decl[0]
	if tblDecl.Token == parser.SchemaToken {
		// schema.table form: tblDecl is the schema, and its child is the table
		refSchema = tblDecl.Lexeme
		if len(tblDecl.Decl) > 0 {
			refTable = tblDecl.Decl[0].Lexeme
		}
	} else {
		// plain table name
		refTable = tblDecl.Lexeme
	}

	// Remaining children are the column list (if present)
	// For column-level FK, typically just one column
	if len(refDecl.Decl) > 1 {
		refCol = refDecl.Decl[1].Lexeme
	}

	return refSchema, refTable, refCol, nil
}
