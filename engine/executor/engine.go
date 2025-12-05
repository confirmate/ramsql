package executor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/proullon/ramsql/engine/agnostic"
	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
)

// Engine is the root struct of RamSQL server
type Engine struct {
	memstore *agnostic.Engine
	dbName   string
}

// New initialize a new RamSQL server
func NewEngine() (e *Engine, err error) {
	return NewEngineWithName("")
}

// NewEngineWithName initialize a new RamSQL server with a database name
func NewEngineWithName(dbName string) (e *Engine, err error) {

	e = &Engine{
		memstore: agnostic.NewEngine(),
		dbName:   dbName,
	}

	return
}

func (e *Engine) Begin() (*Tx, error) {
	tx, err := NewTx(context.Background(), e, sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (e *Engine) Stop() {
}

func createExecutor(t *Tx, decl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {

	if len(decl.Decl) == 0 {
		return 0, 0, nil, nil, ParsingError
	}

	if t.opsExecutors[decl.Decl[0].Token] != nil {
		return t.opsExecutors[decl.Decl[0].Token](t, decl.Decl[0], args)
	}

	return 0, 0, nil, nil, NotImplemented
}

func dropExecutor(t *Tx, decl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {

	if len(decl.Decl) == 0 {
		return 0, 0, nil, nil, ParsingError
	}

	if _, ok := decl.Has(parser.TableToken); ok {
		return dropTable(t, decl.Decl[0], args)
	}
	if _, ok := decl.Has(parser.SchemaToken); ok {
		return dropSchema(t, decl.Decl[0], args)
	}

	return 0, 0, nil, nil, NotImplemented
}

func dropTable(t *Tx, decl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {
	if len(decl.Decl) == 0 {
		return 0, 1, nil, nil, ParsingError
	}

	// Check if 'IF EXISTS' is present
	ifExists := hasIfExists(decl)

	rDecl := decl.Decl[0]
	if ifExists {
		rDecl = decl.Decl[1]
	}

	schema := agnostic.DefaultSchema
	if d, ok := rDecl.Has(parser.SchemaToken); ok {
		schema = d.Lexeme
	}
	relation := rDecl.Lexeme

	exists := t.tx.CheckRelation(schema, relation)
	if !exists && ifExists {
		return 0, 0, nil, nil, nil
	}
	if !exists {
		return 0, 0, nil, nil, fmt.Errorf("relation %s.%s does not exist", schema, relation)
	}

	err := t.tx.DropRelation(schema, relation)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	return 0, 1, nil, nil, nil
}

func dropSchema(t *Tx, decl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {
	if len(decl.Decl) == 0 {
		return 0, 1, nil, nil, ParsingError
	}
	// Check if 'IF EXISTS' is present
	ifExists := hasIfExists(decl)

	rDecl := decl.Decl[0]
	if ifExists {
		rDecl = decl.Decl[1]
	}

	schema := rDecl.Lexeme

	if ifExists && !t.tx.CheckSchema(schema) {
		return 0, 0, nil, nil, nil
	}

	err := t.tx.DropSchema(schema)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	return 0, 1, nil, nil, nil
}

func grantExecutor(*Tx, *parser.Decl, []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {
	return 0, 1, nil, nil, nil
}

func createSchemaExecutor(t *Tx, tableDecl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {
	if len(tableDecl.Decl) == 0 {
		return 0, 0, nil, nil, ParsingError
	}

	// Check if 'IF NOT EXISTS' is present
	ifNotExists := hasIfNotExists(tableDecl)

	name := tableDecl.Decl[0].Lexeme

	if ifNotExists && t.tx.CheckSchema(name) {
		return 0, 0, nil, nil, nil
	}

	err := t.tx.CreateSchema(name)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	return 0, 0, nil, nil, nil
}

func createTableExecutor(t *Tx, tableDecl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {
	var i int
	var schemaName string

	if len(tableDecl.Decl) == 0 {
		return 0, 0, nil, nil, ParsingError
	}

	// Check for specific attribute
	for i < len(tableDecl.Decl) {
		if tableDecl.Decl[i].Token == parser.IfToken {
			i++
			continue
		}

		if t.opsExecutors[tableDecl.Decl[i].Token] == nil {
			break
		}

		_, _, _, _, err := t.opsExecutors[tableDecl.Decl[i].Token](t, tableDecl.Decl[i], args)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		i++
	}

	if d, ok := tableDecl.Has(parser.SchemaToken); ok {
		schemaName = d.Lexeme
	}

	// Check if 'IF NOT EXISTS' is present
	ifNotExists := hasIfNotExists(tableDecl)

	relationName := tableDecl.Decl[i].Lexeme

	exists := t.tx.CheckRelation(schemaName, relationName)
	if exists && ifNotExists {
		return 0, 0, nil, nil, nil
	}
	if exists {
		return 0, 0, nil, nil, errors.New("relation already exists")
	}

	var pk []string
	var attributes []agnostic.Attribute

	// Fetch attributes
	i++
	for i < len(tableDecl.Decl) {
		if tableDecl.Decl[i].Token != parser.StringToken {
			break
		}
		attr, isPk, err := parseAttribute(tableDecl.Decl[i])
		if err != nil {
			return 0, 0, nil, nil, err
		}
		if isPk {
			pk = append(pk, attr.Name())
		}
		attributes = append(attributes, attr)
		i++
	}

	// Fetch table-level PRIMARY KEY
	if i < len(tableDecl.Decl) && tableDecl.Decl[i].Token == parser.PrimaryToken {
		d := tableDecl.Decl[i].Decl[0]
		for _, attr := range d.Decl {
			pk = append(pk, attr.Lexeme)
		}
		i++
	}

	// Fetch table-level FOREIGN KEY constraints and distribute them to attributes directly
	for i < len(tableDecl.Decl) {
		if tableDecl.Decl[i].Token == parser.ForeignToken {
			fk, err := parseTableForeignKey(tableDecl.Decl[i], "")
			if err != nil {
				return 0, 0, nil, nil, err
			}
			// distribute fk to local columns by attaching fk struct to each attribute
			for _, lc := range fk.LocalColumns() {
				for ai := range attributes {
					if attributes[ai].Name() == lc {
						attributes[ai] = attributes[ai].WithForeignKeyStruct(fk)
						break
					}
				}
			}
			i++
			continue
		}
		if tableDecl.Decl[i].Token == parser.ConstraintToken {
			// CONSTRAINT name FOREIGN KEY (...)
			constraintName := ""
			if len(tableDecl.Decl[i].Decl) > 0 {
				constraintName = tableDecl.Decl[i].Decl[0].Lexeme
			}
			if len(tableDecl.Decl[i].Decl) > 1 && tableDecl.Decl[i].Decl[1].Token == parser.ForeignToken {
				fk, err := parseTableForeignKey(tableDecl.Decl[i].Decl[1], constraintName)
				if err != nil {
					return 0, 0, nil, nil, err
				}
				for _, lc := range fk.LocalColumns() {
					for ai := range attributes {
						if attributes[ai].Name() == lc {
							attributes[ai] = attributes[ai].WithForeignKeyStruct(fk)
							break
						}
					}
				}
			}
			i++
			continue
		}
		// Any other token (should be covered by prior loops or parser)
		break
	}

	err := t.tx.CreateRelation(schemaName, relationName, attributes, pk)
	if err != nil {
		return 0, 0, nil, nil, err
	}
	return 0, 1, nil, nil, nil
}

// parseTableForeignKey extracts a ForeignKey from a FOREIGN KEY decl node.
// fkDecl structure: FOREIGN -> KEY -> (list of columns) -> REFERENCES -> ...
func parseTableForeignKey(fkDecl *parser.Decl, constraintName string) (agnostic.ForeignKey, error) {
	fk := agnostic.NewForeignKey(constraintName)

	if len(fkDecl.Decl) == 0 {
		return fk, fmt.Errorf("FOREIGN KEY has no children")
	}

	// First child should be KEY token with column list
	keyDecl := fkDecl.Decl[0]
	if keyDecl.Token != parser.KeyToken {
		return fk, fmt.Errorf("expected KEY token after FOREIGN")
	}

	// Extract local columns (lowercase for consistent matching with attribute names)
	for _, colDecl := range keyDecl.Decl {
		if colDecl.Token == parser.StringToken {
			fk = fk.WithLocalColumn(strings.ToLower(colDecl.Lexeme))
		}
	}

	// Second child should be REFERENCES
	if len(fkDecl.Decl) < 2 || fkDecl.Decl[1].Token != parser.ReferencesToken {
		return fk, fmt.Errorf("expected REFERENCES after FOREIGN KEY columns")
	}

	refDecl := fkDecl.Decl[1]
	if len(refDecl.Decl) == 0 {
		return fk, fmt.Errorf("REFERENCES has no children")
	}

	// First child of REFERENCES is table name (may be schema-qualified)
	tblDecl := refDecl.Decl[0]
	if tblDecl.Token == parser.SchemaToken {
		// schema.table form
		fk = fk.WithRefSchema(strings.ToLower(tblDecl.Lexeme))
		if len(tblDecl.Decl) > 0 {
			fk = fk.WithRefRelation(strings.ToLower(tblDecl.Decl[0].Lexeme))
		}
	} else {
		fk = fk.WithRefRelation(strings.ToLower(tblDecl.Lexeme))
	}

	// Remaining children are referenced columns (lowercase for consistent matching)
	for i := 1; i < len(refDecl.Decl); i++ {
		if refDecl.Decl[i].Token == parser.StringToken {
			fk = fk.WithRefColumn(strings.ToLower(refDecl.Decl[i].Lexeme))
		}
	}

	return fk, nil
}

/*
|-> INSERT

	|-> INTO
	    |-> user
	        |-> last_name
	        |-> first_name
	        |-> email
	|-> VALUES
	    |-> (
	        |-> Roullon
	        |-> Pierre
	        |-> pierre.roullon@gmail.com
	|-> RETURNING
	        |-> email
*/
func insertIntoTableExecutor(t *Tx, insertDecl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {

	var lastInsertedID int64
	var schemaName string
	var returningAttrs []string
	var returningIdx []int
	relationName := insertDecl.Decl[0].Decl[0].Lexeme

	// Check for ON CONFLICT clause
	var onConflictDecl *parser.Decl
	var doUpdateDecl *parser.Decl
	for i := range insertDecl.Decl {
		if insertDecl.Decl[i].Token == parser.OnToken {
			onConflictDecl = insertDecl.Decl[i]
			// Find the DO clause
			for _, child := range onConflictDecl.Decl {
				if child.Token == parser.DoToken && len(child.Decl) > 0 {
					if child.Decl[0].Token == parser.UpdateToken {
						doUpdateDecl = child.Decl[0]
					}
				}
			}
			break
		}
	}

	// Check for RETURNING clause
	if len(insertDecl.Decl) > 2 {
		for i := range insertDecl.Decl {
			if insertDecl.Decl[i].Token == parser.ReturningToken {
				returningDecl := insertDecl.Decl[i]
				returningAttrs = append(returningAttrs, returningDecl.Decl[0].Lexeme)
				idx, _, err := t.tx.RelationAttribute(schemaName, relationName, returningDecl.Decl[0].Lexeme)
				if err != nil {
					return 0, 0, nil, nil, fmt.Errorf("cannot return %s, doesn't exist in relation %s", returningDecl.Decl[0].Lexeme, relationName)
				}
				returningIdx = append(returningIdx, idx)
			}
		}
	}

	var specifiedAttrs []string
	for _, d := range insertDecl.Decl[0].Decl[0].Decl {
		if d.Token == parser.SchemaToken {
			schemaName = d.Lexeme
			continue
		}
		specifiedAttrs = append(specifiedAttrs, d.Lexeme)
	}

	var tuples []*agnostic.Tuple
	valuesDecl := insertDecl.Decl[1]
	for _, valueListDecl := range valuesDecl.Decl {
		values, err := getValues(specifiedAttrs, valueListDecl, args)
		if err != nil {
			return 0, 0, nil, nil, err
		}

		var tuple *agnostic.Tuple

		// If ON CONFLICT is present, check for conflict first
		if onConflictDecl != nil {
			tuple, err = handleOnConflict(t, schemaName, relationName, values, specifiedAttrs, onConflictDecl, doUpdateDecl)
			if err != nil {
				return 0, 0, nil, nil, err
			}
			if tuple == nil {
				// DO NOTHING case - skip this row
				continue
			}
		} else {
			// No ON CONFLICT clause, do normal insert
			tuple, err = t.tx.Insert(schemaName, relationName, values)
			if err != nil {
				return 0, 0, nil, nil, err
			}
		}

		if tuple != nil {
			returningTuple := agnostic.NewTuple()
			for _, idx := range returningIdx {
				returningTuple.Append(tuple.Values()[idx])
			}
			tuples = append(tuples, returningTuple)

			// guess lastInsertedID
			if v := tuple.Values(); len(v) > 0 {
				if reflect.TypeOf(v[0]).ConvertibleTo(reflect.TypeOf(lastInsertedID)) {
					lastInsertedID = reflect.ValueOf(v[0]).Convert(reflect.TypeOf(lastInsertedID)).Int()
				}
			}
		}
	}

	if len(returningAttrs) == 0 {
		return lastInsertedID, int64(len(tuples)), nil, nil, nil
	}

	return lastInsertedID, int64(len(tuples)), returningAttrs, tuples, nil
}

// handleOnConflict handles the ON CONFLICT clause for INSERT statements.
// It returns the resulting tuple (from insert or update), or nil if DO NOTHING was specified.
func handleOnConflict(t *Tx, schemaName, relationName string, values map[string]any, specifiedAttrs []string, onConflictDecl, doUpdateDecl *parser.Decl) (*agnostic.Tuple, error) {
	hasConflict, err := t.tx.CheckPrimaryKeyConflict(schemaName, relationName, values)
	if err != nil {
		return nil, err
	}

	if !hasConflict {
		// No conflict, do normal insert
		return t.tx.Insert(schemaName, relationName, values)
	}

	// Validate ON CONFLICT clause structure
	if len(onConflictDecl.Decl) < 2 {
		return nil, fmt.Errorf("invalid ON CONFLICT clause structure")
	}

	// Check if it's DO NOTHING
	doDecl := onConflictDecl.Decl[1] // DoToken
	if doDecl.Token != parser.DoToken {
		return nil, fmt.Errorf("unexpected ON CONFLICT clause structure: missing DO token")
	}
	if len(doDecl.Decl) > 0 && doDecl.Decl[0].Token == parser.NothingToken {
		// DO NOTHING - return nil to signal skipping this row
		return nil, nil
	}

	if doUpdateDecl == nil {
		return nil, fmt.Errorf("ON CONFLICT DO UPDATE specified but UPDATE clause not found")
	}

	// Get the conflict target columns
	conflictDecl := onConflictDecl.Decl[0] // ConflictToken
	if conflictDecl.Token != parser.ConflictToken {
		return nil, fmt.Errorf("invalid ON CONFLICT clause structure: missing CONFLICT token")
	}
	var conflictCols []string
	for _, colDecl := range conflictDecl.Decl {
		conflictCols = append(conflictCols, strings.ToLower(colDecl.Lexeme))
	}

	// Build predicate for the conflicting row
	predicate := buildConflictPredicate(relationName, conflictCols, values)
	if predicate == nil {
		return nil, fmt.Errorf("conflict target columns must have values in the INSERT statement")
	}

	// Get the SET clause values
	updateValues := extractUpdateValues(doUpdateDecl, values)

	// Perform the update - use star selector to include all columns for RETURNING and DO UPDATE SET
	selectors := []agnostic.Selector{agnostic.NewStarSelector(relationName)}
	_, updatedTuples, err := t.tx.Update(schemaName, relationName, updateValues, selectors, predicate)
	if err != nil {
		return nil, err
	}

	if len(updatedTuples) > 0 {
		return updatedTuples[0], nil
	}

	return nil, fmt.Errorf("internal error: conflict detected but no matching rows found for update")
}

// buildConflictPredicate builds a predicate to match the conflicting row based on conflict columns.
func buildConflictPredicate(relationName string, conflictCols []string, values map[string]any) agnostic.Predicate {
	var predicates []agnostic.Predicate
	for _, col := range conflictCols {
		if val, ok := values[col]; ok {
			p := agnostic.NewEqPredicate(
				agnostic.NewAttributeValueFunctor(relationName, col),
				agnostic.NewConstValueFunctor(val),
			)
			predicates = append(predicates, p)
		}
	}

	if len(predicates) == 0 {
		return nil
	}
	if len(predicates) == 1 {
		return predicates[0]
	}

	predicate := agnostic.NewAndPredicate(predicates[0], predicates[1])
	for i := 2; i < len(predicates); i++ {
		predicate = agnostic.NewAndPredicate(predicate, predicates[i])
	}
	return predicate
}

// extractUpdateValues extracts the column values to update from the DO UPDATE SET clause.
// It handles "excluded"."column" references by looking up values from the INSERT values.
func extractUpdateValues(doUpdateDecl *parser.Decl, insertValues map[string]any) map[string]any {
	if doUpdateDecl == nil || len(doUpdateDecl.Decl) == 0 {
		return make(map[string]any)
	}
	setDecl := doUpdateDecl.Decl[0] // SetToken
	updateValues := make(map[string]any)

	for _, attrDecl := range setDecl.Decl {
		colName := strings.ToLower(attrDecl.Lexeme)
		if len(attrDecl.Decl) == 0 {
			continue
		}

		valueAttrDecl := attrDecl.Decl[0]
		// Check if this is "excluded"."column" reference
		if len(valueAttrDecl.Decl) > 0 {
			// This is a qualified name like excluded.column
			refCol := strings.ToLower(valueAttrDecl.Lexeme)
			tableName := strings.ToLower(valueAttrDecl.Decl[0].Lexeme)
			if tableName == "excluded" {
				// Use the value from the INSERT values
				if val, ok := insertValues[refCol]; ok {
					updateValues[colName] = val
				}
			}
		} else {
			// Direct value reference
			var typeName string
			switch valueAttrDecl.Token {
			case parser.IntToken, parser.NumberToken:
				typeName = "bigint"
			case parser.DateToken:
				typeName = "timestamp"
			case parser.TextToken:
				typeName = "text"
			case parser.FloatToken:
				typeName = "float"
			default:
				typeName = "text"
				if _, err := agnostic.ToInstance(valueAttrDecl.Lexeme, "timestamp"); err == nil {
					typeName = "timestamp"
				}
			}
			v, err := agnostic.ToInstance(valueAttrDecl.Lexeme, typeName)
			if err != nil {
				// fallback to string if conversion fails
				updateValues[colName] = valueAttrDecl.Lexeme
			} else {
				updateValues[colName] = v
			}
		}
	}

	return updateValues
}

func getValues(specifiedAttrs []string, valuesDecl *parser.Decl, args []NamedValue) (map[string]any, error) {
	var typeName string
	var err error
	values := make(map[string]any)
	var odbcIdx int64 = 1

	for i, d := range valuesDecl.Decl {
		if d.Lexeme == "default" || d.Lexeme == "DEFAULT" {
			continue
		}

		switch d.Token {
		case parser.IntToken, parser.NumberToken:
			typeName = "bigint"
		case parser.DateToken:
			typeName = "timestamp"
		case parser.TextToken:
			typeName = "text"
		case parser.FloatToken:
			typeName = "float"
		default:
			typeName = "text"
			if _, err := agnostic.ToInstance(d.Lexeme, "timestamp"); err == nil {
				typeName = "timestamp"
			}
		}

		var v any

		switch d.Token {
		case parser.ArgToken:
			var idx int64
			if d.Lexeme == "?" {
				idx = odbcIdx
				odbcIdx++
			} else {
				idx, err = strconv.ParseInt(d.Lexeme, 10, 64)
				if err != nil {
					return nil, err
				}
			}
			if len(args) <= int(idx)-1 {
				return nil, fmt.Errorf("reference to $%s, but only %d argument provided", d.Lexeme, len(args))
			}
			v = args[idx-1].Value
		case parser.NamedArgToken:
			for _, arg := range args {
				if arg.Name == d.Lexeme {
					v = arg.Value
				}
			}
		default:
			v, err = agnostic.ToInstance(d.Lexeme, typeName)
			if err != nil {
				return nil, err
			}
		}
		values[strings.ToLower(specifiedAttrs[i])] = v
	}

	return values, nil
}

func getSet(specifiedAttrs []string, values map[string]any, valuesDecl *parser.Decl, args []NamedValue) (map[string]any, error) {
	var typeName string
	var err error
	var odbcIdx int64 = 1

	nameDecl := valuesDecl
	valueDecl := nameDecl.Decl[1]

	switch valueDecl.Token {
	case parser.IntToken, parser.NumberToken:
		typeName = "bigint"
	case parser.DecimalToken:
		typeName = "float"
	case parser.DateToken:
		typeName = "timestamp"
	case parser.TextToken:
		typeName = "text"
	default:
		typeName = "text"
		if _, err := agnostic.ToInstance(valueDecl.Lexeme, "timestamp"); err == nil {
			typeName = "timestamp"
		}
	}

	var v any

	switch valueDecl.Token {
	case parser.ArgToken:
		var idx int64
		if valueDecl.Lexeme == "?" {
			idx = odbcIdx
			odbcIdx++
		} else {
			idx, err = strconv.ParseInt(valueDecl.Lexeme, 10, 64)
			if err != nil {
				return nil, err
			}
		}
		if len(args) <= int(idx)-1 {
			return nil, fmt.Errorf("reference to $%s, but only %d argument provided", valueDecl.Lexeme, len(args))
		}
		v = args[idx-1].Value
	default:
		v, err = agnostic.ToInstance(valueDecl.Lexeme, typeName)
		if err != nil {
			return nil, err
		}
	}
	values[nameDecl.Lexeme] = v

	return values, nil
}

func hasIfNotExists(tableDecl *parser.Decl) bool {
	for _, d := range tableDecl.Decl {
		if d.Token == parser.IfToken {
			if len(d.Decl) > 0 && d.Decl[0].Token == parser.NotToken {
				not := d.Decl[0]
				if len(not.Decl) > 0 && not.Decl[0].Token == parser.ExistsToken {
					return true
				}
			}
		}
	}

	return false
}

func hasIfExists(tableDecl *parser.Decl) bool {
	for _, d := range tableDecl.Decl {
		if d.Token == parser.IfToken {
			if len(d.Decl) > 0 && d.Decl[0].Token == parser.ExistsToken {
				return true
			}
		}
	}

	return false
}

/*
|-> SELECT

	|-> *
	|-> FROM
		|-> account
	|-> WHERE
		|-> email
			|-> =
			|-> foo@bar.com
*/
func selectExecutor(t *Tx, selectDecl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {

	var schema string
	var selectors []agnostic.Selector
	var predicate agnostic.Predicate
	var joiners []agnostic.Joiner
	var sorters []agnostic.Sorter
	var tables []string
	var err error
	var aliases map[string]string

	for i := range selectDecl.Decl {
		switch selectDecl.Decl[i].Token {
		case parser.FromToken:
			schema, tables, aliases = getSelectedTables(selectDecl.Decl[i])
		case parser.WhereToken:
			predicate, err = t.getPredicates(selectDecl.Decl[i].Decl, schema, tables[0], args, aliases)
			if err != nil {
				return 0, 0, nil, nil, err
			}
		case parser.JoinToken:
			// Capture alias mapping for joined table, if any
			if len(selectDecl.Decl[i].Decl) > 0 {
				if tbl, al, ok := extractAliasFromTableDecl(selectDecl.Decl[i].Decl[0]); ok {
					if aliases == nil {
						aliases = make(map[string]string)
					}
					aliases[al] = tbl
				}
			}
			j, err := t.getJoin(selectDecl.Decl[i], tables[0], aliases)
			if err != nil {
				return 0, 0, nil, nil, err
			}
			joiners = append(joiners, j)
		case parser.OffsetToken:
			offset, err := strconv.Atoi(selectDecl.Decl[i].Decl[0].Lexeme)
			if err != nil {
				return 0, 0, nil, nil, fmt.Errorf("wrong offset value: %s", err)
			}
			s := agnostic.NewOffsetSorter(offset)
			sorters = append(sorters, s)
		case parser.DistinctToken:
			s, err := t.getDistinctSorter("", selectDecl.Decl[i], selectDecl.Decl[i+1].Lexeme)
			if err != nil {
				return 0, 0, nil, nil, err
			}
			sorters = append(sorters, s)
		case parser.OrderToken:
			s, err := orderbyExecutor(selectDecl.Decl[i], tables)
			if err != nil {
				return 0, 0, nil, nil, err
			}
			sorters = append(sorters, s)
		case parser.LimitToken:
			limit, err := strconv.ParseInt(selectDecl.Decl[i].Decl[0].Lexeme, 10, 64)
			if err != nil {
				return 0, 0, nil, nil, fmt.Errorf("wrong limit value: %s", err)
			}
			s := agnostic.NewLimitSorter(limit)
			sorters = append(sorters, s)
		}
	}

	if predicate == nil {
		predicate = agnostic.NewTruePredicate()
	}

	for i := 0; i < len(selectDecl.Decl); i++ {
		if selectDecl.Decl[i].Token != parser.StringToken &&
			selectDecl.Decl[i].Token != parser.StarToken &&
			selectDecl.Decl[i].Token != parser.CountToken &&
			selectDecl.Decl[i].Token != parser.NumberToken &&
			selectDecl.Decl[i].Token != parser.SimpleQuoteToken &&
			selectDecl.Decl[i].Token != parser.TrueToken &&
			selectDecl.Decl[i].Token != parser.FalseToken &&
			selectDecl.Decl[i].Token != parser.CurrentSchemaToken &&
			selectDecl.Decl[i].Token != parser.CurrentDatabaseToken {
			continue
		}
		// get attribute to select
		selector, err := t.getSelector(selectDecl.Decl[i], schema, tables, aliases)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		selectors = append(selectors, selector)
	}

	// Query handles both cases: with and without FROM clause
	log.Debug("executing '%s' with %s, joining with %s and sorting with %s", selectors, predicate, joiners, sorters)
	cols, res, err := t.tx.Query(schema, selectors, predicate, joiners, sorters)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	return 0, 0, cols, res, nil
}

func createIndexExecutor(t *Tx, indexDecl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {
	var i int
	var schema, relation, index string

	if len(indexDecl.Decl) == 0 {
		return 0, 0, nil, nil, ParsingError
	}

	// Check if 'IF NOT EXISTS' is present
	ifNotExists := hasIfNotExists(indexDecl)

	if ifNotExists {
		i++
	}

	// Fetch index name
	index = indexDecl.Decl[i].Lexeme
	i++

	if d, ok := indexDecl.Has(parser.TableToken); ok {
		relation = d.Lexeme
		i++
	}

	if d, ok := indexDecl.Has(parser.SchemaToken); ok {
		schema = d.Lexeme
	}

	var attrs []string
	for i < len(indexDecl.Decl) {
		attrs = append(attrs, indexDecl.Decl[i].Lexeme)
		i++
	}

	err := t.tx.CreateIndex(schema, relation, index, agnostic.HashIndexType, attrs)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	return 0, 0, nil, nil, nil
}

func updateExecutor(t *Tx, updateDecl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {

	var schema string
	var selectors []agnostic.Selector
	var predicate agnostic.Predicate
	var err error

	if len(updateDecl.Decl) < 3 {
		return 0, 0, nil, nil, ParsingError
	}

	relationDecl := updateDecl.Decl[0]
	setDecl := updateDecl.Decl[1]
	whereDecl := updateDecl.Decl[2]
	relation := relationDecl.Lexeme

	if d, ok := relationDecl.Has(parser.SchemaToken); ok {
		schema = d.Lexeme
	}

	// Check for RETURNING clause
	if len(updateDecl.Decl) > 3 {
		for i := range updateDecl.Decl {
			if updateDecl.Decl[i].Token == parser.ReturningToken {
				returningDecl := updateDecl.Decl[i]
				_, _, err := t.tx.RelationAttribute(schema, relation, returningDecl.Decl[0].Lexeme)
				if err != nil {
					return 0, 0, nil, nil, fmt.Errorf("cannot return %s, doesn't exist in relation %s", returningDecl.Decl[0].Lexeme, relation)
				}
			}
		}
	}

	var specifiedAttrs []string
	for _, d := range setDecl.Decl {
		specifiedAttrs = append(specifiedAttrs, d.Lexeme)
	}

	predicate, err = t.getPredicates(whereDecl.Decl, schema, relation, args, nil)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	if predicate == nil {
		predicate = agnostic.NewTruePredicate()
	}

	//	var tuples []*agnostic.Tuple
	values := make(map[string]any)
	for _, s := range setDecl.Decl {
		_, err = getSet(specifiedAttrs, values, s, args)
		if err != nil {
			return 0, 0, nil, nil, err
		}
	}

	log.Debug("executing update '%s' with values %v and predicate %s", selectors, values, predicate)
	cols, res, err := t.tx.Update(schema, relation, values, selectors, predicate)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	return 0, int64(len(res)), cols, res, nil
}

func deleteExecutor(t *Tx, decl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {
	var schema string
	var selectors []agnostic.Selector
	var predicate agnostic.Predicate
	var err error

	if len(decl.Decl) < 2 {
		return truncateExecutor(t, decl, args)
	}

	fromDecl := decl.Decl[0]
	relationDecl := fromDecl.Decl[0]
	whereDecl := decl.Decl[1]
	relation := relationDecl.Lexeme

	if d, ok := relationDecl.Has(parser.SchemaToken); ok {
		schema = d.Lexeme
	}

	// Check for RETURNING clause
	if len(decl.Decl) > 3 {
		for i := range decl.Decl {
			if decl.Decl[i].Token == parser.ReturningToken {
				returningDecl := decl.Decl[i]
				_, _, err := t.tx.RelationAttribute(schema, relation, returningDecl.Decl[0].Lexeme)
				if err != nil {
					return 0, 0, nil, nil, fmt.Errorf("cannot return %s, doesn't exist in relation %s", returningDecl.Decl[0].Lexeme, relation)
				}
			}
		}
	}

	predicate, err = t.getPredicates(whereDecl.Decl, schema, relation, args, nil)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	if predicate == nil {
		predicate = agnostic.NewTruePredicate()
	}

	_, res, err := t.tx.Delete(schema, relation, selectors, predicate)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	return 0, int64(len(res)), nil, nil, nil
}

func truncateExecutor(t *Tx, trDecl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {
	var schema string

	if len(trDecl.Decl) < 1 {
		return 0, 0, nil, nil, ParsingError
	}

	if d, ok := trDecl.Decl[0].Has(parser.SchemaToken); ok {
		schema = d.Lexeme
	}
	relation := trDecl.Decl[0].Decl[0].Lexeme

	c, err := t.tx.Truncate(schema, relation)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	return 0, c, nil, nil, nil
}

func orderbyExecutor(decl *parser.Decl, tables []string) (agnostic.Sorter, error) {
	var orderingTk int
	var valDecl *parser.Decl
	var attrs []agnostic.SortExpression

	valDecl = decl

	relation := tables[0]

	for i := 0; i < len(valDecl.Decl); i++ {
		attr := valDecl.Decl[i].Lexeme
		attrDecl := valDecl.Decl[i]
		if len(attrDecl.Decl) == 2 {
			relationDecl := attrDecl.Decl[0]
			orderingDecl := attrDecl.Decl[1]
			relation = relationDecl.Lexeme
			orderingTk = orderingDecl.Token
		} else if len(attrDecl.Decl) == 1 {
			switch attrDecl.Decl[0].Token {
			case parser.StringToken:
				orderingTk = parser.AscToken
				relation = attrDecl.Decl[0].Lexeme
			case parser.AscToken, parser.DescToken:
				orderingTk = attrDecl.Decl[0].Token
				relation = tables[0]
			}
		} else {
			orderingTk = parser.AscToken
		}

		switch orderingTk {
		case parser.AscToken:
			attrs = append(attrs, agnostic.NewSortExpression(attr, agnostic.ASC))
		case parser.DescToken:
			attrs = append(attrs, agnostic.NewSortExpression(attr, agnostic.DESC))
		default:
			attrs = append(attrs, agnostic.NewSortExpression(attr, agnostic.ASC))
		}

	}

	sorter := agnostic.NewOrderBySorter(relation, attrs)
	return sorter, nil
}
