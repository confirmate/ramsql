package agnostic

import (
	"fmt"
	"sync"
)

const (
	DefaultSchema = "public"
)

type Engine struct {
	schemas map[string]*Schema

	// searchPath holds the schema search path, with the first schema being the current schema
	// This is used by CURRENT_SCHEMA() to return the first schema in the search path
	searchPath []string

	sync.Mutex
}

func NewEngine() *Engine {
	e := &Engine{}

	// create public schema
	e.schemas = make(map[string]*Schema)
	e.schemas[DefaultSchema] = NewSchema(DefaultSchema)

	// initialize search_path with default schema
	e.searchPath = []string{DefaultSchema}

	// create information_schema with a 'tables' relation used by clients (e.g. GORM)
	info := NewSchema("information_schema")
	// minimal columns used by GORM queries: table_schema, table_name, table_type
	attrs := []Attribute{
		NewAttribute("table_schema", "varchar"),
		NewAttribute("table_name", "varchar"),
		NewAttribute("table_type", "varchar"),
	}
	// create relation (no primary key)
	if r, err := NewRelation("information_schema", "tables", attrs, nil); err == nil {
		info.Add("tables", r)
	}
	e.schemas["information_schema"] = info

	return e
}

func (e *Engine) Begin() (*Transaction, error) {
	t, err := NewTransaction(e)
	return t, err
}

func (e *Engine) createRelation(schema, relation string, attributes []Attribute, pk []string) (*Schema, *Relation, error) {

	s, err := e.schema(schema)
	if err != nil {
		return nil, nil, err
	}

	r, err := NewRelation(schema, relation, attributes, pk)
	if err != nil {
		return nil, nil, err
	}

	s.Add(relation, r)

	return s, r, nil
}

func (e *Engine) dropRelation(schema, relation string) (*Schema, *Relation, error) {

	s, err := e.schema(schema)
	if err != nil {
		return nil, nil, err
	}

	r, err := s.Remove(relation)
	if err != nil {
		return nil, nil, err
	}

	return s, r, nil
}

func (e *Engine) schema(name string) (*Schema, error) {
	if name == "" {
		name = DefaultSchema
	}

	s, ok := e.schemas[name]
	if !ok {
		return nil, fmt.Errorf("schema '%s' does not exist", name)
	}

	return s, nil
}

func (e *Engine) createSchema(name string) (*Schema, error) {
	s, ok := e.schemas[name]
	if ok {
		return nil, fmt.Errorf("schema '%s' already exist", name)
	}

	s = NewSchema(name)
	e.schemas[name] = s
	return s, nil
}

func (e *Engine) dropSchema(name string) (*Schema, error) {
	s, ok := e.schemas[name]
	if !ok {
		return nil, fmt.Errorf("schema '%s' does not exist", name)
	}

	delete(e.schemas, name)
	return s, nil
}

// CurrentSchema returns the first schema in the search path
// This implements the CURRENT_SCHEMA() function behavior
func (e *Engine) CurrentSchema() string {
	if len(e.searchPath) == 0 {
		return DefaultSchema
	}
	return e.searchPath[0]
}
