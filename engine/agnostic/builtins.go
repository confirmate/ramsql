package agnostic

import (
	"time"
)

// NowValueFunctor returns the current timestamp
type NowValueFunctor struct {
}

// NewNowValueFunctor creates a ValueFunctor returning time.Now()
func NewNowValueFunctor() ValueFunctor {
	return &NowValueFunctor{}
}

func (f *NowValueFunctor) Value([]string, *Tuple) any {
	return time.Now()
}

func (f *NowValueFunctor) Relation() string {
	return ""
}

func (f *NowValueFunctor) Attribute() []string {
	return nil
}

func (f NowValueFunctor) String() string {
	return "now()"
}

// CurrentSchemaValueFunctor returns the first schema in the search path
type CurrentSchemaValueFunctor struct {
	engine *Engine
}

// NewCurrentSchemaFunctor creates a ValueFunctor that returns the current schema
func NewCurrentSchemaFunctor(engine *Engine) ValueFunctor {
	return &CurrentSchemaValueFunctor{
		engine: engine,
	}
}

func (f *CurrentSchemaValueFunctor) Value([]string, *Tuple) any {
	if f.engine != nil {
		return f.engine.CurrentSchema()
	}
	return DefaultSchema
}

func (f *CurrentSchemaValueFunctor) Relation() string {
	return ""
}

func (f *CurrentSchemaValueFunctor) Attribute() []string {
	return nil
}

func (f CurrentSchemaValueFunctor) String() string {
	return "current_schema()"
}

// CurrentDatabaseValueFunctor returns the database name
type CurrentDatabaseValueFunctor struct {
	dbName string
}

// NewCurrentDatabaseFunctor creates a ValueFunctor that returns the current database name
func NewCurrentDatabaseFunctor(dbName string) ValueFunctor {
	return &CurrentDatabaseValueFunctor{
		dbName: dbName,
	}
}

func (f *CurrentDatabaseValueFunctor) Value([]string, *Tuple) any {
	return f.dbName
}

func (f *CurrentDatabaseValueFunctor) Relation() string {
	return ""
}

func (f *CurrentDatabaseValueFunctor) Attribute() []string {
	return nil
}

func (f CurrentDatabaseValueFunctor) String() string {
	return "current_database()"
}
