package ramsql

import (
	"database/sql/driver"
	"reflect"
)

// CheckNamedValue implements driver.NamedValueChecker.
// It allows slice and array values to pass through without conversion so
// they can be expanded inside inExecutor when the query is executed.
// All other value types fall back to the default database/sql conversion.
func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	if nv.Value == nil {
		return driver.ErrSkip
	}
	rv := reflect.ValueOf(nv.Value)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		// Accept as-is; executor.inExecutor handles expansion via expandArrayValue.
		return nil
	}
	return driver.ErrSkip
}
