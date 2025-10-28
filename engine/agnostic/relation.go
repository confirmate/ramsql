package agnostic

import (
	"container/list"
	"fmt"
	"strings"
	"sync"
)

type Relation struct {
	name   string
	schema string

	attributes []Attribute
	attrIndex  map[string]int
	// indexes of primary key attributes
	pk []int

	// list of Tuple
	rows *list.List

	indexes []Index

	sync.RWMutex
}

func NewRelation(schema, name string, attributes []Attribute, pk []string) (*Relation, error) {
	r := &Relation{
		name:       name,
		schema:     schema,
		attributes: attributes,
		attrIndex:  make(map[string]int),
		rows:       list.New(),
	}

	// create utils to manage attributes
	for i, a := range r.attributes {
		r.attrIndex[a.name] = i
	}
	for _, k := range pk {
		r.pk = append(r.pk, r.attrIndex[k])
	}

	// if primary key is specified, create Hash index (deduped)
	if len(r.pk) != 0 {
		r.ensureHashIndex("pk_", pk)
	}

	// if unique is specified, create Hash index (deduped)
	for _, a := range r.attributes {
		if a.unique {
			r.ensureHashIndex("unique_", []string{a.name})
		}
	}

	// Create indexes for foreign key local columns to speed lookups and RESTRICT checks
	// Derive unique FK groups from attribute metadata.
	for _, fk := range uniqueRelationFKs(r) {
		local := fk.LocalColumns()
		if len(local) == 0 {
			continue
		}
		r.ensureHashIndex("fk_", local)
	}

	return r, nil
}

func (r *Relation) CheckPrimaryKey(tuple *Tuple) (bool, error) {
	if len(r.pk) == 0 {
		return true, nil
	}

	var index Index
	for i := range r.indexes {
		if strings.HasPrefix(r.indexes[i].Name(), "pk") {
			index = r.indexes[i]
			break
		}
	}
	if index == nil {
		return false, fmt.Errorf("primary key index not found")
	}

	var vals []any
	for _, idx := range r.pk {
		vals = append(vals, tuple.values[idx])
	}

	e, err := index.Get(vals)
	if err != nil {
		return false, err
	}
	if e == nil {
		return true, nil
	}

	return false, nil
}

func (r *Relation) Attribute(name string) (int, Attribute, error) {
	name = strings.ToLower(name)
	index, ok := r.attrIndex[name]
	if !ok {
		return 0, Attribute{}, fmt.Errorf("attribute not defined: %s.%s", r.name, name)
	}
	return index, r.attributes[index], nil
}

func (r *Relation) createIndex(name string, t IndexType, attrs []string) error {

	switch t {
	case HashIndexType:
		var attrsIdx []int
		for _, a := range attrs {
			for i, rela := range r.attributes {
				if a == rela.name {
					attrsIdx = append(attrsIdx, i)
					break
				}
			}
		}
		i := NewHashIndex(name, r.name, r.attributes, attrs, attrsIdx)
		r.indexes = append(r.indexes, i)
		return nil
	case BTreeIndexType:
		return fmt.Errorf("BTree index are not implemented")
	}

	return fmt.Errorf("unknown index type: %d", t)
}

// ensureHashIndex creates a hash index on the given attributes if one with the
// exact same attribute list (and order) does not already exist.
func (r *Relation) ensureHashIndex(prefix string, attrs []string) {
	if len(attrs) == 0 {
		return
	}
	if r.hasIndexOn(attrs) {
		return
	}
	// build attribute indexes
	idxs := make([]int, 0, len(attrs))
	for _, a := range attrs {
		i, ok := r.attrIndex[a]
		if !ok {
			return // attribute not found, skip
		}
		idxs = append(idxs, i)
	}
	iname := prefix + r.schema + "_" + r.name + "_" + strings.Join(attrs, "_")
	r.indexes = append(r.indexes, NewHashIndex(iname, r.name, r.attributes, attrs, idxs))
}

// hasIndexOn reports whether there is already a hash index on the exact attribute
// list (same names in the same order).
func (r *Relation) hasIndexOn(attrs []string) bool {
	for _, ix := range r.indexes {
		if hi, ok := ix.(*HashIndex); ok {
			if len(hi.attrsName) != len(attrs) {
				continue
			}
			match := true
			for i := range attrs {
				if hi.attrsName[i] != attrs[i] {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

func (r *Relation) Truncate() int64 {
	r.Lock()
	defer r.Unlock()

	l := r.rows.Len()

	for _, i := range r.indexes {
		i.Truncate()
	}

	r.rows = list.New()

	return int64(l)
}

func (r *Relation) String() string {
	if r.schema != "" {
		return r.schema + "." + r.name
	}
	return r.name
}
