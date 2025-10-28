package agnostic

import (
	"sort"
	"strings"
)

// ForeignKey represents a foreign key constraint (both column-level and relation-level)
type ForeignKey struct {
	// name is the optional constraint name (e.g., "memberships_user_fkey")
	name string
	// localColumns are the column names in this relation that form the FK
	// For column-level FK, this will have one element
	localColumns []string
	// refSchema is the schema of the referenced relation (empty means same schema)
	refSchema string
	// refRelation is the name of the referenced relation
	refRelation string
	// refColumns are the referenced column names (empty means reference PK)
	// For column-level FK, this will typically have one element
	refColumns []string
}

// Name returns the optional constraint name
func (fk ForeignKey) Name() string {
	return fk.name
}

// LocalColumns returns the column names in this relation that form the FK
func (fk ForeignKey) LocalColumns() []string {
	return fk.localColumns
}

// RefSchema returns the schema of the referenced relation (empty means same schema)
func (fk ForeignKey) RefSchema() string {
	return fk.refSchema
}

// RefRelation returns the name of the referenced relation
func (fk ForeignKey) RefRelation() string {
	return fk.refRelation
}

// RefColumns returns the referenced column names (empty means reference PK)
func (fk ForeignKey) RefColumns() []string {
	return fk.refColumns
}

// NewForeignKey creates a new ForeignKey with the given constraint name
func NewForeignKey(name string) ForeignKey {
	return ForeignKey{
		name:         name,
		localColumns: []string{},
		refColumns:   []string{},
	}
}

// WithLocalColumn adds a local column to the foreign key
func (fk ForeignKey) WithLocalColumn(col string) ForeignKey {
	fk.localColumns = append(fk.localColumns, col)
	return fk
}

// WithRefSchema sets the referenced schema
func (fk ForeignKey) WithRefSchema(schema string) ForeignKey {
	fk.refSchema = schema
	return fk
}

// WithRefRelation sets the referenced relation name
func (fk ForeignKey) WithRefRelation(relation string) ForeignKey {
	fk.refRelation = relation
	return fk
}

// WithRefColumn adds a referenced column
func (fk ForeignKey) WithRefColumn(col string) ForeignKey {
	fk.refColumns = append(fk.refColumns, col)
	return fk
}

// uniqueRelationFKs derives unique foreign key groups from attribute-level metadata.
// It groups foreign keys by the tuple (refSchema, refRelation, localColumns, refColumns)
// and returns one representative per group. Ordering of the result is stable
// (sorted by signature) to keep deterministic behavior.
func uniqueRelationFKs(r *Relation) []ForeignKey {
	seen := make(map[string]ForeignKey)

	for _, at := range r.attributes {
		if at.fk == nil {
			continue
		}
		fk := *at.fk
		// Defensive: if local columns were not populated for a column-level FK,
		// treat it as the single column itself.
		if len(fk.localColumns) == 0 {
			fk.localColumns = []string{at.name}
		}
		sig := fkSignature(&fk)
		if _, ok := seen[sig]; !ok {
			seen[sig] = fk
		}
	}

	if len(seen) == 0 {
		return nil
	}

	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	res := make([]ForeignKey, 0, len(keys))
	for _, k := range keys {
		res = append(res, seen[k])
	}
	return res
}

// fkSignature creates a unique signature string for a foreign key definition.
// The signature is based on (refSchema, refRelation, localColumns, refColumns).
func fkSignature(fk *ForeignKey) string {
	// Normalize empty refSchema to empty string; caller may handle defaults.
	return strings.Join([]string{
		fk.refSchema,
		fk.refRelation,
		strings.Join(fk.localColumns, ","),
		strings.Join(fk.refColumns, ","),
	}, "|")
}
