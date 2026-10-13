package glue

import (
	"fmt"
	"slices"
)

// requireNameAttribute enforces the documented AttributesToGet rule: a supplied list is non-empty and includes NAME.
func requireNameAttribute(attrs []string) error {
	if attrs == nil || slices.Contains(attrs, "NAME") {
		return nil
	}

	return fmt.Errorf("%w: AttributesToGet must include NAME", ErrValidation)
}

func projectTables(tables []*Table, attrs []string) []*Table {
	if attrs == nil || slices.Contains(attrs, "DEFAULT") {
		return tables
	}

	withType := slices.Contains(attrs, "TABLE_TYPE")
	out := make([]*Table, len(tables))

	for i, t := range tables {
		p := &Table{Name: t.Name}
		if withType {
			p.TableType = t.TableType
		}

		out[i] = p
	}

	return out
}

func projectDatabases(dbs []*Database, attrs []string) []*Database {
	if attrs == nil {
		return dbs
	}

	withTarget := slices.Contains(attrs, "TARGET_DATABASE")
	out := make([]*Database, len(dbs))

	for i, d := range dbs {
		p := &Database{Name: d.Name}
		if withTarget {
			p.TargetDatabase = d.TargetDatabase
		}

		out[i] = p
	}

	return out
}
