package iceberg

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Schema evolution errors.
var (
	ErrColumnExists    = errors.New("column already exists")
	ErrColumnNotFound  = errors.New("column not found")
	ErrColumnInUse     = errors.New("column is used by the partition spec")
	ErrBadPromotion    = errors.New("unsupported column type change")
	ErrLastColumn      = errors.New("cannot drop the last column")
	errNestedAddColumn = errors.New("only primitive column types can be added")
)

func (s Schema) index(name string) int {
	return slices.IndexFunc(s.Fields, func(f Field) bool { return strings.EqualFold(f.Name, name) })
}

// WithColumn returns s with a new optional column using the m field id.
func (m *TableMetadata) WithColumn(s Schema, name, hiveType, doc string) (Schema, int, error) {
	if s.index(name) >= 0 {
		return s, 0, fmt.Errorf("%w: %s", ErrColumnExists, name)
	}

	t, err := FromHive(hiveType)
	if err != nil {
		return s, 0, fmt.Errorf("%w: %w", errNestedAddColumn, err)
	}

	raw, _ := json.Marshal(t)
	id := m.LastColumnID + 1
	out := s
	out.Fields = append(slices.Clone(s.Fields), Field{ID: id, Name: name, Doc: doc, Type: raw})

	return out, id, nil
}

// WithoutColumn returns s without the named column; partition source columns cannot be dropped.
func (m *TableMetadata) WithoutColumn(s Schema, name string) (Schema, error) {
	i := s.index(name)
	if i < 0 {
		return s, fmt.Errorf("%w: %s", ErrColumnNotFound, name)
	}

	for _, spec := range m.PartitionSpecs {
		for _, pf := range spec.Fields {
			if pf.SourceID == s.Fields[i].ID {
				return s, fmt.Errorf("%w: %s", ErrColumnInUse, name)
			}
		}
	}

	if len(s.Fields) == 1 {
		return s, ErrLastColumn
	}

	out := s
	out.Fields = slices.Delete(slices.Clone(s.Fields), i, i+1)

	return out, nil
}

// WithRenamedColumn returns s with the column renamed; a non-empty hiveType also promotes its type.
func (m *TableMetadata) WithRenamedColumn(s Schema, from, to, hiveType string) (Schema, error) {
	i := s.index(from)
	if i < 0 {
		return s, fmt.Errorf("%w: %s", ErrColumnNotFound, from)
	}

	if !strings.EqualFold(from, to) && s.index(to) >= 0 {
		return s, fmt.Errorf("%w: %s", ErrColumnExists, to)
	}

	out := s
	out.Fields = slices.Clone(s.Fields)
	f := out.Fields[i]
	f.Name = to

	if hiveType != "" {
		nt, err := FromHive(hiveType)
		if err != nil {
			return s, fmt.Errorf("%w: %w", ErrBadPromotion, err)
		}

		if old := PrimitiveName(f.Type); old != nt {
			if !canPromote(old, nt) {
				return s, fmt.Errorf("%w: %s to %s", ErrBadPromotion, old, nt)
			}

			f.Type, _ = json.Marshal(nt)
		}
	}

	out.Fields[i] = f

	return out, nil
}

// canPromote reports the type changes the table spec allows: int to long,
// float to double, and widening decimal precision at a fixed scale.
func canPromote(from, to string) bool {
	switch {
	case from == typeInt && to == typeLong, from == typeFloat && to == typeDouble:
		return true
	case strings.HasPrefix(from, "decimal(") && strings.HasPrefix(to, "decimal("):
		fp, fs, ok1 := decimalParts(from)
		tp, ts, ok2 := decimalParts(to)

		return ok1 && ok2 && fs == ts && tp >= fp
	default:
		return false
	}
}

func decimalParts(t string) (int, int, bool) {
	var p, s int

	_, err := fmt.Sscanf(strings.ReplaceAll(t, " ", ""), "decimal(%d,%d)", &p, &s)

	return p, s, err == nil
}

// SetSchema makes s a new current schema of m (an update copy).
func (m *TableMetadata) SetSchema(s Schema) {
	id := 0
	for _, e := range m.Schemas {
		id = max(id, e.SchemaID+1)
	}

	s.SchemaID, s.Type = id, typeStruct

	for _, f := range s.Fields {
		m.LastColumnID = max(m.LastColumnID, f.ID)
	}

	m.Schemas = append(m.Schemas, s)
	m.CurrentSchemaID = id
}

// ExpireSnapshots drops the snapshots keep rejects (never the current one) and
// returns them; log entries and refs that pointed at them go too.
func (m *TableMetadata) ExpireSnapshots(keep func(Snapshot) bool) []Snapshot {
	var gone []Snapshot

	kept := make([]Snapshot, 0, len(m.Snapshots))

	for _, s := range m.Snapshots {
		if s.SnapshotID == m.CurrentSnapshotID || keep(s) {
			kept = append(kept, s)
		} else {
			gone = append(gone, s)
		}
	}

	if len(gone) == 0 {
		return nil
	}

	alive := map[int64]bool{}
	for _, s := range kept {
		alive[s.SnapshotID] = true
	}

	m.Snapshots = kept
	m.SnapshotLog = slices.DeleteFunc(slices.Clone(m.SnapshotLog), func(e SnapshotLogEntry) bool {
		return !alive[e.SnapshotID]
	})

	for name, r := range m.Refs {
		if !alive[r.SnapshotID] {
			delete(m.Refs, name)
		}
	}

	return gone
}

// TrimMetadataLog keeps the newest max previous-metadata entries and returns the dropped ones.
func (m *TableMetadata) TrimMetadataLog(maxKeep int) []MetadataLogEntry {
	if maxKeep < 0 || len(m.MetadataLog) <= maxKeep {
		return nil
	}

	cut := len(m.MetadataLog) - maxKeep
	dropped := slices.Clone(m.MetadataLog[:cut])
	m.MetadataLog = slices.Clone(m.MetadataLog[cut:])

	return dropped
}
