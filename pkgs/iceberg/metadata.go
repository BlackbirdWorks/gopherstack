// Package iceberg reads and writes Apache Iceberg v2 table metadata, manifest
// lists and manifests (the table spec, https://iceberg.apache.org/spec/).
package iceberg

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// FormatVersion is the only table format version this package writes.
	FormatVersion = 2
	// NoSnapshot is the current-snapshot-id of a table without snapshots.
	NoSnapshot = int64(-1)
	// MainBranch is the default branch ref name.
	MainBranch = "main"

	firstPartitionFieldID = 1000
	supportedMaxVersion   = 2
)

var (
	// ErrInvalidMetadata is returned for a metadata document that is not valid Iceberg table metadata.
	ErrInvalidMetadata = errors.New("invalid iceberg table metadata")
	// ErrNoSnapshot is returned when a snapshot id or timestamp matches no snapshot.
	ErrNoSnapshot = errors.New("iceberg snapshot not found")
)

// Field is one schema field; Type is a primitive name or a nested type object.
type Field struct {
	Name     string          `json:"name"`
	Doc      string          `json:"doc,omitempty"`
	Type     json.RawMessage `json:"type"`
	ID       int             `json:"id"`
	Required bool            `json:"required"`
}

// Schema is an Iceberg struct schema.
type Schema struct {
	Type               string  `json:"type"`
	Fields             []Field `json:"fields"`
	IdentifierFieldIDs []int   `json:"identifier-field-ids,omitempty"`
	SchemaID           int     `json:"schema-id"`
}

// PartitionField is one partition spec field.
type PartitionField struct {
	Name      string `json:"name"`
	Transform string `json:"transform"`
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
}

// PartitionSpec is a partitioning of a table.
type PartitionSpec struct {
	Fields []PartitionField `json:"fields"`
	SpecID int              `json:"spec-id"`
}

// Snapshot is one table snapshot.
type Snapshot struct {
	Summary          map[string]string `json:"summary,omitempty"`
	ParentSnapshotID *int64            `json:"parent-snapshot-id,omitempty"`
	SchemaID         *int              `json:"schema-id,omitempty"`
	ManifestList     string            `json:"manifest-list,omitempty"`
	SnapshotID       int64             `json:"snapshot-id"`
	SequenceNumber   int64             `json:"sequence-number"`
	TimestampMS      int64             `json:"timestamp-ms"`
}

// SnapshotLogEntry records when a snapshot became current.
type SnapshotLogEntry struct {
	TimestampMS int64 `json:"timestamp-ms"`
	SnapshotID  int64 `json:"snapshot-id"`
}

// MetadataLogEntry records a previous metadata file.
type MetadataLogEntry struct {
	MetadataFile string `json:"metadata-file"`
	TimestampMS  int64  `json:"timestamp-ms"`
}

// Ref is a named snapshot reference.
type Ref struct {
	Type       string `json:"type"`
	SnapshotID int64  `json:"snapshot-id"`
}

// TableMetadata is a table's metadata.json document. Fields this package does
// not model survive a decode/encode round trip.
type TableMetadata struct {
	Properties         map[string]string `json:"properties"`
	Refs               map[string]Ref    `json:"refs,omitempty"`
	extra              map[string]json.RawMessage
	TableUUID          string             `json:"table-uuid"`
	Location           string             `json:"location"`
	Schemas            []Schema           `json:"schemas"`
	PartitionSpecs     []PartitionSpec    `json:"partition-specs"`
	SortOrders         []json.RawMessage  `json:"sort-orders"`
	Snapshots          []Snapshot         `json:"snapshots"`
	SnapshotLog        []SnapshotLogEntry `json:"snapshot-log"`
	MetadataLog        []MetadataLogEntry `json:"metadata-log"`
	LastSequenceNumber int64              `json:"last-sequence-number"`
	LastUpdatedMS      int64              `json:"last-updated-ms"`
	CurrentSnapshotID  int64              `json:"current-snapshot-id"`
	FormatVersion      int                `json:"format-version"`
	LastColumnID       int                `json:"last-column-id"`
	CurrentSchemaID    int                `json:"current-schema-id"`
	DefaultSpecID      int                `json:"default-spec-id"`
	LastPartitionID    int                `json:"last-partition-id"`
	DefaultSortOrderID int                `json:"default-sort-order-id"`
}

func modeledKeys() []string {
	return []string{
		keyFormatVersion, "table-uuid", "location", "last-sequence-number", "last-updated-ms", "last-column-id",
		"current-schema-id", "schemas", "default-spec-id", "partition-specs", "last-partition-id",
		"default-sort-order-id", "sort-orders", "properties", "current-snapshot-id", "snapshots",
		"snapshot-log", "metadata-log", "refs",
	}
}

type metadataAlias TableMetadata

// UnmarshalJSON decodes a v1 or v2 document and keeps unmodeled members.
func (m *TableMetadata) UnmarshalJSON(b []byte) error {
	var v1 struct {
		Schema          *Schema          `json:"schema"`
		PartitionSpecV1 []PartitionField `json:"partition-spec"`
	}

	var a metadataAlias

	a.CurrentSnapshotID = NoSnapshot

	if err := json.Unmarshal(b, &a); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMetadata, err)
	}

	if err := json.Unmarshal(b, &v1); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMetadata, err)
	}

	if len(a.Schemas) == 0 && v1.Schema != nil {
		a.Schemas = []Schema{*v1.Schema}
		a.CurrentSchemaID = v1.Schema.SchemaID
	}

	if len(a.PartitionSpecs) == 0 && v1.PartitionSpecV1 != nil {
		a.PartitionSpecs = []PartitionSpec{{SpecID: 0, Fields: v1.PartitionSpecV1}}
	}

	raw := map[string]json.RawMessage{}
	_ = json.Unmarshal(b, &raw)

	for _, k := range modeledKeys() {
		delete(raw, k)
	}

	*m = TableMetadata(a)
	m.extra = raw

	return nil
}

// MarshalJSON encodes the modeled members over the preserved extras.
func (m *TableMetadata) MarshalJSON() ([]byte, error) {
	out := maps.Clone(m.extra)
	if out == nil {
		out = map[string]json.RawMessage{}
	}

	typed, err := json.Marshal(metadataAlias(*m))
	if err != nil {
		return nil, err
	}

	var fields map[string]json.RawMessage
	if err = json.Unmarshal(typed, &fields); err != nil {
		return nil, err
	}

	maps.Copy(out, fields)

	return json.Marshal(out)
}

// ParseMetadata decodes and validates a metadata.json document.
func ParseMetadata(data []byte) (*TableMetadata, error) {
	var m TableMetadata
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}

	switch {
	case m.FormatVersion < 1 || m.FormatVersion > supportedMaxVersion:
		return nil, fmt.Errorf("%w: unsupported format-version %d", ErrInvalidMetadata, m.FormatVersion)
	case len(m.Schemas) == 0:
		return nil, fmt.Errorf("%w: no schema", ErrInvalidMetadata)
	case m.Location == "":
		return nil, fmt.Errorf("%w: no location", ErrInvalidMetadata)
	}

	if _, ok := m.SchemaByID(m.CurrentSchemaID); !ok {
		return nil, fmt.Errorf("%w: current-schema-id %d not in schemas", ErrInvalidMetadata, m.CurrentSchemaID)
	}

	return &m, nil
}

// SchemaByID returns the schema with the given id.
func (m *TableMetadata) SchemaByID(id int) (Schema, bool) {
	for _, s := range m.Schemas {
		if s.SchemaID == id {
			return s, true
		}
	}

	return Schema{}, false
}

// CurrentSchema is the table's current schema.
func (m *TableMetadata) CurrentSchema() Schema {
	s, _ := m.SchemaByID(m.CurrentSchemaID)

	return s
}

// SpecByID returns the partition spec with the given id.
func (m *TableMetadata) SpecByID(id int) (PartitionSpec, bool) {
	for _, s := range m.PartitionSpecs {
		if s.SpecID == id {
			return s, true
		}
	}

	return PartitionSpec{}, false
}

// DefaultSpec is the spec new data is written with.
func (m *TableMetadata) DefaultSpec() PartitionSpec {
	s, _ := m.SpecByID(m.DefaultSpecID)

	return s
}

// SnapshotByID returns the snapshot with the given id.
func (m *TableMetadata) SnapshotByID(id int64) (Snapshot, bool) {
	for _, s := range m.Snapshots {
		if s.SnapshotID == id {
			return s, true
		}
	}

	return Snapshot{}, false
}

// CurrentSnapshot returns the current snapshot, if the table has one.
func (m *TableMetadata) CurrentSnapshot() (Snapshot, bool) {
	if m.CurrentSnapshotID == NoSnapshot {
		return Snapshot{}, false
	}

	return m.SnapshotByID(m.CurrentSnapshotID)
}

// SnapshotAsOf returns the snapshot that was current at t: the last
// snapshot-log entry at or before t.
func (m *TableMetadata) SnapshotAsOf(t time.Time) (Snapshot, error) {
	ms := t.UnixMilli()

	var best *SnapshotLogEntry

	for i := range m.SnapshotLog {
		e := &m.SnapshotLog[i]
		if e.TimestampMS <= ms && (best == nil || e.TimestampMS >= best.TimestampMS) {
			best = e
		}
	}

	if best == nil {
		for _, s := range m.Snapshots {
			if s.TimestampMS <= ms && (best == nil || s.TimestampMS >= best.TimestampMS) {
				best = &SnapshotLogEntry{TimestampMS: s.TimestampMS, SnapshotID: s.SnapshotID}
			}
		}
	}

	if best == nil {
		return Snapshot{}, fmt.Errorf("%w: no version at or before %s", ErrNoSnapshot, t.UTC().Format(time.RFC3339))
	}

	s, ok := m.SnapshotByID(best.SnapshotID)
	if !ok {
		return Snapshot{}, fmt.Errorf("%w: snapshot %d", ErrNoSnapshot, best.SnapshotID)
	}

	return s, nil
}

// NewParams describes a table to create.
type NewParams struct {
	Now        time.Time
	Properties map[string]string
	Location   string
	SortOrder  json.RawMessage
	Schema     Schema
	Spec       PartitionSpec
}

// NewTableMetadata builds the v2 metadata of a table with no snapshots.
func NewTableMetadata(p NewParams) *TableMetadata {
	schema := p.Schema
	schema.Type = typeStruct
	schema.SchemaID = 0

	lastCol := 0
	for _, f := range schema.Fields {
		lastCol = max(lastCol, f.ID)
	}

	lastPart := firstPartitionFieldID - 1
	for _, f := range p.Spec.Fields {
		lastPart = max(lastPart, f.FieldID)
	}

	order := p.SortOrder
	if order == nil {
		order = json.RawMessage(`{"order-id":0,"fields":[]}`)
	}

	props := map[string]string{}
	maps.Copy(props, p.Properties)

	return &TableMetadata{
		FormatVersion:      FormatVersion,
		TableUUID:          uuid.NewString(),
		Location:           p.Location,
		LastUpdatedMS:      p.Now.UnixMilli(),
		LastColumnID:       lastCol,
		Schemas:            []Schema{schema},
		DefaultSpecID:      p.Spec.SpecID,
		PartitionSpecs:     []PartitionSpec{p.Spec},
		LastPartitionID:    lastPart,
		SortOrders:         []json.RawMessage{order},
		Properties:         props,
		CurrentSnapshotID:  NoSnapshot,
		Snapshots:          []Snapshot{},
		SnapshotLog:        []SnapshotLogEntry{},
		MetadataLog:        []MetadataLogEntry{},
		DefaultSortOrderID: sortOrderID(order),
	}
}

func sortOrderID(order json.RawMessage) int {
	var o struct {
		ID int `json:"order-id"`
	}

	_ = json.Unmarshal(order, &o)

	return o.ID
}

// AppendSnapshot returns a copy of m with snap added as the current snapshot of main.
// previousMetadataFile is recorded in the metadata log.
func (m *TableMetadata) AppendSnapshot(snap Snapshot, previousMetadataFile string, now time.Time) *TableMetadata {
	next := *m
	next.Properties = maps.Clone(m.Properties)
	next.Snapshots = append(slices.Clone(m.Snapshots), snap)
	next.SnapshotLog = append(slices.Clone(m.SnapshotLog),
		SnapshotLogEntry{TimestampMS: snap.TimestampMS, SnapshotID: snap.SnapshotID})
	next.MetadataLog = slices.Clone(m.MetadataLog)

	if previousMetadataFile != "" {
		next.MetadataLog = append(next.MetadataLog,
			MetadataLogEntry{MetadataFile: previousMetadataFile, TimestampMS: m.LastUpdatedMS})
	}

	next.Refs = maps.Clone(m.Refs)
	if next.Refs == nil {
		next.Refs = map[string]Ref{}
	}

	next.Refs[MainBranch] = Ref{Type: "branch", SnapshotID: snap.SnapshotID}
	next.CurrentSnapshotID = snap.SnapshotID
	next.LastSequenceNumber = snap.SequenceNumber
	next.LastUpdatedMS = max(now.UnixMilli(), snap.TimestampMS)

	return &next
}

// NextMetadataLocation names this metadata's own file, numbered by its metadata-log length.
func (m *TableMetadata) NextMetadataLocation() string {
	return fmt.Sprintf("%s/metadata/%05d-%s.metadata.json",
		strings.TrimSuffix(m.Location, "/"), len(m.MetadataLog), uuid.NewString())
}

// InitialMetadataLocation names the first metadata file of a new table.
func InitialMetadataLocation(location string) string {
	return fmt.Sprintf("%s/metadata/00000-%s.metadata.json", strings.TrimSuffix(location, "/"), uuid.NewString())
}
