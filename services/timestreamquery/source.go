package timestreamquery

import "time"

// Measure value types as written by TimestreamWrite.
const (
	measureTypeDouble    = "DOUBLE"
	measureTypeBigint    = "BIGINT"
	measureTypeVarchar   = "VARCHAR"
	measureTypeBoolean   = "BOOLEAN"
	measureTypeTimestamp = "TIMESTAMP"
	measureTypeMulti     = "MULTI"
)

// SourceDimension is one dimension of a stored record.
type SourceDimension struct {
	Name  string
	Value string
}

// SourceMeasure is one named measure of a MULTI record. TIMESTAMP values arrive
// formatted as "2006-01-02 15:04:05.000000000".
type SourceMeasure struct {
	Name  string
	Value string
	Type  string
}

// SourceRecord is a record read from, or written to, a Timestream Write table.
// A record is single-measure when MeasureValueType is not MULTI, else Measures holds its values.
type SourceRecord struct {
	Time             time.Time
	MeasureName      string
	MeasureValue     string
	MeasureValueType string
	Dimensions       []SourceDimension
	Measures         []SourceMeasure
	Version          int64
}

// RecordSource is the seam onto Timestream Write's tables.
type RecordSource interface {
	Catalog
	// TableRecords returns the records still queryable at now.
	TableRecords(region, database, table string, now time.Time) ([]SourceRecord, error)
	ListDatabases(region string) []string
	ListTables(region, database string) []string
	AppendRecords(region, database, table string, records []SourceRecord) error
}

// SetRecordSource wires the record source used to execute queries and scheduled-query writes.
// It also serves as the table catalog.
func (b *InMemoryBackend) SetRecordSource(s RecordSource) {
	b.mu.Lock("SetRecordSource")
	defer b.mu.Unlock()
	b.source = s
	b.catalog = s
}

func (b *InMemoryBackend) recordSource() RecordSource {
	b.mu.RLock("recordSource")
	defer b.mu.RUnlock()

	return b.source
}

// SetClock replaces the wall clock used for now(), ago() and page-token expiry.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()
	b.nowFn = now
}

func (b *InMemoryBackend) clock() time.Time {
	b.mu.RLock("clock")
	fn := b.nowFn
	b.mu.RUnlock()

	if fn == nil {
		return time.Now().UTC()
	}

	return fn().UTC()
}
