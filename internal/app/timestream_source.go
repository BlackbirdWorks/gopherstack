package app

import (
	"sort"
	"strconv"
	"strings"
	"time"

	timestreamquerybackend "github.com/blackbirdworks/gopherstack/services/timestreamquery"
	timestreamwritebackend "github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

const (
	tsNanoLayout  = "2006-01-02 15:04:05.000000000"
	tsMeasureTime = "TIMESTAMP"
	tsNanosUnit   = "NANOSECONDS"
	tsVarchar     = "VARCHAR"
)

// timestreamRecordSource exposes Timestream Write's tables and records to Timestream Query.
type timestreamRecordSource struct {
	timestreamCatalog
}

var _ timestreamquerybackend.RecordSource = (*timestreamRecordSource)(nil)

func newTimestreamRecordSource(h *timestreamwritebackend.Handler) *timestreamRecordSource {
	return &timestreamRecordSource{timestreamCatalog{handler: h}}
}

func (s *timestreamRecordSource) TableRecords(
	region, database, table string, now time.Time,
) ([]timestreamquerybackend.SourceRecord, error) {
	data, err := s.handler.BackendFor(region).TableData(database, table, now)
	if err != nil {
		return nil, err
	}

	out := make([]timestreamquerybackend.SourceRecord, 0, len(data.Records))
	for _, r := range data.Records {
		out = append(out, toSourceRecord(r))
	}

	return out, nil
}

func (s *timestreamRecordSource) ListDatabases(region string) []string {
	dbs := s.handler.BackendFor(region).ListDatabases()
	names := make([]string, 0, len(dbs))

	for _, d := range dbs {
		names = append(names, d.DatabaseName)
	}

	sort.Strings(names)

	return names
}

func (s *timestreamRecordSource) ListTables(region, database string) []string {
	tbls, err := s.handler.BackendFor(region).ListTables(database)
	if err != nil {
		return nil
	}

	names := make([]string, 0, len(tbls))
	for _, t := range tbls {
		names = append(names, t.TableName)
	}

	sort.Strings(names)

	return names
}

func (s *timestreamRecordSource) AppendRecords(
	region, database, table string, records []timestreamquerybackend.SourceRecord,
) error {
	recs := make([]timestreamwritebackend.Record, 0, len(records))
	for _, r := range records {
		recs = append(recs, fromSourceRecord(r))
	}

	_, err := s.handler.BackendFor(region).WriteRecords(database, table, recs)

	return err
}

func toSourceRecord(r timestreamwritebackend.Record) timestreamquerybackend.SourceRecord {
	out := timestreamquerybackend.SourceRecord{
		Time:             r.InternalTimestamp,
		MeasureName:      r.MeasureName,
		MeasureValue:     r.MeasureValue,
		MeasureValueType: r.MeasureValueType,
		Version:          r.Version,
	}

	for _, d := range r.Dimensions {
		out.Dimensions = append(out.Dimensions, timestreamquerybackend.SourceDimension{Name: d.Name, Value: d.Value})
	}

	if strings.EqualFold(r.MeasureValueType, tsMeasureTime) {
		out.MeasureValue = epochToTimestamp(r.MeasureValue, r.TimeUnit)
	}

	for _, m := range r.MeasureValues {
		v := m.Value
		if strings.EqualFold(m.Type, tsMeasureTime) {
			v = epochToTimestamp(v, r.TimeUnit)
		}

		out.Measures = append(out.Measures, timestreamquerybackend.SourceMeasure{Name: m.Name, Value: v, Type: m.Type})
	}

	return out
}

func epochToTimestamp(v, unit string) string {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return v
	}

	var t time.Time

	switch strings.ToUpper(unit) {
	case "SECONDS":
		t = time.Unix(n, 0)
	case "MICROSECONDS":
		t = time.UnixMicro(n)
	case tsNanosUnit:
		t = time.Unix(0, n)
	default:
		t = time.UnixMilli(n)
	}

	return t.UTC().Format(tsNanoLayout)
}

func timestampToNanos(v string) string {
	t, err := time.Parse("2006-01-02 15:04:05.999999999", v)
	if err != nil {
		return v
	}

	return strconv.FormatInt(t.UnixNano(), 10)
}

func fromSourceRecord(r timestreamquerybackend.SourceRecord) timestreamwritebackend.Record {
	out := timestreamwritebackend.Record{
		MeasureName:      r.MeasureName,
		MeasureValue:     r.MeasureValue,
		MeasureValueType: r.MeasureValueType,
		Time:             strconv.FormatInt(r.Time.UnixNano(), 10),
		TimeUnit:         tsNanosUnit,
		Version:          r.Version,
	}

	if strings.EqualFold(r.MeasureValueType, tsMeasureTime) {
		out.MeasureValue = timestampToNanos(r.MeasureValue)
	}

	for _, d := range r.Dimensions {
		out.Dimensions = append(out.Dimensions, timestreamwritebackend.Dimension{
			Name: d.Name, Value: d.Value, DimensionValueType: tsVarchar,
		})
	}

	for _, m := range r.Measures {
		v := m.Value
		if strings.EqualFold(m.Type, tsMeasureTime) {
			v = timestampToNanos(v)
		}

		out.MeasureValues = append(out.MeasureValues, timestreamwritebackend.MeasureValue{
			Name: m.Name, Value: v, Type: m.Type,
		})
	}

	return out
}
