package timestreamquery_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

var errNoTable = errors.New("no such table")

type memSource struct {
	tables map[string][]timestreamquery.SourceRecord
}

func (m *memSource) DatabaseExists(_, db string) bool { return db == "db" }

func (m *memSource) TableExists(_, db, table string) bool {
	_, ok := m.tables[db+"."+table]

	return ok
}

func (m *memSource) TableRecords(_, db, table string, _ time.Time) ([]timestreamquery.SourceRecord, error) {
	recs, ok := m.tables[db+"."+table]
	if !ok {
		return nil, errNoTable
	}

	return recs, nil
}

func (m *memSource) ListDatabases(string) []string { return []string{"db"} }

func (m *memSource) ListTables(string, string) []string { return []string{"t"} }

func (m *memSource) AppendRecords(_, db, table string, recs []timestreamquery.SourceRecord) error {
	m.tables[db+"."+table] = append(m.tables[db+"."+table], recs...)

	return nil
}

func queryRows(t *testing.T, q string) ([][]string, []string) {
	t.Helper()

	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) time.Time { return now.Add(-d) }
	dims := []timestreamquery.SourceDimension{{Name: "host", Value: "a"}}

	src := &memSource{tables: map[string][]timestreamquery.SourceRecord{"db.t": {
		{
			Time:             at(20 * time.Minute),
			MeasureName:      "cpu",
			MeasureValueType: "DOUBLE",
			MeasureValue:     "10",
			Dimensions:       dims,
		},
		{
			Time:             at(5 * time.Minute),
			MeasureName:      "cpu",
			MeasureValueType: "DOUBLE",
			MeasureValue:     "30",
			Dimensions:       dims,
		},
		{Time: at(3 * time.Hour), MeasureName: "cpu", MeasureValueType: "DOUBLE", MeasureValue: "1", Dimensions: dims},
		{Time: at(time.Minute), MeasureName: "n", MeasureValueType: "BIGINT", MeasureValue: "7"},
	}}}

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetClock(func() time.Time { return now })
	b.SetRecordSource(src)

	page, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: q})
	require.NoError(t, err)

	rows := make([][]string, 0, len(page.Rows))
	for _, r := range page.Rows {
		row := make([]string, 0, len(r.Data))

		for _, d := range r.Data {
			if d.NullValue != nil {
				row = append(row, "<null>")

				continue
			}

			row = append(row, *d.ScalarValue)
		}

		rows = append(rows, row)
	}

	types := make([]string, 0, len(page.Columns))
	for _, c := range page.Columns {
		types = append(types, c.Name+":"+c.Type.ScalarType)
	}

	return rows, types
}

func TestQuery_TimestreamSyntax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantRows  [][]string
		wantTypes []string
	}{
		{
			name:      "ago literal and double cast",
			query:     `SELECT measure_value::double FROM db.t WHERE measure_name = 'cpu' AND time > ago(1h) ORDER BY time`,
			wantRows:  [][]string{{"10.0"}, {"30.0"}},
			wantTypes: []string{"measure_value::double:DOUBLE"},
		},
		{
			name:      "ago interval form",
			query:     `SELECT count(*) FROM db.t WHERE time > ago(INTERVAL '10' MINUTE)`,
			wantRows:  [][]string{{"2"}},
			wantTypes: []string{"_col0:BIGINT"},
		},
		{
			name:      "bin five minutes",
			query:     `SELECT bin(time, 5m) AS b FROM db.t WHERE measure_name = 'cpu' AND time > ago(1h) ORDER BY b`,
			wantRows:  [][]string{{"2026-10-31 11:40:00.000000000"}, {"2026-10-31 11:55:00.000000000"}},
			wantTypes: []string{"b:TIMESTAMP"},
		},
		{
			name:      "bin ten days",
			query:     `SELECT bin(TIMESTAMP '2022-06-17 10:15:20', 30day)`,
			wantRows:  [][]string{{"2022-05-28 00:00:00.000000000"}},
			wantTypes: []string{"_col0:TIMESTAMP"},
		},
		{
			name:      "now arithmetic",
			query:     `SELECT now() - 2h`,
			wantRows:  [][]string{{"2026-10-31 10:00:00.000000000"}},
			wantTypes: []string{"_col0:TIMESTAMP"},
		},
		{
			name:      "from_milliseconds",
			query:     `SELECT from_milliseconds(1), to_milliseconds(from_milliseconds(1500))`,
			wantRows:  [][]string{{"1970-01-01 00:00:00.001000000", "1500"}},
			wantTypes: []string{"_col0:TIMESTAMP", "_col1:BIGINT"},
		},
		{
			name:      "doubles render java style",
			query:     `SELECT avg(measure_value::double) AS a FROM db.t WHERE measure_name = 'cpu' AND time > ago(1h)`,
			wantRows:  [][]string{{"20.0"}},
			wantTypes: []string{"a:DOUBLE"},
		},
		{
			name:      "null dimension",
			query:     `SELECT host FROM db.t WHERE measure_name = 'n'`,
			wantRows:  [][]string{{"<null>"}},
			wantTypes: []string{"host:VARCHAR"},
		},
		{
			name:      "string literals untouched",
			query:     `SELECT 'ago(1h) now()'`,
			wantRows:  [][]string{{"ago(1h) now()"}},
			wantTypes: []string{"_col0:VARCHAR"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, types := queryRows(t, tt.query)
			assert.Equal(t, tt.wantRows, rows)
			assert.Equal(t, tt.wantTypes, types)
		})
	}
}
