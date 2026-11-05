package timestreamquery_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

// shapedSource is a memSource that also remembers record shapes after the records age out.
type shapedSource struct {
	*memSource
	shapes []timestreamquery.SourceRecord
}

func (s *shapedSource) TableShapes(_, _, _ string) []timestreamquery.SourceRecord { return s.shapes }

func TestSchemaSurvivesRetention(t *testing.T) {
	t.Parallel()

	shapes := []timestreamquery.SourceRecord{
		{
			MeasureName: "cpu", MeasureValueType: "DOUBLE",
			Dimensions: []timestreamquery.SourceDimension{{Name: "host"}, {Name: "region"}},
		},
		{
			MeasureName: "stats", MeasureValueType: "MULTI",
			Dimensions: []timestreamquery.SourceDimension{{Name: "host"}},
			Measures:   []timestreamquery.SourceMeasure{{Name: "mem", Type: "BIGINT"}},
		},
	}

	tests := []struct {
		name  string
		query string
		want  [][]string
	}{
		{
			"describe", `DESCRIBE db.t`,
			[][]string{
				{"host", "varchar", "DIMENSION"}, {"region", "varchar", "DIMENSION"},
				{"measure_name", "varchar", "MEASURE_NAME"}, {"time", "timestamp", "TIMESTAMP"},
				{"measure_value::double", "double", "MEASURE_VALUE"}, {"mem", "bigint", "MULTI"},
			},
		},
		{
			"show measures", `SHOW MEASURES FROM db.t`,
			[][]string{
				{"cpu", "double", `[{"data_type":"varchar","dimension_name":"host"},{"data_type":"varchar","dimension_name":"region"}]`},
				{"stats", "multi", `[{"data_type":"varchar","dimension_name":"host"}]`},
			},
		},
		{"select star has no rows", `SELECT count(*) FROM db.t`, [][]string{{"0"}}},
		{"columns still resolve", `SELECT host, region, mem FROM db.t`, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := &shapedSource{memSource: &memSource{tables: map[string][]timestreamquery.SourceRecord{"db.t": {}}}, shapes: shapes}
			b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
			b.SetClock(func() time.Time { return fnNow })
			b.SetRecordSource(src)

			page, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: tt.query})
			require.NoError(t, err)

			var got [][]string
			for _, r := range page.Rows {
				var row []string
				for _, d := range r.Data {
					row = append(row, renderDatum(d))
				}

				got = append(got, row)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSchemaWithoutShapesIsEmptyAfterAgeOut(t *testing.T) {
	t.Parallel()

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetRecordSource(&memSource{tables: map[string][]timestreamquery.SourceRecord{"db.t": {}}})

	_, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: `SELECT host FROM db.t`})
	require.ErrorIs(t, err, timestreamquery.ErrValidation, "a plain RecordSource keeps no history")
}
