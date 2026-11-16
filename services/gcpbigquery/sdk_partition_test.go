package gcpbigquery_test

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionDDLMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ddl     string
		check   func(t *testing.T, md *bigquery.TableMetadata)
		wantErr string
	}{
		{
			name: "date column",
			ddl:  "CREATE TABLE p.t (d DATE, v INT64) PARTITION BY d CLUSTER BY v",
			check: func(t *testing.T, md *bigquery.TableMetadata) {
				t.Helper()
				require.NotNil(t, md.TimePartitioning)
				assert.Equal(t, "d", md.TimePartitioning.Field)
				assert.Equal(t, bigquery.DayPartitioningType, md.TimePartitioning.Type)
				require.NotNil(t, md.Clustering)
				assert.Equal(t, []string{"v"}, md.Clustering.Fields)
			},
		},
		{
			name: "timestamp trunc month",
			ddl:  "CREATE TABLE p.t (ts TIMESTAMP) PARTITION BY TIMESTAMP_TRUNC(ts, MONTH)",
			check: func(t *testing.T, md *bigquery.TableMetadata) {
				t.Helper()
				require.NotNil(t, md.TimePartitioning)
				assert.Equal(t, bigquery.MonthPartitioningType, md.TimePartitioning.Type)
				assert.Equal(t, "ts", md.TimePartitioning.Field)
			},
		},
		{
			name: "ingestion time",
			ddl:  "CREATE TABLE p.t (v INT64) PARTITION BY _PARTITIONDATE",
			check: func(t *testing.T, md *bigquery.TableMetadata) {
				t.Helper()
				require.NotNil(t, md.TimePartitioning)
				assert.Empty(t, md.TimePartitioning.Field)
			},
		},
		{
			name: "range bucket",
			ddl:  "CREATE TABLE p.t (n INT64) PARTITION BY RANGE_BUCKET(n, GENERATE_ARRAY(0, 100, 10))",
			check: func(t *testing.T, md *bigquery.TableMetadata) {
				t.Helper()
				require.NotNil(t, md.RangePartitioning)
				assert.Equal(t, "n", md.RangePartitioning.Field)
				assert.EqualValues(t, 100, md.RangePartitioning.Range.End)
				assert.EqualValues(t, 10, md.RangePartitioning.Range.Interval)
			},
		},
		{
			name: "string column", ddl: "CREATE TABLE p.t (s STRING) PARTITION BY s",
			wantErr: "PARTITION BY expression must be",
		},
		{
			name: "unknown cluster", ddl: "CREATE TABLE p.t (d DATE) PARTITION BY d CLUSTER BY nope",
			wantErr: "Unrecognized name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDK(t)
			env.dataset(t, "p")

			if tt.wantErr != "" {
				job, err := env.client.Query(tt.ddl).Run(t.Context())
				if err == nil {
					_, err = job.Wait(t.Context())
				}

				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			env.exec(t, tt.ddl)

			md, err := env.client.Dataset("p").Table("t").Metadata(t.Context())
			require.NoError(t, err)
			tt.check(t, md)
		})
	}
}

func TestRequirePartitionFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{name: "no filter", query: "SELECT v FROM p.t", wantErr: true},
		{name: "filter on other column", query: "SELECT v FROM p.t WHERE v > 1", wantErr: true},
		{name: "partition filter", query: "SELECT v FROM p.t WHERE d >= DATE '2026-01-01'"},
		{name: "partition filter and more", query: "SELECT v FROM p.t WHERE v > 0 AND d = DATE '2026-01-02'"},
		{name: "or does not eliminate", query: "SELECT v FROM p.t WHERE d = DATE '2026-01-02' OR v > 1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDK(t)
			env.dataset(t, "p")
			env.exec(t, "CREATE TABLE p.t (d DATE, v INT64) PARTITION BY d OPTIONS (require_partition_filter = TRUE)")
			env.exec(t, "INSERT INTO p.t VALUES (DATE '2026-01-02', 5)")

			job, err := env.client.Query(tt.query).Run(t.Context())
			if err == nil {
				_, err = job.Wait(t.Context())
			}

			if tt.wantErr {
				require.ErrorContains(t, err, "without a filter over column(s) 'd'")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestTableSample(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	env.dataset(t, "s")
	env.exec(t, "CREATE TABLE s.t AS SELECT x FROM UNNEST(GENERATE_ARRAY(1, 200)) AS x")

	tests := []struct {
		name    string
		query   string
		wantErr string
		params  []bigquery.QueryParameter
		min     int64
		max     int64
	}{
		{name: "none", query: "SELECT COUNT(*) FROM s.t TABLESAMPLE SYSTEM (0 PERCENT)", min: 0, max: 0},
		{name: "all", query: "SELECT COUNT(*) FROM s.t TABLESAMPLE SYSTEM (100 PERCENT)", min: 200, max: 200},
		{name: "some", query: "SELECT COUNT(*) FROM s.t AS a TABLESAMPLE SYSTEM (50 PERCENT)", min: 1, max: 199},
		{
			name: "parameter", query: "SELECT COUNT(*) FROM s.t TABLESAMPLE SYSTEM (@p PERCENT)",
			params: []bigquery.QueryParameter{{Name: "p", Value: 100}}, min: 200, max: 200,
		},
		{
			name: "out of range", query: "SELECT COUNT(*) FROM s.t TABLESAMPLE SYSTEM (101 PERCENT)",
			wantErr: "between 0 and 100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.wantErr != "" {
				job, err := env.client.Query(tt.query).Run(t.Context())
				if err == nil {
					_, err = job.Wait(t.Context())
				}

				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			rows := env.rows(t, tt.query, tt.params...)
			require.Len(t, rows, 1)

			got, _ := rows[0][0].(int64)
			assert.GreaterOrEqual(t, got, tt.min)
			assert.LessOrEqual(t, got, tt.max)
		})
	}
}
