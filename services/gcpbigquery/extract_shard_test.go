package gcpbigquery_test

import (
	"sort"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runExtract(
	t *testing.T, ds *bigquery.Dataset, ref *bigquery.GCSReference, noHeader bool,
) *bigquery.JobStatus {
	t.Helper()

	ex := ds.Table("src").ExtractorTo(ref)
	ex.DisableHeader = noHeader

	job, err := ex.Run(t.Context())
	require.NoError(t, err)

	status, err := job.Wait(t.Context())
	require.NoError(t, err)

	return status
}

func objectNames(env *sdkEnv) []string {
	names := make([]string, 0, len(env.objects.data))
	for k := range env.objects.data {
		names = append(names, k)
	}

	sort.Strings(names)

	return names
}

func TestExtractShardingAndUris(t *testing.T) {
	t.Parallel()

	rows := "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6"

	tests := []struct {
		name     string
		wantErr  string
		uris     []string
		wantObjs []string
		counts   []int64
		shard    int64
	}{
		{
			name: "size shards", uris: []string{"gs://b/o/p-*.csv"}, shard: 16,
			wantObjs: []string{"b/o/p-000000000000.csv", "b/o/p-000000000001.csv", "b/o/p-000000000002.csv"},
			counts:   []int64{3},
		},
		{
			name: "two uris", uris: []string{"gs://b/a-*.csv", "gs://b/c-*.csv"},
			wantObjs: []string{"b/a-000000000000.csv", "b/c-000000000000.csv"},
			counts:   []int64{1, 1},
		},
		{
			name: "single small file", uris: []string{"gs://b/one.csv"},
			wantObjs: []string{"b/one.csv"}, counts: []int64{1},
		},
		{
			name: "too large for one file", uris: []string{"gs://b/one.csv"}, shard: 16,
			wantErr: "Specify a uri including a *",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDK(t)
			ds := env.dataset(t, "ex")
			_, err := env.client.Query("CREATE TABLE ex.src AS " + rows).Run(t.Context())
			require.NoError(t, err)

			env.backend.SetExtractShardBytes(tt.shard)

			ref := bigquery.NewGCSReference(tt.uris...)
			status := runExtract(t, ds, ref, false)

			if tt.wantErr != "" {
				require.Error(t, status.Err())
				assert.Contains(t, status.Err().Error(), tt.wantErr)

				return
			}

			require.NoError(t, status.Err())
			assert.Equal(t, tt.wantObjs, objectNames(env))

			st, ok := status.Statistics.Details.(*bigquery.ExtractStatistics)
			require.True(t, ok)
			assert.Equal(t, tt.counts, st.DestinationURIFileCounts)

			var lines int

			for _, d := range env.objects.data {
				lines += strings.Count(string(d), "\n")
			}

			assert.Equal(t, 6+len(tt.wantObjs), lines)
		})
	}
}

func TestExtractCSVOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*bigquery.GCSReference)
		want   string
		noHead bool
	}{
		{"no header tab", func(r *bigquery.GCSReference) { r.FieldDelimiter = "\t" }, "1\ta\n2\tb\n", true},
		{"header pipe", func(r *bigquery.GCSReference) { r.FieldDelimiter = "|" }, "id|s\n1|a\n2|b\n", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDK(t)
			ds := env.dataset(t, "ex")
			_, err := env.client.Query("CREATE TABLE ex.src AS SELECT 1 AS id, 'a' AS s UNION ALL SELECT 2, 'b'").
				Run(t.Context())
			require.NoError(t, err)

			ref := bigquery.NewGCSReference("gs://b/x.csv")
			tt.mutate(ref)
			require.NoError(t, runExtract(t, ds, ref, tt.noHead).Err())
			assert.Equal(t, tt.want, string(env.objects.data["b/x.csv"]))
		})
	}
}

func TestAvroExtractRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		compression bigquery.Compression
	}{
		{"none", bigquery.None},
		{"deflate", bigquery.Deflate},
		{"snappy", bigquery.Snappy},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDK(t)
			ds := env.dataset(t, "av")
			ddl := "CREATE TABLE av.src AS SELECT 1 AS id, 'a' AS s, 1.5 AS f, TRUE AS b, DATE '2024-01-02' AS d, " +
				"TIMESTAMP '2024-01-02 03:04:05+00' AS ts, NUMERIC '1.25' AS n UNION ALL " +
				"SELECT 2, NULL, 2.5, FALSE, DATE '2024-02-03', TIMESTAMP '2024-02-03 00:00:01+00', NUMERIC '3'"
			_, err := env.client.Query(ddl).Run(t.Context())
			require.NoError(t, err)

			ref := bigquery.NewGCSReference("gs://b/p.avro")
			ref.DestinationFormat = bigquery.Avro
			ref.Compression = tt.compression
			require.NoError(t, runExtract(t, ds, ref, false).Err())

			in := bigquery.NewGCSReference("gs://b/p.avro")
			in.SourceFormat = bigquery.Avro
			job, err := ds.Table("back").LoaderFrom(in).Run(t.Context())
			require.NoError(t, err)
			status, err := job.Wait(t.Context())
			require.NoError(t, err)
			require.NoError(t, status.Err())

			assert.Equal(t,
				env.rows(t, "SELECT * FROM av.src ORDER BY id"), env.rows(t, "SELECT * FROM av.back ORDER BY id"))
		})
	}
}
