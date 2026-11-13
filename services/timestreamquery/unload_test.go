package timestreamquery_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json/v2"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

func unloadBackend(t *testing.T) (*timestreamquery.InMemoryBackend, *memObjects) {
	t.Helper()

	b := fnBackend(t)
	objs := newMemObjects("out")
	b.SetObjectStore(objs)

	return b, objs
}

type unloadResult struct {
	rows     string
	metadata string
	manifest string
}

func runUnload(t *testing.T, b *timestreamquery.InMemoryBackend, q string) unloadResult {
	t.Helper()

	page, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: q})
	require.NoError(t, err, q)
	require.Len(t, page.Rows, 1)
	require.Len(t, page.Columns, 3)

	assert.Equal(t, "rows", page.Columns[0].Name)
	assert.Equal(t, "BIGINT", page.Columns[0].Type.ScalarType)
	assert.Equal(t, "metadataFile", page.Columns[1].Name)
	assert.Equal(t, "VARCHAR", page.Columns[1].Type.ScalarType)
	assert.Equal(t, "manifestFile", page.Columns[2].Name)

	d := page.Rows[0].Data

	return unloadResult{rows: *d[0].ScalarValue, metadata: *d[1].ScalarValue, manifest: *d[2].ScalarValue}
}

func gunzip(t *testing.T, b []byte) string {
	t.Helper()

	r, err := gzip.NewReader(bytes.NewReader(b))
	require.NoError(t, err)

	out, err := io.ReadAll(r)
	require.NoError(t, err)

	return string(out)
}

func (m *memObjects) under(prefix string) []string {
	var out []string

	for _, k := range m.keys() {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}

	sort.Strings(out)

	return out
}

func TestUnload_CSV(t *testing.T) {
	t.Parallel()

	const sel = `SELECT host, measure_value::double AS cpu, time FROM db.t WHERE host = 'a' ORDER BY time`

	tests := []struct {
		name     string
		with     string
		wantExt  string
		wantBody string
	}{
		{
			"gzip default no header",
			``,
			".gz",
			"a,10.0,2026-10-31 11:40:00.000000000\na,20.0,2026-10-31 11:50:00.000000000\na,30.0,2026-10-31 11:55:00.000000000\n",
		},
		{
			"plain with header", `WITH (compression = 'NONE', include_header = 'true')`, ".csv",
			"host,cpu,time\na,10.0,2026-10-31 11:40:00.000000000\na,20.0,2026-10-31 11:50:00.000000000\n" +
				"a,30.0,2026-10-31 11:55:00.000000000\n",
		},
		{
			"pipe delimiter",
			`WITH (format = 'CSV', compression = 'NONE', field_delimiter = '|')`,
			".csv",
			"a|10.0|2026-10-31 11:40:00.000000000\na|20.0|2026-10-31 11:50:00.000000000\na|30.0|2026-10-31 11:55:00.000000000\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, objs := unloadBackend(t)
			res := runUnload(t, b, `UNLOAD (`+sel+`) TO 's3://out/exports/run1' `+tt.with)
			assert.Equal(t, "3", res.rows)

			data := objs.under("out/exports/run1/results/")
			require.Len(t, data, 1)
			assert.True(t, strings.HasSuffix(data[0], tt.wantExt), data[0])

			body := string(objs.objects[data[0]])
			if tt.wantExt == ".gz" {
				body = gunzip(t, objs.objects[data[0]])
			}

			assert.Equal(t, tt.wantBody, body)
			assert.Equal(t, "AES256", string(objs.enc[data[0]]))
		})
	}
}

func TestUnload_ManifestAndMetadata(t *testing.T) {
	t.Parallel()

	b, objs := unloadBackend(t)
	res := runUnload(
		t,
		b,
		`UNLOAD (SELECT host, measure_value::double AS cpu FROM db.t) TO 's3://out/m/' WITH (compression='NONE')`,
	)

	assert.Equal(t, "6", res.rows)
	assert.Regexp(t, `^s3://out/m/[0-9a-f-]+_[0-9a-f-]+_metadata\.json$`, res.metadata)
	assert.Regexp(t, `^s3://out/m/[0-9a-f-]+_[0-9a-f-]+_manifest\.json$`, res.manifest)

	var meta struct {
		Author     map[string]string `json:"Author"`
		ColumnInfo []struct {
			Type struct {
				ScalarType string `json:"ScalarType"`
			} `json:"Type"`
			Name string `json:"Name"`
		} `json:"ColumnInfo"`
	}

	require.NoError(t, json.Unmarshal(objs.objects[strings.TrimPrefix(res.metadata, "s3://")], &meta, jsonx.V1()))
	require.Len(t, meta.ColumnInfo, 2)
	assert.Equal(t, "host", meta.ColumnInfo[0].Name)
	assert.Equal(t, "VARCHAR", meta.ColumnInfo[0].Type.ScalarType)
	assert.Equal(t, "DOUBLE", meta.ColumnInfo[1].Type.ScalarType)
	assert.Equal(t, map[string]string{"Name": "Amazon Timestream", "MetadataFileVersion": "1.0"}, meta.Author)

	var man struct {
		ResultFiles []struct {
			URL          string `json:"url"`
			FileMetadata struct {
				ContentLength int64 `json:"content_length_in_bytes"`
				RowCount      int64 `json:"row_count"`
			} `json:"file_metadata"`
		} `json:"result_files"`
		Author        map[string]string `json:"author"`
		QueryMetadata struct {
			ResultFormat  string `json:"result_format"`
			ResultVersion string `json:"result_version"`
			ContentLength int64  `json:"content_length_in_bytes"`
			TotalRowCount int64  `json:"total_row_count"`
		} `json:"query_metadata"`
	}

	require.NoError(t, json.Unmarshal(objs.objects[strings.TrimPrefix(res.manifest, "s3://")], &man, jsonx.V1()))
	require.Len(t, man.ResultFiles, 1)
	assert.EqualValues(t, 6, man.ResultFiles[0].FileMetadata.RowCount)
	assert.EqualValues(t, len(objs.objects[strings.TrimPrefix(man.ResultFiles[0].URL, "s3://")]),
		man.ResultFiles[0].FileMetadata.ContentLength)
	assert.EqualValues(t, 6, man.QueryMetadata.TotalRowCount)
	assert.Equal(t, "CSV", man.QueryMetadata.ResultFormat)
	assert.Equal(t, "Amazon Timestream version 1.0.0", man.QueryMetadata.ResultVersion)
	assert.Equal(t, map[string]string{"name": "Amazon Timestream", "manifest_file_version": "1.0"}, man.Author)
}

func TestUnload_PartitionedBy(t *testing.T) {
	t.Parallel()

	b, objs := unloadBackend(t)
	res := runUnload(t, b, `UNLOAD (SELECT host, measure_value::double AS cpu FROM db.t) TO 's3://out/p' `+
		`WITH (partitioned_by = ARRAY['host'], compression = 'NONE', include_header = 'true')`)
	assert.Equal(t, "6", res.rows)

	a := objs.under("out/p/results/host=a/")
	bb := objs.under("out/p/results/host=b/")
	require.Len(t, a, 1)
	require.Len(t, bb, 1)

	assert.Equal(
		t,
		"cpu\n10.0\n20.0\n30.0\n",
		string(objs.objects[a[0]]),
		"partition columns are not written to the file",
	)
	assert.Equal(t, "cpu\n5.0\n10.0\n15.0\n", string(objs.objects[bb[0]]))
}

func TestUnload_CSVEscaping(t *testing.T) {
	t.Parallel()

	b, objs := unloadBackend(t)
	runUnload(t, b, `UNLOAD (SELECT 'Time,stream' AS a, 'Time"stream' AS q, 'Time\stream' AS e, 'plain' AS p) `+
		`TO 's3://out/esc' WITH (compression = 'NONE')`)

	data := objs.under("out/esc/results/")
	require.Len(t, data, 1)
	assert.Equal(t, `"Time,stream","Time\"stream","Time\\stream",plain`+"\n", string(objs.objects[data[0]]))
}

func TestUnload_ComplexTypesAsJSON(t *testing.T) {
	t.Parallel()

	b, objs := unloadBackend(t)
	runUnload(t, b, `UNLOAD (SELECT host, create_time_series(time, measure_value::double) AS ts FROM db.t `+
		`WHERE host = 'b' GROUP BY host) TO 's3://out/ts' WITH (compression = 'NONE')`)

	data := objs.under("out/ts/results/")
	require.Len(t, data, 1)
	assert.Equal(t,
		`b,"[{\"time\":\"2026-10-31 11:40:00.000000000Z\",\"value\":5.0},`+
			`{\"time\":\"2026-10-31 11:50:00.000000000Z\",\"value\":10.0},`+
			`{\"time\":\"2026-10-31 11:55:00.000000000Z\",\"value\":15.0}]"`+"\n",
		string(objs.objects[data[0]]))
}

func TestUnload_Parquet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		with string
	}{
		{"default compression", `WITH (format = 'PARQUET')`},
		{"uncompressed", `WITH (format = 'PARQUET', compression = 'NONE')`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, objs := unloadBackend(t)
			res := runUnload(
				t,
				b,
				`UNLOAD (SELECT host, measure_value::double AS cpu, time FROM db.t WHERE host = 'a' `+
					`ORDER BY time) TO 's3://out/pq' `+tt.with,
			)
			assert.Equal(t, "3", res.rows)

			data := objs.under("out/pq/results/")
			require.Len(t, data, 1)
			assert.True(
				t,
				strings.HasSuffix(data[0], ".parquet"),
				"parquet compresses per column, extension stays .parquet",
			)

			f, err := parquetio.Open(objs.objects[data[0]])
			require.NoError(t, err)
			assert.EqualValues(t, 3, f.NumRows())

			var got [][]any

			require.NoError(t, f.ForEach([]string{"host", "cpu", "time"}, func(v []any) error {
				got = append(got, append([]any(nil), v...))

				return nil
			}))

			require.Len(t, got, 3)
			assert.Equal(t, "a", got[0][0])
			assert.InDelta(t, 10.0, got[0][1], 0)
			assert.InDelta(t, 30.0, got[2][1], 0)
			assert.Contains(t, got[0][2], "2026-10-31 11:40:00")
		})
	}
}

func TestUnload_MaxFileSizeSplitsFiles(t *testing.T) {
	t.Parallel()

	b, objs := unloadBackend(t)
	// 16MB is the documented minimum; a wide literal makes each row large enough to split.
	wide := strings.Repeat("x", 6<<20)
	res := runUnload(t, b, `UNLOAD (SELECT '`+wide+`' AS w FROM db.t WHERE host = 'a') TO 's3://out/big' `+
		`WITH (compression = 'NONE', max_file_size = '16MB')`)

	assert.Equal(t, "3", res.rows)
	assert.Len(t, objs.under("out/big/results/"), 2)
}

func TestUnload_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"bad format", `UNLOAD (SELECT 1) TO 's3://out/x' WITH (format = 'JSON')`, "'format' must be CSV or PARQUET"},
		{"bad compression", `UNLOAD (SELECT 1) TO 's3://out/x' WITH (compression = 'ZIP')`, "GZIP or NONE"},
		{"unknown option", `UNLOAD (SELECT 1) TO 's3://out/x' WITH (colour = 'red')`, "unknown UNLOAD option 'colour'"},
		{"missing bucket", `UNLOAD (SELECT 1) TO 's3://nope/x'`, "'nope' does not exist"},
		{"missing to", `UNLOAD (SELECT 1)`, "TO"},
		{
			"bad partition",
			`UNLOAD (SELECT 1 AS a) TO 's3://out/x' WITH (partitioned_by = ARRAY['zzz'])`,
			"Partition column 'zzz'",
		},
		{
			"max file size low",
			`UNLOAD (SELECT 1) TO 's3://out/x' WITH (max_file_size = '1MB')`,
			"between 16MB and 78GB",
		},
		{"kms without key", `UNLOAD (SELECT 1) TO 's3://out/x' WITH (encryption = 'SSE_KMS')`, "kms_key"},
		{"bad delimiter", `UNLOAD (SELECT 1) TO 's3://out/x' WITH (field_delimiter = 'ab')`, "single ASCII character"},
		{"inner error", `UNLOAD (SELECT nope FROM db.t) TO 's3://out/x'`, "nope"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, objs := unloadBackend(t)

			_, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: tt.query})
			require.ErrorIs(t, err, timestreamquery.ErrValidation)
			assert.Contains(t, err.Error(), tt.want)
			assert.Empty(t, objs.keys(), "a failed UNLOAD writes nothing")
		})
	}
}

func TestUnload_KMSEncryption(t *testing.T) {
	t.Parallel()

	b, objs := unloadBackend(t)
	runUnload(t, b, `UNLOAD (SELECT 1 AS a) TO 's3://out/k' WITH (encryption = 'SSE_KMS', kms_key = 'alias/x')`)

	for _, k := range objs.keys() {
		assert.Equal(t, "aws:kms", string(objs.enc[k]), k)
	}
}
