package gcpbigquery_test

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"

	"github.com/blackbirdworks/gopherstack/pkgs/orcio"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

func parquetBytes(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	require.NoError(t, parquetio.Write(&buf,
		[]parquetio.Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"}},
		[][]any{{int64(1), "a"}, {int64(2), "b"}}, ""))

	return buf.Bytes()
}

func orcBytes(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	require.NoError(t, orcio.Write(&buf,
		[]orcio.Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"}},
		[][]any{{int64(1), "a"}, {int64(2), "b"}}, ""))

	return buf.Bytes()
}

func avroBytes(t *testing.T) []byte {
	t.Helper()

	const schema = `{"type":"record","name":"r","fields":[{"name":"id","type":"long"},
		{"name":"name","type":["null","string"]},
		{"name":"day","type":{"type":"int","logicalType":"date"}},
		{"name":"at","type":{"type":"long","logicalType":"timestamp-micros"}},
		{"name":"tags","type":{"type":"array","items":"string"}}]}`

	var buf bytes.Buffer

	enc, err := ocf.NewEncoder(schema, &buf)
	require.NoError(t, err)

	day := time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)
	at := time.Date(2024, 3, 5, 10, 11, 12, 0, time.UTC)

	require.NoError(t, enc.Encode(map[string]any{
		"id": int64(1), "name": "a", "day": day, "at": at, "tags": []string{"x", "y"},
	}))
	require.NoError(t, enc.Encode(map[string]any{
		"id": int64(2), "name": nil, "day": day, "at": at, "tags": []string{},
	}))
	require.NoError(t, enc.Close())

	return buf.Bytes()
}

func TestExternalTableFormats(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	env.dataset(t, "ext")

	env.objects.data["bkt/csv/a.csv"] = []byte("id,name\n1,a\n2,b\n")
	env.objects.data["bkt/csv/b.csv"] = []byte("id,name\n3,c\n")
	env.objects.data["bkt/json/a.json"] = []byte(`{"id":1,"name":"a"}` + "\n" + `{"id":2,"name":"b"}` + "\n")
	env.objects.data["bkt/pq/a.parquet"] = parquetBytes(t)
	env.objects.data["bkt/orc/a.orc"] = orcBytes(t)
	env.objects.data["bkt/avro/a.avro"] = avroBytes(t)

	tests := []struct {
		name string
		ddl  string
		sel  string
		want [][]bigquery.Value
	}{
		{
			"csv", "CREATE EXTERNAL TABLE ext.t_csv (id INT64, name STRING) OPTIONS (format='CSV', " +
				"uris=['gs://bkt/csv/*.csv'], skip_leading_rows=1)",
			"SELECT id, name FROM ext.t_csv ORDER BY id",
			[][]bigquery.Value{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}},
		},
		{
			"json",
			"CREATE EXTERNAL TABLE ext.t_json (id INT64, name STRING) OPTIONS (format='NEWLINE_DELIMITED_JSON', " +
				"uris=['gs://bkt/json/a.json'])",
			"SELECT id, name FROM ext.t_json ORDER BY id",
			[][]bigquery.Value{{int64(1), "a"}, {int64(2), "b"}},
		},
		{
			"parquet",
			"CREATE EXTERNAL TABLE ext.t_pq OPTIONS (format='PARQUET', uris=['gs://bkt/pq/*'])",
			"SELECT id, name FROM ext.t_pq ORDER BY id",
			[][]bigquery.Value{{int64(1), "a"}, {int64(2), "b"}},
		},
		{
			"orc", "CREATE EXTERNAL TABLE ext.t_orc OPTIONS (format='ORC', uris=['gs://bkt/orc/a.orc'])",
			"SELECT id, name FROM ext.t_orc ORDER BY id",
			[][]bigquery.Value{{int64(1), "a"}, {int64(2), "b"}},
		},
		{
			"avro", "CREATE EXTERNAL TABLE ext.t_avro OPTIONS (format='AVRO', uris=['gs://bkt/avro/a.avro'])",
			"SELECT id, name, CAST(day AS STRING), ARRAY_LENGTH(tags) FROM ext.t_avro ORDER BY id",
			[][]bigquery.Value{{int64(1), "a", "2024-03-05", int64(2)}, {int64(2), nil, "2024-03-05", int64(0)}},
		},
		{
			"filename",
			"CREATE EXTERNAL TABLE ext.t_fn (id INT64, name STRING) OPTIONS (format='CSV', " +
				"uris=['gs://bkt/csv/*.csv'], skip_leading_rows=1)",
			"SELECT _FILE_NAME, id FROM ext.t_fn ORDER BY id",
			[][]bigquery.Value{
				{"gs://bkt/csv/a.csv", int64(1)}, {"gs://bkt/csv/a.csv", int64(2)}, {"gs://bkt/csv/b.csv", int64(3)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env.exec(t, tt.ddl)
			assert.Equal(t, tt.want, env.rows(t, tt.sel))
		})
	}
}

func TestExternalTableMetadataAndGuards(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	env.dataset(t, "ext")
	env.objects.data["bkt/a.csv"] = []byte("id,name\n1,a\n")

	ctx := t.Context()
	tbl := env.client.Dataset("ext").Table("t")
	require.NoError(t, tbl.Create(ctx, &bigquery.TableMetadata{
		ExternalDataConfig: &bigquery.ExternalDataConfig{
			SourceFormat: bigquery.CSV, SourceURIs: []string{"gs://bkt/*.csv"}, AutoDetect: true,
			Options: &bigquery.CSVOptions{SkipLeadingRows: 1},
		},
	}))

	md, err := tbl.Metadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, bigquery.ExternalTable, md.Type)
	require.Len(t, md.Schema, 2)
	assert.Equal(t, "id", md.Schema[0].Name)
	assert.Equal(t, bigquery.IntegerFieldType, md.Schema[0].Type)
	require.NotNil(t, md.ExternalDataConfig)

	assert.Equal(t, [][]bigquery.Value{{int64(1), "a"}}, env.rows(t, "SELECT id, name FROM ext.t"))

	env.objects.data["bkt/b.csv"] = []byte("id,name\n2,b\n")
	assert.Len(t, env.rows(t, "SELECT id FROM ext.t"), 2)

	var gerr *googleapi.Error

	type extRow struct {
		ID int64 `bigquery:"id"`
	}

	require.Error(t, tbl.Inserter().Put(ctx, &extRow{ID: 1}))

	for _, sql := range []string{"INSERT INTO ext.t VALUES (9, 'z')", "DELETE FROM ext.t WHERE TRUE"} {
		_, qerr := env.client.Query(sql).Read(ctx)
		require.ErrorAs(t, qerr, &gerr)
		assert.Equal(t, http.StatusBadRequest, gerr.Code)
		assert.Contains(t, gerr.Message, "external table")
	}

	it := tbl.Read(ctx)

	var row []bigquery.Value

	require.ErrorAs(t, it.Next(&row), &gerr)

	delete(env.objects.data, "bkt/a.csv")
	delete(env.objects.data, "bkt/b.csv")

	_, err = env.client.Query("SELECT id FROM ext.t").Read(ctx)
	require.ErrorContains(t, err, "no matching objects")

	bad := env.client.Dataset("ext").Table("bad")
	require.ErrorAs(t, bad.Create(ctx, &bigquery.TableMetadata{
		ExternalDataConfig: &bigquery.ExternalDataConfig{
			SourceFormat: bigquery.GoogleSheets, SourceURIs: []string{"https://docs.google.com/x"},
		},
	}), &gerr)
	assert.Equal(t, http.StatusBadRequest, gerr.Code)
}

func TestExternalTableHivePartitions(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	env.dataset(t, "ext")
	env.objects.data["bkt/sales/dt=2024-01-01/region=eu/f.csv"] = []byte("1,a\n2,b\n")
	env.objects.data["bkt/sales/dt=2024-01-02/region=us/f.csv"] = []byte("3,c\n")

	env.exec(t, "CREATE EXTERNAL TABLE ext.auto (id INT64, name STRING) WITH PARTITION COLUMNS OPTIONS ("+
		"format='CSV', uris=['gs://bkt/sales/*'], hive_partition_uri_prefix='gs://bkt/sales')")
	env.exec(t, "CREATE EXTERNAL TABLE ext.typed (id INT64, name STRING) WITH PARTITION COLUMNS (dt DATE, "+
		"region STRING) OPTIONS (format='CSV', uris=['gs://bkt/sales/*'], hive_partition_uri_prefix='gs://bkt/sales', "+
		"require_hive_partition_filter=true)")

	md, err := env.client.Dataset("ext").Table("auto").Metadata(t.Context())
	require.NoError(t, err)
	require.Len(t, md.Schema, 4)
	assert.Equal(t, bigquery.DateFieldType, md.Schema[2].Type)
	assert.Equal(t, bigquery.StringFieldType, md.Schema[3].Type)

	assert.Equal(t, [][]bigquery.Value{{int64(1), "eu"}, {int64(2), "eu"}},
		env.rows(t, "SELECT id, region FROM ext.auto WHERE dt = '2024-01-01' ORDER BY id"))
	assert.Equal(t, [][]bigquery.Value{{int64(3)}},
		env.rows(t, "SELECT id FROM ext.typed WHERE region = 'us'"))

	_, err = env.client.Query("SELECT id FROM ext.typed").Read(t.Context())
	require.ErrorContains(t, err, "without a filter over column(s)")

	_, err = env.client.Query("SELECT id FROM ext.typed WHERE region = 'us' OR id = 1").Read(t.Context())
	require.ErrorContains(t, err, "without a filter over column(s)")
}

func TestExternalTablePersistence(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	env.dataset(t, "ext")
	env.objects.data["bkt/a.csv"] = []byte("1,a\n")
	env.exec(t, "CREATE EXTERNAL TABLE ext.t (id INT64, name STRING) OPTIONS (format='CSV', uris=['gs://bkt/a.csv'])")

	other := newSDK(t)
	other.objects.data["bkt/a.csv"] = []byte("1,a\n")
	require.NoError(t, other.backend.Restore(t.Context(), env.backend.Snapshot(t.Context())))
	assert.Equal(t, [][]bigquery.Value{{int64(1), "a"}}, other.rows(t, "SELECT id, name FROM ext.t"))
}

func TestLoadAvroAndORC(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ds := env.dataset(t, "ld")
	env.objects.data["bkt/a.avro"] = avroBytes(t)
	env.objects.data["bkt/a.orc"] = orcBytes(t)

	tests := []struct {
		name   string
		uri    string
		format bigquery.DataFormat
		sel    string
		want   [][]bigquery.Value
	}{
		{"avro", "gs://bkt/a.avro", bigquery.Avro, "SELECT id, name, CAST(at AS STRING) FROM ld.avro ORDER BY id",
			[][]bigquery.Value{{int64(1), "a", "2024-03-05 10:11:12+00"}, {int64(2), nil, "2024-03-05 10:11:12+00"}}},
		{"orc", "gs://bkt/a.orc", bigquery.ORC, "SELECT id, name FROM ld.orc ORDER BY id",
			[][]bigquery.Value{{int64(1), "a"}, {int64(2), "b"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ref := bigquery.NewGCSReference(tt.uri)
			ref.SourceFormat = tt.format

			job, err := ds.Table(tt.name).LoaderFrom(ref).Run(t.Context())
			require.NoError(t, err)

			status, err := job.Wait(t.Context())
			require.NoError(t, err)
			require.NoError(t, status.Err())
			assert.Equal(t, tt.want, env.rows(t, tt.sel))
		})
	}
}
