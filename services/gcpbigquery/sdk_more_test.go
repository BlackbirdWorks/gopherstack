package gcpbigquery_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestViewsAndDDL(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "ddl")

	for _, sql := range []string{
		"CREATE TABLE ddl.base (id INT64 NOT NULL, tags ARRAY<STRING>, meta STRUCT<a INT64, b STRING>) " +
			"OPTIONS(description='base table', labels=[('k','v')])",
		"INSERT INTO ddl.base VALUES (1, ['x','y'], STRUCT(1, 'one')), (2, [], STRUCT(2, 'two'))",
		"CREATE VIEW ddl.v AS SELECT id, meta.b AS b FROM ddl.base WHERE id > 1",
		"CREATE TABLE ddl.copy AS SELECT * FROM ddl.base",
		"ALTER TABLE ddl.copy ADD COLUMN extra STRING",
	} {
		job, err := env.client.Query(sql).Run(ctx)
		require.NoError(t, err, sql)

		status, err := job.Wait(ctx)
		require.NoError(t, err, sql)
		require.NoError(t, status.Err(), sql)
	}

	md, err := ds.Table("base").Metadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, "base table", md.Description)
	assert.Equal(t, map[string]string{"k": "v"}, md.Labels)
	assert.Equal(t, uint64(2), md.NumRows)
	require.Len(t, md.Schema, 3)
	assert.True(t, md.Schema[0].Required)
	assert.True(t, md.Schema[1].Repeated)
	assert.Equal(t, bigquery.RecordFieldType, md.Schema[2].Type)

	vm, err := ds.Table("v").Metadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, bigquery.ViewTable, vm.Type)
	assert.Contains(t, vm.ViewQuery, "SELECT id")

	assert.Equal(t, [][]bigquery.Value{{int64(2), "two"}}, env.rows(t, "SELECT * FROM ddl.v"))

	cm, err := ds.Table("copy").Metadata(ctx)
	require.NoError(t, err)
	require.Len(t, cm.Schema, 4)
	assert.Equal(t, uint64(2), cm.NumRows)

	_, err = env.client.Query("DROP TABLE ddl.copy").Run(ctx)
	require.NoError(t, err)

	_, err = ds.Table("copy").Metadata(ctx)

	var gerr *googleapi.Error

	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusNotFound, gerr.Code)

	_, err = env.client.Query("CREATE TABLE ddl.base (a INT64)").Run(ctx)
	require.NoError(t, err)

	it := ds.Table("v").Read(ctx)

	var row []bigquery.Value

	err = it.Next(&row)
	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusBadRequest, gerr.Code)
}

func TestInsertErrors(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "ins")
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType, Required: true}, {Name: "n", Type: bigquery.StringFieldType},
	}
	tbl := ds.Table("t")
	require.NoError(t, tbl.Create(ctx, &bigquery.TableMetadata{Schema: schema}))

	tests := []struct {
		name    string
		savers  []*bigquery.ValuesSaver
		skip    bool
		wantErr bool
		rows    uint64
	}{
		{"missing required", []*bigquery.ValuesSaver{
			{Schema: schema, Row: []bigquery.Value{int64(1), "a"}}, {Schema: schema, Row: []bigquery.Value{nil, "b"}},
		}, false, true, 0},
		{"skip invalid", []*bigquery.ValuesSaver{
			{Schema: schema, Row: []bigquery.Value{int64(2), "a"}}, {Schema: schema, Row: []bigquery.Value{nil, "b"}},
		}, true, true, 1},
	}

	for _, tt := range tests {
		ins := tbl.Inserter()
		ins.SkipInvalidRows = tt.skip

		err := ins.Put(ctx, tt.savers)

		var multi bigquery.PutMultiError

		require.Equal(t, tt.wantErr, errors.As(err, &multi), tt.name)

		md, merr := tbl.Metadata(ctx)
		require.NoError(t, merr)
		assert.Equal(t, tt.rows, md.NumRows, tt.name)
	}
}

func TestQueryDestinationAndCopy(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "dest")

	q := env.client.Query("SELECT x, x * x AS sq FROM UNNEST(GENERATE_ARRAY(1, 4)) AS x")
	q.Dst = ds.Table("squares")
	q.WriteDisposition = bigquery.WriteTruncate

	job, err := q.Run(ctx)
	require.NoError(t, err)

	status, err := job.Wait(ctx)
	require.NoError(t, err)
	require.NoError(t, status.Err())

	md, err := ds.Table("squares").Metadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), md.NumRows)

	cp := ds.Table("squares2").CopierFrom(ds.Table("squares"))
	cj, err := cp.Run(ctx)
	require.NoError(t, err)

	status, err = cj.Wait(ctx)
	require.NoError(t, err)
	require.NoError(t, status.Err())

	assert.Equal(t, [][]bigquery.Value{{int64(30)}}, env.rows(t, "SELECT SUM(sq) FROM dest.squares2"))

	again := ds.Table("squares2").CopierFrom(ds.Table("squares"))
	again.WriteDisposition = bigquery.WriteEmpty
	cj, err = again.Run(ctx)
	require.NoError(t, err)

	status, err = cj.Wait(ctx)
	require.NoError(t, err)
	require.Error(t, status.Err())
	assert.Contains(t, status.Err().Error(), "Already Exists")
}

func TestLoadFromReader(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "up")

	src := bigquery.NewReaderSource(strings.NewReader("a,b\n1,x\n2,y\n"))
	src.SkipLeadingRows = 1
	src.AutoDetect = true

	job, err := ds.Table("t").LoaderFrom(src).Run(ctx)
	require.NoError(t, err)

	status, err := job.Wait(ctx)
	require.NoError(t, err)
	require.NoError(t, status.Err())

	md, err := ds.Table("t").Metadata(ctx)
	require.NoError(t, err)
	require.Len(t, md.Schema, 2)
	assert.Equal(t, bigquery.IntegerFieldType, md.Schema[0].Type)
	assert.Equal(t, bigquery.StringFieldType, md.Schema[1].Type)
	assert.Equal(t, [][]bigquery.Value{{int64(1), "x"}, {int64(2), "y"}}, env.rows(t, "SELECT * FROM up.t ORDER BY a"))
}

func TestParquetRoundTrip(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "pq")

	ddl := "CREATE TABLE pq.src AS SELECT 1 AS id, 'a' AS s, 1.5 AS f, TRUE AS b, DATE '2024-01-02' AS d " +
		"UNION ALL SELECT 2, NULL, 2.5, FALSE, DATE '2024-02-03'"
	_, err := env.client.Query(ddl).Run(ctx)
	require.NoError(t, err)

	ext := bigquery.NewGCSReference("gs://bkt/p.parquet")
	ext.DestinationFormat = bigquery.Parquet

	job, err := ds.Table("src").ExtractorTo(ext).Run(ctx)
	require.NoError(t, err)

	status, err := job.Wait(ctx)
	require.NoError(t, err)
	require.NoError(t, status.Err())

	ref := bigquery.NewGCSReference("gs://bkt/p.parquet")
	ref.SourceFormat = bigquery.Parquet

	job, err = ds.Table("back").LoaderFrom(ref).Run(ctx)
	require.NoError(t, err)

	status, err = job.Wait(ctx)
	require.NoError(t, err)
	require.NoError(t, status.Err())

	assert.Equal(t, env.rows(t, "SELECT * FROM pq.src ORDER BY id"), env.rows(t, "SELECT * FROM pq.back ORDER BY id"))
}

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "snap")

	_, err := env.client.Query("CREATE TABLE snap.t AS SELECT 1 AS a, [1,2] AS arr, STRUCT('x' AS s) AS st").Run(ctx)
	require.NoError(t, err)

	data := env.backend.Snapshot(ctx)
	require.NotEmpty(t, data)

	other := newSDK(t)
	require.NoError(t, other.backend.Restore(ctx, data))

	assert.Equal(t, env.rows(t, "SELECT * FROM snap.t"), other.rows(t, "SELECT * FROM snap.t"))

	_, err = ds.Metadata(ctx)
	require.NoError(t, err)

	other.backend.Reset()

	_, err = other.client.Dataset("snap").Metadata(ctx)
	require.Error(t, err)
}

func TestRoutingAndErrors(t *testing.T) {
	t.Parallel()

	env := newSDK(t)

	tests := []struct {
		name   string
		method string
		path   string
		status int
	}{
		{"unknown route", http.MethodGet, "/bigquery/v2/nope", http.StatusNotFound},
		{"prefixed dataset list", http.MethodGet, "/bigquery/v2/projects/p/datasets", http.StatusOK},
		{"missing dataset", http.MethodGet, "/projects/p/datasets/none", http.StatusNotFound},
		{"missing job", http.MethodGet, "/projects/p/jobs/none", http.StatusNotFound},
		{"bad method", http.MethodPatch, "/projects/p/queries", http.StatusNotFound},
		{"projects", http.MethodGet, "/projects", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequestWithContext(t.Context(), tt.method, env.url+tt.path, nil)
			require.NoError(t, err)

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, tt.status, resp.StatusCode)
		})
	}
}
