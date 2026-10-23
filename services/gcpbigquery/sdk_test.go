package gcpbigquery_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

type person struct {
	Name   string
	Tags   []string
	Born   civilDate
	Age    int64
	Score  float64
	Active bool
}

type civilDate = bigquery.NullDate

func TestDatasetTableLifecycle(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()

	ds := env.client.Dataset("lifecycle")
	require.NoError(t, ds.Create(ctx, &bigquery.DatasetMetadata{
		Name: "Lifecycle", Description: "d1", Location: "EU", Labels: map[string]string{"env": "test"},
		DefaultTableExpiration: time.Hour,
	}))

	md, err := ds.Metadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, "EU", md.Location)
	assert.Equal(t, "Lifecycle", md.Name)
	assert.Equal(t, map[string]string{"env": "test"}, md.Labels)
	assert.Equal(t, time.Hour, md.DefaultTableExpiration)
	assert.NotEmpty(t, md.ETag)
	assert.Equal(t, sdkProject+":lifecycle", md.FullID)

	upd, err := ds.Update(ctx, bigquery.DatasetMetadataToUpdate{Description: "d2"}, md.ETag)
	require.NoError(t, err)
	assert.Equal(t, "d2", upd.Description)
	assert.Equal(t, map[string]string{"env": "test"}, upd.Labels)

	err = ds.Create(ctx, nil)
	var gerr *googleapi.Error
	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusConflict, gerr.Code)

	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
		{Name: "ts", Type: bigquery.TimestampFieldType},
		{Name: "payload", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
			{Name: "k", Type: bigquery.StringFieldType}, {Name: "vals", Type: bigquery.FloatFieldType, Repeated: true},
		}},
	}
	tbl := ds.Table("events")
	require.NoError(t, tbl.Create(ctx, &bigquery.TableMetadata{
		Name: "Events", Schema: schema, Labels: map[string]string{"a": "b"},
		TimePartitioning: &bigquery.TimePartitioning{Type: bigquery.DayPartitioningType, Field: "ts"},
		Clustering:       &bigquery.Clustering{Fields: []string{"id"}},
	}))

	tm, err := tbl.Metadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, schema, tm.Schema)
	assert.Equal(t, bigquery.DayPartitioningType, tm.TimePartitioning.Type)
	assert.Equal(t, "ts", tm.TimePartitioning.Field)
	assert.Equal(t, []string{"id"}, tm.Clustering.Fields)
	assert.Equal(t, bigquery.RegularTable, tm.Type)
	assert.Equal(t, uint64(0), tm.NumRows)

	tu, err := tbl.Update(ctx, bigquery.TableMetadataToUpdate{Description: "events table"}, "")
	require.NoError(t, err)
	assert.Equal(t, "events table", tu.Description)
	assert.Equal(t, bigquery.DayPartitioningType, tu.TimePartitioning.Type)

	var names []string

	it := ds.Tables(ctx)

	for {
		tb, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		names = append(names, tb.TableID)
	}

	assert.Equal(t, []string{"events"}, names)

	err = ds.Delete(ctx)
	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusBadRequest, gerr.Code)

	require.NoError(t, tbl.Delete(ctx))
	_, err = tbl.Metadata(ctx)
	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusNotFound, gerr.Code)

	require.NoError(t, ds.DeleteWithContents(ctx))

	var listed []string

	dit := env.client.Datasets(ctx)

	for {
		d, nerr := dit.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		listed = append(listed, d.DatasetID)
	}

	assert.Empty(t, listed)
}

func TestStreamingInsertAndQuery(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "stream")

	schema, err := bigquery.InferSchema(person{})
	require.NoError(t, err)

	tbl := ds.Table("people")
	require.NoError(t, tbl.Create(ctx, &bigquery.TableMetadata{Schema: schema}))

	ins := tbl.Inserter()
	savers := []*bigquery.StructSaver{
		{
			Schema:   schema,
			InsertID: "a",
			Struct:   person{Name: "ann", Age: 30, Score: 1.5, Active: true, Tags: []string{"x", "y"}},
		},
		{Schema: schema, InsertID: "b", Struct: person{Name: "bob", Age: 41, Score: 2.5, Tags: []string{"z"}}},
		{Schema: schema, InsertID: "a", Struct: person{Name: "dup", Age: 1}},
	}
	require.NoError(t, ins.Put(ctx, savers))

	var got []*person

	it := tbl.Read(ctx)

	for {
		var p person

		nerr := it.Next(&p)
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		got = append(got, &p)
	}

	require.Len(t, got, 2)
	assert.Equal(t, "ann", got[0].Name)
	assert.Equal(t, []string{"x", "y"}, got[0].Tags)

	tests := []struct {
		name string
		sql  string
		want [][]bigquery.Value
	}{
		{
			"agg",
			"SELECT COUNT(*), SUM(age), AVG(score) FROM stream.people",
			[][]bigquery.Value{{int64(2), int64(71), 2.0}},
		},
		{"filter", "SELECT name FROM `" + sdkProject + ".stream.people` WHERE active", [][]bigquery.Value{{"ann"}}},
		{
			"unnest",
			"SELECT t FROM stream.people, UNNEST(tags) AS t ORDER BY t",
			[][]bigquery.Value{{"x"}, {"y"}, {"z"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q := env.client.Query(tt.sql)
			q.DefaultDatasetID = "stream"
			q.DefaultProjectID = sdkProject

			rit, rerr := q.Read(ctx)
			require.NoError(t, rerr)

			var rows [][]bigquery.Value

			for {
				var row []bigquery.Value

				if nerr := rit.Next(&row); nerr != nil {
					break
				}

				rows = append(rows, row)
			}

			assert.Equal(t, tt.want, rows)
		})
	}
}

func TestParameterizedQuery(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ds := env.dataset(t, "params")
	tbl := ds.Table("t")

	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType}, {Name: "name", Type: bigquery.StringFieldType},
		{Name: "at", Type: bigquery.TimestampFieldType},
	}
	require.NoError(t, tbl.Create(t.Context(), &bigquery.TableMetadata{Schema: schema}))

	at := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)
	require.NoError(t, tbl.Inserter().Put(t.Context(), []*bigquery.ValuesSaver{
		{Schema: schema, Row: []bigquery.Value{int64(1), "one", at}},
		{Schema: schema, Row: []bigquery.Value{int64(2), "two", at.Add(time.Hour)}},
		{Schema: schema, Row: []bigquery.Value{int64(3), "three", at.Add(2 * time.Hour)}},
	}))

	tests := []struct {
		name   string
		sql    string
		params []bigquery.QueryParameter
		want   []bigquery.Value
	}{
		{
			"named int",
			"SELECT name FROM params.t WHERE id = @id",
			[]bigquery.QueryParameter{{Name: "id", Value: int64(2)}},
			[]bigquery.Value{"two"},
		},
		{
			"named string",
			"SELECT id FROM params.t WHERE name = @n",
			[]bigquery.QueryParameter{{Name: "n", Value: "three"}},
			[]bigquery.Value{int64(3)},
		},
		{
			"positional",
			"SELECT id FROM params.t WHERE id > ? AND name != ? ORDER BY id LIMIT 1",
			[]bigquery.QueryParameter{{Value: int64(1)}, {Value: "two"}},
			[]bigquery.Value{int64(3)},
		},
		{
			"timestamp",
			"SELECT id FROM params.t WHERE at >= @at ORDER BY id LIMIT 1",
			[]bigquery.QueryParameter{{Name: "at", Value: at.Add(30 * time.Minute)}},
			[]bigquery.Value{int64(2)},
		},
		{
			"array",
			"SELECT COUNT(*) FROM params.t WHERE id IN UNNEST(@ids)",
			[]bigquery.QueryParameter{{Name: "ids", Value: []int64{1, 3}}},
			[]bigquery.Value{int64(2)},
		},
		{"float", "SELECT @f * 2", []bigquery.QueryParameter{{Name: "f", Value: 1.25}}, []bigquery.Value{2.5}},
		{"struct", "SELECT @s.a", []bigquery.QueryParameter{{Name: "s", Value: struct {
			A int64 `bigquery:"a"`
		}{A: 7}}}, []bigquery.Value{int64(7)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows := env.rows(t, tt.sql, tt.params...)
			require.Len(t, rows, 1)
			assert.Equal(t, tt.want, rows[0])
		})
	}
}

type order struct {
	SKU string
	Qty int64
}

type customer struct {
	Name   string
	Addr   address
	Orders []order
	Tags   []string
	Seen   time.Time
	Blob   []byte
}

type address struct {
	City string
	Zip  int64
}

func TestNestedRepeatedRoundTrip(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "nested")

	schema, err := bigquery.InferSchema(customer{})
	require.NoError(t, err)

	tbl := ds.Table("c")
	require.NoError(t, tbl.Create(ctx, &bigquery.TableMetadata{Schema: schema}))

	seen := time.Date(2020, 1, 2, 3, 4, 5, 123456000, time.UTC)
	in := customer{
		Name: "ann", Addr: address{City: "Paris", Zip: 75001}, Orders: []order{{"x", 2}, {"y", 3}},
		Tags: []string{"a", "b"}, Seen: seen, Blob: []byte("hello"),
	}

	require.NoError(t, tbl.Inserter().Put(ctx, &bigquery.StructSaver{Schema: schema, Struct: in}))

	it := tbl.Read(ctx)

	var out customer

	require.NoError(t, it.Next(&out))
	assert.Equal(t, in.Name, out.Name)
	assert.Equal(t, in.Addr, out.Addr)
	assert.Equal(t, in.Orders, out.Orders)
	assert.Equal(t, in.Tags, out.Tags)
	assert.True(t, seen.Equal(out.Seen), "timestamp %v != %v", seen, out.Seen)
	assert.Equal(t, in.Blob, out.Blob)

	rows := env.rows(t, "SELECT addr.city, o.sku, o.qty FROM nested.c, UNNEST(orders) AS o ORDER BY o.sku")
	assert.Equal(t, [][]bigquery.Value{{"Paris", "x", int64(2)}, {"Paris", "y", int64(3)}}, rows)

	rows = env.rows(t, "SELECT orders, addr FROM nested.c")
	require.Len(t, rows, 1)
	assert.Equal(t, []bigquery.Value{
		[]bigquery.Value{[]bigquery.Value{"x", int64(2)}, []bigquery.Value{"y", int64(3)}},
		[]bigquery.Value{"Paris", int64(75001)},
	}, rows[0])
}

func TestDML(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "dml")

	require.NoError(t, ds.Table("t").Create(ctx, &bigquery.TableMetadata{Schema: bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType}, {Name: "v", Type: bigquery.StringFieldType},
	}}))

	tests := []struct {
		name     string
		sql      string
		affected int64
		wantRows int
	}{
		{"insert", "INSERT INTO dml.t (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3, 3},
		{"update", "UPDATE dml.t SET v = CONCAT(v, '!') WHERE id < 3", 2, 3},
		{"delete", "DELETE FROM dml.t WHERE id = 3", 1, 2},
		{"merge", "MERGE dml.t T USING (SELECT 1 AS id, 'z' AS v UNION ALL SELECT 9, 'n') S ON T.id = S.id " +
			"WHEN MATCHED THEN UPDATE SET v = S.v WHEN NOT MATCHED THEN INSERT (id, v) VALUES (S.id, S.v)", 2, 3},
	}

	for _, tt := range tests {
		job, err := env.client.Query(tt.sql).Run(ctx)
		require.NoError(t, err, tt.name)

		status, err := job.Wait(ctx)
		require.NoError(t, err, tt.name)
		require.NoError(t, status.Err(), tt.name)

		qs, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
		require.True(t, ok)
		assert.Equal(t, tt.affected, qs.NumDMLAffectedRows, tt.name)

		md, err := ds.Table("t").Metadata(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(tt.wantRows), md.NumRows, tt.name)
	}

	rows := env.rows(t, "SELECT id, v FROM dml.t ORDER BY id")
	assert.Equal(t, [][]bigquery.Value{{int64(1), "z"}, {int64(2), "b!"}, {int64(9), "n"}}, rows)
}

func TestLoadAndExtract(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "etl")

	env.objects.data["bkt/in/a.csv"] = []byte("id,name,score\n1,ann,1.5\n2,bob,\n")
	env.objects.data["bkt/in/b.json"] = []byte(
		`{"id":3,"name":"cy","score":3.5}` + "\n" + `{"id":4,"name":"di"}` + "\n",
	)

	csvRef := bigquery.NewGCSReference("gs://bkt/in/a.csv")
	csvRef.SkipLeadingRows = 1
	csvRef.Schema = bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType}, {Name: "name", Type: bigquery.StringFieldType},
		{Name: "score", Type: bigquery.FloatFieldType},
	}

	tbl := ds.Table("loaded")
	loader := tbl.LoaderFrom(csvRef)
	loader.WriteDisposition = bigquery.WriteTruncate

	job, err := loader.Run(ctx)
	require.NoError(t, err)

	status, err := job.Wait(ctx)
	require.NoError(t, err)
	require.NoError(t, status.Err())

	jsonRef := bigquery.NewGCSReference("gs://bkt/in/*.json")
	jsonRef.SourceFormat = bigquery.JSON
	jl := tbl.LoaderFrom(jsonRef)
	jl.WriteDisposition = bigquery.WriteAppend

	job, err = jl.Run(ctx)
	require.NoError(t, err)

	status, err = job.Wait(ctx)
	require.NoError(t, err)
	require.NoError(t, status.Err())

	rows := env.rows(t, "SELECT id, name, score FROM etl.loaded ORDER BY id")
	assert.Equal(t, [][]bigquery.Value{
		{int64(1), "ann", 1.5}, {int64(2), "bob", nil}, {int64(3), "cy", 3.5}, {int64(4), "di", nil},
	}, rows)

	ext := bigquery.NewGCSReference("gs://bkt/out/part-*.csv")
	extractor := tbl.ExtractorTo(ext)

	job, err = extractor.Run(ctx)
	require.NoError(t, err)

	status, err = job.Wait(ctx)
	require.NoError(t, err)
	require.NoError(t, status.Err())

	assert.Equal(
		t,
		"id,name,score\n1,ann,1.5\n2,bob,\n3,cy,3.5\n4,di,\n",
		string(env.objects.data["bkt/out/part-000000000000.csv"]),
	)

	bad := bigquery.NewGCSReference("gs://bkt/in/missing.csv")
	bad.Schema = csvRef.Schema
	job, err = ds.Table("never").LoaderFrom(bad).Run(ctx)
	require.NoError(t, err)

	status, err = job.Wait(ctx)
	require.NoError(t, err)
	require.Error(t, status.Err())
	assert.Contains(t, status.Err().Error(), "Not found")
}

func TestQueryErrorsAndJobs(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	env.dataset(t, "errs")

	tests := []struct {
		name string
		sql  string
		code int
	}{
		{"missing table", "SELECT * FROM errs.nope", http.StatusNotFound},
		{"syntax", "SELEC 1", http.StatusBadRequest},
		{"unknown column", "SELECT zzz FROM (SELECT 1 AS a)", http.StatusBadRequest},
	}

	for _, tt := range tests {
		_, err := env.client.Query(tt.sql).Run(ctx)

		var gerr *googleapi.Error

		require.ErrorAs(t, err, &gerr, tt.name)
		assert.Equal(t, tt.code, gerr.Code, tt.name)
	}

	job, err := env.client.Query("SELECT 1 / 0").Run(ctx)
	require.NoError(t, err)

	var rerr *googleapi.Error

	_, err = job.Wait(ctx)
	require.ErrorAs(t, err, &rerr)
	assert.Contains(t, rerr.Message, "division by zero")

	status, err := job.Status(ctx)
	require.NoError(t, err)
	require.Error(t, status.Err())
	assert.Contains(t, status.Err().Error(), "division by zero")

	ok, err := env.client.Query("SELECT 1").Run(ctx)
	require.NoError(t, err)
	_, err = ok.Wait(ctx)
	require.NoError(t, err)

	got, err := env.client.JobFromID(ctx, ok.ID())
	require.NoError(t, err)
	assert.Equal(t, ok.ID(), got.ID())

	var ids []string

	it := env.client.Jobs(ctx)

	for {
		j, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		ids = append(ids, j.ID())
	}

	assert.Contains(t, ids, ok.ID())
	require.NoError(t, ok.Cancel(ctx))

	dry := env.client.Query("SELECT 1 AS a, 'x' AS b")
	dry.DryRun = true
	dj, err := dry.Run(ctx)
	require.NoError(t, err)
	assert.Equal(t, bigquery.Schema{
		{Name: "a", Type: bigquery.IntegerFieldType}, {Name: "b", Type: bigquery.StringFieldType},
	}, dj.LastStatus().Statistics.Details.(*bigquery.QueryStatistics).Schema)
}

func TestQueryPaging(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "pg")
	tbl := ds.Table("n")

	require.NoError(
		t,
		tbl.Create(ctx, &bigquery.TableMetadata{Schema: bigquery.Schema{{Name: "i", Type: bigquery.IntegerFieldType}}}),
	)

	savers := make([]*bigquery.ValuesSaver, 0, 25)

	for i := range 25 {
		savers = append(savers, &bigquery.ValuesSaver{
			Schema: bigquery.Schema{{Name: "i", Type: bigquery.IntegerFieldType}}, Row: []bigquery.Value{int64(i)},
		})
	}

	require.NoError(t, tbl.Inserter().Put(ctx, savers))

	q := env.client.Query("SELECT i FROM pg.n ORDER BY i")
	it, err := q.Read(ctx)
	require.NoError(t, err)

	it.PageInfo().MaxSize = 10

	n := 0

	for {
		var row []bigquery.Value

		if nerr := it.Next(&row); nerr != nil {
			break
		}

		assert.Equal(t, int64(n), row[0])

		n++
	}

	assert.Equal(t, 25, n)
	assert.Equal(t, uint64(25), it.TotalRows)

	tit := tbl.Read(ctx)
	tit.PageInfo().MaxSize = 7

	n = 0

	for {
		var row []bigquery.Value

		if nerr := tit.Next(&row); nerr != nil {
			break
		}

		n++
	}

	assert.Equal(t, 25, n)
}
