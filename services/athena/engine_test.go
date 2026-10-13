package athena_test

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdk_s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

type engineS3 struct {
	objects map[string][]byte
	// blockGet makes GetObject wait for the caller's context to end.
	blockGet bool
	mu       sync.Mutex
}

func (f *engineS3) PutObject(_ context.Context, in *sdk_s3.PutObjectInput) (*sdk_s3.PutObjectOutput, error) {
	body, _ := io.ReadAll(in.Body)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)] = body

	return &sdk_s3.PutObjectOutput{}, nil
}

func (f *engineS3) GetObject(ctx context.Context, in *sdk_s3.GetObjectInput) (*sdk_s3.GetObjectOutput, error) {
	if f.blockGet {
		<-ctx.Done()

		return nil, ctx.Err()
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	body, ok := f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)]
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}

	return &sdk_s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}

func (f *engineS3) ListObjectsV2(
	_ context.Context,
	in *sdk_s3.ListObjectsV2Input,
) (*sdk_s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	prefix := aws.ToString(in.Bucket) + "/" + aws.ToString(in.Prefix)

	var keys []string

	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, strings.TrimPrefix(k, aws.ToString(in.Bucket)+"/"))
		}
	}

	sort.Strings(keys)

	out := &sdk_s3.ListObjectsV2Output{}
	for _, k := range keys {
		out.Contents = append(out.Contents, sdk_s3_types.Object{Key: aws.String(k)})
	}

	return out, nil
}

type engineCatalog struct {
	*fakeGlueSource
	defs  map[string]*athena.GlueTableDef
	parts map[string][]athena.GluePartitionDef
}

func (c *engineCatalog) GetTableDef(db, name string) (*athena.GlueTableDef, error) {
	d, ok := c.defs[db+"/"+name]
	if !ok {
		return nil, athena.ErrCatalogEntityNotFound
	}

	return d, nil
}

func (c *engineCatalog) GetPartitionDefs(db, name string) ([]athena.GluePartitionDef, error) {
	return c.parts[db+"/"+name], nil
}

func (c *engineCatalog) CreateTableDef(db string, def athena.GlueTableDef) error {
	c.defs[db+"/"+def.Name] = &def

	return nil
}

func (c *engineCatalog) DeleteTable(db, name string) error {
	delete(c.defs, db+"/"+name)

	return nil
}

func (c *engineCatalog) CreatePartitionDefs(db, table string, parts []athena.GluePartitionDef) error {
	c.parts[db+"/"+table] = append(c.parts[db+"/"+table], parts...)

	return nil
}

const (
	serdeCSV  = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
	serdeLazy = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
)

func newEngineBackend(t *testing.T, objects map[string][]byte, defs map[string]*athena.GlueTableDef) (
	*athena.InMemoryBackend, *engineS3,
) {
	t.Helper()

	cat := &engineCatalog{
		fakeGlueSource: newFakeGlueSource(),
		defs:           defs,
		parts:          map[string][]athena.GluePartitionDef{},
	}
	store := &engineS3{objects: objects}

	b := athena.NewInMemoryBackend("us-east-1", "123456789012")
	b.SetGlueMetadataSource(cat)
	b.SetS3Backend(store)

	return b, store
}

func salesDef(serde string, params map[string]string) *athena.GlueTableDef {
	return &athena.GlueTableDef{
		Name: "sales", Location: "s3://lake/sales/", SerdeLibrary: serde, Parameters: params,
		Columns: []athena.Column{
			{Name: "id", Type: "int"}, {Name: "category", Type: "string"},
			{Name: "amount", Type: "double"}, {Name: "day", Type: "date"},
		},
	}
}

const salesData = "id,category,amount,day\n" +
	"1,books,10,2024-01-15\n2,books,20.5,2024-02-20\n3,games,5,2024-02-21\n4,games,50,2025-03-01\n"

func start(t *testing.T, b *athena.InMemoryBackend, sql string) string {
	t.Helper()

	id, err := b.StartQueryExecution(sql, "primary",
		athena.QueryExecutionContext{Database: "shop"},
		athena.ResultConfiguration{OutputLocation: "s3://out/res/"}, nil, nil)
	require.NoError(t, err)

	return id
}

func waitDone(t *testing.T, b *athena.InMemoryBackend, id string) *athena.QueryExecution {
	t.Helper()

	var qe *athena.QueryExecution

	require.Eventually(t, func() bool {
		var err error

		qe, err = b.GetQueryExecution(id)

		return err == nil && qe.Status.State != "QUEUED" && qe.Status.State != "RUNNING"
	}, 10*time.Second, time.Millisecond)

	return qe
}

func resultRows(t *testing.T, b *athena.InMemoryBackend, id string) [][]string {
	t.Helper()

	var rows [][]string

	token := ""

	for {
		page, err := b.GetQueryResults(id, token, 2)
		require.NoError(t, err)

		rows = append(rows, page.Rows...)

		if page.NextToken == "" {
			return rows
		}

		token = page.NextToken
	}
}

func TestEngine_TranslationAndFormats(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		sql    string
		data   string
		serde  string
		params map[string]string
		sp     map[string]string
		want   [][]string
	}{
		{
			name: "date_trunc month", serde: serdeCSV, data: salesData,
			params: map[string]string{"skip.header.line.count": "1"},
			sql:    "SELECT date_trunc('month', day) AS m, count(*) AS n FROM sales GROUP BY 1 ORDER BY 1",
			want:   [][]string{{"2024-01-01", "1"}, {"2024-02-01", "2"}, {"2025-03-01", "1"}},
		},
		{
			name: "extract and cast", serde: serdeCSV, data: salesData,
			params: map[string]string{"skip.header.line.count": "1"},
			sql: "SELECT extract(year FROM day) AS y, CAST(sum(amount) AS bigint) AS s " +
				"FROM sales GROUP BY 1 ORDER BY 1",
			want: [][]string{{"2024", "35"}, {"2025", "50"}},
		},
		{
			name: "date literal and diff", serde: serdeCSV, data: salesData,
			params: map[string]string{"skip.header.line.count": "1"},
			sql: "SELECT id, date_diff('day', day, DATE '2024-03-01') AS d FROM sales " +
				"WHERE day < DATE '2024-03-01' ORDER BY id",
			want: [][]string{{"1", "46"}, {"2", "10"}, {"3", "9"}},
		},
		{
			name: "like is case sensitive", serde: serdeCSV, data: salesData,
			params: map[string]string{"skip.header.line.count": "1"},
			sql:    "SELECT count(*) FROM sales WHERE category LIKE 'BOOK%'",
			want:   [][]string{{"0"}},
		},
		{
			name: "cte and join", serde: serdeCSV, data: salesData,
			params: map[string]string{"skip.header.line.count": "1"},
			sql: "WITH big AS (SELECT * FROM sales WHERE amount > 8) " +
				"SELECT b.category, count(*) FROM big b JOIN shop.sales s ON b.id = s.id GROUP BY 1 ORDER BY 1",
			want: [][]string{{"books", "2"}, {"games", "1"}},
		},
		{
			name: "tsv with nulls", serde: serdeLazy,
			data:   "1\tx\t1.5\t2024-01-01\n2\t\\N\t\t\n",
			params: map[string]string{}, sp: map[string]string{"field.delim": `\t`},
			sql:  "SELECT id, category, amount FROM sales ORDER BY id",
			want: [][]string{{"1", "x", "1.5"}, {"2", "", ""}},
		},
		{
			name: "opencsv quoted", serde: serdeCSV,
			data:   `1,"a, b",3,2024-01-01` + "\n",
			params: map[string]string{},
			sql:    "SELECT category FROM sales",
			want:   [][]string{{"a, b"}},
		},
		{
			name: "json lines", serde: "org.openx.data.jsonserde.JsonSerDe",
			data: `{"id":1,"category":"j","amount":2.5,"day":"2024-01-01"}` + "\n" +
				`{"ID":2,"category":null}` + "\n",
			params: map[string]string{},
			sql:    "SELECT id, category FROM sales ORDER BY id",
			want:   [][]string{{"1", "j"}, {"2", ""}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			def := salesDef(tc.serde, tc.params)
			def.SerdeParameters = tc.sp
			b, _ := newEngineBackend(t, map[string][]byte{"lake/sales/a.dat": []byte(tc.data)},
				map[string]*athena.GlueTableDef{"shop/sales": def})

			id := start(t, b, tc.sql)
			qe := waitDone(t, b, id)
			require.Equal(t, "SUCCEEDED", qe.Status.State, qe.Status.StateChangeReason)
			assert.Equal(t, tc.want, resultRows(t, b, id))
			assert.Positive(t, qe.Statistics.DataScannedInBytes)
		})
	}
}

func TestEngine_Failures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		sql        string
		serde      string
		inputFmt   string
		wantReason string
		limits     athena.EngineLimits
	}{
		{
			name:       "parquet serde",
			sql:        "SELECT * FROM sales",
			serde:      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
			wantReason: "NOT_SUPPORTED: unsupported data format: Parquet",
		},
		{
			name:       "parquet input format",
			sql:        "SELECT * FROM sales",
			inputFmt:   "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
			wantReason: "Parquet",
		},
		{
			name:       "orc",
			sql:        "SELECT * FROM sales",
			serde:      "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
			wantReason: "ORC",
		},
		{name: "syntax", sql: "SELECT FROM sales WHERE", serde: serdeCSV, wantReason: "SYNTAX_ERROR"},
		{
			name:       "unknown function",
			sql:        "SELECT nope(id) FROM sales",
			serde:      serdeCSV,
			wantReason: "Function 'nope' not registered",
		},
		{name: "unknown column", sql: "SELECT zzz FROM sales", serde: serdeCSV, wantReason: "COLUMN_NOT_FOUND"},
		{
			name:       "load row cap",
			sql:        "SELECT * FROM sales",
			serde:      serdeCSV,
			limits:     athena.EngineLimits{MaxLoadedRows: 3},
			wantReason: "EXCEEDED_LIMIT: row limit exceeded: more than 3 rows",
		},
		{
			name:       "result row cap",
			sql:        "SELECT * FROM sales",
			serde:      serdeCSV,
			limits:     athena.EngineLimits{MaxResultRows: 2},
			wantReason: "EXCEEDED_LIMIT: result row limit exceeded: more than 2 rows",
		},
		{
			name:       "result byte cap",
			sql:        "SELECT * FROM sales",
			serde:      serdeCSV,
			limits:     athena.EngineLimits{MaxResultBytes: 10},
			wantReason: "result size limit exceeded",
		},
		{
			name:       "scan cap",
			sql:        "SELECT * FROM sales",
			serde:      serdeCSV,
			limits:     athena.EngineLimits{MaxScanBytes: 20},
			wantReason: "scan limit exceeded",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			def := salesDef(tc.serde, map[string]string{"skip.header.line.count": "1"})
			def.InputFormat = tc.inputFmt

			b, _ := newEngineBackend(t, map[string][]byte{"lake/sales/a.csv": []byte(salesData)},
				map[string]*athena.GlueTableDef{"shop/sales": def})
			b.SetEngineLimits(tc.limits)

			qe := waitDone(t, b, start(t, b, tc.sql))
			require.Equal(t, "FAILED", qe.Status.State)
			assert.Contains(t, qe.Status.StateChangeReason, tc.wantReason)
			require.NotNil(t, qe.Status.AthenaError)
		})
	}
}

func TestEngine_StopCancelsRunningQuery(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, store := newEngineBackend(t, map[string][]byte{},
			map[string]*athena.GlueTableDef{"shop/sales": salesDef(serdeCSV, nil)})
		store.blockGet = true
		store.objects["lake/sales/a.csv"] = []byte(salesData)

		id := start(t, b, "SELECT * FROM sales")
		synctest.Wait()

		qe, err := b.GetQueryExecution(id)
		require.NoError(t, err)
		assert.Equal(t, "RUNNING", qe.Status.State)

		require.NoError(t, b.StopQueryExecution(id))
		synctest.Wait()

		qe, err = b.GetQueryExecution(id)
		require.NoError(t, err)
		assert.Equal(t, "CANCELLED", qe.Status.State)
		assert.Error(t, b.StopQueryExecution(id))
	})
}

func TestEngine_TimeoutFailsRunningQuery(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, store := newEngineBackend(t, map[string][]byte{"lake/sales/a.csv": []byte(salesData)},
			map[string]*athena.GlueTableDef{"shop/sales": salesDef(serdeCSV, nil)})
		store.blockGet = true
		b.SetEngineLimits(athena.EngineLimits{QueryTimeout: time.Minute})

		id := start(t, b, "SELECT * FROM sales")
		synctest.Wait()

		qe, err := b.GetQueryExecution(id)
		require.NoError(t, err)
		require.Equal(t, "RUNNING", qe.Status.State)

		time.Sleep(2 * time.Minute)
		synctest.Wait()

		qe, err = b.GetQueryExecution(id)
		require.NoError(t, err)
		assert.Equal(t, "FAILED", qe.Status.State)
		assert.Contains(t, qe.Status.StateChangeReason, "Query timeout")
	})
}

func TestEngine_WritesResultObjects(t *testing.T) {
	t.Parallel()

	b, store := newEngineBackend(
		t,
		map[string][]byte{"lake/sales/a.csv": []byte(salesData)},
		map[string]*athena.GlueTableDef{
			"shop/sales": salesDef(serdeCSV, map[string]string{"skip.header.line.count": "1"}),
		},
	)

	id := start(t, b, "SELECT category, count(*) AS n FROM sales GROUP BY category ORDER BY category")
	require.Equal(t, "SUCCEEDED", waitDone(t, b, id).Status.State)

	store.mu.Lock()
	defer store.mu.Unlock()

	assert.Equal(
		t,
		"\"category\",\"n\"\n\"books\",\"2\"\n\"games\",\"2\"\n",
		string(store.objects["out/res/"+id+".csv"]),
	)
	assert.Contains(t, string(store.objects["out/res/"+id+".csv.metadata"]), "category")
}

func TestEngine_RestoreFailsInterruptedQueries(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, store := newEngineBackend(t, map[string][]byte{"lake/sales/a.csv": []byte(salesData)},
			map[string]*athena.GlueTableDef{"shop/sales": salesDef(serdeCSV, nil)})
		store.blockGet = true

		id := start(t, b, "SELECT * FROM sales")
		synctest.Wait()

		snap := b.Snapshot(t.Context())
		require.NoError(t, b.StopQueryExecution(id))
		synctest.Wait()

		restored := athena.NewInMemoryBackend("us-east-1", "123456789012")
		require.NoError(t, restored.Restore(t.Context(), snap))

		qe, err := restored.GetQueryExecution(id)
		require.NoError(t, err)
		assert.Equal(t, "FAILED", qe.Status.State)
		assert.Equal(t, "Query was interrupted by a restart", qe.Status.StateChangeReason)
	})
}
