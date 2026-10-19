package athena_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

const (
	parquetSerde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
	parquetInput = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
)

func parquetSales(t *testing.T, cols []string) []byte {
	t.Helper()

	all := []parquetio.Column{
		{Name: cols[0], Type: "int"}, {Name: cols[1], Type: "string"},
		{Name: cols[2], Type: "double"}, {Name: cols[3], Type: "date"},
	}

	var buf bytes.Buffer

	require.NoError(t, parquetio.Write(&buf, all, [][]any{
		{int64(1), "books", 10.0, "2024-01-15"},
		{int64(2), "books", 20.5, "2024-02-20"},
		{int64(3), "games", 5.0, "2024-02-21"},
		{int64(4), "games", 50.0, "2025-03-01"},
	}, ""))

	return buf.Bytes()
}

func parquetSalesDef() *athena.GlueTableDef {
	d := salesDef(parquetSerde, nil)
	d.InputFormat = parquetInput

	return d
}

func TestEngine_ParquetTables(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		cols []string
		want [][]string
	}{
		{
			"select all", "SELECT * FROM sales ORDER BY id", []string{"id", "category", "amount", "day"},
			[][]string{
				{"1", "books", "10.0", "2024-01-15"}, {"2", "books", "20.5", "2024-02-20"},
				{"3", "games", "5.0", "2024-02-21"}, {"4", "games", "50.0", "2025-03-01"},
			},
		},
		{
			"group by", "SELECT category, sum(amount) FROM sales GROUP BY category ORDER BY category",
			[]string{"id", "category", "amount", "day"}, [][]string{{"books", "30.5"}, {"games", "55.0"}},
		},
		{
			"case insensitive columns", "SELECT id, category FROM sales WHERE id = 2",
			[]string{"ID", "Category", "AMOUNT", "Day"}, [][]string{{"2", "books"}},
		},
		{
			"date filter", "SELECT count(*) FROM sales WHERE day >= DATE '2024-02-01'",
			[]string{"id", "category", "amount", "day"}, [][]string{{"3"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newEngineBackend(t, map[string][]byte{"lake/sales/part-0.parquet": parquetSales(t, tc.cols)},
				map[string]*athena.GlueTableDef{"shop/sales": parquetSalesDef()})

			id := start(t, b, tc.sql)
			qe := waitDone(t, b, id)
			require.Equal(t, "SUCCEEDED", qe.Status.State, qe.Status.StateChangeReason)
			assert.Equal(t, tc.want, resultRows(t, b, id))
		})
	}
}

func TestEngine_ParquetColumnPruning(t *testing.T) {
	t.Parallel()

	scanned := func(sql string) int64 {
		b, _ := newEngineBackend(
			t,
			map[string][]byte{
				"lake/sales/part-0.parquet": parquetSales(t, []string{"id", "category", "amount", "day"}),
			},
			map[string]*athena.GlueTableDef{"shop/sales": parquetSalesDef()},
		)

		id := start(t, b, sql)
		qe := waitDone(t, b, id)
		require.Equal(t, "SUCCEEDED", qe.Status.State, qe.Status.StateChangeReason)

		return qe.Statistics.DataScannedInBytes
	}

	one := scanned("SELECT id FROM sales")
	all := scanned("SELECT * FROM sales")

	assert.Positive(t, one)
	assert.Less(t, one, all)
	assert.Equal(t, all, scanned("SELECT id, category, amount, day FROM sales"))
}

func TestEngine_ParquetMissingColumnIsNull(t *testing.T) {
	t.Parallel()

	def := parquetSalesDef()
	def.Columns = append(def.Columns, athena.Column{Name: "added_later", Type: "string"})

	b, _ := newEngineBackend(t,
		map[string][]byte{"lake/sales/part-0.parquet": parquetSales(t, []string{"id", "category", "amount", "day"})},
		map[string]*athena.GlueTableDef{"shop/sales": def})

	id := start(t, b, "SELECT count(*), count(added_later) FROM sales")
	require.Equal(t, "SUCCEEDED", waitDone(t, b, id).Status.State)
	assert.Equal(t, [][]string{{"4", "0"}}, resultRows(t, b, id))
}

func TestEngine_ParquetHivePartitions(t *testing.T) {
	t.Parallel()

	def := parquetSalesDef()
	def.Columns = def.Columns[:3]
	def.PartitionKeys = []athena.Column{{Name: "day", Type: "string"}}

	body := func(id int64) []byte {
		var buf bytes.Buffer

		require.NoError(t, parquetio.Write(&buf, []parquetio.Column{
			{Name: "id", Type: "int"}, {Name: "category", Type: "string"}, {Name: "amount", Type: "double"},
		}, [][]any{{id, "c", 1.5}}, "ZSTD"))

		return buf.Bytes()
	}

	cat := &engineCatalog{
		fakeGlueSource: newFakeGlueSource(),
		defs:           map[string]*athena.GlueTableDef{"shop/sales": def},
		parts: map[string][]athena.GluePartitionDef{
			"shop/sales": {{Values: []string{"2024-01-01"}}, {Values: []string{"2024-01-02"}}},
		},
	}
	store := &engineS3{objects: map[string][]byte{
		"lake/sales/day=2024-01-01/a.parquet": body(1),
		"lake/sales/day=2024-01-02/a.parquet": body(2),
	}}

	b := athena.NewInMemoryBackend("us-east-1", "123456789012")
	b.SetGlueMetadataSource(cat)
	b.SetS3Backend(store)

	id := start(t, b, "SELECT id, day FROM sales WHERE day = '2024-01-02'")
	require.Equal(t, "SUCCEEDED", waitDone(t, b, id).Status.State)
	assert.Equal(t, [][]string{{"2", "2024-01-02"}}, resultRows(t, b, id))
}

type nestedRec struct {
	Address struct {
		City string `parquet:"city"`
	} `parquet:"address"`
	Attrs map[string]string `parquet:"attrs"`
	Tags  []string          `parquet:"tags,list"`
	ID    int64             `parquet:"id"`
}

func TestEngine_ParquetNestedColumns(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[nestedRec](&buf)
	r := nestedRec{ID: 1, Tags: []string{"x", "y"}, Attrs: map[string]string{"k": "v"}}
	r.Address.City = "Oslo"
	_, err := w.Write([]nestedRec{r})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	def := &athena.GlueTableDef{
		Name: "people", Location: "s3://lake/people/", SerdeLibrary: parquetSerde, InputFormat: parquetInput,
		Columns: []athena.Column{
			{Name: "id", Type: "bigint"}, {Name: "address", Type: "struct<city:string>"},
			{Name: "tags", Type: "array<string>"}, {Name: "attrs", Type: "map<string,string>"},
		},
	}

	cases := []struct {
		name string
		sql  string
		want [][]string
	}{
		{"struct field", "SELECT json_extract_scalar(address, '$.city') FROM people", [][]string{{"Oslo"}}},
		{"array cardinality", "SELECT cardinality(tags) FROM people", [][]string{{"2"}}},
		{"array unnest", "SELECT t FROM people CROSS JOIN UNNEST(tags) AS u(t) ORDER BY t", [][]string{{"x"}, {"y"}}},
		{"map lookup", "SELECT element_at(attrs, 'k') FROM people", [][]string{{"v"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newEngineBackend(t, map[string][]byte{"lake/people/a.parquet": buf.Bytes()},
				map[string]*athena.GlueTableDef{"shop/people": def})

			id := start(t, b, tc.sql)
			qe := waitDone(t, b, id)
			require.Equal(t, "SUCCEEDED", qe.Status.State, qe.Status.StateChangeReason)
			assert.Equal(t, tc.want, resultRows(t, b, id))
		})
	}
}
