package gcpbigquery_test

import (
	"bytes"
	"math/big"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

const (
	bigMaxText  = "578960446186580977117854925043439539266.34992332820282019728792003956564819967"
	bigMinText  = "-578960446186580977117854925043439539266.34992332820282019728792003956564819968"
	tinyText    = "0.00000000000000000000000000000000000001"
	bigHalfText = "12345678901234567890123456789012345678.5"
	wideNumeric = "12345678901234567890123456789.123456789"
	wideBig     = "12345678901234567890123456789012.12345678901234567890123456789012345678"
)

func ratOf(t *testing.T, s string) *big.Rat {
	t.Helper()

	r, ok := new(big.Rat).SetString(s)
	require.True(t, ok, s)

	return r
}

func requireRats(t *testing.T, row []bigquery.Value, want ...string) {
	t.Helper()

	require.Len(t, row, len(want))

	for i, w := range want {
		if w == "" {
			assert.Nil(t, row[i], "column %d", i)

			continue
		}

		got, ok := row[i].(*big.Rat)
		require.True(t, ok, "column %d is %T", i, row[i])
		assert.Zero(t, got.Cmp(ratOf(t, w)), "column %d: got %s want %s", i, got.FloatString(40), w)
	}
}

func TestSDKNumericExactness(t *testing.T) {
	t.Parallel()

	env := newSDK(t)

	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{"point3", "SELECT NUMERIC '0.1' + NUMERIC '0.2'", []string{"0.3"}},
		{
			"third",
			"SELECT NUMERIC '1' / NUMERIC '3', NUMERIC '2' / NUMERIC '3'",
			[]string{"0.333333333", "0.666666667"},
		},
		{
			"digits38",
			"SELECT NUMERIC '12345678901234567890123456789.123456789' + NUMERIC '0.000000001'",
			[]string{"12345678901234567890123456789.12345679"},
		},
		{
			"floatloses",
			"SELECT NUMERIC '99999999999999999999999999999.999999999' - NUMERIC '0.000000001'",
			[]string{"99999999999999999999999999999.999999998"},
		},
		{
			"bigthird",
			"SELECT BIGNUMERIC '1' / BIGNUMERIC '3'",
			[]string{"0.33333333333333333333333333333333333333"},
		},
		{"extremes", "SELECT BIGNUMERIC '" + bigMaxText + "', BIGNUMERIC '" + bigMinText + "'",
			[]string{bigMaxText, bigMinText}},
		{
			"bigsum",
			"SELECT BIGNUMERIC '1e38' + BIGNUMERIC '1e-38'",
			[]string{"100000000000000000000000000000000000000.00000000000000000000000000000000000001"},
		},
		{
			"casts",
			"SELECT CAST('1.5' AS NUMERIC), CAST(NUMERIC '1.5' AS BIGNUMERIC), CAST(BIGNUMERIC '0.5' AS NUMERIC)",
			[]string{"1.5", "1.5", "0.5"}},
		{"castnum", "SELECT CAST(2 AS NUMERIC) * NUMERIC '0.5', SAFE_CAST('x' AS NUMERIC)", []string{"1", ""}},
		{
			"safe",
			"SELECT SAFE_DIVIDE(NUMERIC '1', NUMERIC '0'), SAFE_ADD(NUMERIC '1', NUMERIC '2')",
			[]string{"", "3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows := env.rows(t, tt.sql)
			require.Len(t, rows, 1)
			requireRats(t, rows[0], tt.want...)
		})
	}
}

func TestSDKNumericErrors(t *testing.T) {
	t.Parallel()

	env := newSDK(t)

	tests := []struct {
		name string
		sql  string
		msg  string
	}{
		{
			"overflow",
			"SELECT NUMERIC '99999999999999999999999999999.999999999' + NUMERIC '1'",
			"numeric overflow",
		},
		{"divzero", "SELECT NUMERIC '1' / NUMERIC '0'", "division by zero"},
		{"bigover", "SELECT BIGNUMERIC '" + bigMaxText + "' + BIGNUMERIC '1'", "BIGNUMERIC overflow"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			job, err := env.client.Query(tt.sql).Run(t.Context())
			require.NoError(t, err)

			st, err := job.Wait(t.Context())
			if err == nil {
				err = st.Err()
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.msg)
		})
	}
}

func TestSDKNumericTable(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ctx := t.Context()
	ds := env.dataset(t, "nm")

	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType},
		{Name: "g", Type: bigquery.StringFieldType},
		{Name: "n", Type: bigquery.NumericFieldType},
		{Name: "b", Type: bigquery.BigNumericFieldType},
	}
	tbl := ds.Table("t")
	require.NoError(t, tbl.Create(ctx, &bigquery.TableMetadata{Schema: schema}))

	put := func(id int64, g, n, b string) *bigquery.ValuesSaver {
		return &bigquery.ValuesSaver{
			Schema: schema, InsertID: g + n,
			Row: []bigquery.Value{id, g, ratOf(t, n), ratOf(t, b)},
		}
	}

	require.NoError(t, tbl.Inserter().Put(ctx, []*bigquery.ValuesSaver{
		put(1, "a", "0.1", "0.00000000000000000000000000000000000001"),
		put(2, "a", "0.2", "12345678901234567890123456789012345678.5"),
		put(3, "b", "-0.3", "-5"),
		put(4, "b", "0.1", "5"),
	}))

	rows := env.rows(t, "SELECT n, b FROM nm.t ORDER BY id")
	require.Len(t, rows, 4)
	requireRats(t, rows[0], "0.1", "0.00000000000000000000000000000000000001")
	requireRats(t, rows[1], "0.2", "12345678901234567890123456789012345678.5")

	rows = env.rows(t, "SELECT n FROM nm.t ORDER BY n DESC, id")
	require.Len(t, rows, 4)

	for i, w := range []string{"0.2", "0.1", "0.1", "-0.3"} {
		requireRats(t, rows[i], w)
	}

	rows = env.rows(t, "SELECT g, SUM(n), AVG(n), MIN(b), MAX(b), COUNT(DISTINCT n) FROM nm.t GROUP BY g ORDER BY g")
	require.Len(t, rows, 2)
	assert.Equal(t, "a", rows[0][0])
	requireRats(t, rows[0][1:5], "0.3", "0.15", tinyText, bigHalfText)
	requireRats(t, rows[1][1:5], "-0.2", "-0.1", "-5", "5")
	assert.Equal(t, int64(2), rows[0][5])
	assert.Equal(t, int64(2), rows[1][5])

	rows = env.rows(t, "SELECT n, COUNT(*) FROM nm.t GROUP BY n ORDER BY n")
	require.Len(t, rows, 3)
	requireRats(t, rows[2][:1], "0.2")
	assert.Equal(t, int64(2), rows[1][1])

	rows = env.rows(t, "SELECT SUM(b) OVER (PARTITION BY g ORDER BY id) FROM nm.t ORDER BY id")
	require.Len(t, rows, 4)
	requireRats(t, rows[3], "0")
	requireRats(t, rows[2], "-5")

	rows = env.rows(t, "SELECT a.id FROM nm.t a JOIN nm.t c ON a.n = c.n AND a.id < c.id")
	require.Len(t, rows, 1)
	assert.Equal(t, int64(1), rows[0][0])
}

func TestSDKNumericParameters(t *testing.T) {
	t.Parallel()

	env := newSDK(t)

	rows := env.rows(t, "SELECT @n + NUMERIC '0.2', @b + BIGNUMERIC '1', @arr, @n = NUMERIC '0.1'",
		bigquery.QueryParameter{Name: "n", Value: big.NewRat(1, 10)},
		bigquery.QueryParameter{Name: "b", Value: &bigquery.QueryParameterValue{
			Type:  bigquery.StandardSQLDataType{TypeKind: "BIGNUMERIC"},
			Value: bigquery.BigNumericString(ratOf(t, "0.00000000000000000000000000000000000001")),
		}},
		bigquery.QueryParameter{Name: "arr", Value: []*big.Rat{big.NewRat(1, 4), big.NewRat(3, 1)}},
	)
	require.Len(t, rows, 1)
	requireRats(t, rows[0][:2], "0.3", "1.00000000000000000000000000000000000001")

	arr, ok := rows[0][2].([]bigquery.Value)
	require.True(t, ok)
	require.Len(t, arr, 2)
	requireRats(t, []bigquery.Value{arr[0], arr[1]}, "0.25", "3")
	assert.Equal(t, true, rows[0][3])
}

func TestSDKNumericDML(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	env.dataset(t, "dm")

	env.exec(t, "CREATE TABLE dm.t (id INT64, price NUMERIC(10, 2), wide BIGNUMERIC)")
	env.exec(t, "INSERT INTO dm.t VALUES (1, 1.125, 1), "+
		"(2, NUMERIC '2.5', BIGNUMERIC '0.123456789012345678901234567890123456789')")
	env.exec(t, "UPDATE dm.t SET price = price * 3, wide = wide + 1 WHERE id = 1")
	env.exec(t, "CREATE TABLE dm.copy AS SELECT id, price + NUMERIC '0.005' AS p, wide FROM dm.t")

	rows := env.rows(t, "SELECT price, wide FROM dm.t ORDER BY id")
	require.Len(t, rows, 2)
	requireRats(t, rows[0], "3.39", "2")
	requireRats(t, rows[1], "2.5", "0.12345678901234567890123456789012345679")

	rows = env.rows(t, "SELECT p FROM dm.copy ORDER BY id")
	requireRats(t, rows[0], "3.395")

	job, err := env.client.Query("INSERT INTO dm.t (id, price) VALUES (3, 1000000000)").Run(t.Context())
	require.NoError(t, err)

	st, err := job.Wait(t.Context())
	if err == nil {
		err = st.Err()
	}

	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")
}

func TestSDKNumericLoads(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ds := env.dataset(t, "ld")

	const avroSchema = `{"type":"record","name":"r","fields":[
		{"name":"id","type":"long"},
		{"name":"n","type":{"type":"bytes","logicalType":"decimal","precision":38,"scale":9}},
		{"name":"b","type":{"type":"bytes","logicalType":"decimal","precision":70,"scale":38}}]}`

	var avroBuf bytes.Buffer

	enc, err := ocf.NewEncoder(avroSchema, &avroBuf)
	require.NoError(t, err)
	require.NoError(t, enc.Encode(map[string]any{
		"id": int64(1), "n": ratOf(t, "12345678901234567890123456789.123456789"),
		"b": ratOf(t, "12345678901234567890123456789012.12345678901234567890123456789012345678"),
	}))
	require.NoError(t, enc.Close())

	var pq bytes.Buffer

	require.NoError(t, parquetio.Write(&pq,
		[]parquetio.Column{{Name: "id", Type: "bigint"}, {Name: "n", Type: "decimal(38,9)"}},
		[][]any{{int64(1), "12345678901234567890123456789.123456789"}}, ""))

	env.objects.data["bkt/a.avro"] = avroBuf.Bytes()
	env.objects.data["bkt/a.parquet"] = pq.Bytes()
	env.objects.data["bkt/a.csv"] = []byte("id,n,b\n1,\"1234.5\"," + tinyText + "\n2,-0.000000001,5e3\n")
	env.objects.data["bkt/a.json"] = []byte(`{"id":1,"n":0.1,"b":"1e-38"}` + "\n" +
		`{"id":2,"n":"7","b":123456789012345678901234567890.1}` + "\n")

	numSchema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType},
		{Name: "n", Type: bigquery.NumericFieldType},
		{Name: "b", Type: bigquery.BigNumericFieldType},
	}

	tests := []struct {
		name   string
		uri    string
		format bigquery.DataFormat
		schema bigquery.Schema
		want   [][]string
	}{
		{"avro", "gs://bkt/a.avro", bigquery.Avro, nil,
			[][]string{{wideNumeric, wideBig}}},
		{"parquet", "gs://bkt/a.parquet", bigquery.Parquet, bigquery.Schema{numSchema[0], numSchema[1]},
			[][]string{{"12345678901234567890123456789.123456789"}}},
		{"csv", "gs://bkt/a.csv", bigquery.CSV, numSchema,
			[][]string{{"1234.5", "0.00000000000000000000000000000000000001"}, {"-0.000000001", "5000"}}},
		{"json", "gs://bkt/a.json", bigquery.JSON, numSchema,
			[][]string{{"0.1", "0.00000000000000000000000000000000000001"}, {"7", "123456789012345678901234567890.1"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ref := bigquery.NewGCSReference(tt.uri)
			ref.SourceFormat = tt.format
			ref.Schema = tt.schema

			if tt.format == bigquery.CSV {
				ref.SkipLeadingRows = 1
			}

			job, runErr := ds.Table(tt.name).LoaderFrom(ref).Run(t.Context())
			require.NoError(t, runErr)

			status, waitErr := job.Wait(t.Context())
			require.NoError(t, waitErr)
			require.NoError(t, status.Err())

			sel := "SELECT n, b FROM ld." + tt.name + " ORDER BY id"
			if tt.name == "parquet" {
				sel = "SELECT n FROM ld.parquet ORDER BY id"
			}

			rows := env.rows(t, sel)
			require.Len(t, rows, len(tt.want))

			for i, w := range tt.want {
				requireRats(t, rows[i], w...)
			}
		})
	}
}

func TestSDKNumericAvroExtractExact(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ds := env.dataset(t, "ex")

	env.exec(t, "CREATE TABLE ex.src AS SELECT NUMERIC '12345678901234567890123456789.123456789' AS n, "+
		"BIGNUMERIC '12345678901234567890123456789012345678.12345678901234567890123456789012345678' AS b")

	ref := bigquery.NewGCSReference("gs://b/p.avro")
	ref.DestinationFormat = bigquery.Avro
	require.NoError(t, runExtract(t, ds, ref, false).Err())

	in := bigquery.NewGCSReference("gs://b/p.avro")
	in.SourceFormat = bigquery.Avro

	job, err := ds.Table("back").LoaderFrom(in).Run(t.Context())
	require.NoError(t, err)

	status, err := job.Wait(t.Context())
	require.NoError(t, err)
	require.NoError(t, status.Err())

	rows := env.rows(t, "SELECT n, b FROM ex.back")
	require.Len(t, rows, 1)
	requireRats(t, rows[0], "12345678901234567890123456789.123456789",
		"12345678901234567890123456789012345678.12345678901234567890123456789012345678")
}

func TestSDKNumericScriptsAndRoutines(t *testing.T) {
	t.Parallel()

	env := newSDK(t)

	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			"script",
			"DECLARE x NUMERIC DEFAULT NUMERIC '1.5'; SET x = x + 1; SELECT x, x * 2;",
			[]string{"2.5", "5"},
		},
		{"scriptint", "DECLARE x NUMERIC(5, 2) DEFAULT 1; SET x = x / 3; SELECT x;", []string{"0.33"}},
		{
			"udf",
			"CREATE TEMP FUNCTION f(x NUMERIC) RETURNS NUMERIC AS (x * 2); SELECT f(1), f(NUMERIC '0.25');",
			[]string{"2", "0.5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows := env.rows(t, tt.sql)
			require.Len(t, rows, 1)
			requireRats(t, rows[0], tt.want...)
		})
	}
}

func TestSDKNumericSnapshotLegacyRows(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	env.dataset(t, "old")
	env.exec(t, "CREATE TABLE old.t AS SELECT 7 AS id, NUMERIC '1.5' AS n, BIGNUMERIC '2.25' AS b, "+
		"[NUMERIC '0.5'] AS arr, STRUCT(NUMERIC '3.75' AS x) AS st")

	data := env.backend.Snapshot(t.Context())

	for _, from := range []string{`"1.5"`, `"2.25"`, `"0.5"`, `"3.75"`} {
		require.Contains(t, string(data), from)
	}

	legacy := string(data)

	for from, to := range map[string]string{
		`"1.5"`: `"1.500000000"`, `"2.25"`: `"2.250000000"`, `"0.5"`: `"5e-1"`, `"3.75"`: `"+3.75"`,
	} {
		legacy = strings.ReplaceAll(legacy, from, to)
	}

	other := newSDK(t)
	require.NoError(t, other.backend.Restore(t.Context(), []byte(legacy)))

	rows := other.rows(t, "SELECT n + 1, b * 2, arr[OFFSET(0)] + 1, st.x + 1, n = NUMERIC '1.5' FROM old.t")
	require.Len(t, rows, 1)
	requireRats(t, rows[0][:4], "2.5", "4.5", "1.5", "4.75")
	assert.Equal(t, true, rows[0][4])

	rows = other.rows(t, "SELECT n, b FROM old.t")
	requireRats(t, rows[0], "1.5", "2.25")
}
