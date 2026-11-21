package gcpbigquery_test

import (
	"bytes"
	"io"
	"math/big"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

var encodedDecimalText = regexp.MustCompile(`#[0-9a-f]{64}`)

func httpJSON(t *testing.T, env *sdkEnv, method, path, body string) (int, string) {
	t.Helper()

	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, env.url+path, rd)
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out)
}

func jsonString(s string) string {
	return `"` + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`) + `"`
}

func TestSDKNumericEgressNeverEncoded(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ds := env.dataset(t, "eg")

	env.exec(t, "CREATE TABLE eg.src AS SELECT 1 AS id, NUMERIC '1.5' AS n, "+
		"BIGNUMERIC '0.00000000000000000000000000000000000001' AS b, [NUMERIC '2.5', NUMERIC '3'] AS arr, "+
		"STRUCT(NUMERIC '4.25' AS x, BIGNUMERIC '5' AS y) AS rec, [STRUCT(NUMERIC '6.5' AS x, 'a' AS tag)] AS recs")

	queries := []string{
		"SELECT * FROM eg.src",
		"SELECT n + 1, b * 2, arr, rec, recs FROM eg.src",
		"SELECT TO_JSON_STRING(rec), TO_JSON_STRING(recs), FORMAT('%s', arr), FORMAT('%T', n) FROM eg.src",
		"SELECT MIN(n), MAX(b), SUM(n), AVG(b), ARRAY_AGG(n) FROM eg.src",
		"DECLARE x NUMERIC DEFAULT NUMERIC '1.5'; SET x = x + 1; SELECT x, [x], STRUCT(x AS v);",
		"SELECT x, [x, x * 2] FROM UNNEST([NUMERIC '1', NUMERIC '2']) x",
		"SELECT n IS NULL, CAST(n AS STRING), CAST(b AS STRING), CAST(n AS FLOAT64) FROM eg.src",
		"CREATE TEMP FUNCTION f(x NUMERIC) RETURNS NUMERIC AS (x * 2); SELECT f(n), f(1) FROM eg.src",
		"SELECT @p, [@p], STRUCT(@p AS v)",
	}

	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			t.Parallel()

			body := `{"query":` + jsonString(q) + `,"useLegacySql":false,` +
				`"queryParameters":[{"name":"p","parameterType":{"type":"NUMERIC"},"parameterValue":{"value":"7.25"}}],` +
				`"parameterMode":"NAMED"}`
			if !strings.Contains(q, "@p") {
				body = `{"query":` + jsonString(q) + `,"useLegacySql":false}`
			}

			code, out := httpJSON(t, env, http.MethodPost, "/projects/"+sdkProject+"/queries", body)
			assert.Equal(t, http.StatusOK, code, out)
			assert.NotRegexp(t, encodedDecimalText, out)
		})
	}

	t.Run("tabledata", func(t *testing.T) {
		t.Parallel()

		code, out := httpJSON(t, env, http.MethodGet, "/projects/"+sdkProject+"/datasets/eg/tables/src/data", "")
		assert.Equal(t, http.StatusOK, code)
		assert.NotRegexp(t, encodedDecimalText, out)
		assert.Contains(t, out, `"1.5"`)
		assert.Contains(t, out, `"0.00000000000000000000000000000000000001"`)
	})

	t.Run("extract", func(t *testing.T) {
		t.Parallel()

		for name, format := range map[string]bigquery.DataFormat{
			"json": bigquery.JSON,
		} {
			ref := bigquery.NewGCSReference("gs://b/" + name + ".out")
			ref.DestinationFormat = format
			require.NoError(t, runExtract(t, ds, ref, false).Err(), name)
		}

		for name, data := range env.objects.data {
			assert.False(t, encodedDecimalText.Match(data), name)
		}

		assert.Contains(t, string(env.objects.data["b/json.out"]), "0.00000000000000000000000000000000000001")
	})
}

func TestSDKNumericFlatExtracts(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ds := env.dataset(t, "pq")

	env.exec(t, "CREATE TABLE pq.src AS SELECT NUMERIC '1.5' AS n, "+
		"BIGNUMERIC '0.00000000000000000000000000000000000001' AS b")

	csv := bigquery.NewGCSReference("gs://b/p.csv")
	csv.DestinationFormat = bigquery.CSV
	require.NoError(t, runExtract(t, ds, csv, false).Err())
	assert.Contains(t, string(env.objects.data["b/p.csv"]), "1.5,0.00000000000000000000000000000000000001")

	av := bigquery.NewGCSReference("gs://b/p.avro")
	av.DestinationFormat = bigquery.Avro
	require.NoError(t, runExtract(t, ds, av, false).Err())

	dec, err := ocf.NewDecoder(bytes.NewReader(env.objects.data["b/p.avro"]))
	require.NoError(t, err)

	var rec map[string]any

	require.True(t, dec.HasNext())
	require.NoError(t, dec.Decode(&rec))
	assert.Zero(t, rec["b"].(*big.Rat).Cmp(ratOf(t, tinyText)))
	assert.Zero(t, rec["n"].(*big.Rat).Cmp(ratOf(t, "1.5")))
	assert.Contains(t, string(env.objects.data["b/p.avro"]), `"scale":38`)

	ref := bigquery.NewGCSReference("gs://b/p.parquet")
	ref.DestinationFormat = bigquery.Parquet
	require.NoError(t, runExtract(t, ds, ref, false).Err())
	assert.False(t, encodedDecimalText.Match(env.objects.data["b/p.parquet"]))

	in := bigquery.NewGCSReference("gs://b/p.parquet")
	in.SourceFormat = bigquery.Parquet
	in.Schema = bigquery.Schema{
		{Name: "n", Type: bigquery.NumericFieldType}, {Name: "b", Type: bigquery.BigNumericFieldType},
	}

	job, err := ds.Table("back").LoaderFrom(in).Run(t.Context())
	require.NoError(t, err)

	status, err := job.Wait(t.Context())
	require.NoError(t, err)
	require.NoError(t, status.Err())

	rows := env.rows(t, "SELECT n, b FROM pq.back")
	require.Len(t, rows, 1)
	requireRats(t, rows[0], "1.5", tinyText)
}

func TestSDKNumericInsertAllValues(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	env.dataset(t, "ia")

	env.exec(t, "CREATE TABLE ia.t (id INT64, n NUMERIC, p NUMERIC(5, 2), b BIGNUMERIC)")

	tests := []struct {
		name   string
		row    string
		want   string
		failed bool
	}{
		{"number", `{"id":1,"n":1.5,"p":1.25,"b":2.5}`, "1.5", false},
		{"string", `{"id":2,"n":"1234.5","p":"3","b":"1e-38"}`, "1234.5", false},
		{"exponent", `{"id":3,"n":"1.5e3","p":1e1,"b":"5E-1"}`, "1500", false},
		{"over scale", `{"id":4,"n":"0.0000000005","p":"1.005","b":"0.5"}`, "0.000000001", false},
		{"bad text", `{"id":5,"n":"abc"}`, "", true},
		{"overflow", `{"id":6,"n":"100000000000000000000000000000"}`, "", true},
		{"precision", `{"id":7,"p":"1000"}`, "", true},
		{"bool", `{"id":8,"n":true}`, "", true},
		{"encoded text", `{"id":9,"n":"#8000000000000000000000000000000070d8f2fc87ca26b70e4f336000000000"}`, "", true},
	}

	for _, tt := range tests {
		code, out := httpJSON(t, env, http.MethodPost, "/projects/"+sdkProject+"/datasets/ia/tables/t/insertAll",
			`{"rows":[{"json":`+tt.row+`}]}`)
		require.Equal(t, http.StatusOK, code, tt.name+out)
		assert.NotRegexp(t, encodedDecimalText, out, tt.name)

		if tt.failed {
			assert.Contains(t, out, `"insertErrors"`, tt.name)
			assert.Contains(t, out, `"index":0`, tt.name)

			continue
		}

		assert.NotContains(t, out, `"insertErrors"`, tt.name)
	}

	rows := env.rows(t, "SELECT id, n, p FROM ia.t ORDER BY id")
	require.Len(t, rows, 4)
	requireRats(t, rows[0][1:], "1.5", "1.25")
	requireRats(t, rows[1][1:], "1234.5", "3")
	requireRats(t, rows[2][1:], "1500", "10")
	requireRats(t, rows[3][1:], "0.000000001", "1.01")
}

func TestSDKNumericSchemaParameters(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	ds := env.dataset(t, "sp")

	schema := bigquery.Schema{
		{Name: "n", Type: bigquery.NumericFieldType, Precision: 10, Scale: 2},
		{Name: "b", Type: bigquery.BigNumericFieldType, Precision: 40, Scale: 20},
		{Name: "plain", Type: bigquery.NumericFieldType},
	}
	require.NoError(t, ds.Table("t").Create(t.Context(), &bigquery.TableMetadata{Schema: schema}))

	md, err := ds.Table("t").Metadata(t.Context())
	require.NoError(t, err)
	require.Len(t, md.Schema, 3)
	assert.Equal(t, int64(10), md.Schema[0].Precision)
	assert.Equal(t, int64(2), md.Schema[0].Scale)
	assert.Equal(t, int64(40), md.Schema[1].Precision)
	assert.Equal(t, int64(20), md.Schema[1].Scale)
	assert.Zero(t, md.Schema[2].Precision)

	bad := bigquery.Schema{{Name: "n", Type: bigquery.NumericFieldType, Precision: 50, Scale: 2}}
	require.Error(t, ds.Table("bad").Create(t.Context(), &bigquery.TableMetadata{Schema: bad}))
}

func TestSDKNumericMixedDMLAndDynamicSQL(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	env.dataset(t, "dm")

	env.exec(t, "CREATE TABLE dm.t (id INT64, n NUMERIC, b BIGNUMERIC)")
	env.exec(t, "INSERT INTO dm.t VALUES (1, 1, 1), (2, 2.5, 3), (3, 5, 5), (4, NULL, 4)")
	env.exec(t, "UPDATE dm.t SET n = n * id + id / 4 WHERE n = id OR b = id")
	env.exec(t, "DELETE FROM dm.t WHERE b = n AND id = 3")
	env.exec(t, "MERGE dm.t T USING (SELECT 2 AS k, NUMERIC '9.5' AS v) S ON T.n = S.k OR T.b = S.k "+
		"WHEN MATCHED THEN UPDATE SET n = S.v WHEN NOT MATCHED THEN INSERT (id, n, b) VALUES (S.k, S.v, S.k)")

	rows := env.rows(t, "SELECT id, n FROM dm.t ORDER BY id, n")
	require.Len(t, rows, 4)
	requireRats(t, rows[0][1:], "1.25")
	requireRats(t, rows[1][1:], "2.5")
	requireRats(t, rows[2][1:], "9.5")
	assert.Nil(t, rows[3][1])

	rows = env.rows(t, "EXECUTE IMMEDIATE 'SELECT ? + NUMERIC \"1\", @named' USING NUMERIC '2.5', NUMERIC '3' AS named")
	require.Len(t, rows, 1)
	requireRats(t, rows[0], "3.5", "3")

	rows = env.rows(t, "SELECT @s.x + 1, @s.y",
		bigquery.QueryParameter{Name: "s", Value: struct {
			X *big.Rat `bigquery:"x"`
			Y string   `bigquery:"y"`
		}{X: big.NewRat(1, 4), Y: "lit"}})
	require.Len(t, rows, 1)
	requireRats(t, rows[0][:1], "1.25")
	assert.Equal(t, "lit", rows[0][1])
}

func TestSDKNumericSnapshotRoundTripLossless(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	env.dataset(t, "rt")
	env.exec(t, "CREATE TABLE rt.t AS SELECT BIGNUMERIC '"+bigMaxText+"' AS hi, BIGNUMERIC '"+bigMinText+"' AS lo, "+
		"BIGNUMERIC '"+tinyText+"' AS tiny, [BIGNUMERIC '"+tinyText+"'] AS arr, "+
		"STRUCT(NUMERIC '"+wideNumeric+"' AS x) AS rec")

	data := env.backend.Snapshot(t.Context())
	assert.NotRegexp(t, encodedDecimalText, string(data))
	assert.Contains(t, string(data), tinyText)

	other := newSDK(t)
	require.NoError(t, other.backend.Restore(t.Context(), data))

	rows := other.rows(t, "SELECT hi, lo, tiny, rec.x FROM rt.t")
	require.Len(t, rows, 1)
	requireRats(t, rows[0], bigMaxText, bigMinText, tinyText, wideNumeric)

	job, err := other.client.Query("SELECT hi + tiny FROM rt.t").Run(t.Context())
	require.NoError(t, err)

	st, err := job.Wait(t.Context())
	if err == nil {
		err = st.Err()
	}

	require.Error(t, err, "the maximum plus one unit overflows after a restore too")
	assert.Contains(t, err.Error(), "BIGNUMERIC overflow")
}

func TestSDKNumericViewsCTEsAndTableFunctions(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	env.dataset(t, "vw")

	env.exec(t, "CREATE TABLE vw.t AS SELECT 1 AS id, NUMERIC '1.5' AS n, BIGNUMERIC '2.25' AS b "+
		"UNION ALL SELECT 2, NUMERIC '-0.5', BIGNUMERIC '3'")
	env.exec(t, "CREATE VIEW vw.v AS SELECT id, n * 2 AS n2, b FROM vw.t")
	env.exec(t, "CREATE TABLE FUNCTION vw.tf(lim NUMERIC) AS SELECT id, n FROM vw.t WHERE n > lim")
	env.exec(t, "CREATE FUNCTION vw.f(x NUMERIC, y BIGNUMERIC) RETURNS BIGNUMERIC AS (x + y)")

	tests := []struct {
		name string
		sql  string
		want [][]string
	}{
		{"view", "SELECT n2 + 1, b FROM vw.v WHERE id = 1", [][]string{{"4", "2.25"}}},
		{"view compare", "SELECT n2 FROM vw.v WHERE n2 = id + 2", [][]string{{"3"}}},
		{
			"cte",
			"WITH c AS (SELECT n AS m, id FROM vw.t) SELECT SUM(m), MAX(m) FROM c WHERE m > id - 2",
			[][]string{{"1.5", "1.5"}},
		},
		{"tvf", "SELECT SUM(n) FROM vw.tf(0)", [][]string{{"1.5"}}},
		{"tvf int arg", "SELECT SUM(n) FROM vw.tf(-1)", [][]string{{"1"}}},
		{"udf", "SELECT vw.f(n, b) FROM vw.t WHERE id = 1", [][]string{{"3.75"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows := env.rows(t, tt.sql)
			require.Len(t, rows, len(tt.want))

			for i, w := range tt.want {
				requireRats(t, rows[i], w...)
			}
		})
	}
}
