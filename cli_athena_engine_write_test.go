package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (f *athenaEngineFx) salesTable(t *testing.T) {
	t.Helper()

	const db = "shop"

	f.createDB(t, db)
	f.put(t, db+"-sales/part-0.csv", []byte(salesCSV))

	_, err := f.glue.CreateTable(t.Context(), &glue.CreateTableInput{
		DatabaseName: aws.String(db),
		TableInput: &gluetypes.TableInput{
			Name: aws.String("sales"),
			StorageDescriptor: &gluetypes.StorageDescriptor{
				Location: aws.String("s3://lake/" + db + "-sales/"),
				Columns: []gluetypes.Column{
					{Name: aws.String("id"), Type: aws.String("int")},
					{Name: aws.String("category"), Type: aws.String("string")},
					{Name: aws.String("amount"), Type: aws.String("double")},
				},
				SerdeInfo: &gluetypes.SerDeInfo{
					SerializationLibrary: aws.String("org.apache.hadoop.hive.serde2.OpenCSVSerde"),
				},
			},
			Parameters: map[string]string{"skip.header.line.count": "1"},
		},
	})
	require.NoError(t, err)
}

func (f *athenaEngineFx) ok(t *testing.T, sql string) string {
	t.Helper()

	id, qe := f.run(t, "shop", sql)
	require.Equal(t, athenatypes.QueryExecutionStateSucceeded, qe.Status.State, sql+": "+
		aws.ToString(qe.Status.StateChangeReason))

	return id
}

func (f *athenaEngineFx) keys(t *testing.T, prefix string) []string {
	t.Helper()

	out, err := f.s3c.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket: aws.String("lake"), Prefix: aws.String(prefix),
	})
	require.NoError(t, err)

	keys := make([]string, 0, len(out.Contents))
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}

	return keys
}

func (f *athenaEngineFx) body(t *testing.T, key string) []byte {
	t.Helper()

	out, err := f.s3c.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String("lake"), Key: aws.String(key)})
	require.NoError(t, err)

	b, err := io.ReadAll(out.Body)
	require.NoError(t, err)

	return b
}

func TestAthenaEngine_CTAS(t *testing.T) {
	t.Parallel()

	f := newAthenaEngineFx(t)
	f.salesTable(t)

	cases := []struct {
		check  func(t *testing.T, keys []string)
		name   string
		create string
		prefix string
		table  string
		want   [][]string
	}{
		{
			name: "csv",
			create: "CREATE TABLE shop.t_csv WITH (format='TEXTFILE', field_delimiter=',', " +
				"external_location='s3://lake/ctas/csv/') AS SELECT id, category, amount FROM shop.sales WHERE amount > 4",
			prefix: "ctas/csv/",
			table:  "t_csv",
			want:   [][]string{{"n", "s"}, {"5", "92.0"}},
			check: func(t *testing.T, keys []string) {
				t.Helper()
				require.Len(t, keys, 1)
				got := string(f.body(t, keys[0]))
				assert.Contains(t, got, "1,books,10.0\n")
				assert.Equal(t, 5, strings.Count(got, "\n"))
			},
		},
		{
			name: "json",
			create: "CREATE TABLE shop.t_json WITH (format='JSON', external_location='s3://lake/ctas/json/') " +
				"AS SELECT id, category, amount FROM shop.sales WHERE amount > 4",
			prefix: "ctas/json/",
			table:  "t_json",
			want:   [][]string{{"n", "s"}, {"5", "92.0"}},
			check: func(t *testing.T, keys []string) {
				t.Helper()
				require.Len(t, keys, 1)
				assert.Contains(t, string(f.body(t, keys[0])), `{"id":1,"category":"books","amount":10.0}`+"\n")
			},
		},
		{
			name: "gzip",
			create: "CREATE TABLE shop.t_gz WITH (format='TEXTFILE', field_delimiter=',', write_compression='GZIP', " +
				"external_location='s3://lake/ctas/gz/') AS SELECT id, category, amount FROM shop.sales",
			prefix: "ctas/gz/",
			table:  "t_gz",
			want:   [][]string{{"n", "s"}, {"6", "95.0"}},
			check: func(t *testing.T, keys []string) {
				t.Helper()
				require.Len(t, keys, 1)
				assert.True(t, strings.HasSuffix(keys[0], ".gz"))

				zr, err := gzip.NewReader(bytes.NewReader(f.body(t, keys[0])))
				require.NoError(t, err)

				plain, err := io.ReadAll(zr)
				require.NoError(t, err)
				assert.Equal(t, 6, strings.Count(string(plain), "\n"))
			},
		},
		{
			name: "partitioned",
			create: "CREATE TABLE shop.t_part WITH (format='TEXTFILE', field_delimiter=',', " +
				"partitioned_by=ARRAY['category'], external_location='s3://lake/ctas/part/') " +
				"AS SELECT id, amount, category FROM shop.sales",
			prefix: "ctas/part/",
			table:  "t_part",
			want:   [][]string{{"n", "s"}, {"6", "95.0"}},
			check: func(t *testing.T, keys []string) {
				t.Helper()
				require.Len(t, keys, 3)
				assert.Contains(t, keys[0], "ctas/part/category=books/")
				assert.Contains(t, keys[1], "ctas/part/category=games/")
				assert.Contains(t, keys[2], "ctas/part/category=toys/")

				parts, err := f.glue.GetPartitions(t.Context(), &glue.GetPartitionsInput{
					DatabaseName: aws.String("shop"), TableName: aws.String("t_part"),
				})
				require.NoError(t, err)
				assert.Len(t, parts.Partitions, 3)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f.ok(t, tc.create)
			tc.check(t, f.keys(t, tc.prefix))

			tbl, err := f.glue.GetTable(t.Context(), &glue.GetTableInput{
				DatabaseName: aws.String("shop"), Name: aws.String(tc.table),
			})
			require.NoError(t, err)
			assert.Equal(t, "s3://lake/"+tc.prefix, aws.ToString(tbl.Table.StorageDescriptor.Location))
			assert.NotEmpty(t, aws.ToString(tbl.Table.StorageDescriptor.SerdeInfo.SerializationLibrary))

			id := f.ok(t, "SELECT count(*) AS n, sum(amount) AS s FROM shop."+tc.table)
			assert.Equal(t, tc.want, f.rows(t, id))
		})
	}
}

func TestAthenaEngine_CTASDefaultLocationAndFailures(t *testing.T) {
	t.Parallel()

	f := newAthenaEngineFx(t)
	f.salesTable(t)
	f.put(t, "taken/x.csv", []byte("a\n"))
	f.ok(t, "CREATE TABLE shop.dup WITH (format='JSON', external_location='s3://lake/dup/') "+
		"AS SELECT id FROM shop.sales")

	id := f.ok(t, "CREATE TABLE shop.dflt WITH (format='JSON') AS SELECT id FROM shop.sales")
	assert.Len(t, f.s3Keys(t, f.out, "out/tables/"+id+"/"), 1)

	cases := []struct {
		name   string
		query  string
		reason string
	}{
		{
			name: "non empty location",
			query: "CREATE TABLE shop.taken WITH (format='JSON', external_location='s3://lake/taken/') " +
				"AS SELECT id FROM shop.sales",
			reason: "HIVE_PATH_ALREADY_EXISTS",
		},
		{
			name: "table exists",
			query: "CREATE TABLE shop.dup WITH (format='JSON', external_location='s3://lake/dup2/') " +
				"AS SELECT id FROM shop.sales",
			reason: "TABLE_ALREADY_EXISTS",
		},
		{
			name:   "parquet default",
			query:  "CREATE TABLE shop.pq WITH (external_location='s3://lake/pq/') AS SELECT id FROM shop.sales",
			reason: "NOT_SUPPORTED",
		},
		{
			name: "missing partition column",
			query: "CREATE TABLE shop.pp WITH (format='JSON', partitioned_by=ARRAY['nope'], " +
				"external_location='s3://lake/pp/') AS SELECT id FROM shop.sales",
			reason: "COLUMN_NOT_FOUND",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, qe := f.run(t, "shop", tc.query)
			require.Equal(t, athenatypes.QueryExecutionStateFailed, qe.Status.State)
			assert.Contains(t, aws.ToString(qe.Status.StateChangeReason), tc.reason)
		})
	}

	assert.Empty(t, f.keys(t, "pq/"))
	assert.Empty(t, f.keys(t, "dup2/"))
}

func (f *athenaEngineFx) s3Keys(t *testing.T, bucket, prefix string) []string {
	t.Helper()

	out, err := f.s3c.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket), Prefix: aws.String(prefix),
	})
	require.NoError(t, err)

	keys := make([]string, 0, len(out.Contents))
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}

	return keys
}

func TestAthenaEngine_Insert(t *testing.T) {
	t.Parallel()

	f := newAthenaEngineFx(t)
	f.salesTable(t)
	f.ok(t, "CREATE TABLE shop.ins WITH (format='TEXTFILE', field_delimiter=',', "+
		"external_location='s3://lake/ins/') AS SELECT id, category, amount FROM shop.sales WHERE id <= 2")
	f.ok(t, "CREATE TABLE shop.insp WITH (format='JSON', partitioned_by=ARRAY['category'], "+
		"external_location='s3://lake/insp/') AS SELECT id, amount, category FROM shop.sales WHERE id <= 2")

	count := func(table string) string {
		id := f.ok(t, "SELECT count(*) AS n FROM shop."+table)

		return f.rows(t, id)[1][0]
	}

	require.Equal(t, "2", count("ins"))

	f.ok(t, "INSERT INTO shop.ins SELECT id, category, amount FROM shop.sales WHERE id > 4")
	assert.Equal(t, "4", count("ins"))
	assert.Len(t, f.keys(t, "ins/"), 2)

	f.ok(t, "INSERT INTO shop.ins VALUES (100, 'misc', 1.5), (101, 'misc', 2.5)")
	assert.Equal(t, "6", count("ins"))

	f.ok(t, "INSERT INTO shop.ins (id, category) VALUES (7, 'nul')")
	id := f.ok(t, "SELECT id, amount FROM shop.ins WHERE id = 7")
	assert.Equal(t, [][]string{{"id", "amount"}, {"7", ""}}, f.rows(t, id))

	require.Equal(t, "2", count("insp"))
	f.ok(t, "INSERT INTO shop.insp SELECT id, amount, category FROM shop.sales WHERE id IN (3, 4, 5)")
	assert.Equal(t, "5", count("insp"))

	parts, err := f.glue.GetPartitions(t.Context(), &glue.GetPartitionsInput{
		DatabaseName: aws.String("shop"), TableName: aws.String("insp"),
	})
	require.NoError(t, err)
	assert.Len(t, parts.Partitions, 3)

	_, qe := f.run(t, "shop", "INSERT INTO shop.ins SELECT id FROM shop.sales")
	assert.Equal(t, athenatypes.QueryExecutionStateFailed, qe.Status.State)

	_, qe = f.run(t, "shop", "INSERT INTO shop.ghost SELECT id FROM shop.sales")
	assert.Equal(t, athenatypes.QueryExecutionStateFailed, qe.Status.State)
}

func TestAthenaEngine_Unload(t *testing.T) {
	t.Parallel()

	f := newAthenaEngineFx(t)
	f.salesTable(t)
	f.put(t, "unl-taken/x", []byte("x"))

	f.ok(t, "UNLOAD (SELECT id, category FROM shop.sales WHERE id <= 3) TO 's3://lake/unl/csv/' "+
		"WITH (format='TEXTFILE', field_delimiter='|')")
	keys := f.keys(t, "unl/csv/")
	require.Len(t, keys, 1)
	assert.Equal(t, "1|books\n2|books\n3|games\n", string(f.body(t, keys[0])))

	f.ok(t, "UNLOAD (SELECT id, category FROM shop.sales) TO 's3://lake/unl/json/' "+
		"WITH (format='JSON', compression='GZIP', partitioned_by=ARRAY['category'])")
	assert.Len(t, f.keys(t, "unl/json/category=toys/"), 1)

	_, qe := f.run(t, "shop", "UNLOAD (SELECT id FROM shop.sales) TO 's3://lake/unl-taken/' WITH (format='JSON')")
	assert.Equal(t, athenatypes.QueryExecutionStateFailed, qe.Status.State)
	assert.Contains(t, aws.ToString(qe.Status.StateChangeReason), "HIVE_PATH_ALREADY_EXISTS")

	_, qe = f.run(t, "shop", "UNLOAD (SELECT id FROM shop.sales) TO 's3://lake/unl/pq/'")
	assert.Equal(t, athenatypes.QueryExecutionStateFailed, qe.Status.State)
	assert.Contains(t, aws.ToString(qe.Status.StateChangeReason), "NOT_SUPPORTED")

	tables, err := f.glue.GetTables(t.Context(), &glue.GetTablesInput{DatabaseName: aws.String("shop")})
	require.NoError(t, err)
	assert.Len(t, tables.TableList, 1)
}

func TestAthenaEngine_NullVersusEmptyString(t *testing.T) {
	t.Parallel()

	f := newAthenaEngineFx(t)
	f.salesTable(t)

	id := f.ok(t, "SELECT NULL AS a, '' AS b, 'x' AS c FROM shop.sales LIMIT 1")

	res, err := f.ath.GetQueryResults(t.Context(), &athena.GetQueryResultsInput{QueryExecutionId: aws.String(id)})
	require.NoError(t, err)
	require.Len(t, res.ResultSet.Rows, 2)

	data := res.ResultSet.Rows[1].Data
	require.Len(t, data, 3)
	assert.Nil(t, data[0].VarCharValue)
	require.NotNil(t, data[1].VarCharValue)
	assert.Empty(t, *data[1].VarCharValue)
	assert.Equal(t, "x", aws.ToString(data[2].VarCharValue))

	obj := f.s3c
	out, err := obj.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String(f.out), Key: aws.String("out/" + id + ".csv"),
	})
	require.NoError(t, err)

	csvBody, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	assert.Equal(t, "\"a\",\"b\",\"c\"\n,\"\",\"x\"\n", string(csvBody))
}

func TestAthenaEngine_CTASOptions(t *testing.T) {
	t.Parallel()

	f := newAthenaEngineFx(t)
	f.salesTable(t)

	f.ok(t, "CREATE TABLE shop.nulls WITH (format='TEXTFILE', field_delimiter=',', "+
		"external_location='s3://lake/opt/nulls/') AS SELECT id, CASE WHEN id = 1 THEN NULL ELSE category END AS c "+
		"FROM shop.sales WHERE id <= 2")
	id := f.ok(t, "SELECT id, c FROM shop.nulls ORDER BY id")
	assert.Equal(t, [][]string{{"id", "c"}, {"1", ""}, {"2", "books"}}, f.rows(t, id))

	res, err := f.ath.GetQueryResults(t.Context(), &athena.GetQueryResultsInput{QueryExecutionId: aws.String(id)})
	require.NoError(t, err)
	assert.Nil(t, res.ResultSet.Rows[1].Data[1].VarCharValue)

	cases := []struct {
		name   string
		query  string
		reason string
		keys   int
	}{
		{
			name: "if not exists",
			query: "CREATE TABLE IF NOT EXISTS shop.nulls WITH (format='JSON', external_location='s3://lake/opt/again/') " +
				"AS SELECT id FROM shop.sales",
		},
		{
			name: "no data",
			query: "CREATE TABLE shop.empty WITH (format='JSON', external_location='s3://lake/opt/empty/') " +
				"AS SELECT id FROM shop.sales WITH NO DATA",
		},
		{
			name: "bad compression",
			query: "CREATE TABLE shop.bz WITH (format='JSON', write_compression='ZSTD', " +
				"external_location='s3://lake/opt/bz/') AS SELECT id FROM shop.sales",
			reason: "NOT_SUPPORTED",
		},
		{
			name: "unknown property",
			query: "CREATE TABLE shop.up WITH (format='JSON', bogus='1', external_location='s3://lake/opt/up/') " +
				"AS SELECT id FROM shop.sales",
			reason: "INVALID_TABLE_PROPERTY",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, qe := f.run(t, "shop", tc.query)
			if tc.reason == "" {
				assert.Equal(t, athenatypes.QueryExecutionStateSucceeded, qe.Status.State,
					aws.ToString(qe.Status.StateChangeReason))

				return
			}

			require.Equal(t, athenatypes.QueryExecutionStateFailed, qe.Status.State)
			assert.Contains(t, aws.ToString(qe.Status.StateChangeReason), tc.reason)
		})
	}

	assert.Empty(t, f.keys(t, "opt/again/"))
	assert.Empty(t, f.keys(t, "opt/empty/"))
	assert.Empty(t, f.keys(t, "opt/bz/"))
}
