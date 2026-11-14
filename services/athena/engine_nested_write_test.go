package athena_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
	"github.com/blackbirdworks/gopherstack/pkgs/orcio"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

const nestedSelect = "SELECT 1 AS id, ARRAY[1, 2] AS a, MAP(ARRAY['k'], ARRAY[1.5]) AS m, " +
	"CAST(ROW(7, 'x') AS ROW(n int, s varchar)) AS r, ROW(2, 'y') AS anon"

func TestSDK_CTASWritesNestedColumns(t *testing.T) {
	t.Parallel()

	for _, format := range []string{"PARQUET", "ORC"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()

			defs := map[string]*athena.GlueTableDef{}
			b, store := newEngineBackend(t, map[string][]byte{}, defs)
			client := newTestAthenaClient(t, athena.NewHandler(b))

			sdkRun(t, client, "CREATE DATABASE shop")
			sdkRun(
				t,
				client,
				"CREATE TABLE shop.n WITH (format = '"+format+"', external_location = 's3://lake/n/') AS "+
					nestedSelect,
			)

			want := map[string]string{
				"a": "array<bigint>", "m": "map<string,double>", "r": "struct<n:bigint,s:string>",
				"anon": "struct<_field0:bigint,_field1:string>",
			}

			for _, c := range defs["shop/n"].Columns {
				if w, ok := want[c.Name]; ok {
					assert.Equal(t, w, c.Type, c.Name)
				}
			}

			var file []byte

			store.mu.Lock()
			for k, v := range store.objects {
				if strings.HasPrefix(k, "lake/n/") {
					file = v
				}
			}
			store.mu.Unlock()

			var rows [][]any

			collect := func(cols []string, forEach func([]string, func([]any) error) error) {
				require.NoError(t, forEach(cols, func(v []any) error {
					rows = append(rows, v)

					return nil
				}))
			}

			if format == "ORC" {
				f, err := orcio.Open(file)
				require.NoError(t, err)
				collect([]string{"a", "m", "r", "anon"}, f.ForEach)
			} else {
				f, err := parquetio.Open(file)
				require.NoError(t, err)
				collect([]string{"a", "m", "r", "anon"}, f.ForEach)
			}

			require.Len(t, rows, 1)
			assert.Len(t, rows[0][0], 2)
			assert.Contains(t, rows[0][1], "k")
			assert.Len(t, rows[0][2], 2)

			got := sdkRun(t, client, "SELECT a[2], m['k'], r.n, r.s, anon._field1 FROM shop.n")
			assert.Equal(t, [][]string{{"2", "1.5", "7", "x", "y"}}, got.rows)
		})
	}
}

func TestIceberg_NestedColumns(t *testing.T) {
	t.Parallel()

	for _, format := range []string{"parquet", "orc", "avro"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()

			e := newIceEnv(t)
			e.mustRun(t, "CREATE DATABASE shop")
			e.mustRun(t, "CREATE TABLE n (id bigint, tags array<string>, attrs map<string,double>, "+
				"owner struct<first:string,age:int>) LOCATION 's3://lake/n/' "+
				"TBLPROPERTIES ('table_type'='ICEBERG', 'format'='"+format+"')")
			e.mustRun(t, "INSERT INTO n SELECT 1, ARRAY['a', 'b'], MAP(ARRAY['x'], ARRAY[2.5]), "+
				"CAST(ROW('Ann', 30) AS ROW(first varchar, age int))")
			e.mustRun(t, "INSERT INTO n VALUES (2, NULL, NULL, NULL)")

			assert.Equal(t, [][]string{{"1", "b", "2.5", "Ann", "30"}, {"2", "", "", "", ""}},
				e.mustRun(t, "SELECT id, tags[2], attrs['x'], owner.first, owner.age FROM n ORDER BY id"))

			meta, _ := e.metadata(t, "n")
			schema := meta.CurrentSchema()
			assert.Equal(t, 9, meta.LastColumnID)
			assert.Equal(t, map[string]int{"tags.element": 5}, iceberg.NestedIDs(schema.Fields[1]))

			e.mustRun(t, "DELETE FROM n WHERE id = 2")
			assert.Equal(t, [][]string{{"1"}}, e.mustRun(t, "SELECT id FROM n"))

			e.mustRun(t, "CREATE TABLE c WITH (table_type='ICEBERG', format='"+format+"', location='s3://lake/c/') "+
				"AS SELECT id, tags, owner FROM n")
			assert.Equal(t, [][]string{{"1", "a", "Ann"}}, e.mustRun(t, "SELECT id, tags[1], owner.first FROM c"))
		})
	}
}

func TestIceberg_ParquetNestedFieldIDs(t *testing.T) {
	t.Parallel()

	e := newIceEnv(t)
	e.mustRun(t, "CREATE TABLE n (id bigint, tags array<string>, owner struct<first:string>) LOCATION 's3://lake/n/' "+
		"TBLPROPERTIES ('table_type'='ICEBERG')")
	e.mustRun(t, "INSERT INTO n SELECT 1, ARRAY['a'], CAST(ROW('Ann') AS ROW(first varchar))")

	var file []byte

	e.store.mu.Lock()
	for k, v := range e.store.objects {
		if strings.HasPrefix(k, "lake/n/data/") && strings.HasSuffix(k, ".parquet") {
			file = v
		}
	}
	e.store.mu.Unlock()

	pf, err := parquet.OpenFile(bytes.NewReader(file), int64(len(file)))
	require.NoError(t, err)

	ids := map[string]int{}

	for _, f := range pf.Schema().Fields() {
		ids[f.Name()] = f.ID()

		for _, c := range f.Fields() {
			ids[f.Name()+"."+c.Name()] = c.ID()
		}
	}

	assert.Equal(t, 1, ids["id"])
	assert.Equal(t, 2, ids["tags"])
	assert.Equal(t, 3, ids["owner"])
	assert.Equal(t, 5, ids["owner.first"])
}
