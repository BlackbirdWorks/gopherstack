package athena_test

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/orcio"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

const (
	orcSerde = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
	orcInput = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
)

func pyarrowORC(t *testing.T) []byte {
	t.Helper()

	b, err := os.ReadFile("../../pkgs/orcio/testdata/pyarrow_zlib.orc")
	require.NoError(t, err)

	return b
}

func orcEventsDef() *athena.GlueTableDef {
	return &athena.GlueTableDef{
		Name: "orcs", Location: "s3://lake/orcs/", SerdeLibrary: orcSerde, InputFormat: orcInput,
		Columns: []athena.Column{
			{Name: "b", Type: "boolean"}, {Name: "i8", Type: "tinyint"}, {Name: "i32", Type: "int"},
			{Name: "i64", Type: "bigint"}, {Name: "d", Type: "double"}, {Name: "s", Type: "string"},
			{Name: "dt", Type: "date"}, {Name: "ts", Type: "timestamp"}, {Name: "dec", Type: "decimal(12,2)"},
			{Name: "arr", Type: "array<int>"}, {Name: "mp", Type: "map<string,double>"},
			{Name: "st", Type: "struct<a:bigint,b:string>"},
		},
	}
}

func TestSDK_ORCRead(t *testing.T) {
	t.Parallel()

	client := sdkClientFor(t, map[string][]byte{"lake/orcs/a.orc": pyarrowORC(t)},
		map[string]*athena.GlueTableDef{"shop/orcs": orcEventsDef()})

	cases := []struct {
		name, sql string
		want      [][]string
	}{
		{"count", "SELECT count(*) FROM orcs", [][]string{{"60"}}},
		{
			"scalars", "SELECT b, i8, i32, i64, d, s, dt, ts, dec FROM orcs WHERE i64 = -100000007",
			[][]string{{"false", "-99", "-4999000", "-100000007", "0.14285714285714285", "val1", "2020-01-02",
				"2020-01-01 00:00:37.001", "0.13"}},
		},
		{"nested", "SELECT arr[1], mp['k0'], st.a, st.b FROM orcs WHERE i64 = -100000007",
			[][]string{{"0", "0", "1", "x1"}}},
		{"nulls", "SELECT count(*) FROM orcs WHERE s IS NULL", [][]string{{"12"}}},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.want, sdkRun(t, client, tc.sql).rows, tc.name)
	}
}

func TestSDK_ORCLegacyColumnNames(t *testing.T) {
	t.Parallel()

	data := mustORC(t, []orcio.Column{{Name: "_col0", Type: "int"}, {Name: "_col1", Type: "string"}},
		[][]any{{int64(1), "one"}, {int64(2), "two"}})
	def := &athena.GlueTableDef{
		Name: "legacy", Location: "s3://lake/legacy/", SerdeLibrary: orcSerde, InputFormat: orcInput,
		Columns: []athena.Column{{Name: "id", Type: "int"}, {Name: "label", Type: "string"}},
	}
	client := sdkClientFor(
		t,
		map[string][]byte{"lake/legacy/a.orc": data},
		map[string]*athena.GlueTableDef{"shop/legacy": def},
	)

	assert.Equal(t, [][]string{{"1", "one"}, {"2", "two"}},
		sdkRun(t, client, "SELECT id, label FROM legacy ORDER BY id").rows)
}

func mustORC(t *testing.T, cols []orcio.Column, rows [][]any) []byte {
	t.Helper()

	var b bytes.Buffer

	require.NoError(t, orcio.Write(&b, cols, rows, "ZLIB"))

	return b.Bytes()
}

func TestSDK_ORCWriteRoundTrip(t *testing.T) {
	t.Parallel()

	for _, codec := range []string{"", "ZLIB", "SNAPPY", "LZ4", "ZSTD", "NONE"} {
		t.Run("codec="+codec, func(t *testing.T) {
			t.Parallel()

			b, store := newEngineBackend(t,
				map[string][]byte{"lake/orcs/a.orc": pyarrowORC(t)},
				map[string]*athena.GlueTableDef{"shop/orcs": orcEventsDef()})
			client := newTestAthenaClient(t, athena.NewHandler(b))

			props := ""
			if codec != "" {
				props = ", write_compression = '" + codec + "'"
			}

			sdkRun(t, client, "CREATE DATABASE shop")
			sdkRun(t, client, "CREATE TABLE shop.copy WITH (format = 'ORC', external_location = 's3://lake/copy/'"+
				props+") AS SELECT i64, s, dt, ts, dec, d FROM orcs WHERE i32 IS NOT NULL")

			store.mu.Lock()

			var file []byte

			for k, v := range store.objects {
				if strings.HasPrefix(k, "lake/copy/") {
					file = v
				}
			}
			store.mu.Unlock()

			f, err := orcio.Open(file)
			require.NoError(t, err)
			assert.Equal(t, []string{"i64", "s", "dt", "ts", "dec", "d"}, f.Columns())

			got := sdkRun(t, client, "SELECT i64, s, dt, ts, dec FROM shop.copy WHERE i64 = -100000007")
			assert.Equal(
				t,
				[][]string{{"-100000007", "val1", "2020-01-02", "2020-01-01 00:00:37.001", "0.13"}},
				got.rows,
			)

			sdkRun(
				t,
				client,
				"INSERT INTO shop.copy SELECT i64 + 1, s, dt, ts, dec, d FROM orcs WHERE i64 = -100000007",
			)
			assert.Equal(
				t,
				[][]string{{"2"}},
				sdkRun(t, client, "SELECT count(*) FROM shop.copy WHERE i64 IN (-100000007, -100000006)").rows,
			)
		})
	}
}

func TestSDK_ORCInsertNestedColumns(t *testing.T) {
	t.Parallel()

	b, store := newEngineBackend(t,
		map[string][]byte{"lake/orcs/a.orc": pyarrowORC(t)},
		map[string]*athena.GlueTableDef{"shop/orcs": orcEventsDef()})
	client := newTestAthenaClient(t, athena.NewHandler(b))

	sdkRun(t, client, "INSERT INTO orcs SELECT b, i8, i32, i64 + 1000, d, s, dt, ts, dec, arr, mp, st "+
		"FROM orcs WHERE i64 = -100000007")

	got := sdkRun(t, client, "SELECT i64, arr[1], mp['k0'], st.a, st.b FROM orcs WHERE i64 = -99999007")
	assert.Equal(t, [][]string{{"-99999007", "0", "0", "1", "x1"}}, got.rows)

	store.mu.Lock()
	defer store.mu.Unlock()

	var written []byte

	for k, v := range store.objects {
		if strings.HasPrefix(k, "lake/orcs/") && k != "lake/orcs/a.orc" {
			written = v
		}
	}

	f, err := orcio.Open(written)
	require.NoError(t, err)

	var rows [][]any

	require.NoError(t, f.ForEach([]string{"arr", "mp", "st"}, func(v []any) error {
		rows = append(rows, v)

		return nil
	}))
	require.Len(t, rows, 1)
	assert.Equal(
		t,
		[]any{[]any{int64(0)}, map[string]any{"k0": 0.0}, map[string]any{"a": int64(1), "b": "x1"}},
		rows[0],
	)
}

func TestSDK_ORCUnload(t *testing.T) {
	t.Parallel()

	b, store := newEngineBackend(t,
		map[string][]byte{"lake/orcs/a.orc": pyarrowORC(t)},
		map[string]*athena.GlueTableDef{"shop/orcs": orcEventsDef()})
	client := newTestAthenaClient(t, athena.NewHandler(b))

	sdkRun(t, client, "UNLOAD (SELECT i64, s FROM orcs WHERE i64 < -50000000) TO 's3://lake/unload/' "+
		"WITH (format = 'ORC', compression = 'SNAPPY')")

	store.mu.Lock()
	defer store.mu.Unlock()

	var n int64

	for k, v := range store.objects {
		if strings.HasPrefix(k, "lake/unload/") {
			f, err := orcio.Open(v)
			require.NoError(t, err)

			n += f.NumRows()
		}
	}

	assert.Positive(t, n)
}

func TestSDK_ORCWriteErrors(t *testing.T) {
	t.Parallel()

	client := sdkClientFor(t, map[string][]byte{"lake/orcs/a.orc": pyarrowORC(t)},
		map[string]*athena.GlueTableDef{"shop/orcs": orcEventsDef()})
	sdkRun(t, client, "CREATE DATABASE shop")

	assert.Contains(t, sdkFailure(t, client,
		"CREATE TABLE shop.o WITH (format = 'ORC', write_compression = 'GZIP', external_location = 's3://lake/o/') "+
			"AS SELECT i64 FROM orcs"), "compression GZIP is not supported for ORC")
}
