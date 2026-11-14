package parquetio_test

import (
	"bytes"
	"encoding/json/v2"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

func collect(t *testing.T, data []byte, cols []string) [][]any {
	t.Helper()

	f, err := parquetio.Open(data)
	require.NoError(t, err)

	var out [][]any

	require.NoError(t, f.ForEach(cols, func(v []any) error {
		out = append(out, v)

		return nil
	}))

	return out
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   any
		want any
		name string
		typ  string
	}{
		{true, true, "boolean", "boolean"},
		{int64(7), int64(7), "tinyint", "tinyint"},
		{"300", int64(300), "smallint", "smallint"},
		{int64(-5), int64(-5), "int", "int"},
		{int64(1 << 40), int64(1 << 40), "bigint", "bigint"},
		{1.5, 1.5, "double", "double"},
		{2.5, 2.5, "float", "float"},
		{"héllo", "héllo", "string", "string"},
		{"abc", "abc", "varchar", "varchar(10)"},
		{"2024-02-29", "2024-02-29", "date", "date"},
		{"2024-02-29 13:14:15.123", "2024-02-29 13:14:15.123", "timestamp", "timestamp"},
		{"-12345.67", "-12345.67", "decimal32", "decimal(7,2)"},
		{"12.5", "12.5000", "decimal64", "decimal(18,4)"},
		{"-99999999999999999999.12345", "-99999999999999999999.12345", "decimal128", "decimal(30,5)"},
		{nil, nil, "null", "int"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			err := parquetio.Write(&buf, []parquetio.Column{{Name: "c", Type: tt.typ}}, [][]any{{tt.in}}, "")
			require.NoError(t, err)

			rows := collect(t, buf.Bytes(), []string{"C"})
			require.Len(t, rows, 1)
			assert.Equal(t, tt.want, rows[0][0])
		})
	}
}

func TestWriteErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		typ  string
		in   any
		comp string
	}{
		{"bad int", "int", "x", ""},
		{"bad date", "date", "nope", ""},
		{"bad decimal", "decimal(5,2)", "abc", ""},
		{"bad compression", "int", int64(1), "LZO"},
		{"bad precision", "decimal(40,2)", "1", ""},
		{"tinyint overflow", "tinyint", int64(300), ""},
		{"smallint overflow", "smallint", int64(70000), ""},
		{"int overflow", "int", int64(1) << 40, ""},
		{"int32 decimal overflow", "decimal(5,2)", "99999", ""},
		{"int64 decimal overflow", "decimal(15,0)", "12345678901234567890", ""},
		{"wide decimal overflow", "decimal(20,2)", "1e40", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := parquetio.Write(&bytes.Buffer{}, []parquetio.Column{{Name: "c", Type: tt.typ}},
				[][]any{{tt.in}}, tt.comp)
			require.Error(t, err)
		})
	}
}

func TestOpenGarbage(t *testing.T) {
	t.Parallel()

	_, err := parquetio.Open([]byte("not parquet"))
	require.ErrorIs(t, err, parquetio.ErrBadFile)
}

func TestCompressions(t *testing.T) {
	t.Parallel()

	for _, c := range []string{"SNAPPY", "gzip", "ZSTD", "NONE", "LZ4"} {
		t.Run(c, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			require.NoError(t, parquetio.Write(&buf, []parquetio.Column{{Name: "s", Type: "string"}},
				[][]any{{"a"}, {"b"}}, c))
			assert.Len(t, collect(t, buf.Bytes(), []string{"s"}), 2)
		})
	}
}

type addr struct {
	City string `parquet:"city"`
	Zip  int32  `parquet:"zip"`
}

type rec struct {
	Name  *string           `parquet:"Name"`
	Addr  addr              `parquet:"addr"`
	Attrs map[string]string `parquet:"attrs"`
	Tags  []string          `parquet:"tags,list"`
	ID    int64             `parquet:"id"`
	Day   int32             `parquet:"d,date"`
}

func nestedFile(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	n := "bob"
	w := parquet.NewGenericWriter[rec](&buf)
	_, err := w.Write([]rec{
		{ID: 1, Name: &n, Addr: addr{"x", 5}, Tags: []string{"a", "b"}, Attrs: map[string]string{"k": "v"}, Day: 19000},
		{ID: 2},
	})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return buf.Bytes()
}

func TestNestedAndProjection(t *testing.T) {
	t.Parallel()

	data := nestedFile(t)
	rows := collect(t, data, []string{"id", "name", "addr", "tags", "attrs", "d", "missing"})

	require.Len(t, rows, 2)
	assert.Equal(t, int64(1), rows[0][0])
	assert.Equal(t, "bob", rows[0][1])
	assert.Equal(t, "x", rows[0][2].(map[string]any)["city"])
	assert.Len(t, rows[0][3], 2)
	assert.NotNil(t, rows[0][4])
	assert.Equal(t, "2022-01-08", rows[0][5])
	assert.Nil(t, rows[0][6])
	assert.Nil(t, rows[1][1])
}

func TestScanBytesPrunes(t *testing.T) {
	t.Parallel()

	f, err := parquetio.Open(nestedFile(t))
	require.NoError(t, err)

	all := f.ScanBytes(f.Columns())
	one := f.ScanBytes([]string{"ID"})

	assert.Positive(t, one)
	assert.Less(t, one, all)
	assert.Equal(t, int64(2), f.NumRows())
}

func TestNoColumnsCountsRows(t *testing.T) {
	t.Parallel()

	assert.Len(t, collect(t, nestedFile(t), nil), 2)
}

func TestWriteKeepsColumnOrder(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	cols := []parquetio.Column{{Name: "z", Type: "int"}, {Name: "a", Type: "string"}, {Name: "m", Type: "double"}}
	require.NoError(t, parquetio.Write(&buf, cols, [][]any{{int64(1), "x", 2.5}}, ""))

	f, err := parquetio.Open(buf.Bytes())
	require.NoError(t, err)
	assert.Equal(t, []string{"z", "a", "m"}, f.Columns())

	rows := collect(t, buf.Bytes(), []string{"m", "z", "a"})
	assert.Equal(t, [][]any{{2.5, int64(1), "x"}}, rows)
}

func TestWriteNestedColumns(t *testing.T) {
	t.Parallel()

	cols := []parquetio.Column{
		{Name: "id", Type: "bigint", FieldID: 1},
		{Name: "arr", Type: "array<int>"},
		{Name: "mp", Type: "map<string,double>"},
		{Name: "st", Type: "struct<a:int,b:array<string>>"},
		{Name: "nest", Type: "array<struct<k:string,v:map<string,bigint>>>"},
	}
	rows := [][]any{
		{
			int64(1), []any{int64(1), nil, int64(3)}, map[string]any{"x": 1.5, "y": nil},
			map[string]any{"a": int64(7), "b": []any{"p", "q"}},
			[]any{map[string]any{"k": "n", "v": map[string]any{"z": int64(9)}}, map[string]any{"k": "m", "v": nil}},
		},
		{int64(2), []any{}, map[string]any{}, map[string]any{"a": nil, "b": nil}, nil},
		{int64(3), nil, nil, nil, []any{}},
		{int64(4), "\x01[5,6]", "\x01{\"k\":2.5}", "{\"a\":1,\"b\":[\"s\"]}", "[{\"k\":\"r\",\"v\":{\"w\":1}}]"},
	}

	var buf bytes.Buffer
	require.NoError(t, parquetio.Write(&buf, cols, rows, "SNAPPY"))

	f, err := parquetio.Open(buf.Bytes())
	require.NoError(t, err)
	assert.Equal(t, int64(4), f.NumRows())

	var got [][]any

	require.NoError(t, f.ForEach([]string{"id", "arr", "mp", "st", "nest"}, func(v []any) error {
		got = append(got, v)

		return nil
	}))
	require.Len(t, got, 4)

	js := func(v any) string {
		b, merr := json.Marshal(v, jsonx.V1())
		require.NoError(t, merr)

		return string(b)
	}

	assert.Equal(t, int64(1), got[0][0])
	assert.JSONEq(t, `[1,null,3]`, js(got[0][1]))
	assert.JSONEq(t, `{"a":7,"b":["p","q"]}`, js(got[0][3]))
	assert.JSONEq(t, `[{"k":"n","v":{"z":9}},{"k":"m","v":null}]`, js(got[0][4]))
	assert.JSONEq(t, `[]`, js(got[1][1]))
	assert.Nil(t, got[2][1])
	assert.Nil(t, got[2][3])
	assert.JSONEq(t, `[]`, js(got[2][4]))
	assert.JSONEq(t, `[5,6]`, js(got[3][1]))
	assert.JSONEq(t, `{"a":1,"b":["s"]}`, js(got[3][3]))
}
