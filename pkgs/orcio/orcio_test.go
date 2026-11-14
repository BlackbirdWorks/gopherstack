package orcio_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/orcio"
)

func repeat(v int64, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = v
	}

	return out
}

func TestRLEVectorsFromSpec(t *testing.T) {
	t.Parallel()

	seq := func(from, step int64, n int) []int64 {
		out := make([]int64, n)
		for i := range out {
			out[i] = from + step*int64(i)
		}

		return out
	}

	tests := []struct {
		name   string
		in     []byte
		want   []int64
		v2     bool
		signed bool
	}{
		{"v2 short repeat", []byte{0x0a, 0x27, 0x10}, []int64{10000, 10000, 10000, 10000, 10000}, true, false},
		{
			"v2 direct", []byte{0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef},
			[]int64{23713, 43806, 57005, 48879}, true, false,
		},
		{
			"v2 patched base",
			[]byte{
				0x8e, 0x13, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0x64,
				0x6e, 0x78, 0x82, 0x8c, 0x96, 0xa0, 0xaa, 0xb4, 0xbe, 0xfc, 0xe8,
			},
			[]int64{
				2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090, 2100, 2110, 2120, 2130, 2140, 2150,
				2160, 2170, 2180, 2190,
			},
			true, false,
		},
		{
			"v2 delta", []byte{0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46},
			[]int64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}, true, false,
		},
		{"v1 run", []byte{0x61, 0x00, 0x07}, repeat(7, 100), false, false},
		{"v1 descending run", []byte{0x61, 0xff, 0x64}, seq(100, -1, 100), false, false},
		{"v1 literal", []byte{0xfb, 0x02, 0x03, 0x06, 0x07, 0x0b}, []int64{2, 3, 6, 7, 11}, false, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := orcio.DecodeIntsForTest(tc.in, len(tc.want), tc.signed, tc.v2)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	got, err := orcio.DecodeByteRLEForTest([]byte{0xfe, 0x44, 0x45}, 2)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x44, 0x45}, got)
}

func TestWriteReadRoundTrip(t *testing.T) {
	t.Parallel()

	cols := []orcio.Column{
		{Name: "b", Type: "boolean", FieldID: 1},
		{Name: "t", Type: "tinyint", FieldID: 2},
		{Name: "i", Type: "int", FieldID: 3},
		{Name: "l", Type: "bigint", FieldID: 4},
		{Name: "f", Type: "float"},
		{Name: "d", Type: "double"},
		{Name: "s", Type: "string"},
		{Name: "dt", Type: "date"},
		{Name: "ts", Type: "timestamp"},
		{Name: "dec", Type: "decimal(10,2)"},
		{Name: "arr", Type: "array<int>"},
		{Name: "mp", Type: "map<string,double>"},
		{Name: "st", Type: "struct<a:int,b:string>"},
	}
	rows := [][]any{
		{
			true, int64(-3), int64(100000), int64(-1) << 40, 1.5, -2.25, "héllo", "2021-03-04",
			"2021-03-04 05:06:07.250", "-123.45", []any{int64(1), nil, int64(3)},
			map[string]any{"x": 1.5, "y": 2.0}, map[string]any{"a": int64(7), "b": "z"},
		},
		{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{
			false, int64(5), int64(0), int64(0), 0.0, 0.0, "", "1969-12-31",
			"2014-12-31 23:59:59.000", "0.01", []any{}, map[string]any{}, map[string]any{"a": nil, "b": "q"},
		},
	}

	for _, codec := range []string{"NONE", "ZLIB", "SNAPPY", "LZ4", "ZSTD"} {
		t.Run(codec, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			require.NoError(t, orcio.Write(&buf, cols, rows, codec))

			f, err := orcio.Open(buf.Bytes())
			require.NoError(t, err)
			assert.Equal(t, int64(3), f.NumRows())
			assert.Equal(t, map[int]string{1: "b", 2: "t", 3: "i", 4: "l"}, f.ColumnIDs())
			assert.Positive(t, f.ScanBytes([]string{"s"}))

			names := make([]string, len(cols))
			for i, c := range cols {
				names[i] = c.Name
			}

			var got [][]any

			require.NoError(t, f.ForEach(names, func(vals []any) error {
				got = append(got, vals)

				return nil
			}))

			want := [][]any{
				{
					true, int64(-3), int64(100000), int64(-1) << 40, 1.5, -2.25, "héllo", "2021-03-04",
					"2021-03-04 05:06:07.250", "-123.45", []any{int64(1), nil, int64(3)},
					map[string]any{"x": 1.5, "y": 2.0}, map[string]any{"a": int64(7), "b": "z"},
				},
				rows[1],
				{
					false, int64(5), int64(0), int64(0), 0.0, 0.0, "", "1969-12-31",
					"2014-12-31 23:59:59.000", "0.01", []any{}, map[string]any{}, map[string]any{"a": nil, "b": "q"},
				},
			}
			assert.Equal(t, want, got)
		})
	}
}

func TestWriteNestedFromJSONText(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	require.NoError(t, orcio.Write(&buf, []orcio.Column{{Name: "a", Type: "array<struct<k:string,v:bigint>>"}},
		[][]any{{"\x01[{\"k\":\"x\",\"v\":1},{\"k\":\"y\",\"v\":9007199254740993}]"}}, "ZLIB"))

	f, err := orcio.Open(buf.Bytes())
	require.NoError(t, err)

	var got []any

	require.NoError(t, f.ForEach([]string{"a"}, func(vals []any) error {
		got = vals

		return nil
	}))
	assert.Equal(t, []any{[]any{
		map[string]any{"k": "x", "v": int64(1)}, map[string]any{"k": "y", "v": int64(9007199254740993)},
	}}, got)
}

func TestOpenRejectsGarbageAndUnknownTypes(t *testing.T) {
	t.Parallel()

	_, err := orcio.Open([]byte("not an orc file at all"))
	require.ErrorIs(t, err, orcio.ErrBadFile)

	err = orcio.Write(&bytes.Buffer{}, []orcio.Column{{Name: "x", Type: "geometry"}}, nil, "ZLIB")
	require.Error(t, err)

	err = orcio.Write(&bytes.Buffer{}, []orcio.Column{{Name: "x", Type: "int"}}, nil, "LZO")
	require.ErrorIs(t, err, orcio.ErrUnsupported)
}

func TestManyRowsAcrossStripes(t *testing.T) {
	t.Parallel()

	rows := make([][]any, 120000)
	for i := range rows {
		rows[i] = []any{int64(i), "row"}
	}

	var buf bytes.Buffer
	require.NoError(
		t,
		orcio.Write(&buf, []orcio.Column{{Name: "n", Type: "bigint"}, {Name: "s", Type: "string"}}, rows, "SNAPPY"),
	)

	f, err := orcio.Open(buf.Bytes())
	require.NoError(t, err)

	var count, sum int64

	require.NoError(t, f.ForEach([]string{"n"}, func(vals []any) error {
		count++
		sum += vals[0].(int64)

		return nil
	}))
	assert.Equal(t, int64(120000), count)
	assert.Equal(t, int64(120000)*119999/2, sum)
}

func TestReadsFilesWrittenByAnotherImplementation(t *testing.T) {
	t.Parallel()

	for _, codec := range []string{"zlib", "snappy", "zstd", "lz4", "dict"} {
		t.Run(codec, func(t *testing.T) {
			t.Parallel()

			data, err := os.ReadFile("testdata/pyarrow_" + codec + ".orc")
			require.NoError(t, err)

			f, err := orcio.Open(data)
			require.NoError(t, err)
			assert.Equal(t, int64(60), f.NumRows())
			assert.Equal(t, []string{
				"b", "i8", "i16", "i32", "i64", "f", "d", "s", "su", "bin", "dt", "ts", "dec", "arr", "mp", "st",
			}, f.Columns())

			var rows [][]any

			require.NoError(t, f.ForEach(f.Columns(), func(vals []any) error {
				rows = append(rows, vals)

				return nil
			}))
			require.Len(t, rows, 60)
			assert.Equal(t, []any{
				false, int64(-99), int64(-2997), int64(-4999000), int64(-100000007), 0.5, 0.14285714285714285,
				"val1", "unique-1", "\x01A", "2020-01-02", "2020-01-01 00:00:37.001", "0.13", []any{int64(0)},
				map[string]any{"k0": 0.0}, map[string]any{"a": int64(1), "b": "x1"},
			}, rows[1])
			assert.Nil(t, rows[0][13])
			assert.Nil(t, rows[0][15])
		})
	}
}
