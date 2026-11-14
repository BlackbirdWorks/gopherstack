package orcio_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/orcio"
)

func fixtureFiles(tb testing.TB) [][]byte {
	tb.Helper()

	paths, err := filepath.Glob("testdata/*.orc")
	require.NoError(tb, err)
	require.NotEmpty(tb, paths)

	out := make([][]byte, 0, len(paths)+1)

	for _, p := range paths {
		b, rerr := os.ReadFile(p)
		require.NoError(tb, rerr)

		out = append(out, b)
	}

	var buf bytes.Buffer

	require.NoError(tb, orcio.Write(&buf, []orcio.Column{
		{Name: "i", Type: "bigint"}, {Name: "s", Type: "string"}, {Name: "d", Type: "decimal(10,2)"},
		{Name: "t", Type: "timestamp"}, {Name: "n", Type: "array<struct<a:int,b:map<string,double>>>"},
	}, [][]any{
		{
			int64(1),
			"x",
			"1.50",
			"2020-01-02 03:04:05.5",
			[]any{map[string]any{"a": int64(1), "b": map[string]any{"k": 2.5}}},
		},
		{nil, nil, nil, nil, nil},
	}, "ZLIB"))

	return append(out, buf.Bytes())
}

// readAll opens data and reads every column; it must return an error or succeed, never panic.
func readAll(data []byte) error {
	f, err := orcio.Open(data)
	if err != nil {
		return err
	}

	_ = f.NumRows()
	_ = f.ColumnIDs()
	_ = f.ScanBytes(f.Columns())

	return f.ForEach(f.Columns(), func([]any) error { return nil })
}

func TestMalformedFilesNeverPanic(t *testing.T) {
	t.Parallel()

	for _, base := range fixtureFiles(t) {
		size := len(base)

		t.Run("truncate", func(t *testing.T) {
			t.Parallel()

			for _, cut := range []int{0, 1, 2, 3, 4, 10, size / 8, size / 4, size / 2, size - 20, size - 5, size - 2, size - 1} {
				if cut < 0 || cut > size {
					continue
				}

				_ = readAll(append([]byte(nil), base[:cut]...))
			}
		})

		t.Run("flip", func(t *testing.T) {
			t.Parallel()

			// header, stripe data and streams, stripe footer, file footer and postscript
			for off := 0; off < size; off += max(1, size/200) {
				for _, mask := range []byte{0xff, 0x01, 0x80} {
					mut := append([]byte(nil), base...)
					mut[off] ^= mask

					_ = readAll(mut)
				}
			}

			for off := max(0, size-48); off < size; off++ {
				mut := append([]byte(nil), base...)
				mut[off] ^= 0xff

				_ = readAll(mut)
			}
		})
	}
}

func TestMalformedFilesReportErrors(t *testing.T) {
	t.Parallel()

	for _, base := range fixtureFiles(t) {
		require.NoError(t, readAll(base))
		require.Error(t, readAll(base[:len(base)/2]))
		require.Error(t, readAll(base[:3]))
		require.Error(t, readAll(nil))
	}
}

func FuzzRead(f *testing.F) {
	for _, b := range fixtureFiles(f) {
		f.Add(b)
	}

	f.Fuzz(func(_ *testing.T, data []byte) { _ = readAll(data) })
}
