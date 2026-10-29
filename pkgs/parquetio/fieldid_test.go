package parquetio_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

func TestColumnIDs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cols []parquetio.Column
		want map[int]string
	}{
		{"ids written", []parquetio.Column{{Name: "a", Type: "bigint", FieldID: 7}, {Name: "b", Type: "string", FieldID: 9}},
			map[int]string{7: "a", 9: "b"}},
		{"no ids", []parquetio.Column{{Name: "a", Type: "bigint"}}, map[int]string{}},
		{"mixed", []parquetio.Column{{Name: "a", Type: "bigint"}, {Name: "b", Type: "string", FieldID: 2}}, map[int]string{2: "b"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			require.NoError(t, parquetio.Write(&buf, tc.cols, [][]any{make([]any, len(tc.cols))}, "SNAPPY"))

			f, err := parquetio.Open(buf.Bytes())
			require.NoError(t, err)
			assert.Equal(t, tc.want, f.ColumnIDs())
		})
	}
}
