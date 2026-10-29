package deltalog_test

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/deltalog"
)

type memStorage map[string][]byte

func (m memStorage) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string

	for k := range m {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)

	return keys, nil
}

func (m memStorage) Get(_ context.Context, key string) ([]byte, error) {
	b, ok := m[key]
	if !ok {
		return nil, assert.AnError
	}

	return b, nil
}

const schema = `{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},` +
	`{"name":"tags","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{}}]}`

type cpAdd struct {
	PartitionValues map[string]string `parquet:"partitionValues"`
	Path            string            `parquet:"path"`
	Size            int64             `parquet:"size"`
}

type cpMeta struct {
	Configuration    map[string]string `parquet:"configuration"`
	SchemaString     string            `parquet:"schemaString"`
	PartitionColumns []string          `parquet:"partitionColumns"`
}

type cpProto struct {
	MinReaderVersion int32 `parquet:"minReaderVersion"`
}

type cpRow struct {
	Add      *cpAdd   `parquet:"add,optional"`
	MetaData *cpMeta  `parquet:"metaData,optional"`
	Protocol *cpProto `parquet:"protocol,optional"`
}

func checkpoint(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[cpRow](&buf)
	_, err := w.Write([]cpRow{
		{Protocol: &cpProto{MinReaderVersion: 1}},
		{MetaData: &cpMeta{SchemaString: schema, PartitionColumns: []string{}, Configuration: map[string]string{}}},
		{Add: &cpAdd{Path: "a.parquet", Size: 5, PartitionValues: map[string]string{}}},
		{Add: &cpAdd{Path: "b%20c.parquet", Size: 6, PartitionValues: map[string]string{}}},
	})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return buf.Bytes()
}

func TestLoad(t *testing.T) {
	t.Parallel()

	meta := `{"metaData":{"schemaString":` + quote(schema) + `,"partitionColumns":[],"configuration":{}}}`

	cases := []struct {
		wantErr   error
		files     memStorage
		name      string
		wantPaths []string
		wantCols  []string
		wantVer   int64
	}{
		{
			name: "commits replay in order",
			files: memStorage{
				"_delta_log/00000000000000000000.json": []byte(
					meta + "\n" + `{"add":{"path":"a.parquet","size":1}}` + "\n",
				),
				"_delta_log/00000000000000000001.json": []byte(`{"add":{"path":"b%20c.parquet","size":1}}` + "\n" +
					`{"remove":{"path":"a.parquet"}}` + "\n"),
			},
			wantPaths: []string{"b c.parquet"}, wantVer: 1, wantCols: []string{"bigint", "array<string>"},
		},
		{
			name: "checkpoint then later commit",
			files: memStorage{
				"_delta_log/00000000000000000010.checkpoint.parquet": checkpoint(t),
				"_delta_log/_last_checkpoint":                        []byte(`{"version":10,"size":4}`),
				"_delta_log/00000000000000000011.json":               []byte(`{"remove":{"path":"a.parquet"}}` + "\n"),
			},
			wantPaths: []string{"b c.parquet"}, wantVer: 11, wantCols: []string{"bigint", "array<string>"},
		},
		{
			name: "checkpoint without pointer",
			files: memStorage{
				"_delta_log/00000000000000000010.checkpoint.parquet": checkpoint(t),
			},
			wantPaths: []string{"a.parquet", "b c.parquet"}, wantVer: 10, wantCols: []string{"bigint", "array<string>"},
		},
		{name: "empty log", files: memStorage{}, wantErr: deltalog.ErrNotDeltaTable},
		{
			name: "deletion vectors",
			files: memStorage{
				"_delta_log/00000000000000000000.json": []byte(meta + "\n" +
					`{"add":{"path":"a.parquet","deletionVector":{"storageType":"u","pathOrInlineDv":"x"}}}` + "\n"),
			},
			wantErr: deltalog.ErrUnsupported,
		},
		{
			name: "gap",
			files: memStorage{
				"_delta_log/00000000000000000000.json": []byte(meta + "\n"),
				"_delta_log/00000000000000000002.json": []byte(`{}` + "\n"),
			},
			wantErr: deltalog.ErrBadLog,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			snap, err := deltalog.Load(t.Context(), tc.files)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantVer, snap.Version)

			var paths, cols []string
			for _, f := range snap.Files {
				paths = append(paths, f.Path)
			}

			for _, c := range snap.Columns {
				cols = append(cols, c.Type)
			}

			assert.Equal(t, tc.wantPaths, paths)
			assert.Equal(t, tc.wantCols, cols)
		})
	}
}

func quote(s string) string {
	var sb strings.Builder

	sb.WriteByte('"')

	for _, r := range s {
		if r == '"' || r == '\\' {
			sb.WriteByte('\\')
		}

		sb.WriteRune(r)
	}

	sb.WriteByte('"')

	return sb.String()
}
