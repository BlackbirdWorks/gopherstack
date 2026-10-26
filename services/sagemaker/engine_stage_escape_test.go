package sagemaker //nolint:testpackage // exercises unexported staging and extraction helpers.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubStore struct{}

func (stubStore) GetObjectBytes(context.Context, string, string) ([]byte, error) {
	return []byte("x"), nil
}

func (stubStore) PutObjectBytes(context.Context, string, string, []byte) error { return nil }

func (stubStore) ListObjectKeys(_ context.Context, _, prefix string) ([]string, error) {
	return []string{prefix + "a.csv"}, nil
}

func TestStageS3_RejectsEscapingSubdir(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sub     string
		wantErr bool
	}{
		{name: "plain", sub: "train"},
		{name: "dotted", sub: "my.channel"},
		{name: "parent", sub: "..", wantErr: true},
		{name: "traversal", sub: "../../escaped", wantErr: true},
		{name: "self", sub: ".", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := t.TempDir()
			root := filepath.Join(base, "root")
			require.NoError(t, os.MkdirAll(root, 0o750))

			dir, err := stageS3(t.Context(), stubStore{}, "s3://b/p/", "S3Prefix", root, tt.sub)
			if tt.wantErr {
				require.ErrorIs(t, err, errUnsafeArchive)
				assert.NoDirExists(t, filepath.Join(base, "escaped"))

				return
			}

			require.NoError(t, err)
			assert.FileExists(t, filepath.Join(dir, "a.csv"))
		})
	}
}

func TestExtractTarGz_RejectsEscapingEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		entry   string
		wantErr bool
	}{
		{name: "nested", entry: "a/b.txt"},
		{name: "traversal", entry: "../evil.txt", wantErr: true},
		{name: "inner traversal", entry: "a/../../evil.txt", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			zw := gzip.NewWriter(&buf)
			tw := tar.NewWriter(zw)
			require.NoError(t, tw.WriteHeader(&tar.Header{Name: tt.entry, Mode: 0o600, Size: 1, Typeflag: tar.TypeReg}))
			_, err := tw.Write([]byte("x"))
			require.NoError(t, err)
			require.NoError(t, tw.Close())
			require.NoError(t, zw.Close())

			base := t.TempDir()
			dest := filepath.Join(base, "dest")
			require.NoError(t, os.MkdirAll(dest, 0o750))

			err = extractTarGz(buf.Bytes(), dest)
			if tt.wantErr {
				require.ErrorIs(t, err, errUnsafeArchive)
				assert.NoFileExists(t, filepath.Join(base, "evil.txt"))

				return
			}

			require.NoError(t, err)
		})
	}
}
