package gcpcloudbuild_test

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

func TestExtractArchive(t *testing.T) {
	t.Parallel()

	evil := func(name string, typ byte) []byte {
		var buf bytes.Buffer

		gz := gzip.NewWriter(&buf)
		tw := tar.NewWriter(gz)
		require.NoError(
			t,
			tw.WriteHeader(
				&tar.Header{
					Name:     name,
					Mode:     0o644,
					Size:     1,
					Typeflag: typ,
					Linkname: "/etc/passwd",
				},
			),
		)

		if typ == tar.TypeReg {
			_, _ = tw.Write([]byte("x"))
		}

		require.NoError(t, tw.Close())
		require.NoError(t, gz.Close())

		return buf.Bytes()
	}

	var zbuf bytes.Buffer

	zw := zip.NewWriter(&zbuf)
	w, err := zw.Create("dir/z.txt")
	require.NoError(t, err)
	_, _ = w.Write([]byte("zip"))
	require.NoError(t, zw.Close())

	tests := []struct {
		name    string
		wantErr string
		file    string
		data    []byte
	}{
		{name: "tar.gz", data: tarGz(t, map[string]string{"a/b.txt": "hi"}), file: "a/b.txt"},
		{name: "zip", data: zbuf.Bytes(), file: "dir/z.txt"},
		{
			name:    "traversal",
			data:    evil("../escape.txt", tar.TypeReg),
			wantErr: "escapes the workspace",
		},
		{name: "absolute", data: evil("/abs.txt", tar.TypeReg), wantErr: "escapes the workspace"},
		{name: "symlinks are skipped", data: evil("link", tar.TypeSymlink)},
		{name: "not an archive", data: []byte("plain text"), wantErr: "must be a .tar.gz or .zip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dest := t.TempDir()
			extractErr := gcpcloudbuild.ExtractArchive(tt.data, dest)

			if tt.wantErr != "" {
				require.ErrorContains(t, extractErr, tt.wantErr)

				return
			}

			require.NoError(t, extractErr)

			if tt.file != "" {
				_, statErr := os.Stat(filepath.Join(dest, tt.file))
				assert.NoError(t, statErr)
			}
		})
	}
}

func TestRegistryTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		image   string
		want    string
		wantErr bool
	}{
		{image: "us-central1-docker.pkg.dev/p/r/app:1", want: "h:1/p/r/app:1"},
		{image: "docker.pkg.dev/p/r/app", want: "h:1/p/r/app:latest"},
		{image: "us-docker.pkg.dev/p/r/sub/app:1", want: "h:1/p/r/sub/app:1"},
		{image: "us-docker.pkg.dev/p/app", wantErr: true},
		{image: "gcr.io/p/app", wantErr: true},
		{image: "app", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.image, func(t *testing.T) {
			t.Parallel()

			got, err := gcpcloudbuild.RegistryTarget("h:1", tt.image)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSubstitute(t *testing.T) {
	t.Parallel()

	vals := map[string]string{"PROJECT_ID": "p", "_A": "a", "_EMPTY": ""}

	tests := []struct {
		name    string
		in      string
		want    string
		loose   bool
		wantErr bool
	}{
		{name: "plain", in: "x", want: "x"},
		{name: "bare", in: "$PROJECT_ID/img", want: "p/img"},
		{name: "braced", in: "${_A}b", want: "ab"},
		{name: "escaped", in: "$$HOME and $$$_A", want: "$HOME and $a"},
		{name: "lowercase untouched", in: "$home ${x}", want: "$home ${x}"},
		{name: "empty value", in: "[$_EMPTY]", want: "[]"},
		{name: "undefined", in: "$_NOPE", wantErr: true},
		{name: "undefined builtin", in: "$NOPE", wantErr: true},
		{name: "undefined loose", in: "[$_NOPE]", loose: true, want: "[]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := &cloudbuildpb.Build{Tags: []string{tt.in}}
			if tt.loose {
				b.Options = &cloudbuildpb.BuildOptions{
					SubstitutionOption: cloudbuildpb.BuildOptions_ALLOW_LOOSE,
				}
			}

			err := gcpcloudbuild.Substitute(b, vals)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, b.GetTags()[0])
		})
	}
}

func TestDependencies(t *testing.T) {
	t.Parallel()

	steps := []*cloudbuildpb.BuildStep{
		{
			Id: "a",
		},
		{Id: "b", WaitFor: []string{"-"}},
		{Id: "c", WaitFor: []string{"a", "b"}},
		{Id: "d"},
		{Id: "e", WaitFor: []string{"a"}},
	}

	assert.Empty(t, gcpcloudbuild.Dependencies(steps, 0))
	assert.Empty(t, gcpcloudbuild.Dependencies(steps, 1))
	assert.Equal(t, []int{0, 1}, gcpcloudbuild.Dependencies(steps, 2))
	assert.Equal(t, []int{0, 1, 2}, gcpcloudbuild.Dependencies(steps, 3))
	assert.Equal(t, []int{0}, gcpcloudbuild.Dependencies(steps, 4))
}

func TestProjectNumber(t *testing.T) {
	t.Parallel()

	assert.Len(t, gcpcloudbuild.ProjectNumber("anything"), 12)
	first, again := gcpcloudbuild.ProjectNumber("p"), gcpcloudbuild.ProjectNumber("p")
	assert.Equal(t, first, again)
	assert.NotEqual(t, gcpcloudbuild.ProjectNumber("p"), gcpcloudbuild.ProjectNumber("q"))
}
