package lambda_test

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestBaseImageForRuntime(t *testing.T) {
	t.Parallel()

	cases := []struct {
		runtime string
		wantImg string
		wantHit bool
	}{
		{"python3.12", "public.ecr.aws/lambda/python:3.12", true},
		{"python3.11", "public.ecr.aws/lambda/python:3.11", true},
		{"python3.9", "public.ecr.aws/lambda/python:3.9", true},
		{"nodejs20.x", "public.ecr.aws/lambda/nodejs:20", true},
		{"nodejs18.x", "public.ecr.aws/lambda/nodejs:18", true},
		{"java21", "public.ecr.aws/lambda/java:21", true},
		{"dotnet8", "public.ecr.aws/lambda/dotnet:8", true},
		{"ruby3.3", "public.ecr.aws/lambda/ruby:3.3", true},
		{"provided.al2023", "public.ecr.aws/lambda/provided:al2023", true},
		{"unknown-runtime", "", false},
		{"", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.runtime, func(t *testing.T) {
			t.Parallel()

			got := lambda.BaseImageForRuntime(tc.runtime)
			assert.Equal(t, tc.wantImg, got)

			if tc.wantHit {
				assert.NotEmpty(t, got)
			} else {
				assert.Empty(t, got)
			}
		})
	}
}

func TestExtractZip_Basic(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	w := zip.NewWriter(&buf)

	f, err := w.Create("handler.py")
	require.NoError(t, err)

	_, err = f.Write([]byte("def handler(event, context): return 'hello'"))
	require.NoError(t, err)

	require.NoError(t, w.Close())

	dir, err := lambda.ExtractZip(buf.Bytes())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	data, err := os.ReadFile(filepath.Join(dir, "handler.py"))
	require.NoError(t, err)
	assert.Equal(t, "def handler(event, context): return 'hello'", string(data))
}

func TestExtractZip_MultipleFiles(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	w := zip.NewWriter(&buf)

	files := map[string]string{
		"index.js": "exports.handler = async (e) => ({body: 'ok'});",
		"utils.js": "module.exports = {};",
	}

	for name, content := range files {
		f, err := w.Create(name)
		require.NoError(t, err)

		_, err = f.Write([]byte(content))
		require.NoError(t, err)
	}

	require.NoError(t, w.Close())

	dir, err := lambda.ExtractZip(buf.Bytes())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	for name, content := range files {
		fileData, readErr := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, readErr)
		assert.Equal(t, content, string(fileData))
	}
}

func TestExtractZip_InvalidZip(t *testing.T) {
	t.Parallel()

	_, err := lambda.ExtractZip([]byte("not a zip file"))
	require.Error(t, err)
}

func TestExtractZip_Empty(t *testing.T) {
	t.Parallel()

	_, err := lambda.ExtractZip(nil)
	require.Error(t, err)
}

func TestExtractZip_SubDirectory(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	w := zip.NewWriter(&buf)

	f, err := w.Create("subdir/file.py")
	require.NoError(t, err)

	_, err = f.Write([]byte("# content"))
	require.NoError(t, err)

	require.NoError(t, w.Close())

	dir, err := lambda.ExtractZip(buf.Bytes())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	data, err := os.ReadFile(filepath.Join(dir, "subdir", "file.py"))
	require.NoError(t, err)
	assert.Equal(t, "# content", string(data))
}
