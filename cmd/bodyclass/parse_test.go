package main

import (
	"go/token"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutputFieldCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want int
	}{
		{
			name: "void, only result metadata and marker",
			src: `package svc

type FooOutput struct {
	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}
`,
			want: 0,
		},
		{
			name: "one real field alongside result metadata",
			src: `package svc

type FooOutput struct {
	Name *string

	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}
`,
			want: 1,
		},
		{
			name: "grouped field names count individually",
			src: `package svc

type FooOutput struct {
	A, B *string

	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}
`,
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "api_op_Foo.go"), []byte(tt.src), 0o600))

			n, err := outputFieldCount(token.NewFileSet(), dir, "Foo")

			require.NoError(t, err)
			assert.Equal(t, tt.want, n)
		})
	}
}

func TestOutputFieldCountMissingFile(t *testing.T) {
	t.Parallel()

	_, err := outputFieldCount(token.NewFileSet(), t.TempDir(), "DoesNotExist")

	require.Error(t, err)
}
