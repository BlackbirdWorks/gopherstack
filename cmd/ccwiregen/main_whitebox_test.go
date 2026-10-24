package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGeneratedFilesUpToDate(t *testing.T) {
	t.Parallel()

	for _, s := range specs() {
		t.Run(s.module, func(t *testing.T) {
			t.Parallel()

			want, err := render(s)
			require.NoError(t, err)

			got, err := os.ReadFile(filepath.Join("..", "..", s.outFile()))
			require.NoError(t, err)
			assert.Equal(t, string(want), string(got), "run `go run ./cmd/ccwiregen` from the repo root")
		})
	}
}
