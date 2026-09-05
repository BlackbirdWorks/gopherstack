package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrintReport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		results []result
	}{
		{
			name:    "clean",
			results: []result{{service: "dlm", path: "services/dlm/PARITY.md"}},
			want:    "parityfmtcheck: 1 manifests checked, front-matter checks out clean\n",
		},
		{
			name: "one issue",
			results: []result{
				{
					service: "dlm",
					path:    "services/dlm/PARITY.md",
					issues:  []string{"service: field missing or empty"},
				},
			},
			want: "services/dlm/PARITY.md: service: field missing or empty\n" +
				"parityfmtcheck: 1 issue(s) across 1 manifest(s) (see above)\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			printReport(&buf, tt.results)

			assert.Equal(t, tt.want, buf.String())
		})
	}
}

func TestWriteJSON(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "out.json")
	results := []result{
		{service: "dlm", path: "services/dlm/PARITY.md", docSlug: "dlm"},
		{
			service: "broken", path: "services/broken/PARITY.md",
			issues: []string{"service: field missing or empty"},
		},
	}

	require.NoError(t, writeJSON(path, results))

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var got []jsonResult
	require.NoError(t, json.Unmarshal(data, &got))
	require.Len(t, got, 2)

	assert.Equal(t, "dlm", got[0].Service)
	assert.Empty(t, got[0].Issues)
	assert.Equal(t, []string{"service: field missing or empty"}, got[1].Issues)
}
