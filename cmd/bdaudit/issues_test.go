package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadIssues(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "issues.jsonl")

	content := `{"id":"gopherstack-aaaa","title":"first","status":"open"}
{"id":"gopherstack-bbbb","title":"second","status":"closed","close_reason":"done"}

{"id":"gopherstack-aaaa","title":"first, restated","status":"closed"}
`
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	issues, err := loadIssues(path)
	require.NoError(t, err)

	assert.Len(t, issues, 2)
	assert.Equal(t, "first, restated", issues["gopherstack-aaaa"].Title)
	assert.True(t, issues["gopherstack-aaaa"].closed())
	assert.True(t, issues["gopherstack-bbbb"].closed())
}

func TestLoadIssuesMissingFile(t *testing.T) {
	t.Parallel()

	_, err := loadIssues(filepath.Join(t.TempDir(), "nope.jsonl"))
	require.Error(t, err)
}

func TestLoadIssuesMalformedLine(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "issues.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("not json\n"), 0o600))

	_, err := loadIssues(path)
	require.Error(t, err)
}

func TestIssueClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
		want   bool
	}{
		{name: "closed", status: "closed", want: true},
		{name: "open", status: "open", want: false},
		{name: "in progress", status: "in_progress", want: false},
		{name: "empty", status: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, issue{Status: tt.status}.closed())
		})
	}
}
