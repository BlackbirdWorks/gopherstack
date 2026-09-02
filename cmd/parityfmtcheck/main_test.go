package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscoverManifests(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "dlm"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "dlm", parityFileName), []byte("service: dlm\n"), 0o600,
	))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "qldb"), 0o755)) // no PARITY.md -- removed service.
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "opsworks"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "opsworks", parityFileName), []byte("service: opsworks\n"), 0o600,
	))

	manifests, err := discoverManifests(dir)
	require.NoError(t, err)
	require.Len(t, manifests, 2)

	assert.Equal(t, "dlm", manifests[0].service)
	assert.Equal(t, "opsworks", manifests[1].service)
}

func TestDiscoverManifests_MissingDir(t *testing.T) {
	t.Parallel()

	_, err := discoverManifests(filepath.Join(t.TempDir(), "does-not-exist"))
	require.Error(t, err)
}

func TestExitCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		results []result
		want    int
	}{
		{
			name:    "no results",
			results: nil,
			want:    exitClean,
		},
		{
			name:    "all clean",
			results: []result{{service: "dlm"}, {service: "opsworks"}},
			want:    exitClean,
		},
		{
			name: "one finding",
			results: []result{
				{service: "dlm"},
				{service: "opsworks", issues: []string{"service: field missing or empty"}},
			},
			want: exitFindings,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, exitCode(tt.results))
		})
	}
}

func TestRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "dlm"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "dlm", parityFileName), []byte("service: dlm\nlast_audit_commit: abc1234\n"), 0o600,
	))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "broken"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "broken", parityFileName), []byte("service: dlm\n"), 0o600,
	))

	results, err := run(dir)
	require.NoError(t, err)
	require.Len(t, results, 2)

	// discoverManifests sorts by slug: "broken" < "dlm".
	require.Len(t, results[0].issues, 1, "broken manifest's service: doesn't match its directory")
	assert.Contains(t, results[0].issues[0], `does not match directory "broken"`)
	assert.Empty(t, results[1].issues, "dlm manifest should be clean")
}
