package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveSDKModules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{
			name: "direct import matching directory name",
			files: map[string]string{
				"handler.go": `package svc

import "github.com/aws/aws-sdk-go-v2/service/polly"

var _ = polly.Client{}
`,
			},
			want: []string{"polly"},
		},
		{
			name: "aliased import, module differs from directory name",
			files: map[string]string{
				"handler.go": `package svc

import dms "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"

var _ = dms.Client{}
`,
			},
			want: []string{"databasemigrationservice"},
		},
		{
			name: "multiple distinct sdk imports across files",
			files: map[string]string{
				"handler.go": `package svc

import "github.com/aws/aws-sdk-go-v2/service/sfn"
`,
				"integrations.go": `package svc

import awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

var _ = awss3.Client{}
`,
			},
			want: []string{"s3", "sfn"},
		},
		{
			name: "no sdk import found",
			files: map[string]string{
				"handler.go": `package svc

var x = 1
`,
			},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			for name, content := range tt.files {
				require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
			}

			got := resolveSDKModules(dir)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHasGoFiles(t *testing.T) {
	t.Parallel()

	t.Run("directory with a go file", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.go"), []byte("package svc\n"), 0o600))

		assert.True(t, hasGoFiles(dir))
	})

	t.Run("tombstone directory with only a readme", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("deprecated\n"), 0o600))

		assert.False(t, hasGoFiles(dir))
	})

	t.Run("nonexistent directory", func(t *testing.T) {
		t.Parallel()

		assert.False(t, hasGoFiles(filepath.Join(t.TempDir(), "missing")))
	})
}

// TestCensusService_ConstKeyedOpsTable covers the dms shape that motivated
// this fix: GetSupportedOperations ranges over a struct field ("h.ops")
// built by delegating to family functions that return map literals keyed by
// package-level op-name consts, not string literals. Before this fix, the
// whole-package fallback scan only recognized string-literal map keys and
// missed this shape entirely, producing zero List/Describe/Get ops for a
// service that has plenty.
func TestCensusService_ConstKeyedOpsTable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := `package svc

import "github.com/aws/aws-sdk-go-v2/service/examplesvc"

const (
	opListWidgets   = "ListWidgets"
	opDescribeGizmo = "DescribeGizmo"
	opGetThing      = "GetThing"
)

type Handler struct {
	ops map[string]func()
}

func (h *Handler) buildOps() map[string]func() {
	return map[string]func(){
		opListWidgets:   h.handleListWidgets,
		opDescribeGizmo: h.handleDescribeGizmo,
		opGetThing:      h.handleGetThing,
	}
}

func (h *Handler) GetSupportedOperations() []string {
	names := make([]string, 0, len(h.ops))
	for op := range h.ops {
		names = append(names, op)
	}

	return names
}

var _ = examplesvc.Client{}
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.go"), []byte(src), 0o600))

	got := censusService(dir, "examplesvc")

	assert.False(t, got.Error, "reason: %s", got.ErrorReason)
	assert.Equal(t, resolutionDynamicFallback, got.Resolution)
	assert.Equal(t, 3, got.Total)
	assert.Equal(t, []string{"ListWidgets"}, got.ListOps)
	assert.Equal(t, []string{"DescribeGizmo"}, got.DescribeOps)
	assert.Equal(t, []string{"GetThing"}, got.GetOps)
	assert.Equal(t, []string{"examplesvc"}, got.SDKModules)
	assert.False(t, got.Aliased)
}

func TestCensusService_ErrorRows(t *testing.T) {
	t.Parallel()

	t.Run("no sdk module resolvable is an error, not a zero", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		src := `package svc

func (h *Handler) GetSupportedOperations() []string {
	return []string{"ListThings"}
}
`
		require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.go"), []byte(src), 0o600))

		got := censusService(dir, "unknownsvc")

		require.True(t, got.Error)
		assert.Contains(t, got.ErrorReason, "no aws-sdk-go-v2/service/* import found")
		assert.Equal(t, 1, got.Total, "op resolution itself still succeeded")
	})

	t.Run("no operations resolvable is an error, not a zero", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		src := `package svc

import "github.com/aws/aws-sdk-go-v2/service/emptysvc"

var _ = emptysvc.Client{}
`
		require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.go"), []byte(src), 0o600))

		got := censusService(dir, "emptysvc")

		require.True(t, got.Error)
		assert.Contains(t, got.ErrorReason, "no operations resolved")
		assert.Equal(t, resolutionUnresolved, got.Resolution)
	})

	t.Run("aliased module with resolvable ops is not an error", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		src := `package svc

import dms "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"

func (h *Handler) GetSupportedOperations() []string {
	return []string{"ListThings"}
}

var _ = dms.Client{}
`
		require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.go"), []byte(src), 0o600))

		got := censusService(dir, "dms")

		require.False(t, got.Error)
		assert.True(t, got.Aliased)
		assert.Equal(t, []string{"databasemigrationservice"}, got.SDKModules)
	})
}
