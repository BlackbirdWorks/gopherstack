package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestHasErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		results []serviceResult
		want    bool
	}{
		{
			name:    "no results",
			results: nil,
			want:    false,
		},
		{
			name:    "all clean",
			results: []serviceResult{{Service: "s3"}, {Service: "ec2"}},
			want:    false,
		},
		{
			name:    "one error row",
			results: []serviceResult{{Service: "s3"}, {Service: "ssm", Error: true}},
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, hasErrors(tt.results))
		})
	}
}

// TestCensusService_RealServices pins the actual defect gopherstack-jq8x
// tracks: on the code this repo carried before this fix, ssm and
// route53resolver each resolve zero operations -- not because they have
// none (ssm alone dispatches 152, hand-verified by calling
// ssm.NewHandler(...).GetSupportedOperations() directly), but because their
// GetSupportedOperations reads its ops table through a shape the walker
// couldn't follow: ssm via `slices.Collect(maps.Keys(h.ops))` (no for-range
// at all), route53resolver via a bare `return h.supportedOpsCache` two hops
// removed from the map that actually builds it. Both used to print as a
// bare 0, indistinguishable from a real small, clean service -- the exact
// failure mode this issue exists to close. This test fails against that
// prior code (resolution comes back "unresolved", Total 0 for both) and
// passes against this file.
func TestCensusService_RealServices(t *testing.T) {
	t.Parallel()

	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	servicesDir := filepath.Join(repoRoot, "services")

	tests := []struct {
		name     string
		total    int
		ldg      int
		listOps  int
		describe int
		getOps   int
	}{
		{name: "ssm", total: 152, ldg: 80, listOps: 19, describe: 33, getOps: 28},
		{name: "route53resolver", total: 72, ldg: 32, listOps: 17, describe: 0, getOps: 15},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := filepath.Join(servicesDir, tt.name)
			if _, statErr := os.Stat(dir); statErr != nil {
				t.Skipf("services/%s not present: %v", tt.name, statErr)
			}

			got := censusService(dir, tt.name)

			require.False(t, got.Error, "reason: %s", got.ErrorReason)
			require.NotEqual(t, resolutionUnresolved, got.Resolution,
				"a service with real handlers must never report as unresolved -- that's a silent zero")
			assert.Equal(t, tt.total, got.Total, "hand-verified against NewHandler(...).GetSupportedOperations()")
			assert.Equal(t, tt.ldg, got.LDG)
			assert.Len(t, got.ListOps, tt.listOps)
			assert.Len(t, got.DescribeOps, tt.describe)
			assert.Len(t, got.GetOps, tt.getOps)
		})
	}
}

// TestCensusService_FieldChase pins the two indirection shapes that
// motivated gopherstack-jq8x, in isolation from the real services (which
// can change their op counts over time). Neither uses a for-range over the
// ops field, which is the only shape the walker recognized before this fix.
func TestCensusService_FieldChase(t *testing.T) {
	t.Parallel()

	t.Run("single hop through maps.Keys/slices.Collect, ssm's shape", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		src := `package svc

import (
	"maps"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/examplesvc"
)

type actionFn func()

type Handler struct {
	ops map[string]actionFn
}

func NewHandler() *Handler {
	h := &Handler{}
	h.ops = h.buildOps()

	return h
}

func (h *Handler) buildOps() map[string]actionFn {
	return map[string]actionFn{
		"ListWidgets":   h.listWidgets,
		"DescribeGizmo": h.describeGizmo,
	}
}

func (h *Handler) listWidgets()   {}
func (h *Handler) describeGizmo() {}

func (h *Handler) GetSupportedOperations() []string {
	ops := slices.Collect(maps.Keys(h.ops))
	slices.Sort(ops)

	return ops
}

var _ = examplesvc.Client{}
`
		require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.go"), []byte(src), 0o600))

		got := censusService(dir, "examplesvc")

		require.False(t, got.Error, "reason: %s", got.ErrorReason)
		assert.NotEqual(t, resolutionUnresolved, got.Resolution)
		assert.Equal(t, []string{"ListWidgets"}, got.ListOps)
		assert.Equal(t, []string{"DescribeGizmo"}, got.DescribeOps)
		assert.Equal(t, 2, got.Total)
	})

	t.Run("two hops through an intermediate local variable, route53resolver's shape", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		src := `package svc

import (
	"github.com/aws/aws-sdk-go-v2/service/examplesvc"
)

type actionFn func()

type Handler struct {
	ops   map[string]actionFn
	cache []string
}

func NewHandler() *Handler {
	h := &Handler{}
	h.ops = h.buildOps()
	keys := sortedKeys(h.ops)
	h.cache = keys

	return h
}

func sortedKeys(m map[string]actionFn) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}

func (h *Handler) buildOps() map[string]actionFn {
	return map[string]actionFn{
		"GetThing": h.getThing,
	}
}

func (h *Handler) getThing() {}

func (h *Handler) GetSupportedOperations() []string {
	return h.cache
}

var _ = examplesvc.Client{}
`
		require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.go"), []byte(src), 0o600))

		got := censusService(dir, "examplesvc")

		require.False(t, got.Error, "reason: %s", got.ErrorReason)
		assert.NotEqual(t, resolutionUnresolved, got.Resolution)
		assert.Equal(t, []string{"GetThing"}, got.GetOps)
		assert.Equal(t, 1, got.Total)
	})
}

// TestCensusService_HandlerNotWalked pins the false-positive this fix had to
// avoid while closing jq8x: once the walker can chase through a builder
// method to a dispatch table (needed for ssm/route53resolver), the same
// mechanism can just as easily wander into an operation HANDLER wired into
// that table as a value, and start counting whatever op-name-shaped strings
// that handler's own business logic happens to contain. Both a map literal
// with a handler value and a struct literal with a named field (network
// manager's route{op: "...", fn: h.someHandler} shape) are covered.
func TestCensusService_HandlerNotWalked(t *testing.T) {
	t.Parallel()

	t.Run("map dispatch table value is a handler, not more of the table", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		src := `package svc

import "github.com/aws/aws-sdk-go-v2/service/examplesvc"

type actionFn func()

type Handler struct {
	ops map[string]actionFn
}

func NewHandler() *Handler {
	h := &Handler{}
	h.ops = h.buildOps()

	return h
}

func (h *Handler) buildOps() map[string]actionFn {
	return map[string]actionFn{
		"ListWidgets": h.listWidgets,
	}
}

// listWidgets is a handler, not a table-builder: its own body mentions a
// string that matches the op-name shape but is not an operation.
func (h *Handler) listWidgets() {
	_ = "NotARealOperation"
}

func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for op := range h.ops {
		ops = append(ops, op)
	}

	return ops
}

var _ = examplesvc.Client{}
`
		require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.go"), []byte(src), 0o600))

		got := censusService(dir, "examplesvc")

		require.False(t, got.Error, "reason: %s", got.ErrorReason)
		assert.Equal(t, []string{"ListWidgets"}, got.AllOps)
		assert.NotContains(t, got.AllOps, "NotARealOperation")
	})

	t.Run("struct dispatch table entry with a handler field, networkmanager's shape", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		src := `package svc

import "github.com/aws/aws-sdk-go-v2/service/examplesvc"

type route struct {
	op string
	fn func()
}

type Handler struct{}

func (h *Handler) routeTable() []route {
	return []route{
		{op: "ListWidgets", fn: h.listWidgets},
	}
}

// listWidgets is a handler, not a table-builder: its own body mentions a
// string that matches the op-name shape but is not an operation.
func (h *Handler) listWidgets() {
	_ = "NotARealOperation"
}

func (h *Handler) GetSupportedOperations() []string {
	table := h.routeTable()
	ops := make([]string, 0, len(table))
	for _, r := range table {
		ops = append(ops, r.op)
	}

	return ops
}

var _ = examplesvc.Client{}
`
		require.NoError(t, os.WriteFile(filepath.Join(dir, "handler.go"), []byte(src), 0o600))

		got := censusService(dir, "examplesvc")

		require.False(t, got.Error, "reason: %s", got.ErrorReason)
		assert.Equal(t, []string{"ListWidgets"}, got.AllOps)
		assert.NotContains(t, got.AllOps, "NotARealOperation")
	})
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

// TestCensusAll_ErrorsSurfaceLoudly is the top-level guard for the exit-code
// behavior gopherstack-jq8x asks for: main() must exit non-zero whenever any
// service could not be fully resolved, so a caller that only checks the
// process exit code -- not the printed table -- still can't mistake an
// unresolved service for a clean one.
func TestCensusAll_ErrorsSurfaceLoudly(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "brokensvc"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "brokensvc", "handler.go"), []byte(`package svc
`), 0o600))

	results, err := censusAll(dir)
	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.True(t, hasErrors(results),
		"a service with no GetSupportedOperations method must be flagged, not silently dropped")
}

// TestCensusAll_TombstoneSkipped confirms qldb/qldbsession's shape --
// a directory with no .go files at all -- is excluded from the census
// rather than counted as a 0-op row, which would again be a silent zero
// wearing a different disguise.
func TestCensusAll_TombstoneSkipped(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "qldb"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "qldb", "README.md"), []byte("deprecated\n"), 0o600))

	results, err := censusAll(dir)
	require.NoError(t, err)
	assert.Empty(t, results)
}
