package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func servicesFixture() []opService {
	return []opService{
		{
			Service:    "s3",
			SDKModules: []string{"s3"},
			AllOps:     []string{"CreateBucket", "ListBuckets", "ListMultipartUploads"},
		},
		{
			Service:    "glacier",
			SDKModules: []string{"glacier", "s3"},
			AllOps:     []string{"ListMultipartUploads", "UploadArchive"},
		},
		{
			Service:    "dynamodb",
			SDKModules: []string{"dynamodb"},
			AllOps:     []string{"PutItem", "GetItem"},
		},
	}
}

func TestResolveOwner(t *testing.T) {
	t.Parallel()

	idx := buildModuleIndex(servicesFixture())

	tests := []struct {
		name        string
		module      string
		op          string
		wantService string
		wantAmbig   bool
	}{
		{name: "single candidate", module: "dynamodb", op: "PutItem", wantService: "dynamodb"},
		{
			name:   "shared module, exact-name tiebreak picks native owner",
			module: "s3", op: "ListMultipartUploads", wantService: "s3",
		},
		{
			name:   "shared module, op only on non-native candidate is still a single match",
			module: "s3", op: "UploadArchive", wantService: "glacier",
		},
		{name: "unknown module", module: "nope", op: "PutItem", wantService: ""},
		{name: "known module, unknown op", module: "dynamodb", op: "Frobnicate", wantService: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, ambiguous := idx.resolveOwner(tt.module, tt.op)
			assert.Equal(t, tt.wantService, svc)
			assert.Equal(t, tt.wantAmbig, ambiguous)
		})
	}
}

func TestResolveOwner_TrueAmbiguity(t *testing.T) {
	t.Parallel()

	services := []opService{
		{Service: "alpha", SDKModules: []string{"shared"}, AllOps: []string{"DoThing"}},
		{Service: "beta", SDKModules: []string{"shared"}, AllOps: []string{"DoThing"}},
	}
	idx := buildModuleIndex(services)

	svc, ambiguous := idx.resolveOwner("shared", "DoThing")
	assert.Empty(t, svc, "neither candidate's own name matches the module, so no tiebreak applies")
	assert.True(t, ambiguous)
}

// TestResolveOwner_ExactNameTiebreakMirrorsRealRepo pins the real-world
// shape that motivated the tiebreak: bedrock's own source imports
// bedrockagent's SDK module and shares several op names with it (CreateAgent
// et al.), but the exact-name candidate is the true owner.
func TestResolveOwner_ExactNameTiebreakMirrorsRealRepo(t *testing.T) {
	t.Parallel()

	services := []opService{
		{Service: "bedrock", SDKModules: []string{"bedrockagent"}, AllOps: []string{"CreateAgent"}},
		{
			Service:    "bedrockagent",
			SDKModules: []string{"bedrockagent"},
			AllOps:     []string{"CreateAgent"},
		},
	}
	idx := buildModuleIndex(services)

	svc, ambiguous := idx.resolveOwner("bedrockagent", "CreateAgent")
	assert.Equal(t, "bedrockagent", svc)
	assert.False(t, ambiguous)
}

func parseOne(t *testing.T, dir, filename, src string) *ast.File {
	t.Helper()

	path := filepath.Join(dir, filename)
	require.NoError(t, os.WriteFile(path, []byte(src), 0o600))

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	require.NoError(t, err)

	return f
}

func TestParseSDKImports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want fileImports
		name string
		src  string
	}{
		{
			name: "direct import",
			src: `package p
import "github.com/aws/aws-sdk-go-v2/service/s3"
var _ = s3.Client{}
`,
			want: fileImports{"s3": "s3"},
		},
		{
			name: "aliased import",
			src: `package p
import cesdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"
var _ = cesdk.Client{}
`,
			want: fileImports{"cesdk": "costexplorer"},
		},
		{
			name: "types subpackage excluded",
			src: `package p
import "github.com/aws/aws-sdk-go-v2/service/s3/types"
var _ = types.Object{}
`,
			want: fileImports{},
		},
		{
			name: "no sdk import",
			src: `package p
var x = 1
`,
			want: fileImports{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			f := parseOne(t, dir, "f.go", tt.src)
			assert.Equal(t, tt.want, parseSDKImports(f))
		})
	}
}

// TestRun_DirectClientAssign pins the plainest idiom: a helper function that
// directly calls <pkg>.NewFromConfig and returns *pkg.Client, then a test
// that assigns its result and calls an op on it.
func TestRun_DirectClientAssign(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := `package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func createS3Client(t *testing.T) *s3.Client {
	t.Helper()
	return s3.NewFromConfig(nil)
}

func TestBuckets(t *testing.T) {
	client := createS3Client(t)
	client.CreateBucket(nil, nil)
	client.ListBuckets(nil, nil)
	client.Options()
}
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "s3_test.go"), []byte(src), 0o600))

	result, err := run([]string{dir}, servicesFixture())
	require.NoError(t, err)

	assert.Contains(t, result.covered["s3"], "CreateBucket")
	assert.Contains(t, result.covered["s3"], "ListBuckets")
	assert.NotContains(
		t,
		result.covered["s3"],
		"Options",
		"Options is not a real op, so must be filtered by op-set membership",
	)
}

// TestRun_FuncLitParam pins test/integration's dominant closure idiom: a
// func-literal parameter typed *pkg.Client, with no direct NewFromConfig
// call visible in the same function.
func TestRun_FuncLitParam(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := `package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestBucketTable(t *testing.T) {
	tests := []struct {
		verify func(t *testing.T, client *s3.Client)
	}{
		{
			verify: func(t *testing.T, client *s3.Client) {
				client.CreateBucket(nil, nil)
			},
		},
	}
	_ = tests
}
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "s3_test.go"), []byte(src), 0o600))

	result, err := run([]string{dir}, servicesFixture())
	require.NoError(t, err)

	assert.Contains(t, result.covered["s3"], "CreateBucket")
}

// TestRun_ChainedHelper pins the appmesh-style pattern: a helper whose
// signature returns (*Handler, *pkg.Client) -- the client is not the first
// result, and the constructing call itself is one function away from any
// NewFromConfig call, resolved purely via the outer helper's declared
// signature.
func TestRun_ChainedHelper(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := `package svc_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Handler struct{}

func newRoundTripClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	return dynamodb.NewFromConfig(nil)
}

func newTestHandlerAndClient(t *testing.T) (*Handler, *dynamodb.Client) {
	t.Helper()
	return &Handler{}, newRoundTripClient(t)
}

func TestPutItem(t *testing.T) {
	_, client := newTestHandlerAndClient(t)
	client.PutItem(nil, nil)
}
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "svc_test.go"), []byte(src), 0o600))

	result, err := run([]string{dir}, servicesFixture())
	require.NoError(t, err)

	assert.Contains(t, result.covered["dynamodb"], "PutItem")
}

// TestRun_NamedFuncInTable pins the redshift-style idiom: a top-level test
// function typed *pkg.Client on its OWN parameter list, referenced by name
// as a table value (`run: testDescribeThing`) rather than called or defined
// inline as a func literal. Before the entryParams fix, walkFunc only ever
// seeded bindings from nested *ast.FuncLit parameters found while walking a
// function's body, so a client bound solely via the enclosing FuncDecl's own
// signature was invisible -- this exact shape measured 0/60 for redshift
// despite dozens of real client.<Op> calls.
func TestRun_NamedFuncInTable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := `package svc_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func testCreateBucket(t *testing.T, client *s3.Client) {
	client.CreateBucket(nil, nil)
}

func TestTable(t *testing.T) {
	tests := []struct {
		run func(t *testing.T, client *s3.Client)
	}{
		{run: testCreateBucket},
	}
	_ = tests
}
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "s3_test.go"), []byte(src), 0o600))

	result, err := run([]string{dir}, servicesFixture())
	require.NoError(t, err)

	assert.Contains(t, result.covered["s3"], "CreateBucket")
}

// TestRun_AmbiguousModuleSkipped pins that a module shared by two services
// which BOTH declare the called op, with neither an exact-name match, is
// counted nowhere and surfaces in Ambiguous rather than being guessed at.
func TestRun_AmbiguousModuleSkipped(t *testing.T) {
	t.Parallel()

	services := []opService{
		{Service: "alpha", SDKModules: []string{"shared"}, AllOps: []string{"DoThing"}},
		{Service: "beta", SDKModules: []string{"shared"}, AllOps: []string{"DoThing"}},
	}

	dir := t.TempDir()
	src := `package svc_test

import (
	"testing"

	shared "github.com/aws/aws-sdk-go-v2/service/shared"
)

func TestX(t *testing.T) {
	client := shared.NewFromConfig(nil)
	client.DoThing(nil, nil)
}
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "svc_test.go"), []byte(src), 0o600))

	result, err := run([]string{dir}, services)
	require.NoError(t, err)

	assert.Empty(t, result.covered["alpha"])
	assert.Empty(t, result.covered["beta"])
	assert.Len(t, result.ambiguous, 1)
}

func TestBuildReport_WorstFirst(t *testing.T) {
	t.Parallel()

	services := []opService{
		{Service: "big", AllOps: []string{"A", "B", "C", "D"}},
		{Service: "small", AllOps: []string{"A", "B"}},
	}
	result := newCoverageResult()
	result.covered["small"] = map[string]evidence{"A": {file: "x", line: 1}}

	rpt := buildReport(services, result)

	require.Len(t, rpt.Services, 2)
	assert.Equal(
		t,
		"big",
		rpt.Services[0].Service,
		"4 uncovered ops outranks small's 1 uncovered op",
	)
	assert.Equal(t, 4, rpt.Services[0].Total)
	assert.Equal(t, 0, rpt.Services[0].Covered)
	assert.Equal(t, "small", rpt.Services[1].Service)
	assert.Equal(t, 1, rpt.Services[1].Covered)
	assert.Equal(t, []string{"B"}, rpt.Services[1].UncoveredOps)
	assert.Equal(t, 6, rpt.TotalOps)
	assert.Equal(t, 1, rpt.CoveredOps)
}

func TestFindTestFiles(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "a_test.go"), []byte("package p\n"), 0o600))
	require.NoError(
		t,
		os.WriteFile(filepath.Join(root, "sub", "b_test.go"), []byte("package p\n"), 0o600),
	)
	require.NoError(t, os.WriteFile(filepath.Join(root, "helper.go"), []byte("package p\n"), 0o600))

	groups, err := findTestFiles(root)
	require.NoError(t, err)

	assert.Len(t, groups[root], 1)
	assert.Len(t, groups[filepath.Join(root, "sub")], 1)
}
