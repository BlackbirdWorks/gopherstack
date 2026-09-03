package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustParseSrc(t *testing.T, src string) ([]*ast.File, *token.FileSet) {
	t.Helper()

	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, "test.go", src, 0)
	require.NoError(t, err)

	return []*ast.File{f}, fset
}

// TestWrapOpResolvesRequestType is gopherstack-4shm's own proof case: a
// request type reached ONLY through service.WrapOp's second type
// parameter -- no literal json.Unmarshal call anywhere -- must still be
// seen and its fields checked. This is the exact shape the bug report
// describes: a scan anchored on literal decode calls alone would find
// nothing here at all.
func TestWrapOpResolvesRequestType(t *testing.T) {
	t.Parallel()

	src := `package svc

import "github.com/blackbirdworks/gopherstack/pkgs/service"

type getFooInput struct {
	Name string ` + "`json:\"Name\"`" + `
	Unread string ` + "`json:\"Unread\"`" + `
}
type getFooOutput struct{}

func (h *Handler) handleGetFoo(ctx context.Context, in *getFooInput) (*getFooOutput, error) {
	_ = in.Name
	return nil, nil
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetFoo": service.WrapOp(h.handleGetFoo),
	}
}

func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for k := range h.ops {
		ops = append(ops, k)
	}
	return ops
}
`
	files, fset := mustParseSrc(t, src)
	scan := scanFiles(files, fset)

	require.Len(t, scan.Dispatch, 1)
	entry := scan.Dispatch[0]
	assert.Equal(t, "GetFoo", entry.Op)
	assert.Equal(t, "wrapop", entry.Anchor)
	assert.Equal(t, "getFooInput", entry.ReqType)

	assert.True(t, scan.Coverage[coverageKey{"getFooInput", "Name"}].Read, "Name is read via in.Name")
	assert.False(t, scan.Coverage[coverageKey{"getFooInput", "Unread"}].Read, "Unread is never referenced")
}

// TestLiteralDecodeSiteLinkedForNonWrapOpOp covers batch's TagResource
// shape: an op named in GetSupportedOperations's static list that is
// dispatched OUTSIDE any WrapOp call (its own json.Unmarshal instead).
func TestLiteralDecodeSiteLinkedForNonWrapOpOp(t *testing.T) {
	t.Parallel()

	src := `package svc

import "encoding/json"

type tagResourceInput struct {
	Tags map[string]string ` + "`json:\"tags\"`" + `
}

func (h *Handler) handleTagResource(body []byte) error {
	var in tagResourceInput
	json.Unmarshal(body, &in)
	return nil
}

func (h *Handler) GetSupportedOperations() []string {
	return []string{"TagResource"}
}
`
	files, fset := mustParseSrc(t, src)
	scan := scanFiles(files, fset)

	require.Len(t, scan.Dispatch, 1)
	assert.Equal(t, "literal", scan.Dispatch[0].Anchor)
	assert.Equal(t, "tagResourceInput", scan.Dispatch[0].ReqType)
}

// TestUnresolvedOpStillCountsInDenominator ensures an op this scan cannot
// resolve at all is still added to the dispatch table -- never silently
// dropped from the coverage fraction's denominator.
func TestUnresolvedOpStillCountsInDenominator(t *testing.T) {
	t.Parallel()

	src := `package svc

func (h *Handler) GetSupportedOperations() []string {
	return []string{"NoSuchHandler"}
}
`
	files, fset := mustParseSrc(t, src)
	scan := scanFiles(files, fset)

	require.Len(t, scan.Dispatch, 1)
	assert.Equal(t, "unresolved", scan.Dispatch[0].Anchor)
}

// TestWholeStructConversionSuppression covers the false-positive shape
// gopherstack-4shm's report calls out explicitly: `SomeType(*req)` uses
// every field of req at once with no per-field selector anywhere. Without
// this suppression every field would wrongly be flagged unread.
func TestWholeStructConversionSuppression(t *testing.T) {
	t.Parallel()

	src := `package svc

import "github.com/blackbirdworks/gopherstack/pkgs/service"

type convertMeInput struct {
	A string ` + "`json:\"A\"`" + `
	B string ` + "`json:\"B\"`" + `
}
type internalReq struct {
	A string
	B string
}
type convertMeOutput struct{}

func (h *Handler) handleConvertMe(ctx context.Context, in *convertMeInput) (*convertMeOutput, error) {
	internal := internalReq(*in)
	_ = internal
	return nil, nil
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"ConvertMe": service.WrapOp(h.handleConvertMe),
	}
}
`
	files, fset := mustParseSrc(t, src)
	scan := scanFiles(files, fset)

	require.Len(t, scan.Dispatch, 1)
	require.Equal(t, "convertMeInput", scan.Dispatch[0].ReqType)

	for _, field := range []string{"A", "B"} {
		info := scan.Coverage[coverageKey{"convertMeInput", field}]
		assert.True(t, info.Read, "field %s should be covered via the whole-struct conversion", field)
		assert.True(t, info.ViaConversion, "field %s should be tagged covered-via-conversion", field)
	}
}

// TestFieldReadInHelperFunction covers the wider-than-single-hop binding
// rule: a helper function that receives the request struct as its own
// typed parameter and reads a field there is caught too, not only the one
// function WrapOp was handed.
func TestFieldReadInHelperFunction(t *testing.T) {
	t.Parallel()

	src := `package svc

import "github.com/blackbirdworks/gopherstack/pkgs/service"

type deleteFooInput struct {
	Name    string ` + "`json:\"Name\"`" + `
	Cascade bool   ` + "`json:\"Cascade\"`" + `
}
type deleteFooOutput struct{}

func validateDelete(in *deleteFooInput) bool {
	return in.Cascade
}

func (h *Handler) handleDeleteFoo(ctx context.Context, in *deleteFooInput) (*deleteFooOutput, error) {
	_ = in.Name
	validateDelete(in)
	return nil, nil
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DeleteFoo": service.WrapOp(h.handleDeleteFoo),
	}
}
`
	files, fset := mustParseSrc(t, src)
	scan := scanFiles(files, fset)

	assert.True(t, scan.Coverage[coverageKey{"deleteFooInput", "Cascade"}].Read,
		"Cascade is read inside validateDelete, a different function than the WrapOp handler")
}

// TestCaseInsensitiveHandlerNameFallback covers route53resolver's real
// AssociateResolverEndpointIpAddress shape: the AWS operation name does not
// capitalize "Ip" as an acronym, but this repo's Go handler name does
// (handleAssociateResolverEndpointIPAddress) -- a bare "handle" + opName
// concatenation must not silently drop this op to unresolved.
func TestCaseInsensitiveHandlerNameFallback(t *testing.T) {
	t.Parallel()

	src := `package svc

import "github.com/blackbirdworks/gopherstack/pkgs/service"

type assocInput struct {
	IPAddress string ` + "`json:\"IpAddress\"`" + `
}
type assocOutput struct{}

func (h *Handler) handleAssociateResolverEndpointIPAddress(
	ctx context.Context, in *assocInput,
) (*assocOutput, error) {
	_ = in.IPAddress
	return nil, nil
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AssociateResolverEndpointIpAddress": service.WrapOp(h.handleAssociateResolverEndpointIPAddress),
	}
}
`
	files, fset := mustParseSrc(t, src)
	scan := scanFiles(files, fset)

	require.Len(t, scan.Dispatch, 1)
	assert.Equal(t, "wrapop", scan.Dispatch[0].Anchor)
	assert.Equal(t, "assocInput", scan.Dispatch[0].ReqType)
}

// TestBuildServiceReport_CoverageFractions exercises the report's
// before/after WrapOp resolution split directly: a 4-op dispatch table
// where one op resolves via WrapOp (with one unread field), one via a
// linked literal decode, and two stay unresolved.
func TestBuildServiceReport_CoverageFractions(t *testing.T) {
	t.Parallel()

	scan := &packageScan{
		Structs: map[string]structDef{
			"fooInput": {
				Name: "fooInput",
				Fields: []fieldDef{
					{Name: "Read", File: "x.go", Line: 1},
					{Name: "Unread", File: "x.go", Line: 2},
				},
			},
			"barInput": {Name: "barInput", Fields: []fieldDef{{Name: "OK", File: "y.go", Line: 1}}},
		},
		Coverage: map[coverageKey]coverageInfo{
			{"fooInput", "Read"}:   {Read: true},
			{"fooInput", "Unread"}: {},
			{"barInput", "OK"}:     {Read: true},
		},
		Dispatch: []dispatchEntry{
			{Op: "GetFoo", Anchor: "wrapop", ReqType: "fooInput"},
			{Op: "TagBar", Anchor: "literal", ReqType: "barInput"},
			{Op: "Unresolved1", Anchor: "unresolved", Reason: "no handler"},
			{Op: "Unresolved2", Anchor: "unresolved", Reason: "no handler"},
		},
	}

	r := buildServiceReport("mysvc", scan)

	assert.Equal(t, 4, r.DispatchTotal)
	assert.Equal(t, 1, r.LiteralOnlyCount)
	assert.Equal(t, 2, r.ResolvedCount)
	assert.Equal(t, 2, r.TypesFound)
	assert.Equal(t, 3, r.FieldsFound)
	assert.Len(t, r.UnresolvedOps, 2)
	require.Len(t, r.FlaggedFields, 1)
	assert.Equal(t, "fooInput", r.FlaggedFields[0].Type)
	assert.Equal(t, "Unread", r.FlaggedFields[0].Field)
	assert.Equal(t, []string{"GetFoo"}, r.FlaggedFields[0].Ops)
}

// TestCollectStaticOpList covers batch's GetSupportedOperations shape: a
// hardcoded []string{} literal mixing plain string literals and resolved
// package consts.
func TestCollectStaticOpList(t *testing.T) {
	t.Parallel()

	src := `package svc

const opFoo = "Foo"

func (h *Handler) GetSupportedOperations() []string {
	return []string{opFoo, "Bar"}
}
`
	files, _ := mustParseSrc(t, src)
	pkgConsts := collectPackageStringConsts(files)
	ops := collectStaticOpList(files, pkgConsts)

	assert.Equal(t, []string{"Foo", "Bar"}, ops)
}

// TestSliceOfStructDispatchTableResolves covers gopherstack-43o8 blind spot
// 1: glue's real shape, a []struct{name string; bind func(*Handler)
// service.JSONOpFunc}{...} dispatch table instead of a map literal. Before
// the fix this found no dispatch entries at all -- 0 of 0, not a plausible
// small number but an invisible one -- and the field never got checked.
func TestSliceOfStructDispatchTableResolves(t *testing.T) {
	t.Parallel()

	src := `package svc

import "github.com/blackbirdworks/gopherstack/pkgs/service"

type getBarInput struct {
	Name   string ` + "`json:\"Name\"`" + `
	Unread string ` + "`json:\"Unread\"`" + `
}
type getBarOutput struct{}

func (h *Handler) handleGetBar(ctx context.Context, in *getBarInput) (*getBarOutput, error) {
	_ = in.Name
	return nil, nil
}

//nolint:gochecknoglobals
var opBindings = []struct {
	bind func(*Handler) service.JSONOpFunc
	name string
}{
	{
		name: "GetBar",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetBar)
		},
	},
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	ops := make(map[string]service.JSONOpFunc, len(opBindings))
	for _, b := range opBindings {
		ops[b.name] = b.bind(h)
	}
	return ops
}

func (h *Handler) GetSupportedOperations() []string {
	names := make([]string, len(opBindings))
	for i, b := range opBindings {
		names[i] = b.name
	}
	return names
}
`
	files, fset := mustParseSrc(t, src)
	scan := scanFiles(files, fset)

	require.Len(t, scan.Dispatch, 1, "the slice-of-struct table must not report an empty (0-of-0) dispatch table")
	entry := scan.Dispatch[0]
	assert.Equal(t, "GetBar", entry.Op)
	assert.Equal(t, "wrapop", entry.Anchor)
	assert.Equal(t, "getBarInput", entry.ReqType)

	assert.True(t, scan.Coverage[coverageKey{"getBarInput", "Name"}].Read)
	assert.False(t, scan.Coverage[coverageKey{"getBarInput", "Unread"}].Read)
}

// TestLocalWrapOpWrapperResolves covers gopherstack-43o8 blind spot 2:
// cognitoidp's wrapAccuracy[I,O](fn) generic wrapper (handler.go:484),
// whose own body is `return service.WrapOp(fn)`. Before the fix, matching
// only the literal selector name "WrapOp" made every call site reached
// through the wrapper invisible.
func TestLocalWrapOpWrapperResolves(t *testing.T) {
	t.Parallel()

	src := `package svc

import "github.com/blackbirdworks/gopherstack/pkgs/service"

func wrapAccuracy[I any, O any](fn func(context.Context, *I) (*O, error)) service.JSONOpFunc {
	return service.WrapOp(fn)
}

type signUpAccurateInput struct {
	Username string ` + "`json:\"Username\"`" + `
	Unread   string ` + "`json:\"Unread\"`" + `
}
type signUpAccurateOutput struct{}

func (h *Handler) handleSignUpAccurate(ctx context.Context, in *signUpAccurateInput) (*signUpAccurateOutput, error) {
	_ = in.Username
	return nil, nil
}

func (h *Handler) authOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"SignUp": wrapAccuracy(h.handleSignUpAccurate),
	}
}
`
	files, fset := mustParseSrc(t, src)
	scan := scanFiles(files, fset)

	require.Len(t, scan.Dispatch, 1)
	entry := scan.Dispatch[0]
	assert.Equal(t, "SignUp", entry.Op)
	assert.Equal(
		t,
		"wrapop",
		entry.Anchor,
		"a call through a local WrapOp-forwarding wrapper must resolve like a direct WrapOp call",
	)
	assert.Equal(t, "signUpAccurateInput", entry.ReqType)

	assert.True(t, scan.Coverage[coverageKey{"signUpAccurateInput", "Username"}].Read)
	assert.False(t, scan.Coverage[coverageKey{"signUpAccurateInput", "Unread"}].Read)
}

// TestSuffixedHandlerNameResolvesThroughDispatchBinder covers gopherstack-
// 43o8 blind spot 3: a handler named handle<Op>Full/Accurate/WithOpts does
// not match a reconstructed handle<Op>. Resolving an op through the value
// actually bound to it in its own dispatch-table entry -- rather than by
// reconstructing "handle"+opName and searching for a matching handler
// name -- sidesteps the naming convention entirely, regardless of suffix.
func TestSuffixedHandlerNameResolvesThroughDispatchBinder(t *testing.T) {
	t.Parallel()

	src := `package svc

import "github.com/blackbirdworks/gopherstack/pkgs/service"

type createUserPoolInput struct {
	PoolName string ` + "`json:\"PoolName\"`" + `
	Unread   string ` + "`json:\"Unread\"`" + `
}
type createUserPoolOutput struct{}

func (h *Handler) handleCreateUserPoolWithOpts(
	ctx context.Context, in *createUserPoolInput,
) (*createUserPoolOutput, error) {
	_ = in.PoolName
	return nil, nil
}

func (h *Handler) userPoolOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateUserPool": service.WrapOp(h.handleCreateUserPoolWithOpts),
	}
}
`
	files, fset := mustParseSrc(t, src)
	scan := scanFiles(files, fset)

	require.Len(t, scan.Dispatch, 1)
	entry := scan.Dispatch[0]
	assert.Equal(t, "CreateUserPool", entry.Op)
	assert.Equal(t, "wrapop", entry.Anchor,
		"handleCreateUserPoolWithOpts must resolve for CreateUserPool despite not matching handle+opName")
	assert.Equal(t, "createUserPoolInput", entry.ReqType)
}

// TestTypeAliasResolvesToStruct covers gopherstack-43o8 blind spot 4:
// glue's `type updateJobFromSourceControlInput = jobSourceControlInput`
// (handler_jobs.go:386) -- a WrapOp handler's request type reached only
// through a Go type alias, invisible to a struct collector that only
// registers ast.StructType TypeSpecs by name.
func TestTypeAliasResolvesToStruct(t *testing.T) {
	t.Parallel()

	src := `package svc

import "github.com/blackbirdworks/gopherstack/pkgs/service"

type jobSourceControlInput struct {
	JobName string ` + "`json:\"JobName\"`" + `
	Unread  string ` + "`json:\"Unread\"`" + `
}
type updateJobFromSourceControlInput = jobSourceControlInput
type updateJobFromSourceControlOutput struct{}

func (h *Handler) handleUpdateJobFromSourceControl(
	ctx context.Context, in *updateJobFromSourceControlInput,
) (*updateJobFromSourceControlOutput, error) {
	_ = in.JobName
	return nil, nil
}

func (h *Handler) jobOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"UpdateJobFromSourceControl": service.WrapOp(h.handleUpdateJobFromSourceControl),
	}
}
`
	files, fset := mustParseSrc(t, src)
	scan := scanFiles(files, fset)

	require.Len(t, scan.Dispatch, 1)
	entry := scan.Dispatch[0]
	assert.Equal(t, "wrapop", entry.Anchor)
	assert.Equal(t, "updateJobFromSourceControlInput", entry.ReqType,
		"the handler's own alias type name must resolve, not just its underlying struct name")

	assert.True(t, scan.Coverage[coverageKey{"updateJobFromSourceControlInput", "JobName"}].Read)
	assert.False(t, scan.Coverage[coverageKey{"updateJobFromSourceControlInput", "Unread"}].Read)
}

// TestAnonymousInlineStructDecodeResolves covers a fifth dispatch shape
// found while validating this fix, not in the original four: opsworks's
// handlers implement service.JSONOpFunc directly (no WrapOp at all) and
// decode their body into an anonymous `var req struct{...}` literal, which
// never gets a name for either the struct collector or the existing
// literal-decode-site linker to key coverage by. Before the fix this
// service reported 0 of 74 resolved.
func TestAnonymousInlineStructDecodeResolves(t *testing.T) {
	t.Parallel()

	src := `package svc

import "encoding/json"

func (h *Handler) handleAssignInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string ` + "`json:\"InstanceId\"`" + `
		Unread     string ` + "`json:\"Unread\"`" + `
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	_ = req.InstanceID

	return map[string]any{}, nil
}

func (h *Handler) GetSupportedOperations() []string {
	return []string{"AssignInstance"}
}
`
	files, fset := mustParseSrc(t, src)
	scan := scanFiles(files, fset)

	require.Len(t, scan.Dispatch, 1)
	entry := scan.Dispatch[0]
	assert.Equal(t, "literal", entry.Anchor, "an anonymous-struct decode outside WrapOp resolves via the literal path")
	require.NotEmpty(t, entry.ReqType)

	assert.True(t, scan.Coverage[coverageKey{entry.ReqType, "InstanceID"}].Read)
	assert.False(t, scan.Coverage[coverageKey{entry.ReqType, "Unread"}].Read)
}

// TestLowConfidenceGuard_ZeroDispatchWithJSONOpFunc proves the coverage
// guard gopherstack-43o8 asked for: a package that mentions
// service.JSONOpFunc but resolves to zero dispatch entries must say so
// loudly rather than silently print (or be skipped as) a clean 0-of-0.
func TestLowConfidenceGuard_ZeroDispatchWithJSONOpFunc(t *testing.T) {
	t.Parallel()

	src := `package svc

import "github.com/blackbirdworks/gopherstack/pkgs/service"

var _ service.JSONOpFunc
`
	files, fset := mustParseSrc(t, src)
	scan := scanFiles(files, fset)
	r := buildServiceReport("svc", scan)

	assert.NotEmpty(t, r.LowConfidence)
}

// TestLowConfidenceGuard_SilentForNonJSONOpFuncPackage is the guard's own
// false-positive check: a package that never mentions service.JSONOpFunc
// at all (this repo's Query/XML-protocol and REST-routed services, e.g.
// sns's map[string]snsActionFn) is legitimately outside this scan's
// documented ground truth. A guard that fired on every such package would
// repeat cmd/enumcheck's own over-broad-detector mistake.
func TestLowConfidenceGuard_SilentForNonJSONOpFuncPackage(t *testing.T) {
	t.Parallel()

	src := `package svc

type actionFn func(body []byte) ([]byte, error)

func (h *Handler) buildActions() map[string]actionFn {
	return map[string]actionFn{}
}
`
	files, fset := mustParseSrc(t, src)
	scan := scanFiles(files, fset)
	r := buildServiceReport("svc", scan)

	assert.Empty(t, r.LowConfidence)
}

// TestLowConfidenceGuard_LowResolvedFraction proves the second guard
// trigger: a JSONOpFunc-using package whose resolved fraction falls below
// lowCoverageThreshold is flagged even when its denominator isn't zero --
// cognitoidp's real pre-fix 62% is exactly this shape.
func TestLowConfidenceGuard_LowResolvedFraction(t *testing.T) {
	t.Parallel()

	scan := &packageScan{
		UsesJSONOpFunc: true,
		Structs:        map[string]structDef{},
		Coverage:       map[coverageKey]coverageInfo{},
		Dispatch: []dispatchEntry{
			{Op: "Resolved", Anchor: "wrapop", ReqType: "fooInput"},
			{Op: "Unresolved1", Anchor: "unresolved", Reason: "no handler"},
			{Op: "Unresolved2", Anchor: "unresolved", Reason: "no handler"},
			{Op: "Unresolved3", Anchor: "unresolved", Reason: "no handler"},
		},
	}

	r := buildServiceReport("svc", scan)

	assert.NotEmpty(t, r.LowConfidence)
}

// TestMethodReceiverBindsRequestFields covers codecommit's real
// mergeBranchesRequest shape: a request struct's own method
// (`func (r mergeBranchesRequest) options()`) reads fields off its
// receiver, never a parameter or a local. Before this fix
// collectLocalBindings bound only a function's parameters and locals, never
// its receiver, so every field read only this way was a FALSE POSITIVE --
// flagged unread despite being read in production code. Table-driven: one
// case for a value receiver (codecommit's real shape), one for a pointer
// receiver, and a control case proving a field genuinely never read by
// anything -- receiver included -- is still reported unread.
func TestMethodReceiverBindsRequestFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		src      string
		field    string
		wantRead bool
	}{
		{
			name: "value receiver reads field",
			src: `package svc

type mergeBranchesRequest struct {
	TargetBranch string ` + "`json:\"targetBranch\"`" + `
}

func (r mergeBranchesRequest) options() string {
	return r.TargetBranch
}
`,
			field:    "TargetBranch",
			wantRead: true,
		},
		{
			name: "pointer receiver reads field",
			src: `package svc

type mergeBranchesRequest struct {
	CommitMessage string ` + "`json:\"commitMessage\"`" + `
}

func (r *mergeBranchesRequest) options() string {
	return r.CommitMessage
}
`,
			field:    "CommitMessage",
			wantRead: true,
		},
		{
			name: "field never read anywhere, receiver included, is still flagged",
			src: `package svc

type mergeBranchesRequest struct {
	Unread string ` + "`json:\"unread\"`" + `
}

func (r mergeBranchesRequest) options() string {
	return "constant"
}
`,
			field:    "Unread",
			wantRead: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			files, fset := mustParseSrc(t, tt.src)
			scan := scanFiles(files, fset)

			info := scan.Coverage[coverageKey{"mergeBranchesRequest", tt.field}]
			assert.Equal(t, tt.wantRead, info.Read)
		})
	}
}

// TestCollectStaticOpList_EmptyWhenBuiltFromMapKeys covers route53resolver/
// workspaces/dms's shape: GetSupportedOperations built at runtime from
// h.ops's own keys has no static []string{} literal to find, so the
// denominator correctly falls back to the WrapOp map's own key set.
func TestCollectStaticOpList_EmptyWhenBuiltFromMapKeys(t *testing.T) {
	t.Parallel()

	src := `package svc

func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for k := range h.ops {
		ops = append(ops, k)
	}
	return ops
}
`
	files, _ := mustParseSrc(t, src)
	pkgConsts := collectPackageStringConsts(files)
	ops := collectStaticOpList(files, pkgConsts)

	assert.Empty(t, ops)
}

// TestLowerKeyedHandlers_Deterministic is gopherstack-fr30's regression
// test for this package's own instance of the bug reqfielddiff's
// findHandlerByName was reported with: two DIFFERENTLY spelled handler
// names that fold to the same lowercase key (handleFooBAR vs handleFoobar
// -- differing only in the casing of an AWS acronym, exactly the shape
// route53resolver's real IPAddress/Ipaddress split is) used to let
// whichever one Go's randomized map iteration visited LAST silently win.
// Runs the index build many times -- Go picks a fresh random start point
// on every `range` over a map, even within one process -- to catch that
// without shelling out to separate `go run` processes.
func TestLowerKeyedHandlers_Deterministic(t *testing.T) {
	t.Parallel()

	wrapOpFuncs := map[string]resolvedHandler{
		"handleFooBAR":    {ReqType: "fooBARInput", File: "a.go", Line: 1},
		"handleFoobar":    {ReqType: "foobarInput", File: "b.go", Line: 2},
		"handleUnrelated": {ReqType: "unrelatedInput", File: "c.go", Line: 3},
	}

	first := lowerKeyedHandlers(wrapOpFuncs)
	require.Equal(t, "fooBARInput", first["handlefoobar"].ReqType,
		"lexicographically smallest original name (handleFooBAR) must win the collision, deterministically")

	const iterations = 200

	for range iterations {
		again := lowerKeyedHandlers(wrapOpFuncs)
		require.Equal(t, first, again, "the collision winner must be identical on every call")
	}
}
