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
