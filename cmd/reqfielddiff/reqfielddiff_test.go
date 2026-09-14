package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parseSrc parses one in-memory Go source file into a *packageIndex, the
// same entry point buildPackageIndex uses for a real services/<dir> --
// fixtures below never touch the filesystem.
func parseSrc(t *testing.T, src string) *packageIndex {
	t.Helper()

	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, "fixture.go", src, 0)
	require.NoError(t, err)

	return buildPackageIndexFromFiles([]*ast.File{f}, fset, "")
}

func mustField(name, docText string, required bool) sdkField {
	return sdkField{Name: name, Type: "*string", DocText: docText, Required: required}
}

func TestNormalizeWireName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"pascal", "RetentionMode", "retentionmode"},
		{"camel", "retentionMode", "retentionmode"},
		{"snake", "retention_mode", "retentionmode"},
		{"mixedAcronym", "IPAddress", "ipaddress"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, normalizeWireName(tt.in))
		})
	}
}

func TestFindMissing_AgreeingService(t *testing.T) {
	t.Parallel()

	op := sdkOp{Name: "GetThing", Fields: []sdkField{mustField("ThingId", "", true)}}
	res := opResolution{
		Fields:    map[string]emuField{"thingid": {WireName: "thingId", GoName: "ThingID"}},
		Found:     true,
		HasSignal: true,
	}

	require.Empty(t, findMissing(op, res))
}

func TestTriageOne_DocumentedDefaultRanksTop(t *testing.T) {
	t.Parallel()

	m := missingField{Op: "StartRun", Field: mustField(
		"RetentionMode",
		"The retention mode for the run. The default value is RETAIN.",
		false,
	)}

	f := triageOne(m, map[string]bool{})
	require.Equal(t, tierDocumentedDefault, f.Tier)
	require.Contains(t, f.Signals, "documented default")
}

func TestTriageOne_OutputOnlyLikeFieldRanksLow(t *testing.T) {
	t.Parallel()

	// A field with no default language, not a collection op, and declared
	// nowhere else in the service -- exactly the "no strong signal" shape
	// this tool disclosed it can't distinguish from a real bug.
	m := missingField{Op: "CreateWidget", Field: mustField("EngineSettings", "Engine-specific settings.", false)}

	f := triageOne(m, map[string]bool{})
	require.Equal(t, tierNoSignal, f.Tier)
	require.Empty(t, f.Signals)
}

func TestTriageOne_DeprecatedExcluded(t *testing.T) {
	t.Parallel()

	m := missingField{Op: "GetThing", Field: mustField("LegacyId", "Deprecated: use ThingId instead.", false)}

	f := triageOne(m, map[string]bool{})
	require.True(t, f.Deprecated)
}

func TestTriageOne_CollectionFilterSignal(t *testing.T) {
	t.Parallel()

	m := missingField{Op: "ListThings", Field: mustField("MaxResults", "The maximum number of results.", false)}

	f := triageOne(m, map[string]bool{})
	require.Equal(t, tierCollectionFilter, f.Tier)
}

func TestTriageOne_CollectionHintDoesNotFalseMatchSubstring(t *testing.T) {
	t.Parallel()

	// Regression for the "to" substring bug found validating this tool
	// against omics ground truth: StorageType and WorkflowBucketOwnerId
	// both contain "to" as a bare substring and neither is a range filter.
	m := missingField{Op: "CreateThing", Field: mustField("StorageType", "The storage type for the run.", false)}

	f := triageOne(m, map[string]bool{})
	require.Equal(t, tierNoSignal, f.Tier, "StorageType must not false-match a range-filter hint")
}

func TestTriageOne_SiblingSignal(t *testing.T) {
	t.Parallel()

	m := missingField{Op: "GetThing", Field: mustField("OwnerId", "The owner.", false)}

	f := triageOne(m, map[string]bool{"ownerid": true})
	require.Equal(t, tierSiblingDeclares, f.Tier)
}

// TestResolveOp_AnonymousInlineStruct reproduces cmd/reqfieldscan's fifth
// inherited blind spot -- opsworks's real shape, and omics' handleStartRun
// (this tool's own ground truth): a WrapOp-free handler decoding directly
// into `var req struct{...}`, registered in a generic
// map[string]func(*Handler,*echo.Context,string) error dispatch closure
// (omics' actual shape, not service.JSONOpFunc at all).
func TestResolveOp_AnonymousInlineStruct(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

var ops = map[string]func(*Handler, *Context, string) error{
	"StartRun": func(h *Handler, c *Context, _ string) error {
		return h.handleStartRun(c)
	},
}

func (h *Handler) handleStartRun(c *Context) error {
	var req struct {
		WorkflowID string ` + "`json:\"workflowId\"`" + `
		RoleArn    string ` + "`json:\"roleArn\"`" + `
	}
	if err := readJSON(c, &req); err != nil {
		return err
	}
	return nil
}
`
	idx := parseSrc(t, src)
	res := idx.resolveOps([]sdkOp{{Name: "StartRun"}})["StartRun"]

	require.True(t, res.Found)
	require.True(t, res.HasSignal)
	_, ok := res.Fields[normalizeWireName("WorkflowID")]
	require.True(t, ok)
	_, ok = res.Fields[normalizeWireName("RoleArn")]
	require.True(t, ok)
	// The undeclared ground-truth shape: a field the SDK declares but this
	// anonymous struct never does.
	_, ok = res.Fields[normalizeWireName("RetentionMode")]
	require.False(t, ok)
}

// TestResolveOp_LocalGenericWrapper reproduces cmd/reqfieldscan's second
// inherited blind spot -- cognitoidp's wrapAccuracy[I,O](fn) shape: a
// package-level generic function whose entire body forwards to
// service.WrapOp, called through a dispatch-table value.
func TestResolveOp_LocalGenericWrapper(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}
type ctx struct{}

type getThingInput struct {
	ThingID string ` + "`json:\"thingId\"`" + `
}

func wrapAccuracy[I any, O any](fn func(ctx, *I) (*O, error)) service.JSONOpFunc {
	return service.WrapOp(fn)
}

var ops = map[string]service.JSONOpFunc{
	"GetThing": wrapAccuracy(handleGetThing),
}

func handleGetThing(c ctx, in *getThingInput) (*getThingOutput, error) {
	return nil, nil
}
`
	idx := parseSrc(t, src)
	res := idx.resolveOps([]sdkOp{{Name: "GetThing"}})["GetThing"]

	require.True(t, res.Found)
	require.True(t, res.HasSignal)
	_, ok := res.Fields[normalizeWireName("ThingID")]
	require.True(t, ok)
}

// TestResolveOp_SwitchDispatch reproduces acmpca's real shape: a switch
// statement over the operation name string, not a map literal at all --
// this scan initially reported zero of acmpca's 23 operations resolved
// until switch-statement dispatch was added; this pins that fix.
func TestResolveOp_SwitchDispatch(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

type createCAInput struct {
	CertificateAuthorityConfiguration string ` + "`json:\"CertificateAuthorityConfiguration\"`" + `
}

func (h *Handler) dispatchJSON(action string, body []byte) (any, error) {
	switch action {
	case "CreateCertificateAuthority":
		return h.jsonCreateCA(body)
	default:
		return nil, nil
	}
}

func (h *Handler) jsonCreateCA(body []byte) (any, error) {
	var in createCAInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, err
	}
	return nil, nil
}
`
	idx := parseSrc(t, src)
	res := idx.resolveOps([]sdkOp{{Name: "CreateCertificateAuthority"}})["CreateCertificateAuthority"]

	require.True(t, res.Found)
	require.True(t, res.HasSignal)
	_, ok := res.Fields[normalizeWireName("CertificateAuthorityConfiguration")]
	require.True(t, ok)
}

// TestResolveOp_NamedFuncTypeDispatchTable reproduces apigateway's real
// shape: map[string]actionFn, a locally-declared named func type rather
// than service.JSONOpFunc or a literal func type.
func TestResolveOp_NamedFuncTypeDispatchTable(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}
type actionFn func([]byte) (int, any, error)

type getResourcesInput struct {
	RestAPIID string ` + "`json:\"restApiId\"`" + `
	Position  string ` + "`json:\"position\"`" + `
}

func (h *Handler) actions() map[string]actionFn {
	return map[string]actionFn{
		// Deliberately NOT named by any name-convention fallback
		// (handle+Op, Op+Action, lowerCamel(Op)+Action, bare
		// lowerCamel(Op)) -- this method is reachable ONLY through the
		// named-func-type dispatch table itself, so this test actually
		// isolates that resolution path rather than incidentally passing
		// through the name-convention fallback too.
		"GetResources": h.resourcesEndpoint,
	}
}

func (h *Handler) resourcesEndpoint(b []byte) (int, any, error) {
	var input getResourcesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	return 0, nil, nil
}
`
	idx := parseSrc(t, src)
	res := idx.resolveOps([]sdkOp{{Name: "GetResources"}})["GetResources"]

	require.True(t, res.Found)
	require.True(t, res.HasSignal)
	_, ok := res.Fields[normalizeWireName("RestAPIID")]
	require.True(t, ok)
	// Ground truth: Embed is documented on the real SDK's GetResourcesInput
	// but never declared here -- exactly the shape this tool exists to catch.
	_, ok = res.Fields[normalizeWireName("Embed")]
	require.False(t, ok)
}

// TestResolveOp_QueryParamNoStruct reproduces the no-struct-at-all shape:
// a handler that reads echo query params directly, with no decode struct
// in between. A literal QueryParam("name") call is harvested as a declared
// wire field on its own.
func TestResolveOp_QueryParamNoStruct(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

func (h *Handler) handleListThings(c *Context) error {
	position := c.QueryParam("position")
	_ = position
	return nil
}
`
	idx := parseSrc(t, src)
	res := idx.resolveOps([]sdkOp{{Name: "ListThings"}})["ListThings"]

	require.True(t, res.Found)
	require.True(t, res.HasSignal)
	_, ok := res.Fields[normalizeWireName("position")]
	require.True(t, ok)
}

// TestResolveOp_SingleHopHelper reproduces cloudfront's real shape: the
// dispatched handler contains no decode call itself, but calls a package
// helper whose OWN declared return type is a known local struct.
func TestResolveOp_SingleHopHelper(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}
type Context struct{}

type listBody struct {
	RealtimeLogConfigArn string ` + "`json:\"RealtimeLogConfigArn\"`" + `
}

func (h *Handler) handleListDistributionsByRealtimeLogConfig(c *Context) error {
	req := decodeListBody(c)
	_ = req
	return nil
}

func decodeListBody(c *Context) listBody {
	return listBody{}
}
`
	idx := parseSrc(t, src)
	res := idx.resolveOps([]sdkOp{{Name: "ListDistributionsByRealtimeLogConfig"}})["ListDistributionsByRealtimeLogConfig"]

	require.True(t, res.Found)
	require.True(t, res.HasSignal)
	_, ok := res.Fields[normalizeWireName("RealtimeLogConfigArn")]
	require.True(t, ok)
	// Ground truth: RealtimeLogConfigName is documented on the real input
	// but never declared here.
	_, ok = res.Fields[normalizeWireName("RealtimeLogConfigName")]
	require.False(t, ok)
}

// TestResolveOp_ReturnsStructCallGatedToHandlerReceiver reproduces
// gopherstack-id70's lambda miss: UpdateFunctionUrlConfig's real handler
// calls a backend method spelled identically to no one else in the package
// except that one backend method, `lambdaBk.UpdateFunctionURLConfig(...)`,
// which returns *FunctionURLConfig -- the RESPONSE struct, which (like the
// AWS response object it models) happens to also declare an InvokeMode
// field. Before the fix, matchReturnsStructCall resolved that call by method
// name alone, with no check on the receiver, and merged FunctionURLConfig's
// fields into the operation's "declared" set -- so UpdateFunctionUrlConfig's
// own genuinely undeclared InvokeMode request field silently matched via a
// completely different struct and never got reported at all. The sibling
// operation (CreateThing) declares its own InvokeMode correctly and must
// still be clean.
func TestResolveOp_ReturnsStructCallGatedToHandlerReceiver(t *testing.T) {
	t.Parallel()

	src := `package fixture

import "encoding/json"

type Handler struct{}
type Context struct{}
type Backend struct{}

type CreateThingInput struct {
	InvokeMode string ` + "`json:\"InvokeMode\"`" + `
}

type UpdateThingInput struct {
	Name string ` + "`json:\"Name\"`" + `
}

type ThingConfig struct {
	InvokeMode string ` + "`json:\"InvokeMode\"`" + `
}

func (h *Handler) handleCreateThing(c *Context, body []byte) error {
	var input CreateThingInput
	json.Unmarshal(body, &input)
	return nil
}

func (h *Handler) handleUpdateThing(c *Context, body []byte, bk *Backend) error {
	var input UpdateThingInput
	json.Unmarshal(body, &input)
	cfg := bk.UpdateThing()
	_ = cfg
	return nil
}

func (b *Backend) UpdateThing() *ThingConfig {
	return &ThingConfig{}
}
`
	idx := parseSrc(t, src)
	sdkOps := []sdkOp{
		{Name: "CreateThing", Fields: []sdkField{mustField("InvokeMode", "", false)}},
		{Name: "UpdateThing", Fields: []sdkField{mustField("Name", "", false), mustField("InvokeMode", "", false)}},
	}
	resolutions := idx.resolveOps(sdkOps)

	updateRes := resolutions["UpdateThing"]
	require.True(t, updateRes.Found)
	_, declared := updateRes.Fields[normalizeWireName("InvokeMode")]
	require.False(t, declared,
		"a business-logic call on a non-handler receiver must never leak its "+
			"return struct's fields in as falsely \"declared\"")

	missing := findMissing(sdkOps[1], updateRes)
	names := make([]string, len(missing))
	for i, m := range missing {
		names[i] = m.Field.Name
	}

	require.Contains(t, names, "InvokeMode")

	createRes := resolutions["CreateThing"]
	require.Empty(t, findMissing(sdkOps[0], createRes))
}

func TestCoverageWarnings_ZeroOpsResolved(t *testing.T) {
	t.Parallel()

	r := serviceReport{OpsTotal: 10, OpsHandlerFound: 0}
	warnings := coverageWarnings(r)
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0], "ZERO")
}

func TestCoverageWarnings_LowSignalRatio(t *testing.T) {
	t.Parallel()

	r := serviceReport{OpsTotal: 10, OpsHandlerFound: 10, OpsWithSignal: 2}
	warnings := coverageWarnings(r)
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0], "UNVERIFIED")
}

func TestCoverageWarnings_LowFieldRatio(t *testing.T) {
	t.Parallel()

	r := serviceReport{
		OpsTotal: 5, OpsHandlerFound: 5, OpsWithSignal: 5,
		SDKFieldsResolved: 400, EmuFieldsResolved: 3,
	}
	warnings := coverageWarnings(r)
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0], "resolution bug in this tool")
}

func TestCoverageWarnings_Clean(t *testing.T) {
	t.Parallel()

	r := serviceReport{
		OpsTotal: 5, OpsHandlerFound: 5, OpsWithSignal: 5,
		SDKFieldsResolved: 20, EmuFieldsResolved: 18,
	}
	require.Empty(t, coverageWarnings(r))
}

// TestResolveOp_FormReadScalarField reproduces ec2/rds's real query-protocol
// shape: a scalar field read via `vals.Get("Name")` off a url.Values
// parameter, with no struct decode anywhere. Ground truth: this is exactly
// the shape 26 of ec2's identifier-list findings turned out to be --
// correctly read, invisible to a struct-declaration scan.
func TestResolveOp_FormReadScalarField(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

func (h *Handler) handleDescribeKeyPairs(vals url.Values, reqID string) (any, error) {
	name := vals.Get("KeyName")
	_ = name
	return nil, nil
}
`
	idx := parseSrc(t, src)
	op := sdkOp{Name: "DescribeKeyPairs", Fields: []sdkField{mustField("KeyName", "", false)}}
	res := idx.resolveOps([]sdkOp{op})["DescribeKeyPairs"]

	require.True(t, res.HasSignal)
	require.Empty(t, findMissing(op, res), "KeyName read via vals.Get must not be reported as missing")
}

// TestResolveOp_FormReadIndexedListMember reproduces ec2's parseMemberList
// shape: a plural SDK field (KeyNames) read from singular indexed query
// keys (KeyName.1, KeyName.2, ...) via a package-level helper whose own
// first parameter is url.Values -- recognised structurally by that
// signature, not by the helper's name.
func TestResolveOp_FormReadIndexedListMember(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

func parseMemberList(vals url.Values, prefix string) []string {
	return nil
}

func (h *Handler) handleDescribeKeyPairs(vals url.Values, reqID string) (any, error) {
	names := parseMemberList(vals, "KeyName")
	_ = names
	return nil, nil
}
`
	idx := parseSrc(t, src)
	op := sdkOp{Name: "DescribeKeyPairs", Fields: []sdkField{mustField("KeyNames", "", false)}}
	res := idx.resolveOps([]sdkOp{op})["DescribeKeyPairs"]

	require.True(t, res.HasSignal)
	require.Empty(t, findMissing(op, res),
		"KeyNames read via indexed KeyName.N members must not be reported as missing")
}

// TestResolveOp_FormReadStillReportsAbsentField confirms a query-protocol
// handler that genuinely never reads a declared SDK field is still
// reported -- form-read detection must narrow the queue, not silence it.
func TestResolveOp_FormReadStillReportsAbsentField(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

func (h *Handler) handleDescribeKeyPairs(vals url.Values, reqID string) (any, error) {
	name := vals.Get("KeyName")
	_ = name
	return nil, nil
}
`
	idx := parseSrc(t, src)
	op := sdkOp{Name: "DescribeKeyPairs", Fields: []sdkField{
		mustField("KeyName", "", false),
		mustField("IncludePublicKey", "", false),
	}}
	res := idx.resolveOps([]sdkOp{op})["DescribeKeyPairs"]

	missing := findMissing(op, res)
	require.Len(t, missing, 1)
	require.Equal(t, "IncludePublicKey", missing[0].Field.Name)
}

// TestResolveOp_FormReadIgnoresGetOnNonURLValuesReceiver is the regression
// this scan's own package doc says was deliberately never chased with a
// blanket `.Get("literal")` signal: a .Get call on something that is NOT
// this operation's url.Values parameter must never count as a declared
// read, even when its literal key happens to spell a real SDK field name.
func TestResolveOp_FormReadIgnoresGetOnNonURLValuesReceiver(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}
type Cache struct{}

func (c *Cache) Get(key string) string { return "" }

func (h *Handler) handleDescribeKeyPairs(vals url.Values, reqID string) (any, error) {
	cache := &Cache{}
	v := cache.Get("KeyName")
	_ = v
	return nil, nil
}
`
	idx := parseSrc(t, src)
	op := sdkOp{Name: "DescribeKeyPairs", Fields: []sdkField{mustField("KeyName", "", false)}}
	res := idx.resolveOps([]sdkOp{op})["DescribeKeyPairs"]

	missing := findMissing(op, res)
	require.Len(t, missing, 1, "an unrelated Cache.Get(\"KeyName\") must not suppress the real finding")
	require.Equal(t, "KeyName", missing[0].Field.Name)
}

// TestResolveOp_FormReadDoesNotOvermatchUnrelatedField confirms the
// candidate key set is scoped per field: reading one field off vals must
// not also mark a sibling, unrelated field on the same operation as
// declared.
func TestResolveOp_FormReadDoesNotOvermatchUnrelatedField(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

func (h *Handler) handleModifyActivityStream(vals url.Values) (any, error) {
	mode := vals.Get("Mode")
	_ = mode
	return nil, nil
}
`
	idx := parseSrc(t, src)
	op := sdkOp{Name: "ModifyActivityStream", Fields: []sdkField{
		mustField("Mode", "", false),
		mustField("ResourceArn", "", true),
	}}
	res := idx.resolveOps([]sdkOp{op})["ModifyActivityStream"]

	missing := findMissing(op, res)
	require.Len(t, missing, 1)
	require.Equal(t, "ResourceArn", missing[0].Field.Name)
}

// TestFindHandlerByNameFold_Deterministic is gopherstack-fr30's regression
// test. Before the fix, this fixture's fallback scan picked a winner via
// Go's randomized map iteration order -- CreateAPI (an exported Backend
// method, appsync's and s3's real shape: business logic, not a decode
// site) and createAPI (the actual unexported dispatch handler) both match
// "CreateApi" case-insensitively with no "handle" prefix on either, so
// nothing here breaks the tie except the stated rule. Runs the resolution
// many times over the SAME handlerResolveCtx -- Go picks a fresh random
// start point on every `range` over a map, even within one process, so
// repeated calls are enough to catch the old nondeterminism without
// shelling out to separate `go run` processes.
func TestFindHandlerByNameFold_Deterministic(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Backend struct{}

func (b *Backend) CreateAPI() error { return nil }

type Handler struct{}

func (h *Handler) createAPI(c *Context) error { return nil }
`
	idx := parseSrc(t, src)

	fd, names := findHandlerByName("CreateApi", idx.ctx)
	require.NotNil(t, fd)
	require.Equal(t, []string{"CreateAPI", "createAPI"}, names, "both candidates must be reported as ambiguous")
	require.Equal(
		t,
		"createAPI",
		fd.Name.Name,
		"the unexported dispatch handler must win, never the exported backend method",
	)

	const iterations = 200

	for range iterations {
		again, _ := findHandlerByName("CreateApi", idx.ctx)
		require.Same(t, fd, again, "resolution must be identical on every call, not dependent on map iteration order")
	}
}

// TestFindHandlerByNameFold_HandlePrefixBeatsBare covers the OTHER
// collision shape the census found (177 operations, 26 services): a bare,
// exported name that matches this op case-insensitively (a Backend method
// sharing the operation's own name) alongside a "handle"+op-prefixed
// match. The "handle" match must win regardless of export status or which
// the map visits first.
func TestFindHandlerByNameFold_HandlePrefixBeatsBare(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Backend struct{}

func (b *Backend) EnableVpcClassicLinkDNSSupport() error { return nil }

type Handler struct{}

func (h *Handler) handleEnableVpcClassicLinkDNSSupport(c *Context) error { return nil }
`
	idx := parseSrc(t, src)

	fd, names := findHandlerByName("EnableVpcClassicLinkDnsSupport", idx.ctx)
	require.NotNil(t, fd)
	require.ElementsMatch(t, []string{"EnableVpcClassicLinkDNSSupport", "handleEnableVpcClassicLinkDNSSupport"}, names)
	require.Equal(t, "handleEnableVpcClassicLinkDNSSupport", fd.Name.Name)

	const iterations = 200

	for range iterations {
		again, _ := findHandlerByName("EnableVpcClassicLinkDnsSupport", idx.ctx)
		require.Same(t, fd, again)
	}
}

// TestResolveOp_GenericDecodeCallbackWrapper reproduces gopherstack-99nj's
// second blind-spot shape (xhu2t slice 2): a LOCAL generic dispatch-wrapper
// function whose callback parameter's own signature carries the request
// struct -- ssm's jsonOp[I,O](fn func(ctx,*I)(O,error)) used as a
// dispatch-table VALUE (`"Op": jsonOp(h.Backend.Op)`), and apigatewayv2's
// handleCreate[I,O](..., backendFn func(I)(*O,error)) called as a plain
// STATEMENT inside an already-resolved handler's own body, passing the
// request struct BY VALUE via a func literal. 45 of ssm's 52 tier-1
// findings were hand-verified already declared this way.
func TestResolveOp_GenericDecodeCallbackWrapper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		src       string
		op        string
		wantField string
	}{
		{
			name:      "dispatch table value, pointer callback",
			op:        "CreateThing",
			wantField: "Name",
			src: `package fixture

type Handler struct{ Backend Backend }
type Backend interface{}

type CreateThingInput struct {
	Name string ` + "`json:\"Name\"`" + `
}

func jsonOp[I, O any](fn func(ctx int, in *I) (O, error)) func(int, []byte) (any, error) {
	return nil
}

func (b *Backend) CreateThing(ctx int, in *CreateThingInput) (*CreateThingInput, error) {
	return nil, nil
}

var ops = map[string]func(int, []byte) (any, error){
	"CreateThing": jsonOp(h.Backend.CreateThing),
}
`,
		},
		{
			name:      "inline body call, value callback",
			op:        "CreateAuthorizer",
			wantField: "EnableSimpleResponses",
			src: `package fixture

type Handler struct{}

type CreateAuthorizerInput struct {
	EnableSimpleResponses bool ` + "`json:\"enableSimpleResponses\"`" + `
}
type Authorizer struct{}

func handleCreate[I, O any](c int, resourceName string, backendFn func(I) (*O, error)) error {
	return nil
}

func (h *Handler) handleCreateAuthorizer(c int) error {
	return handleCreate(c, "authorizer", func(input CreateAuthorizerInput) (*Authorizer, error) {
		return nil, nil
	})
}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idx := parseSrc(t, tt.src)
			res := idx.resolveOps([]sdkOp{{Name: tt.op}})[tt.op]

			require.True(t, res.Found)
			require.True(t, res.HasSignal)
			assert.Contains(t, res.Fields, normalizeWireName(tt.wantField))
		})
	}
}

// TestResolveOp_GenericCallbackDoesNotOvermatchUnrelatedArgument is the
// orphan-risk guard for collectGenericDecodeWrapperFuncs: a package-level
// generic helper matching the callback SHAPE for a completely unrelated
// reason (a generic Filter-like function) must never manufacture a
// declared field just because its own signature happens to match --
// resolution only ever produces a field when the ACTUAL call-site argument
// resolves to a known local struct.
func TestResolveOp_GenericCallbackDoesNotOvermatchUnrelatedArgument(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

func filterThings[T any](items []T, keep func(T) bool) []T {
	return nil
}

func (h *Handler) handleListThings(c int) error {
	names := []string{"a", "b"}
	kept := filterThings(names, isLong)
	_ = kept
	return nil
}

func isLong(s string) bool {
	return len(s) > 3
}
`
	idx := parseSrc(t, src)
	res := idx.resolveOps([]sdkOp{{Name: "ListThings"}})["ListThings"]

	require.True(t, res.Found)
	assert.False(t, res.HasSignal, "an unrelated generic helper must never fabricate a declared field")
	assert.Empty(t, res.Fields)
}

// TestResolveOp_DecodeDstWrapper reproduces iot's readBody(c, dst any)
// error shape (gopherstack-99nj slice 1): the wrapper decodes directly
// into its own `any`-typed parameter with no address-of at all, since the
// caller already passed one (`&input`). 19 of iot's tier-1 findings were
// this shape plus query reads.
func TestResolveOp_DecodeDstWrapper(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

type CreateAuthorizerInput struct {
	EnableCachingForHTTP bool ` + "`json:\"enableCachingForHttp\"`" + `
}

func readBody(c int, dst any) error {
	return jsonDecode(c, dst)
}

func jsonDecode(c int, dst any) error { return nil }

func (h *Handler) handleCreateAuthorizer(c int) error {
	var input CreateAuthorizerInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	return nil
}
`
	idx := parseSrc(t, src)
	res := idx.resolveOps([]sdkOp{{Name: "CreateAuthorizer"}})["CreateAuthorizer"]

	require.True(t, res.Found)
	require.True(t, res.HasSignal)
	assert.Contains(t, res.Fields, normalizeWireName("EnableCachingForHTTP"))
}

// TestResolveOp_QueryAccessorWrapper reproduces cleanrooms' qp(c,"key") and
// iot's parseInt32QueryParam(c,"name") shapes: a local helper forwarding
// one of its own string parameters straight into a queryParamSelectors
// call. Cleanrooms' 21 List*.MaxResults findings and part of iot's 19 are
// this shape.
func TestResolveOp_QueryAccessorWrapper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wrapper string
		call    string
	}{
		{
			name:    "single param forwarder",
			wrapper: `func qp(c *Context, key string) string { return c.QueryParam(key) }`,
			call:    `qp(c, "maxResults")`,
		},
		{
			name: "forwarder with extra parsing logic",
			wrapper: `func parseInt32QueryParam(c *Context, name string) int32 {
				v := c.QueryParam(name)
				_ = v
				return 0
			}`,
			call: `parseInt32QueryParam(c, "maxResults")`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := `package fixture

type Handler struct{}
type Context struct{}

func (c *Context) QueryParam(key string) string { return "" }

` + tt.wrapper + `

func (h *Handler) handleListThings(c *Context) error {
	v := ` + tt.call + `
	_ = v
	return nil
}
`
			idx := parseSrc(t, src)
			res := idx.resolveOps([]sdkOp{{Name: "ListThings"}})["ListThings"]

			require.True(t, res.Found)
			require.True(t, res.HasSignal)
			assert.Contains(t, res.Fields, normalizeWireName("maxResults"))
		})
	}
}

// TestResolveOp_QueryAccessorWrapperRequiresForwardedParam is the
// orphan-risk guard for collectQueryAccessorWrappers directly: a function
// that merely CALLS a queryParamSelectors method with a HARDCODED literal,
// never forwarding one of its own parameters into it, must not be
// registered as a wrapper at all -- only the exact forwarding shape
// qualifies. Checked against collectQueryAccessorWrappers itself rather
// than through resolveOp: a full resolveOp run would also recurse into
// notAWrapper's body and pick up its hardcoded literal via the existing,
// unconditional matchQueryParamCall recognizer, which would mask exactly
// the distinction this test exists to pin.
func TestResolveOp_QueryAccessorWrapperRequiresForwardedParam(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Context struct{}

func (c *Context) QueryParam(key string) string { return "" }

func notAWrapper(c *Context) string {
	return c.QueryParam("hardcodedLiteral")
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "fixture.go", src, 0)
	require.NoError(t, err)

	wrappers := collectQueryAccessorWrappers([]*ast.File{f})
	assert.NotContains(t, wrappers, "notAWrapper")
}

// TestResolveOp_HeaderRead is gopherstack-7fve's regression: s3's
// PutBucketAcl/PutObjectAcl read the canned ACL off the X-Amz-Acl request
// header, and CopyObject reads its CopySource off X-Amz-Copy-Source --
// both stripped of their header prefix before matching op's own SDK field
// names.
func TestResolveOp_HeaderRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		header string
		field  string
	}{
		{"acl header", "X-Amz-Acl", "ACL"},
		{"copy source header", "X-Amz-Copy-Source", "CopySource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := `package fixture

type Handler struct{}
type Header struct{}
type Request struct{ Header Header }

func (h Header) Get(name string) string { return "" }

func (h *Handler) handlePutObject(r *Request) error {
	v := r.Header.Get("` + tt.header + `")
	_ = v
	return nil
}
`
			idx := parseSrc(t, src)
			op := sdkOp{Name: "PutObject", Fields: []sdkField{mustField(tt.field, "", false)}}
			res := idx.resolveOps([]sdkOp{op})["PutObject"]

			require.True(t, res.HasSignal)
			assert.Empty(t, findMissing(op, res))
		})
	}
}

// TestResolveOp_MapFieldRead reproduces quicksight's hand-decoded
// `body map[string]any` shape (gopherstack-99nj slice 3): a field read via
// a strField/mapField/boolField-style accessor call, via DIRECT indexing
// (personalize's own independent instance of this same shape), and via a
// multi-return wrapper (`body, err := readBody(c)`) that establishes body
// as a map[string]any local in the first place. 25 of quicksight's 37
// tier-1 findings were this shape.
func TestResolveOp_MapFieldRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
	}{
		{
			name: "accessor call on a map-any parameter",
			src: `package fixture

type Handler struct{}

func strField(body map[string]any, key string) string {
	v, _ := body[key].(string)
	return v
}

func (h *Handler) handleUpdateUser(body map[string]any) error {
	role := strField(body, "Role")
	_ = role
	return nil
}
`,
		},
		{
			name: "direct indexing on a map-any parameter",
			src: `package fixture

type Handler struct{}

func (h *Handler) handleUpdateUser(input map[string]any) error {
	role, _ := input["Role"].(string)
	_ = role
	return nil
}
`,
		},
		{
			name: "accessor call on a body bound via a multi-return wrapper",
			src: `package fixture

type Handler struct{}

func readBody(c int) (map[string]any, error) {
	return nil, nil
}

func strField(body map[string]any, key string) string {
	v, _ := body[key].(string)
	return v
}

func (h *Handler) handleUpdateUser(c int) error {
	body, err := readBody(c)
	if err != nil {
		return err
	}
	role := strField(body, "Role")
	_ = role
	return nil
}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idx := parseSrc(t, tt.src)
			res := idx.resolveOps([]sdkOp{{Name: "UpdateUser"}})["UpdateUser"]

			require.True(t, res.Found)
			require.True(t, res.HasSignal)
			assert.Contains(t, res.Fields, normalizeWireName("Role"))
		})
	}
}

// TestResolveOp_MapFieldDoesNotOvermatchNonMapReceiver is the orphan-risk
// guard for matchMapFieldCall/matchMapIndexExpr: a call to a
// strField-shaped accessor, or an index expression, on something that is
// NOT one of this scan's own known map[string]any locals must never
// declare a field, even if its literal key spells a real SDK field name --
// the same discipline TestResolveOp_FormReadIgnoresGetOnNonURLValuesReceiver
// already pins for the url.Values case.
func TestResolveOp_MapFieldDoesNotOvermatchNonMapReceiver(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}
type Cache map[string]string

func strField(body map[string]any, key string) string {
	v, _ := body[key].(string)
	return v
}

func (h *Handler) handleUpdateUser(c int) error {
	cache := Cache{}
	role := cache["Role"]
	_ = role
	return nil
}
`
	idx := parseSrc(t, src)
	op := sdkOp{Name: "UpdateUser", Fields: []sdkField{mustField("Role", "", false)}}
	res := idx.resolveOps([]sdkOp{op})["UpdateUser"]

	missing := findMissing(op, res)
	require.Len(t, missing, 1, "a same-named-key index into an unrelated map type must not suppress the real finding")
	assert.Equal(t, "Role", missing[0].Field.Name)
}

// TestResolveOp_NamedDispatchMapType reproduces appstream's real shape: a
// dispatch table built from a composite literal of a package-level `type
// opTable = map[string]F` alias, spelled as a bare *ast.Ident at the
// composite-literal site rather than a literal map type -- invisible to a
// literal-map-type-only check. 16 of appstream's 19 tier-1 findings were
// this shape (every op resolves through it).
func TestResolveOp_NamedDispatchMapType(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

type CreateFleetInput struct {
	Name string ` + "`json:\"Name\"`" + `
}

type opTable = map[string]func([]byte) (any, error)

func (h *Handler) buildOps() opTable {
	return opTable{
		"CreateFleet": h.opCreateFleet,
	}
}

func (h *Handler) opCreateFleet(body []byte) (any, error) {
	var input CreateFleetInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}
	return nil, nil
}
`
	idx := parseSrc(t, src)
	res := idx.resolveOps([]sdkOp{{Name: "CreateFleet"}})["CreateFleet"]

	require.True(t, res.Found)
	require.True(t, res.HasSignal)
	assert.Contains(t, res.Fields, normalizeWireName("Name"))
}

// TestResolveOp_OwnParamNameDeclaresField reproduces apigatewayv2's
// UpdateStage/ResetAuthorizersCache shape: StageName arrives as a plain
// function parameter threaded in by an upstream REST-path dispatcher, with
// no read call of its OWN inside the handler at all -- "a label is
// required to route", so its value necessarily reaches the handler, just
// never through a call this scan could otherwise see.
func TestResolveOp_OwnParamNameDeclaresField(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

func (h *Handler) handleUpdateStage(c int, apiID, stageName string) error {
	return nil
}
`
	idx := parseSrc(t, src)
	op := sdkOp{Name: "UpdateStage", Fields: []sdkField{mustField("StageName", "", true)}}
	res := idx.resolveOps([]sdkOp{op})["UpdateStage"]

	require.True(t, res.HasSignal)
	assert.Empty(t, findMissing(op, res))
}

// TestResolveOp_OwnParamNameIsHopZeroOnly confirms matchOwnParamNames is
// gated to hop 0: a RECURSED-INTO helper's own parameter names are that
// helper's locals, not this operation's wire values, and must not be
// credited just because one happens to share a name with an SDK field.
func TestResolveOp_OwnParamNameIsHopZeroOnly(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

func (h *Handler) handleUpdateStage(c int) error {
	return h.applyUpdate(c, "unrelated-value")
}

func (h *Handler) applyUpdate(c int, stageName string) error {
	return nil
}
`
	idx := parseSrc(t, src)
	op := sdkOp{Name: "UpdateStage", Fields: []sdkField{mustField("StageName", "", true)}}
	res := idx.resolveOps([]sdkOp{op})["UpdateStage"]

	missing := findMissing(op, res)
	require.Len(t, missing, 1, "a recursed-into helper's own parameter name must not count as this op's declaration")
	assert.Equal(t, "StageName", missing[0].Field.Name)
}

// TestResolveOp_PathSegmentLocalDeclaresField reproduces quicksight's
// `namespace := seg(segs, segResID)` shape: a path-segment value bound to
// a LOCAL variable (not a parameter) via a helper matching seg's own
// `func([]string, T) string` signature, with no literal name anywhere at
// the call site at all -- only the resulting local's own name spells the
// wire field.
func TestResolveOp_PathSegmentLocalDeclaresField(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

func seg(segs []string, i int) string {
	return segs[i]
}

func (h *Handler) handleUpdateUser(segs []string) error {
	namespace := seg(segs, 1)
	_ = namespace
	return nil
}
`
	idx := parseSrc(t, src)
	op := sdkOp{Name: "UpdateUser", Fields: []sdkField{mustField("Namespace", "", true)}}
	res := idx.resolveOps([]sdkOp{op})["UpdateUser"]

	require.True(t, res.HasSignal)
	assert.Empty(t, findMissing(op, res))
}

func TestPascalWords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"simple", "CreateThing", []string{"Create", "Thing"}},
		{"three words", "EventSourceMapping", []string{"Event", "Source", "Mapping"}},
		{"leading acronym", "IPAddress", []string{"IP", "Address"}},
		{"single word", "Create", []string{"Create"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, pascalWords(tt.in))
		})
	}
}

func TestAbbreviatedHandlerName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
		want string
		ok   bool
	}{
		{"lambda esm create", "CreateEventSourceMapping", "handleCreateESM", true},
		{"lambda esm update", "UpdateEventSourceMapping", "handleUpdateESM", true},
		{"too few words", "CreateThing", "", false},
		{"single word", "Invoke", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := abbreviatedHandlerName(tt.op)
			require.Equal(t, tt.ok, ok)

			if tt.ok {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

// TestResolveOp_AbbreviatedHandlerNameResolvesPerFamilySwitchHandler
// reproduces lambda's real shape (gopherstack-99nj slice 4): ESM ops are
// routed through dispatchSpecialRoutes -> a per-family HTTP path/method
// switch, calling handleCreateESM directly -- an abbreviated name no
// existing candidate (exact or case-insensitive fold) could ever reach,
// since CreateEventSourceMapping folds to "createeventsourcemapping", not
// "createesm". 19 of this family's 21 tier-1 findings were hand-verified
// already declared this way.
func TestResolveOp_AbbreviatedHandlerNameResolvesPerFamilySwitchHandler(t *testing.T) {
	t.Parallel()

	src := `package fixture

type Handler struct{}

type handleCreateESMInput struct {
	KMSKeyArn string ` + "`json:\"KMSKeyArn\"`" + `
}

func (h *Handler) dispatchSpecialRoutes(path, method string) (bool, error) {
	if path == "/event-source-mappings" {
		return true, h.handleESMRoute(path, method)
	}
	return false, nil
}

func (h *Handler) handleESMRoute(path, method string) error {
	if method == "POST" {
		return h.handleCreateESM(nil)
	}
	return nil
}

func (h *Handler) handleCreateESM(body []byte) error {
	var req handleCreateESMInput
	if err := json.Unmarshal(body, &req); err != nil {
		return err
	}
	return nil
}
`
	idx := parseSrc(t, src)
	res := idx.resolveOps([]sdkOp{{Name: "CreateEventSourceMapping"}})["CreateEventSourceMapping"]

	require.True(t, res.Found)
	require.True(t, res.HasSignal)
	assert.Contains(t, res.Fields, normalizeWireName("KMSKeyArn"))
}

// TestResolveOp_CrossPackageGenericCallback reproduces dynamodb's real
// shape (gopherstack-99nj/xhu2t slice 1, "a SUBPACKAGE"): the generic
// handleOp[WireIn,...] dispatch wrapper's toSDK callback argument is
// `models.ToSDKCreateTableInput`, a bare function in an IMPORTED in-repo
// subpackage (services/dynamodb/models) rather than this scan's own
// package -- both the wrapper call's argument AND that argument's own
// parameter type live one level of indirection away from anything the
// single-package struct/func collectors can see on their own. Requires a
// real on-disk fixture (testdata/dynamodbstyle/) since subpackage
// resolution is driven by the SCANNED DIRECTORY's own filesystem layout,
// not by in-memory parser.ParseFile output. 14 of dynamodb's 17 tier-1
// findings were this shape.
func TestResolveOp_CrossPackageGenericCallback(t *testing.T) {
	t.Parallel()

	idx, err := buildPackageIndex("testdata/dynamodbstyle")
	require.NoError(t, err)

	op := sdkOp{Name: "CreateTable", Fields: []sdkField{
		mustField("TableName", "", true),
		mustField("KMSMasterKeyId", "", false),
	}}
	res := idx.resolveOps([]sdkOp{op})["CreateTable"]

	require.True(t, res.Found)
	require.True(t, res.HasSignal)
	assert.Empty(t, findMissing(op, res))
}
