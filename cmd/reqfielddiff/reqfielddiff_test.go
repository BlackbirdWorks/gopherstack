package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

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

	return buildPackageIndexFromFiles([]*ast.File{f}, fset)
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
	res := idx.resolveOps([]string{"StartRun"})["StartRun"]

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
	res := idx.resolveOps([]string{"GetThing"})["GetThing"]

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
	res := idx.resolveOps([]string{"CreateCertificateAuthority"})["CreateCertificateAuthority"]

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
	res := idx.resolveOps([]string{"GetResources"})["GetResources"]

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
	res := idx.resolveOps([]string{"ListThings"})["ListThings"]

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
	res := idx.resolveOps([]string{"ListDistributionsByRealtimeLogConfig"})["ListDistributionsByRealtimeLogConfig"]

	require.True(t, res.Found)
	require.True(t, res.HasSignal)
	_, ok := res.Fields[normalizeWireName("RealtimeLogConfigArn")]
	require.True(t, ok)
	// Ground truth: RealtimeLogConfigName is documented on the real input
	// but never declared here.
	_, ok = res.Fields[normalizeWireName("RealtimeLogConfigName")]
	require.False(t, ok)
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
