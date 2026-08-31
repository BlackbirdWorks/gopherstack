package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// parseSrc parses one in-memory Go source file into a *pkgIndex -- fixtures
// below never touch the filesystem, matching cmd/reqfielddiff's own
// parseSrc test helper.
func parseSrc(t *testing.T, src string) *pkgIndex {
	t.Helper()

	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, "fixture.go", src, 0)
	require.NoError(t, err)

	return buildPkgIndexFromFiles([]*ast.File{f}, fset)
}

// newTestModuleGroundTruth builds a synthetic single-module ground truth --
// perOp is (op name -> declared code set), allCodes is every code this
// service's SDK models anywhere (the class A/B boundary).
func newTestModuleGroundTruth(perOp map[string]map[string]bool, allCodes map[string]bool) *moduleGroundTruth {
	mgt := newModuleGroundTruth()
	mgt.PerOp = perOp
	mgt.AllCodes = allCodes

	for op := range perOp {
		mgt.OpFuncs[op] = true
	}

	return mgt
}

func singleModuleTruth(mgt *moduleGroundTruth) *serviceModuleTruth {
	const mod = "fixture"

	return &serviceModuleTruth{Modules: map[string]*moduleGroundTruth{mod: mgt}}
}

// findingCodes returns the (op, code) pairs a scan reported, for compact
// assertions.
func findingCodes(findings []finding) map[string]string {
	out := map[string]string{}
	for _, f := range findings {
		out[f.Op] = f.Code
	}

	return out
}

// sharedSentinelFixture is the exact shape this tool exists to catch:
// GetThing and DeleteThing both call into a Backend method (a DIFFERENT
// receiver from the Handler, exercising this tool's ANY-receiver one-hop
// recursion) that returns the same package-level sentinel, mapped through
// one shared switch-based mapper (writeError) to a single wire code. Op
// resolution goes through switch-statement dispatch (dispatch(action)),
// covering the switch-dispatch structural shape this tool inherited from
// cmd/reqfieldscan/cmd/reqfielddiff.
const sharedSentinelFixture = `
package fixture

import (
	"errors"
	"fmt"
)

var ErrNotFound = errors.New("not found")

func writeError(err error) string {
	switch {
	case errors.Is(err, ErrNotFound):
		return "ResourceNotFoundException"
	}
	return "UnmappedFailureCode"
}

const opGetThing = "GetThing"
const opDeleteThing = "DeleteThing"

type Handler struct {
	Backend *Backend
}

func (h *Handler) dispatch(action string) error {
	switch action {
	case opGetThing:
		return h.handleGetThing()
	case opDeleteThing:
		return h.handleDeleteThing()
	}
	return nil
}

func (h *Handler) handleGetThing() error {
	return h.Backend.GetThing()
}

func (h *Handler) handleDeleteThing() error {
	return h.Backend.DeleteThing()
}

type Backend struct{}

func (b *Backend) GetThing() error {
	return fmt.Errorf("%w: thing", ErrNotFound)
}

func (b *Backend) DeleteThing() error {
	return fmt.Errorf("%w: thing", ErrNotFound)
}
`

// TestScan_SharedSentinel_AttributedPerOperation is this tool's central
// case: a shared sentinel/mapper is correct for GetThing (its own declared
// set has ResourceNotFoundException: no finding) and wrong for DeleteThing
// (its declared set does not: a finding, attributed to DeleteThing alone,
// never bleeding into GetThing). Covers "declared code -> no finding" and
// "undeclared code -> finding" in one fixture, since both are the same
// mapper output checked against two different operations' truth.
func TestScan_SharedSentinel_AttributedPerOperation(t *testing.T) {
	t.Parallel()

	idx := parseSrc(t, sharedSentinelFixture)
	smt := singleModuleTruth(newTestModuleGroundTruth(
		map[string]map[string]bool{
			"GetThing":    {"ResourceNotFoundException": true},
			"DeleteThing": {"UnmappedFailureCode": true},
		},
		map[string]bool{"ResourceNotFoundException": true, "UnmappedFailureCode": true},
	))

	sr := scanWithIndex("fixture", []string{"fixture"}, "/repo", idx, smt)

	codes := findingCodes(sr.Findings)
	require.NotContains(t, codes, "GetThing", "declared code must not be flagged")
	require.Equal(
		t,
		"ResourceNotFoundException",
		codes["DeleteThing"],
		"undeclared code must be flagged and attributed to DeleteThing",
	)
	require.Len(t, sr.Findings, 1, "the shared sentinel must not also produce a spurious GetThing finding")

	require.Equal(t, 2, sr.OpsResolved)
}

// TestScan_ClassB_NotFabricated_NoFinding confirms a code absent from the
// SERVICE-WIDE code universe (never declared by ANY operation -- class B,
// cmd/errcodeaudit's job) produces no finding here, even though it is also
// absent from the specific operation's own declared set.
func TestScan_ClassB_NotFabricated_NoFinding(t *testing.T) {
	t.Parallel()

	idx := parseSrc(t, sharedSentinelFixture)
	smt := singleModuleTruth(newTestModuleGroundTruth(
		map[string]map[string]bool{
			"GetThing":    {"SomeOtherException": true},
			"DeleteThing": {"SomeOtherException": true},
		},
		map[string]bool{"SomeOtherException": true}, // ResourceNotFoundException never declared anywhere
	))

	sr := scanWithIndex("fixture", []string{"fixture"}, "/repo", idx, smt)

	require.Empty(t, sr.Findings, "a code no operation ever declares is class B, out of this tool's scope")
}

// constructorFixture is services/networkmanager's real shape: a
// constructor function (notFoundError) that never mentions a code literal
// itself, building a locally-declared error type whose field wraps a known
// sentinel one hop down. Also exercises name-convention-only resolution
// (no dispatch table at all), the anonymous-inline-struct blind spot's
// closest analogue for a tool with no decode-struct concept: a handler this
// tool can find ONLY via "handle"+Op naming, never through a dispatch
// table entry.
const constructorFixture = `
package fixture

import "errors"

var errNotFoundSentinel = errors.New("resource not found")

type apiError struct {
	cause error
	message string
}

func (e *apiError) Error() string { return e.message }
func (e *apiError) Unwrap() error { return e.cause }

func notFoundError(msg string) error {
	return &apiError{cause: errNotFoundSentinel, message: msg}
}

func classifyError(err error) string {
	switch {
	case errors.Is(err, errNotFoundSentinel):
		return "ResourceNotFoundException"
	}
	return "InternalServerException"
}

type Handler struct {
	Backend *Backend
}

func (h *Handler) handleCreateThing() error {
	return h.Backend.CreateThing()
}

type Backend struct{}

func (b *Backend) CreateThing() error {
	return notFoundError("thing not found")
}
`

// TestScan_ConstructorPropagation_NameConventionOnly resolves CreateThing
// with NO dispatch table entry present at all (findHandlersByName's
// "handle"+Op fallback is the only path), and the finding's code must come
// from following the constructor one hop to its wrapped sentinel.
func TestScan_ConstructorPropagation_NameConventionOnly(t *testing.T) {
	t.Parallel()

	idx := parseSrc(t, constructorFixture)
	require.Empty(t, idx.Dispatch, "fixture deliberately has no dispatch table")

	smt := singleModuleTruth(newTestModuleGroundTruth(
		map[string]map[string]bool{"CreateThing": {"SomePlaceholderCode": true}},
		map[string]bool{"ResourceNotFoundException": true, "SomePlaceholderCode": true},
	))

	sr := scanWithIndex("fixture", []string{"fixture"}, "/repo", idx, smt)

	codes := findingCodes(sr.Findings)
	require.Equal(t, "ResourceNotFoundException", codes["CreateThing"])
	require.Equal(t, "constructor classifier: notFoundError", sr.Findings[0].Sites[0].Mechanism)
}

// TestCoverageWarnings_ImplausibleResolution covers the loud-failure guard:
// a service where most ground-truth operations never resolved to a handler
// is reported as UNVERIFIED, not silently clean.
func TestCoverageWarnings_ImplausibleResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sr         serviceScan
		wantWarned bool
	}{
		{"zero resolved of many", serviceScan{OpsGroundTruth: 40, OpsResolved: 0}, true},
		{"low ratio", serviceScan{OpsGroundTruth: 40, OpsResolved: 10}, true},
		{"healthy ratio", serviceScan{OpsGroundTruth: 40, OpsResolved: 38}, false},
		{"small N below guard threshold", serviceScan{OpsGroundTruth: 3, OpsResolved: 1}, false},
		{"no ground truth at all", serviceScan{OpsGroundTruth: 0, OpsResolved: 0}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			warnings := coverageWarnings(tt.sr)
			if tt.wantWarned {
				require.NotEmpty(t, warnings)
			} else {
				require.Empty(t, warnings)
			}
		})
	}
}

// switchDispatchFixture isolates switch-statement dispatch resolution --
// cmd/reqfieldscan's own "took one service from 0 of 23 to 23 of 23" shape
// -- with no map literal anywhere.
const switchDispatchFixture = `
package fixture

type Handler struct{}

func (h *Handler) route(action string) error {
	switch action {
	case "PutWidget", "ReplaceWidget":
		return h.handlePutWidget()
	default:
		return nil
	}
}

func (h *Handler) handlePutWidget() error { return nil }
`

func TestResolveOpRoots_SwitchDispatch(t *testing.T) {
	t.Parallel()

	idx := parseSrc(t, switchDispatchFixture)

	for _, op := range []string{"PutWidget", "ReplaceWidget"} {
		roots := resolveOpRoots(op, idx)
		require.NotEmpty(t, roots, "op %s must resolve via switch-case dispatch (multi-value case list)", op)
	}
}

func TestResolveOpRoots_NameConventionFallback(t *testing.T) {
	t.Parallel()

	idx := parseSrc(t, constructorFixture)

	roots := resolveOpRoots("CreateThing", idx)
	require.Len(t, roots, 1)
	require.Equal(t, "Handler", roots[0].Domain)
}

// overrideFixture is services/iot's real post-fix shape: an override
// helper takes the comparison sentinel as ITS OWN parameter, so a hop-0
// call site can locally override what a hop-1 backend sentinel reference
// renders as.
const overrideFixture = `
package fixture

import (
	"errors"
	"fmt"
)

var ErrResourceNotFound = errors.New("not found")

const errTypeInvalidRequest = "InvalidRequestException"

func writeError(err error) string {
	switch {
	case errors.Is(err, ErrResourceNotFound):
		return "ResourceNotFoundException"
	}
	return "UnmappedFailureCode"
}

func respondAsInvalidRequest(err, sentinel error) string {
	if errors.Is(err, sentinel) {
		return errTypeInvalidRequest
	}
	return writeError(err)
}

type Handler struct {
	Backend *Backend
}

func (h *Handler) handleCancelJob() error {
	err := h.Backend.CancelJob()
	if err != nil {
		respondAsInvalidRequest(err, ErrResourceNotFound)
	}
	return err
}

type Backend struct{}

func (b *Backend) CancelJob() error {
	return fmt.Errorf("%w: job", ErrResourceNotFound)
}
`

// TestScan_OverrideMapper_SuppressesGeneralMapping confirms the
// respondAsInvalidRequest shape: CancelJob's own declared set includes
// InvalidRequestException (the override's code) but not
// ResourceNotFoundException (the general mapper's code) -- with the
// override modeled, this must be CLEAN, not a false positive.
func TestScan_OverrideMapper_SuppressesGeneralMapping(t *testing.T) {
	t.Parallel()

	idx := parseSrc(t, overrideFixture)
	smt := singleModuleTruth(newTestModuleGroundTruth(
		map[string]map[string]bool{"CancelJob": {"InvalidRequestException": true}},
		map[string]bool{
			"ResourceNotFoundException": true,
			"InvalidRequestException":   true,
			"UnmappedFailureCode":       true,
		},
	))

	sr := scanWithIndex("fixture", []string{"fixture"}, "/repo", idx, smt)

	require.Empty(t, sr.Findings, "override-mapper resolution must prevent the general-mapper false positive")
}

func TestDetectOverrideFuncs(t *testing.T) {
	t.Parallel()

	idx := parseSrc(t, overrideFixture)
	cls := buildClassifiers(idx, map[string]bool{"CancelJob": true})

	ov, ok := cls.Overrides["respondAsInvalidRequest"]
	require.True(t, ok)
	require.Equal(t, 1, ov.ParamIndex)
	require.Equal(t, "InvalidRequestException", ov.Code)
}

func TestSentinelCodes_ErrorsIsSwitch(t *testing.T) {
	t.Parallel()

	idx := parseSrc(t, sharedSentinelFixture)
	codes := sentinelCodes(idx)

	require.Equal(t, "ResourceNotFoundException", codes["ErrNotFound"])
}

func TestConstructorCode_OneHopPropagation(t *testing.T) {
	t.Parallel()

	idx := parseSrc(t, constructorFixture)
	sentinels := sentinelCodes(idx)

	var found string

	for _, f := range idx.Files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Name.Name != "notFoundError" {
				continue
			}

			code, ok := constructorCode(fd, sentinels)
			require.True(t, ok)
			found = code
		}
	}

	require.Equal(t, "ResourceNotFoundException", found)
}

// TestBatchItemCodeField_NotFlagged is the confirmed false positive found
// during this tool's own validation (services/bedrock's
// BatchDeleteAdvancedPromptOptimizationJobError{Code: "..."}): a per-item
// result field named "Code" inside a 200-OK batch response, not a wire
// error envelope. isCodeFieldLabel must exclude bare "Code".
func TestBatchItemCodeField_NotFlagged(t *testing.T) {
	t.Parallel()

	require.False(t, isCodeFieldLabel("Code"))
	require.True(t, isCodeFieldLabel("ErrorCode"))
	require.True(t, isCodeFieldLabel("Type"))
}

func TestStringSwitchCaseLiteral_RPCv2CBORShape(t *testing.T) {
	t.Parallel()

	src := `
package fixture

func deserializeOpErrorPutThing() error {
	switch string(errorName) {
	case "ConflictException":
		return nil
	}
	return nil
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "d.go", src, 0)
	require.NoError(t, err)

	var found []string

	ast.Inspect(f, func(n ast.Node) bool {
		if lit, ok := stringSwitchCaseLiteral(n); ok {
			found = append(found, lit)
		}

		return true
	})

	require.Equal(t, []string{"ConflictException"}, found)
}

func TestGenericProtocolCodes_InternalServerException(t *testing.T) {
	t.Parallel()

	require.True(
		t,
		genericProtocolCodes["InternalServerException"],
		"must be allowlisted -- see genericcodes.go's doc for the 90-false-positive mgn case this fixes",
	)
}

// collisionScopedFixture is services/eks's real shape (gopherstack-0yva,
// commit 43416bbd7): handleError and handleTagError both branch on the SAME
// identifier ErrNotFound to DIFFERENT codes. DescribeThing's own path calls
// only handleError; TagResourceValidated's calls only handleTagError. It
// also carries the "mirror" false positive from the same commit: a
// constructor (validateTagInput, returning bare ErrValidation) whose ONLY
// package-wide resolution comes from handleError, called from an operation
// whose own path never reaches handleError at all.
const collisionScopedFixture = `
package fixture

import (
	"errors"
	"fmt"
)

var ErrNotFound = errors.New("not found")
var ErrValidation = errors.New("invalid")

type Handler struct {
	Backend *Backend
}

func (h *Handler) handleError(err error) string {
	switch {
	case errors.Is(err, ErrNotFound):
		return "ResourceNotFoundException"
	case errors.Is(err, ErrValidation):
		return "InvalidParameterException"
	}
	return "InternalFailure"
}

func (h *Handler) handleTagError(err error) string {
	if errors.Is(err, ErrNotFound) {
		return "NotFoundException"
	}
	return "BadRequestException"
}

func (h *Handler) handleDescribeThing() error {
	err := h.Backend.DescribeThing()
	if err != nil {
		h.handleError(err)
	}
	return err
}

func validateTagInput() error {
	return ErrValidation
}

func (h *Handler) handleTagResourceValidated() error {
	if err := validateTagInput(); err != nil {
		return err
	}

	err := h.Backend.TagResourceInternal()
	if err != nil {
		h.handleTagError(err)
	}
	return err
}

type Backend struct{}

func (b *Backend) DescribeThing() error {
	return fmt.Errorf("%w: thing", ErrNotFound)
}

func (b *Backend) TagResourceInternal() error {
	return fmt.Errorf("%w: thing", ErrNotFound)
}
`

// TestFlattenSentinelCodes_CollisionOmitted confirms the package-wide
// fallback table never silently picks a winner between two mapper functions
// that map the same identifier to different codes.
func TestFlattenSentinelCodes_CollisionOmitted(t *testing.T) {
	t.Parallel()

	idx := parseSrc(t, collisionScopedFixture)
	flat := flattenSentinelCodes(funcSentinelCodes(idx))

	_, collided := flat["ErrNotFound"]
	require.False(t, collided, "a sentinel mapped to two different codes by two mappers must not resolve to either")

	require.Equal(t, "InvalidParameterException", flat["ErrValidation"], "a non-colliding sentinel must still resolve")
}

// TestLocalMapperScope_ScopesPerReachableMapper confirms an operation's own
// effective sentinel table comes from ONLY the mapper(s) its own hop-0 root
// actually calls, resolving the same identifier to two different codes for
// two different operations in the same package.
func TestLocalMapperScope_ScopesPerReachableMapper(t *testing.T) {
	t.Parallel()

	idx := parseSrc(t, collisionScopedFixture)
	cls := buildClassifiers(idx, map[string]bool{"DescribeThing": true, "TagResourceValidated": true})

	describeRoots := resolveOpRoots("DescribeThing", idx)
	require.NotEmpty(t, describeRoots)

	describeScope, describeScoped := localMapperScope(describeRoots, cls.ByFunc)
	require.True(t, describeScoped)
	require.Equal(t, "ResourceNotFoundException", describeScope["ErrNotFound"])

	tagRoots := resolveOpRoots("TagResourceValidated", idx)
	require.NotEmpty(t, tagRoots)

	tagScope, tagScoped := localMapperScope(tagRoots, cls.ByFunc)
	require.True(t, tagScoped)
	require.Equal(t, "NotFoundException", tagScope["ErrNotFound"])
}

// TestScan_SentinelCollision_ScopedPerMapper_NoFalsePositives is the
// gopherstack-0yva regression: services/eks's real 49-finding event,
// reproduced structurally. Must fail (produce findings) against a version
// that reverts to a single package-wide flat sentinel table.
func TestScan_SentinelCollision_ScopedPerMapper_NoFalsePositives(t *testing.T) {
	t.Parallel()

	idx := parseSrc(t, collisionScopedFixture)
	smt := singleModuleTruth(newTestModuleGroundTruth(
		map[string]map[string]bool{
			"DescribeThing":        {"ResourceNotFoundException": true},
			"TagResourceValidated": {"NotFoundException": true},
		},
		map[string]bool{
			"ResourceNotFoundException": true,
			"NotFoundException":         true,
			"InvalidParameterException": true,
			"BadRequestException":       true,
			"InternalFailure":           true,
		},
	))

	sr := scanWithIndex("fixture", []string{"fixture"}, "/repo", idx, smt)

	require.Empty(t, sr.Findings,
		"same-named sentinels resolved through different reachable mappers must not cross-contaminate, and a "+
			"constructor whose call site never reaches the resolving mapper must not be attributed that code")
}

// unresolvedCollisionFixture has two mapper functions colliding on the same
// identifier, like collisionScopedFixture, but NEITHER is called from the
// operation's own hop-0 root -- mirroring a mapper invoked outside this
// scan's modeled call graph. Neither code may be attributed.
const unresolvedCollisionFixture = `
package fixture

import (
	"errors"
	"fmt"
)

var ErrNotFound = errors.New("not found")

func mapperA(err error) string {
	switch {
	case errors.Is(err, ErrNotFound):
		return "ResourceNotFoundException"
	}
	return "InternalFailure"
}

func mapperB(err error) string {
	switch {
	case errors.Is(err, ErrNotFound):
		return "NotFoundException"
	}
	return "InternalFailure"
}

type Handler struct {
	Backend *Backend
}

func (h *Handler) handleGetThing() error {
	return h.Backend.GetThing()
}

type Backend struct{}

func (b *Backend) GetThing() error {
	return fmt.Errorf("%w: thing", ErrNotFound)
}
`

// TestScan_UnresolvableCollision_RefusesRatherThanGuesses confirms the
// "loud failure" fallback: when a collision cannot be pinned to a reachable
// mapper, the sentinel is dropped from resolution entirely -- neither
// mapper's code is attributed. Must fail (produce a finding for whichever
// mapper is visited last) against a version without collision detection.
func TestScan_UnresolvableCollision_RefusesRatherThanGuesses(t *testing.T) {
	t.Parallel()

	idx := parseSrc(t, unresolvedCollisionFixture)
	smt := singleModuleTruth(newTestModuleGroundTruth(
		map[string]map[string]bool{"GetThing": {"SomeOtherException": true}},
		map[string]bool{
			"SomeOtherException":        true,
			"ResourceNotFoundException": true,
			"NotFoundException":         true,
		},
	))

	sr := scanWithIndex("fixture", []string{"fixture"}, "/repo", idx, smt)

	require.Empty(t, sr.Findings,
		"neither colliding mapper's code is reachable through this operation's own call path; "+
			"the tool must refuse to report rather than guess which one applies")
}

// TestScan_SharedSentinel_NonCollidingManyCallers is a table-driven
// confirmation that flattenSentinelCodes/localMapperScope leave a NON-
// colliding shared sentinel's normal attribution untouched: many operations
// legitimately declare the shared mapper's code, and only the one that
// doesn't is reported -- gopherstack-0yva's fix must not suppress this
// shape, only the collision shape.
func TestScan_SharedSentinel_NonCollidingManyCallers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		declare bool
		wantLen int
	}{
		{"declares the shared mapper's code: clean", true, 0},
		{"does not declare it: reported", false, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idx := parseSrc(t, sharedSentinelFixture)

			declared := map[string]bool{"UnmappedFailureCode": true}
			if tt.declare {
				declared = map[string]bool{"ResourceNotFoundException": true}
			}

			smt := singleModuleTruth(newTestModuleGroundTruth(
				map[string]map[string]bool{
					"GetThing":    {"ResourceNotFoundException": true},
					"DeleteThing": declared,
				},
				map[string]bool{"ResourceNotFoundException": true, "UnmappedFailureCode": true},
			))

			sr := scanWithIndex("fixture", []string{"fixture"}, "/repo", idx, smt)

			codes := findingCodes(sr.Findings)
			require.NotContains(t, codes, "GetThing")
			require.Len(t, sr.Findings, tt.wantLen)
		})
	}
}
