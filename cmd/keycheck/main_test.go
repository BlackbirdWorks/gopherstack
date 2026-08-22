package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
}

// sdkGoodFixture reproduces the real, correctly-cased restjson1 shape from
// scheduler@v1.20.4 (post-fix, commit 8469dcdd9): GetScheduleOutput.Target
// wraps EcsParameters.NetworkConfiguration, whose deserializer switches on
// the lowercase-first "awsvpcConfiguration".
const sdkGoodFixture = `package fakesdk

type GetScheduleOutput struct{}
type Target struct{}
type EcsParameters struct{}
type NetworkConfiguration struct{}

func awsRestjson1_deserializeOpDocumentGetScheduleOutput(v **GetScheduleOutput, value interface{}) error {
	shape, ok := value.(map[string]interface{})
	if !ok {
		return nil
	}
	sv := *v
	for key, val := range shape {
		switch key {
		case "Target":
			if err := awsRestjson1_deserializeDocumentTarget(&sv.Target, val); err != nil {
				return err
			}
		}
	}
	*v = sv
	return nil
}

func awsRestjson1_deserializeDocumentTarget(v **Target, value interface{}) error {
	shape, ok := value.(map[string]interface{})
	if !ok {
		return nil
	}
	sv := *v
	for key, val := range shape {
		switch key {
		case "EcsParameters":
			if err := awsRestjson1_deserializeDocumentEcsParameters(&sv.EcsParameters, val); err != nil {
				return err
			}
		}
	}
	*v = sv
	return nil
}

func awsRestjson1_deserializeDocumentEcsParameters(v **EcsParameters, value interface{}) error {
	shape, ok := value.(map[string]interface{})
	if !ok {
		return nil
	}
	sv := *v
	for key, val := range shape {
		switch key {
		case "NetworkConfiguration":
			ncErr := awsRestjson1_deserializeDocumentNetworkConfiguration(&sv.NetworkConfiguration, val)
			if ncErr != nil {
				return ncErr
			}
		}
	}
	*v = sv
	return nil
}

func awsRestjson1_deserializeDocumentNetworkConfiguration(v **NetworkConfiguration, value interface{}) error {
	shape, ok := value.(map[string]interface{})
	if !ok {
		return nil
	}
	sv := *v
	for key := range shape {
		switch key {
		case "awsvpcConfiguration":
			_ = sv
		}
	}
	*v = sv
	return nil
}
`

// svcGoodFixture writes the real wire key "awsvpcConfiguration".
const svcGoodFixture = `package svc

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "GetSchedule":
		return h.handleGetSchedule(body)
	}
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": map[string]any{
					"awsvpcConfiguration": map[string]any{},
				},
			},
		},
	}
	_ = resp
	return nil
}
`

// svcBadFixture reproduces gopherstack's actual pre-fix bug from
// 8469dcdd9: the Go-field-name spelling "AwsvpcConfiguration" instead of
// the real wire key "awsvpcConfiguration". An exact-case real client drops
// the whole nested object silently.
const svcBadFixture = `package svc

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "GetSchedule":
		return h.handleGetSchedule(body)
	}
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": map[string]any{
					"AwsvpcConfiguration": map[string]any{},
				},
			},
		},
	}
	_ = resp
	return nil
}
`

func TestRunCheck_CapitalizationBug(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		svc           string
		wantCaseFound string
		wantMismatch  bool
	}{
		{
			name: "known-bad pre-fix scheduler casing (8469dcdd9)", svc: svcBadFixture,
			wantMismatch: true, wantCaseFound: "AwsvpcConfiguration",
		},
		{name: "known-good post-fix scheduler casing", svc: svcGoodFixture, wantMismatch: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sdkDir := t.TempDir()
			writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
			svcDir := t.TempDir()
			writeFile(t, svcDir, "handler.go", tc.svc)

			res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
			require.NoError(t, err)
			require.Len(t, res.OpsChecked, 1)
			require.Empty(t, res.UnresolvedOps)

			op := res.OpsChecked[0]
			if !tc.wantMismatch {
				assert.Empty(t, op.NotInTree, "known-good casing must not report a mismatch")

				return
			}

			require.NotEmpty(t, op.NotInTree,
				"keycheck did not catch the known-bad capitalization it exists to catch")
			assert.Contains(t, op.NotInTree, tc.wantCaseFound)
			require.NotEmpty(t, op.CaseMismatch,
				"wrong-cased key should be flagged as a case mismatch, not just absent")
			assert.Contains(t, op.CaseMismatch[0], "sdk expects: awsvpcConfiguration")
		})
	}
}

func TestRunCheck_FailLoud(t *testing.T) {
	t.Parallel()

	t.Run("wrong prefix resolves zero sdk ops and types", func(t *testing.T) {
		t.Parallel()

		sdkDir := t.TempDir()
		writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
		svcDir := t.TempDir()
		writeFile(t, svcDir, "handler.go", svcGoodFixture)

		res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsAwsjson11_", svcDir, "")
		require.NoError(t, err)
		assert.Zero(t, res.SDKOpsResolved)
		assert.Zero(t, res.SDKTypesResolved)
		assert.Equal(t, exitUnresolved, report(res, svcDir, "awsAwsjson11_"))
	})

	t.Run("unrecognised routing style resolves zero handler dispatch", func(t *testing.T) {
		t.Parallel()

		sdkDir := t.TempDir()
		writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
		svcDir := t.TempDir()
		writeFile(t, svcDir, "handler.go", `package svc

func helper(x int) int { return x + 1 }
`)

		res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
		require.NoError(t, err)
		assert.Zero(t, res.HandlerOpsResolved)
		assert.Equal(t, exitUnresolved, report(res, svcDir, "awsRestjson1_"))
	})

	t.Run("dispatched op with no sdk resolution at all is unresolved, not silently skipped", func(t *testing.T) {
		t.Parallel()

		sdkDir := t.TempDir()
		writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
		svcDir := t.TempDir()
		writeFile(t, svcDir, "handler.go", `package svc

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "TotallyUnknownOp":
		return h.handleTotallyUnknownOp(body)
	}
	return nil
}

func (h *Handler) handleTotallyUnknownOp(body []byte) []byte {
	resp := map[string]any{"Whatever": true}
	_ = resp
	return nil
}
`)

		res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
		require.NoError(t, err)
		require.Contains(t, res.UnresolvedOps, "TotallyUnknownOp")
		assert.Equal(t, exitUnresolved, report(res, svcDir, "awsRestjson1_"))
	})
}

// sdkEmptyOutputFixture mirrors ssoadmin's six genuinely-empty-output ops
// (e.g. DeleteApplicationAccessScope): the SDK generates a wrapper
// HandleDeserialize that never calls any deserializeOpDocument function,
// because the Output struct has no members beyond ResultMetadata.
const sdkEmptyOutputFixture = `package fakesdk

import (
	"context"

	"github.com/aws/smithy-go/middleware"
)

type DeleteScheduleOutput struct{}

type awsRestjson1_deserializeOpDeleteSchedule struct{}

func (*awsRestjson1_deserializeOpDeleteSchedule) ID() string { return "OperationDeserializer" }

func (m *awsRestjson1_deserializeOpDeleteSchedule) HandleDeserialize(
	ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
) (
	out middleware.DeserializeOutput, metadata middleware.Metadata, err error,
) {
	out, metadata, err = next.HandleDeserialize(ctx, in)
	if err != nil {
		return out, metadata, err
	}
	output := &DeleteScheduleOutput{}
	out.Result = output
	return out, metadata, nil
}
`

func TestRunCheck_EmptyOutputOp(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkEmptyOutputFixture)

	t.Run("empty-output op writing nothing resolves clean, not unresolved", func(t *testing.T) {
		t.Parallel()

		svcDir := t.TempDir()
		writeFile(t, svcDir, "handler.go", `package svc

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "DeleteSchedule":
		return h.handleDeleteSchedule(body)
	}
	return nil
}

func (h *Handler) handleDeleteSchedule(body []byte) []byte {
	return nil
}
`)

		res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
		require.NoError(t, err)
		require.Empty(t, res.UnresolvedOps)
		require.Len(t, res.OpsChecked, 1)
		assert.True(t, res.OpsChecked[0].EmptyOutput)
		assert.Empty(t, res.OpsChecked[0].NotInTree)
	})

	t.Run("empty-output op writing any key is a mismatch", func(t *testing.T) {
		t.Parallel()

		svcDir := t.TempDir()
		writeFile(t, svcDir, "handler.go", `package svc

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "DeleteSchedule":
		return h.handleDeleteSchedule(body)
	}
	return nil
}

func (h *Handler) handleDeleteSchedule(body []byte) []byte {
	resp := map[string]any{"ScheduleName": "oops"}
	_ = resp
	return nil
}
`)

		res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
		require.NoError(t, err)
		require.Empty(t, res.UnresolvedOps)
		require.Len(t, res.OpsChecked, 1)
		assert.True(t, res.OpsChecked[0].EmptyOutput)
		assert.Contains(t, res.OpsChecked[0].NotInTree, "ScheduleName")
	})
}

// TestRunCheck_WrappedMapDispatch reproduces applicationautoscaling's real
// dispatch table shape (services/applicationautoscaling/handler.go): op
// names map to a handler wrapped in a helper call
// (service.WrapOp(h.handleX)), not a bare method value. Found live during
// the gopherstack-zquj sweep: 36 services use this shape and all initially
// misreported as HandlerOpsResolved == 0 ("unrecognised routing style").
func TestRunCheck_WrappedMapDispatch(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", `package svc

type Handler struct{}
type opFunc func([]byte) []byte

func wrapOp(f func([]byte) []byte) opFunc { return f }

var dispatchTable = map[string]opFunc{
	"GetSchedule": wrapOp(new(Handler).handleGetSchedule),
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": map[string]any{
					"awsvpcConfiguration": map[string]any{},
				},
			},
		},
	}
	_ = resp
	return nil
}
`)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)
	require.NotZero(t, res.HandlerOpsResolved, "wrapped map-dispatch value must still resolve a handler")
	require.Empty(t, res.UnresolvedOps)
	require.Len(t, res.OpsChecked, 1)
	assert.Empty(t, res.OpsChecked[0].NotInTree)
}

// svcConstKeyDispatchFixture reproduces dms's real dispatch-table shape
// (services/dms/handler_*.go, e.g. opsTags(): map[string]service.JSONOpFunc{
// opAddTagsToResource: service.WrapOp(h.handleAddTagsToResource), ...}):
// dispatch keys are package-level string consts, not string literals. Found
// live during the gopherstack-zquj sweep -- dms defines 96 ops this way and
// only 4 (the ones some family funcs happened to key with an inline literal)
// were visible to the pre-fix scanner. The other 92 didn't appear as
// UnresolvedOps either: they vanished from the report entirely, which is
// what made runCheck report dms as a false "N/A: writes zero map[string]<T>
// literal keys" (read as clean) instead of "mostly never actually checked".
const svcConstKeyDispatchFixture = `package svc

const opGetSchedule = "GetSchedule"

type Handler struct{}
type opFunc func([]byte) []byte

var dispatchTable = map[string]opFunc{
	"TotallyUnknownOp": (&Handler{}).handleTotallyUnknownOp,
	opGetSchedule:      (&Handler{}).handleGetSchedule,
}

func (h *Handler) handleTotallyUnknownOp(body []byte) []byte {
	resp := map[string]any{"Whatever": true}
	_ = resp
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": map[string]any{
					"awsvpcConfiguration": map[string]any{},
				},
			},
		},
	}
	_ = resp
	return nil
}
`

func TestRunCheck_ConstKeyedMapDispatch(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", svcConstKeyDispatchFixture)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)

	assert.Equal(t, 2, res.HandlerOpsResolved,
		"both the literal-keyed and const-keyed dispatch entries must resolve")
	assert.Contains(t, res.UnresolvedOps, "TotallyUnknownOp")

	var found bool
	for _, or := range res.OpsChecked {
		if or.Op == "GetSchedule" {
			found = true
			assert.Empty(t, or.NotInTree)
		}
	}
	require.True(t, found,
		"a dispatch table keyed by a package-level const (dms's real shape) must not vanish from the report")
}

func TestRunCheck_InternalOpSkipped(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", `package svc

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "__SimulateAttack":
		return h.handleSimulateAttack(body)
	}
	return nil
}

func (h *Handler) handleSimulateAttack(body []byte) []byte {
	resp := map[string]any{"AttackId": "x"}
	_ = resp
	return nil
}
`)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)
	assert.Contains(t, res.InternalOpsSkipped, "__SimulateAttack")
	assert.NotContains(t, res.UnresolvedOps, "__SimulateAttack")
	assert.Empty(t, res.OpsChecked, "an internal-only op should not be checked against the SDK at all")
}

// svcBareLowercaseDispatchFixture reproduces comprehend/personalize/
// translate's real dispatch-table shape (e.g. personalize's buildOps():
// map[string]opFunc{"CreateDatasetGroup": h.createDatasetGroup, ...}): op
// names map to a bare lowercase method value with no "handle"/"json" prefix
// at all. Found live during the gopherstack-0kk8 sweep -- all three services
// reported HandlerOpsResolved == 0 against the pre-fix scanner, which
// resolves only the "handle"/"json"-prefixed convention.
const svcBareLowercaseDispatchFixture = `package svc

type opFunc func([]byte) []byte

type Handler struct{}

var dispatchTable = map[string]opFunc{
	"GetSchedule": (&Handler{}).getSchedule,
}

func (h *Handler) getSchedule(body []byte) []byte {
	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": map[string]any{
					%s
				},
			},
		},
	}
	_ = resp
	return nil
}
`

func TestRunCheck_BareLowercaseMethodDispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		field        string
		wantMismatch bool
	}{
		{name: "correct wire casing", field: `"awsvpcConfiguration": map[string]any{},`},
		{name: "wrong go-field casing", field: `"AwsvpcConfiguration": map[string]any{},`, wantMismatch: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sdkDir := t.TempDir()
			writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
			svcDir := t.TempDir()
			writeFile(t, svcDir, "handler.go", fmt.Sprintf(svcBareLowercaseDispatchFixture, tc.field))

			res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
			require.NoError(t, err)
			require.Equal(t, 1, res.HandlerOpsResolved,
				"a bare lowercase method value (no handle/json prefix) must still resolve a handler")
			require.Empty(t, res.UnresolvedOps)
			require.Len(t, res.OpsChecked, 1)

			if !tc.wantMismatch {
				assert.Empty(t, res.OpsChecked[0].NotInTree)

				return
			}
			assert.Contains(t, res.OpsChecked[0].NotInTree, "AwsvpcConfiguration")
		})
	}
}

// svcWrappedBackendCallDispatchFixture reproduces ssm's real dispatch-table
// shape (services/ssm/handler.go's ssm*Ops() family): op names map to a
// real backend method wrapped in a single-argument helper call
// (jsonOp(h.Backend.PutParameter)) -- the backend method name has no
// handle/json prefix either, and findHandlerSelector's AST walk previously
// found the "jsonOp" call itself before ever considering its argument.
const svcWrappedBackendCallDispatchFixture = `package svc

type ssmActionFn func([]byte) []byte

type Backend struct{}
type Handler struct{ Backend *Backend }

var h = &Handler{}

func jsonOp(fn func([]byte) []byte) ssmActionFn { return fn }

var dispatchTable = map[string]ssmActionFn{
	"GetSchedule": jsonOp(h.Backend.getSchedule),
}

func (b *Backend) getSchedule(body []byte) []byte {
	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": map[string]any{
					"awsvpcConfiguration": map[string]any{},
				},
			},
		},
	}
	_ = resp
	return nil
}
`

func TestRunCheck_WrappedBackendCallDispatch(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", svcWrappedBackendCallDispatchFixture)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)
	require.Equal(t, 1, res.HandlerOpsResolved,
		"a real backend method wrapped in a single-arg helper call (ssm's jsonOp) must still resolve a handler")
	require.Empty(t, res.UnresolvedOps)
	require.Len(t, res.OpsChecked, 1)
	assert.Empty(t, res.OpsChecked[0].NotInTree)
}

// svcClosureDispatchFixture reproduces ssm's two inline-closure dispatch
// entries (handler.go's AddTagsToResource/RemoveTagsFromResource): the
// dispatch value is a func literal that decodes the body via a stdlib call
// (encoding/json.Unmarshal, which has no local FuncDecl and must never be
// mistaken for the handler) and calls the real backend method only in its
// return statement. Scoping the search to return statements -- not the
// whole closure body -- is what keeps this from grabbing an unrelated
// helper call that happens to run earlier in the closure.
const svcClosureDispatchFixture = `package svc

import "encoding/json"

type ssmActionFn func([]byte) []byte

type Backend struct{}
type Handler struct{ Backend *Backend }

var h = &Handler{}

var dispatchTable = map[string]ssmActionFn{
	"GetSchedule": func(body []byte) []byte {
		var input struct{}
		_ = json.Unmarshal(body, &input)
		return h.Backend.getSchedule(body)
	},
}

func (b *Backend) getSchedule(body []byte) []byte {
	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": map[string]any{
					"awsvpcConfiguration": map[string]any{},
				},
			},
		},
	}
	_ = resp
	return nil
}
`

func TestRunCheck_ClosureDispatch(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", svcClosureDispatchFixture)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)
	require.Equal(t, 1, res.HandlerOpsResolved,
		"a closure that decodes the body then calls the real backend method in a return statement "+
			"must still resolve to that backend method, not the json.Unmarshal decode step")
	require.Empty(t, res.UnresolvedOps)
	require.Len(t, res.OpsChecked, 1)
	assert.Equal(t, "getSchedule", res.OpsChecked[0].Handler)
	assert.Empty(t, res.OpsChecked[0].NotInTree)
}

// svcSliceBindingDispatchFixture reproduces glue's real dispatch-table shape
// (handler_routing.go's glueOpBindings): the single ordered source of truth
// is a package-level []struct{ bind func(*Handler) service.JSONOpFunc; name
// string }{...} literal iterated in buildOps(), not a map[string]X{...}
// literal -- recordMapDispatch only ever inspected CompositeLit map
// literals with KeyValueExpr elements, never a slice of binding structs.
// The bind field already uses the ordinary service.WrapOp(h.handleX) shape
// this repo's other 36 wrapped-map-dispatch services use, so this pins only
// the new slice-shape recognition, not any matcher loosening.
const svcSliceBindingDispatchFixture = `package svc

type opFunc func([]byte) []byte

type Handler struct{}

func wrapOp(f func([]byte) []byte) opFunc { return f }

var glueOpBindings = []struct {
	bind func(*Handler) opFunc
	name string
}{
	{
		name: "GetSchedule",
		bind: func(h *Handler) opFunc {
			return wrapOp(h.handleGetSchedule)
		},
	},
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": map[string]any{
					"awsvpcConfiguration": map[string]any{},
				},
			},
		},
	}
	_ = resp
	return nil
}
`

func TestRunCheck_SliceBindingDispatch(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", svcSliceBindingDispatchFixture)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)
	require.Equal(t, 1, res.HandlerOpsResolved,
		"an ordered-binding-slice dispatch table (glue's real shape) must not vanish from the report")
	require.Empty(t, res.UnresolvedOps)
	require.Len(t, res.OpsChecked, 1)
	assert.Equal(t, "handleGetSchedule", res.OpsChecked[0].Handler)
	assert.Empty(t, res.OpsChecked[0].NotInTree)
}

// TestRunCheck_NoWrittenKeys covers what remains genuinely undetectable
// after gopherstack-v4a4 taught keycheck to read *Output-suffixed struct
// literals: an ANONYMOUS struct literal (no named type, so collectStructTagKeys'
// v.Type.(*ast.Ident) match cannot fire, KNOWN BLIND SPOT #5). Before
// gopherstack-v4a4 this same N/A path was (wrongly) reported for every
// struct-literal-only service, including named *Output structs like glue's
// -- see the struct-tag-mismatch test below for proof that named case is now
// actually checked, not just excused as N/A.
func TestRunCheck_NoWrittenKeys(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", `package svc

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) any {
	switch op {
	case "GetSchedule":
		return h.handleGetSchedule(body)
	}
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) any {
	return struct{ Target string }{Target: "x"}
}
`)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)
	assert.True(t, res.NoWrittenKeys, "an anonymous struct literal should report N/A, not a false clean")
	assert.Zero(t, res.TotalWritten)
	assert.Empty(t, res.UnresolvedOps)
	assert.Equal(t, exitClean, report(res, svcDir, "awsRestjson1_"))
}

// sdkQuerySchemaVersionMetadataFixture reproduces the real awsjson1.1 shape
// (glue@v1.152.0) whose case list this campaign's actual bug (c3aa73e59) was
// checked against: MetadataInfoMap and SchemaVersionId.
const sdkQuerySchemaVersionMetadataFixture = `package fakesdk

type QuerySchemaVersionMetadataOutput struct{}

func awsAwsjson11_deserializeOpDocumentQuerySchemaVersionMetadataOutput(
	v **QuerySchemaVersionMetadataOutput, value interface{},
) error {
	shape, ok := value.(map[string]interface{})
	if !ok {
		return nil
	}
	sv := *v
	for key, val := range shape {
		switch key {
		case "MetadataInfoMap":
			_ = val
		case "SchemaVersionId":
			_ = val
		}
	}
	*v = sv
	return nil
}
`

// svcQuerySchemaVersionMetadataFixture builds the wire response from a
// locally-declared *Output struct, parameterised on its json tag so the
// table below can drive both the real pre-fix bug (json:"MetadataInfo") and
// its fix (json:"MetadataInfoMap").
func svcQuerySchemaVersionMetadataFixture(metadataTag string) string {
	return `package svc

type Handler struct{}

type querySchemaVersionMetadataOutput struct {
	MetadataInfo    map[string]any ` + "`json:\"" + metadataTag + "\"`" + `
	SchemaVersionID string         ` + "`json:\"SchemaVersionId\"`" + `
}

func (h *Handler) Dispatch(op string, body []byte) any {
	switch op {
	case "QuerySchemaVersionMetadata":
		return h.handleQuerySchemaVersionMetadata(body)
	}
	return nil
}

func (h *Handler) handleQuerySchemaVersionMetadata(body []byte) any {
	return querySchemaVersionMetadataOutput{
		MetadataInfo:    map[string]any{},
		SchemaVersionID: "v1",
	}
}
`
}

// TestRunCheck_StructTagMismatch is gopherstack-v4a4's own precedent test:
// it reproduces glue's actual pre-fix bug (c3aa73e59) as a synthetic
// fixture and confirms the struct-tag scan (KNOWN BLIND SPOT #5's fix)
// catches it. Before this session's change, BOTH rows below reported
// NoWrittenKeys (N/A) -- keycheck never read a struct tag at all, so the
// bad tag was as invisible to the tool as it was to the two raw-body tests
// that shipped it (both decoded through their own locally re-typed struct
// tagged the same wrong way).
func TestRunCheck_StructTagMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tag          string
		wantMismatch bool
	}{
		{
			name: "known-bad pre-fix glue tag (c3aa73e59)", tag: "MetadataInfo",
			wantMismatch: true,
		},
		{name: "known-good post-fix glue tag", tag: "MetadataInfoMap", wantMismatch: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sdkDir := t.TempDir()
			writeFile(t, sdkDir, "deserializers.go", sdkQuerySchemaVersionMetadataFixture)
			svcDir := t.TempDir()
			writeFile(t, svcDir, "handler.go", svcQuerySchemaVersionMetadataFixture(tc.tag))

			res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsAwsjson11_", svcDir, "")
			require.NoError(t, err)
			require.Len(t, res.OpsChecked, 1)
			require.Empty(t, res.UnresolvedOps)
			require.False(t, res.NoWrittenKeys,
				"the struct-tag scan must read the *Output literal, not excuse it as N/A")

			op := res.OpsChecked[0]
			if !tc.wantMismatch {
				assert.Empty(t, op.NotInTree, "known-good tag must not report a mismatch")

				return
			}

			require.NotEmpty(t, op.NotInTree,
				"keycheck did not catch the known-bad struct tag it was extended to catch")
			assert.Contains(t, op.NotInTree, "MetadataInfo")
			assert.NotContains(t, op.NotInTree, "SchemaVersionId",
				"the correctly-tagged sibling field must not false-positive")
		})
	}
}

// svcAmbiguousHandlerBindingFixture and svcAmbiguousHandlerBindingQueryFixture
// reproduce sqs's real gopherstack-kiwf shape: TWO package-level dispatch
// tables, in separate files, bind the SAME op to two DIFFERENT handlers -- a
// modern "handle<Op>" JSON handler (handler.go, matched by the strict
// handleNameRe rule) and a legacy "query<Op>" handler wrapped in a closure
// (query.go, resolved only through the findHandlerSelectorLoose fallback).
// "query.go" sorts after "handler.go" alphabetically, exactly like sqs's
// real handler.go/query.go, so the pre-fix last-write-wins scan silently
// resolved the op to the wrong (query) handler and compared its fields
// against the JSON SDK's key set -- 85 fabricated MISMATCH rows on real sqs.
const svcAmbiguousHandlerBindingFixture = `package svc

type Handler struct{}
type dispatchFn func([]byte) []byte

func (h *Handler) dispatchTable() map[string]dispatchFn {
	return map[string]dispatchFn{
		"GetSchedule": h.handleGetSchedule,
	}
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": map[string]any{
					"awsvpcConfiguration": map[string]any{},
				},
			},
		},
	}
	_ = resp
	return nil
}
`

const svcAmbiguousHandlerBindingQueryFixture = `package svc

func (h *Handler) queryActionTable() map[string]dispatchFn {
	return map[string]dispatchFn{
		"GetSchedule": func(body []byte) []byte {
			return h.queryGetSchedule(body)
		},
	}
}

func (h *Handler) queryGetSchedule(body []byte) []byte {
	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": map[string]any{
					"AwsvpcConfiguration": map[string]any{},
				},
			},
		},
	}
	_ = resp
	return nil
}
`

func TestRunCheck_AmbiguousHandlerBinding(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", svcAmbiguousHandlerBindingFixture)
	writeFile(t, svcDir, "query.go", svcAmbiguousHandlerBindingQueryFixture)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)

	require.Contains(t, res.AmbiguousOps, "GetSchedule",
		"an op bound to two different handlers in separate files must be reported ambiguous, "+
			"not silently resolved by file-processing order")
	assert.ElementsMatch(t, []string{"handleGetSchedule", "queryGetSchedule"}, res.AmbiguousHandlers["GetSchedule"],
		"both conflicting handler names must be visible in the output")

	for _, or := range res.OpsChecked {
		assert.NotEqual(t, "GetSchedule", or.Op,
			"an ambiguous op must never be checked against the SDK -- that means comparing the "+
				"wrong handler's keys, which is how gopherstack-kiwf's 85 fabricated sqs "+
				"mismatches happened")
	}
	assert.Empty(t, res.UnresolvedOps, "ambiguity is its own category, not an unresolved-sdk-op")
	assert.Equal(t, exitUnresolved, report(res, svcDir, "awsRestjson1_"),
		"an ambiguous binding must fail loud like every other 'not actually checked' state")
}

// TestRunCheck_StructTagIgnoresUnexportedField reproduces the real false
// positive found live while triaging the blind-spot-#7 sweep's newly-checked
// batch service: ComputeEnvironment carries an unexported `region string`
// field (services/batch/models.go) whose own doc comment explains it is kept
// unexported specifically so a plain json.Marshal never emits it. Before this
// fix, structTagFields' no-json-tag fallback used field.Names[0].Name with no
// exported check, so this single unexported field fabricated a "region"
// MISMATCH on every op reachable from DescribeComputeEnvironmentsOutput ->
// ComputeEnvironment -- five real batch ops, none of which have any actual
// bug. encoding/json never marshals an unexported field regardless of tag.
func TestRunCheck_StructTagIgnoresUnexportedField(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", `package svc

type getScheduleOutput struct {
	Target string
	region string
}

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) any {
	switch op {
	case "GetSchedule":
		return h.handleGetSchedule(body)
	}
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) any {
	return getScheduleOutput{Target: "x", region: "us-east-1"}
}
`)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)
	require.Len(t, res.OpsChecked, 1)

	assert.Empty(t, res.OpsChecked[0].NotInTree,
		"an unexported Go field must never be treated as a wire key -- encoding/json never marshals it")
}

// svcPathKeyedDispatchFixture reproduces mgn's real dispatch-table shape
// (services/mgn/handler_routes.go, e.g. routesTags():
// map[string]routeEntry{"DELETE tags": {op: "UntagResource",
// fn: h.handleUntagResource}, ...}): the dispatch table's KEY is a
// method+path string, never the SDK's PascalCase operation name -- unlike
// dms/glue/ssm's dispatch-shape variants (all fixed above), which just use a
// different SYNTAX to say the real op name, mgn's syntax doesn't contain the
// real op name in the key at ALL. Before KNOWN BLIND SPOT #7's fix,
// idx.ops["POST GetSchedule"] and idx.ops["DELETE tags"] both miss and both
// ops become UnresolvedOps even though HandlerOpsResolved shows the dispatch
// table itself resolved (this is the real shape scored 95/95 for mgn during
// the gopherstack-zquj re-sweep).
const svcPathKeyedDispatchFixture = `package svc

type routeEntry struct {
	op string
	fn func([]byte) []byte
}

type Handler struct{}

func (h *Handler) routes() map[string]routeEntry {
	return map[string]routeEntry{
		"POST GetSchedule": {op: "GetSchedule", fn: h.handleGetSchedule},
		"DELETE tags":      {op: "UntagResource", fn: h.handleUntagResource},
	}
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": map[string]any{
					"awsvpcConfiguration": map[string]any{},
				},
			},
		},
	}
	_ = resp
	return nil
}

func (h *Handler) handleUntagResource(body []byte) []byte {
	return nil
}
`

// TestRunCheck_PathKeyedDispatchOpNameRecovery pins KNOWN BLIND SPOT #7's
// fix against mgn's real REST-path-keyed dispatch shape. Against the
// unfixed tool (idx.ops looked up directly by the raw dispatch key, no
// recoverOpName fallback) this reproduces exactly what the gopherstack-zquj
// re-sweep found: HandlerOpsResolved shows the dispatch table fully
// resolved (2/2 here), yet BOTH ops land in UnresolvedOps --
//
//	handler dispatch resolved: 2 ops
//	ERROR: op POST GetSchedule has no deserializeOpDocumentPOST GetScheduleOutput function ...
//	ERROR: op DELETE tags has no deserializeOpDocumentDELETE tagsOutput function ...
//
// -- even though GetSchedule is a real, fully-resolvable SDK op in
// sdkGoodFixture. Confirmed by hand-reverting resolveOpNames to a bare
// `idx.ops[op]` lookup (this test's own pre-fix state) before writing the
// fix.
func TestRunCheck_PathKeyedDispatchOpNameRecovery(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", svcPathKeyedDispatchFixture)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)

	require.Equal(t, 2, res.HandlerOpsResolved,
		"the dispatch table itself must resolve fully -- this is a naming gap, not a dispatch-shape gap")
	require.Len(t, res.OpsChecked, 1, "GetSchedule is the only op in this fixture's minimal SDK")

	or := res.OpsChecked[0]
	assert.Equal(t, "GetSchedule", or.Op, "the recovered op name must be the real SDK name, not the raw dispatch key")
	assert.Equal(t, "POST GetSchedule", or.DispatchKey,
		"the raw dispatch key must stay visible in the report so a recovered op is traceable")
	assert.Empty(t, or.NotInTree)

	// "DELETE tags"/UntagResource has no match in this fixture's minimal SDK
	// (only GetSchedule exists) -- it is correctly reported unresolved,
	// proving recovery never fabricates a match that isn't really there.
	assert.Contains(t, res.UnresolvedOps, "DELETE tags")

	assert.Equal(t, exitPartial, report(res, svcDir, "awsRestjson1_"))
}

// TestRunCheck_PathKeyedDispatchAmbiguousRecovery proves the recovery added
// for KNOWN BLIND SPOT #7 refuses to guess, the same contract KNOWN BLIND
// SPOT #6 established: two different REST-path dispatch keys whose
// handlers recover to the SAME real op name under DIFFERENT handler
// functions must be reported ambiguous, never silently resolved to whichever
// one happened to iterate first.
func TestRunCheck_PathKeyedDispatchAmbiguousRecovery(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", `package svc

type routeEntry struct {
	fn func([]byte) []byte
}

type Handler struct{}

func (h *Handler) routes() map[string]routeEntry {
	return map[string]routeEntry{
		"POST v1/schedule":   {fn: h.handleGetSchedule},
		"POST v2/scheduleGet": {fn: h.jsonGetSchedule},
	}
}

func (h *Handler) handleGetSchedule(body []byte) []byte { return nil }
func (h *Handler) jsonGetSchedule(body []byte) []byte   { return nil }
`)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)

	require.Equal(t, 2, res.HandlerOpsResolved)
	require.Empty(t, res.UnresolvedOps, "ambiguity is its own category, not an unresolved-sdk-op")
	require.Contains(t, res.AmbiguousOps, "GetSchedule")
	assert.ElementsMatch(t, []string{"handleGetSchedule", "jsonGetSchedule"}, res.AmbiguousHandlers["GetSchedule"],
		"both conflicting handlers recovered to the same real op name must be visible in the output")

	for _, or := range res.OpsChecked {
		assert.NotEqual(t, "GetSchedule", or.Op,
			"two dispatch keys colliding on the same recovered op name under different handlers "+
				"must never be silently checked against the wrong one")
	}

	assert.Equal(t, exitUnresolved, report(res, svcDir, "awsRestjson1_"))
}

// TestReport_PartialVerdictDistinguishesFromUnresolved is priority #1 of
// gopherstack-zquj's blind-spot-#7 pass: before this, res.UnresolvedOps>0
// forced exitUnresolved regardless of how much of the service WAS checked,
// so a service with 100+ ops checked and real mismatch data (cognitoidp:
// 102 ops checked, 304 mismatched keys) exited identically to a service
// with zero ops ever dispatched. exitPartial (and the VERDICT line) make
// the two visually and mechanically distinguishable.
func TestReport_PartialVerdictDistinguishesFromUnresolved(t *testing.T) {
	t.Parallel()

	t.Run("substantially checked service with a residual unresolved op is PARTIAL, not UNRESOLVED", func(t *testing.T) {
		t.Parallel()

		sdkDir := t.TempDir()
		writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
		svcDir := t.TempDir()
		writeFile(t, svcDir, "handler.go", `package svc

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "GetSchedule":
		return h.handleGetSchedule(body)
	case "TotallyUnknownOp":
		return h.handleTotallyUnknownOp(body)
	}
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": map[string]any{
					"awsvpcConfiguration": map[string]any{},
				},
			},
		},
	}
	_ = resp
	return nil
}

func (h *Handler) handleTotallyUnknownOp(body []byte) []byte { return nil }
`)

		res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
		require.NoError(t, err)
		require.Len(t, res.OpsChecked, 1)
		require.Contains(t, res.UnresolvedOps, "TotallyUnknownOp")

		assert.Equal(t, exitPartial, report(res, svcDir, "awsRestjson1_"),
			"one checked op plus one unresolved op must report PARTIAL, distinct from a wholly "+
				"unresolved service")
	})

	t.Run("zero ops checked at all is UNRESOLVED, not PARTIAL", func(t *testing.T) {
		t.Parallel()

		sdkDir := t.TempDir()
		writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
		svcDir := t.TempDir()
		writeFile(t, svcDir, "handler.go", `package svc

func helper(x int) int { return x + 1 }
`)

		res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
		require.NoError(t, err)
		require.Empty(t, res.OpsChecked)

		assert.Equal(t, exitUnresolved, report(res, svcDir, "awsRestjson1_"))
	})
}
