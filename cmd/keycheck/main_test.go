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
	assert.Empty(t, res.DeterministicOverrides,
		"sqs's real shape: two tables never merged via maps.Copy into a shared destination -- "+
			"resolveDeterministicOverrides must find no chain containing both sides and leave this "+
			"genuinely ambiguous, not guess a winner")
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

// svcEnumSwitchFixture reproduces glacier's real false-positive shape
// (handler_jobs.go's `switch j.Action { case jobTypeInventoryRetrieval: ...
// case jobTypeSelect: ... }`, gopherstack-85e3): a nested switch on a
// struct field, inside an already-dispatched op's own handler, whose case
// values are handler-shaped-call-bound strings that are not real SDK
// operations at all. GetSchedule is the only real op in sdkGoodFixture;
// ModeA/ModeB never appear there.
const svcEnumSwitchFixture = `package svc

type cfg struct{ Mode string }

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "GetSchedule":
		return h.handleGetSchedule(body)
	}
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	c := cfg{}
	switch c.Mode {
	case "ModeA":
		return h.handleModeA(body)
	case "ModeB":
		return h.handleModeB(body)
	}

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

func (h *Handler) handleModeA(body []byte) []byte { return nil }
func (h *Handler) handleModeB(body []byte) []byte { return nil }
`

// TestFilterEnumGroups_WholeGroupUnresolvedIsFiltered pins gopherstack-85e3's
// enum/type-string-table class: a switch batting 0-for-2 against the real
// SDK op index (ModeA/ModeB) is moved out of UnresolvedOps into FilteredOps,
// visibly, while GetSchedule -- dispatched from a DIFFERENT switch -- is
// still checked normally.
func TestFilterEnumGroups_WholeGroupUnresolvedIsFiltered(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", svcEnumSwitchFixture)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"ModeA", "ModeB"}, res.FilteredOps,
		"a switch whose every candidate op fails SDK resolution must be filtered as an enum table")
	assert.Empty(t, res.UnresolvedOps,
		"a fully-filtered group must not also linger in UnresolvedOps")

	require.Len(t, res.OpsChecked, 1)
	assert.Equal(t, "GetSchedule", res.OpsChecked[0].Op)
	assert.Empty(t, res.OpsChecked[0].NotInTree)
}

// sdkTwoOpsFixture extends sdkGoodFixture with a second real op, ModeA, so
// filterEnumGroups' "must not over-suppress" tests can exercise a group
// where SOME candidates genuinely resolve.
const sdkTwoOpsFixture = `package fakesdk

type GetScheduleOutput struct{}
type ModeAOutput struct{}

func awsRestjson1_deserializeOpDocumentGetScheduleOutput(v **GetScheduleOutput, value interface{}) error {
	return nil
}

func awsRestjson1_deserializeOpDocumentModeAOutput(v **ModeAOutput, value interface{}) error {
	shape, ok := value.(map[string]interface{})
	if !ok {
		return nil
	}
	sv := *v
	for key := range shape {
		switch key {
		case "Result":
		}
	}
	*v = sv
	return nil
}
`

func TestFilterEnumGroups_DoesNotOverSuppress(t *testing.T) {
	t.Parallel()

	t.Run("a group with even one resolved candidate is left untouched", func(t *testing.T) {
		t.Parallel()

		sdkDir := t.TempDir()
		writeFile(t, sdkDir, "deserializers.go", sdkTwoOpsFixture)
		svcDir := t.TempDir()
		writeFile(t, svcDir, "handler.go", `package svc

type cfg struct{ Mode string }

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "GetSchedule":
		return h.handleGetSchedule(body)
	}
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	c := cfg{}
	switch c.Mode {
	case "ModeA":
		return h.handleModeA(body)
	case "ModeB":
		return h.handleModeB(body)
	}
	return nil
}

func (h *Handler) handleModeA(body []byte) []byte { return nil }
func (h *Handler) handleModeB(body []byte) []byte { return nil }
`)

		res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
		require.NoError(t, err)

		assert.Empty(t, res.FilteredOps,
			"ModeA genuinely resolves, so its sibling ModeB must stay a normal unresolved op, not get "+
				"swept away as if the whole switch were fake")
		assert.Contains(t, res.UnresolvedOps, "ModeB")

		var found bool
		for _, or := range res.OpsChecked {
			if or.Op == "ModeA" {
				found = true
			}
		}
		assert.True(t, found, "ModeA must still be checked normally")
	})

	t.Run("a singleton unresolved candidate has no sibling to corroborate it, so it is never filtered",
		func(t *testing.T) {
			t.Parallel()

			sdkDir := t.TempDir()
			writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
			svcDir := t.TempDir()
			writeFile(t, svcDir, "handler.go", `package svc

type cfg struct{ Mode string }

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "GetSchedule":
		return h.handleGetSchedule(body)
	}
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	c := cfg{}
	switch c.Mode {
	case "ModeA":
		return h.handleModeA(body)
	}
	return nil
}

func (h *Handler) handleModeA(body []byte) []byte { return nil }
`)

			res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
			require.NoError(t, err)

			assert.Empty(t, res.FilteredOps,
				"a lone unresolved op is exactly KNOWN BLIND SPOT #7 territory -- it must stay visible, "+
					"never silently reclassified for lack of any sibling to corroborate the guess")
			assert.Contains(t, res.UnresolvedOps, "ModeA")
		})
}

// svcMapsCopyOverrideFixture reproduces cognitoidp's real dispatchTable()
// shape (gopherstack-ck9f refinement #2): two op-family maps (opsA/opsB),
// each returned from its own zero-arg method, merged into one destination
// table via SEQUENTIAL maps.Copy calls. Go's maps.Copy overwrites on
// collision, so opsB's entry (copied last) is the one the real dispatcher
// actually uses -- opsA's is a legacy handler superseded, not a competing
// top-level binding nobody can resolve.
const svcMapsCopyOverrideFixture = `package svc

import "maps"

type opFunc func([]byte) []byte

type Handler struct{}

func (h *Handler) opsA() map[string]opFunc {
	return map[string]opFunc{"GetSchedule": h.handleGetScheduleLegacy}
}

func (h *Handler) opsB() map[string]opFunc {
	return map[string]opFunc{"GetSchedule": h.handleGetScheduleAccurate}
}

func (h *Handler) dispatchTable() map[string]opFunc {
	table := make(map[string]opFunc)
	maps.Copy(table, h.opsA())
	maps.Copy(table, h.opsB())
	return table
}

func (h *Handler) handleGetScheduleLegacy(body []byte) []byte {
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

func (h *Handler) handleGetScheduleAccurate(body []byte) []byte {
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

func TestResolveDeterministicOverrides_MapsCopyOrderWins(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", svcMapsCopyOverrideFixture)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)

	require.NotContains(t, res.AmbiguousOps, "GetSchedule",
		"both family maps are merged into the SAME table by the SAME assembler, in a fixed textual "+
			"order -- that is knowable, not a guess, so it must not be reported ambiguous")
	require.Len(t, res.DeterministicOverrides, 1)
	assert.Contains(t, res.DeterministicOverrides[0], "GetSchedule -> handleGetScheduleAccurate")

	require.Len(t, res.OpsChecked, 1)
	or := res.OpsChecked[0]
	assert.Equal(t, "handleGetScheduleAccurate", or.Handler,
		"opsB is copied LAST in dispatchTable(), so its handler is the one the real dispatcher uses")
	assert.Empty(t, or.NotInTree,
		"the winning (opsB) handler writes the correct casing -- picking the loser would report a "+
			"fabricated mismatch, exactly gopherstack-kiwf's sqs shape")
}

// svcInterfaceBoundaryFixture reproduces cognitoidp's real Lambda-trigger
// shape (gopherstack-ck9f refinement #1): a struct field typed as a
// package-local interface, invoked from a helper that builds its own
// envelope map and hands it across that boundary. The envelope's keys
// (triggerSource/userName) must never be attributed to GetSchedule's own
// response, which is built entirely separately.
const svcInterfaceBoundaryFixture = `package svc

type Invoker interface {
	Invoke(event map[string]any) (map[string]any, error)
}

type Handler struct {
	invoker Invoker
}

func (h *Handler) invokeTrigger(clientID string) (map[string]any, error) {
	event := map[string]any{
		"triggerSource": "X",
		"userName":      clientID,
	}
	result, err := h.invoker.Invoke(event)
	if err != nil {
		return nil, err
	}
	resp, _ := result["response"].(map[string]any)
	return resp, nil
}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "GetSchedule":
		return h.handleGetSchedule(body)
	}
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	_, _ = h.invokeTrigger("someone")

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

func TestBoundaryExclusion_InterfaceCrossingEnvelopeNotAttributed(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", svcInterfaceBoundaryFixture)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)
	require.Len(t, res.OpsChecked, 1)

	or := res.OpsChecked[0]
	assert.NotContains(t, or.Written, "triggerSource",
		"a map that only ever flows into an interface-boundary call must not be attributed to the "+
			"op's own response")
	assert.NotContains(t, or.Written, "userName")
	assert.Empty(t, or.NotInTree)
}

// TestBoundaryExclusion_ValueAlsoReturnedIsNotSuppressed is the "does not
// over-suppress" half of the interface-boundary narrowing: a map that flows
// into the boundary call AND is independently handed back by the SAME
// function must still count -- excluding it just because it ALSO crossed
// the boundary would risk hiding a real dropped key exactly like the ones
// this tool exists to catch.
func TestBoundaryExclusion_ValueAlsoReturnedIsNotSuppressed(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", `package svc

type Invoker interface {
	Invoke(event map[string]any) (map[string]any, error)
}

type Handler struct {
	invoker Invoker
}

func (h *Handler) invokeAndEcho() map[string]any {
	event := map[string]any{"triggerSource": "X"}
	_, _ = h.invoker.Invoke(event)
	return event
}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "GetSchedule":
		return h.handleGetSchedule(body)
	}
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	echoed := h.invokeAndEcho()
	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": echoed,
			},
		},
	}
	_ = resp
	return nil
}
`)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)
	require.Len(t, res.OpsChecked, 1)

	assert.Contains(t, res.OpsChecked[0].Written, "triggerSource",
		"event is ALSO returned directly by invokeAndEcho, so it must still be treated as reachable "+
			"wire-output data, not excluded just because it also crossed the boundary")
}

// svcMapConversionProducerFixture reproduces cognitoidp's real
// attrs["sub"]/sortedAttributeList shape (gopherstack-ck9f refinement #1's
// second, narrower instance): userAttrsBuilder exists solely to build a
// map[string]string that every one of its callers immediately feeds to
// toNameValueList (whose signature -- map param, slice-of-named-type result
// -- marks it a conversion, not a direct marshal). The map's keys become
// list-item field VALUES, never JSON keys, so "sub" must not be attributed
// to GetSchedule's response.
const svcMapConversionProducerFixture = `package svc

type nameValue struct {
	Name  string
	Value string
}

func toNameValueList(m map[string]string) []nameValue {
	out := make([]nameValue, 0, len(m))
	for k, v := range m {
		out = append(out, nameValue{Name: k, Value: v})
	}
	return out
}

func userAttrsBuilder(sub string) map[string]string {
	attrs := map[string]string{}
	attrs["sub"] = sub
	return attrs
}

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "GetSchedule":
		return h.handleGetSchedule(body)
	}
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	_ = toNameValueList(userAttrsBuilder("abc"))

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

func TestBoundaryExclusion_MapConversionProducerNotAttributed(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", svcMapConversionProducerFixture)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)
	require.Len(t, res.OpsChecked, 1)

	or := res.OpsChecked[0]
	assert.NotContains(t, or.Written, "sub",
		"userAttrsBuilder's only caller feeds its result straight into a map->slice conversion func -- "+
			"\"sub\" becomes a Name VALUE, never a JSON key, and must not be attributed to GetSchedule")
	assert.Empty(t, or.NotInTree)
}

// TestBoundaryExclusion_MapConversionDoesNotOverSuppress proves the producer
// narrowing requires EVERY call site to feed the conversion func -- a
// second caller that returns the SAME map as its own real output must keep
// that map's keys visible.
func TestBoundaryExclusion_MapConversionDoesNotOverSuppress(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", `package svc

type nameValue struct {
	Name  string
	Value string
}

func toNameValueList(m map[string]string) []nameValue {
	out := make([]nameValue, 0, len(m))
	for k, v := range m {
		out = append(out, nameValue{Name: k, Value: v})
	}
	return out
}

func userAttrsBuilder(sub string) map[string]string {
	attrs := map[string]string{}
	attrs["sub"] = sub
	return attrs
}

type Handler struct{}

func (h *Handler) Dispatch(op string, body []byte) []byte {
	switch op {
	case "GetSchedule":
		return h.handleGetSchedule(body)
	}
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) []byte {
	_ = toNameValueList(userAttrsBuilder("abc"))
	direct := userAttrsBuilder("direct-use")

	resp := map[string]any{
		"Target": map[string]any{
			"EcsParameters": map[string]any{
				"NetworkConfiguration": direct,
			},
		},
	}
	_ = resp
	return nil
}
`)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)
	require.Len(t, res.OpsChecked, 1)

	assert.Contains(t, res.OpsChecked[0].Written, "sub",
		"userAttrsBuilder has a SECOND call site that uses its result directly as real output, so it "+
			"is not a pure conversion producer and its keys must stay visible")
}
