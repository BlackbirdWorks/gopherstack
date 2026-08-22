package main

import (
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

func TestRunCheck_NoMapAnyLiterals(t *testing.T) {
	t.Parallel()

	sdkDir := t.TempDir()
	writeFile(t, sdkDir, "deserializers.go", sdkGoodFixture)
	svcDir := t.TempDir()
	writeFile(t, svcDir, "handler.go", `package svc

type Handler struct{}

type getScheduleOutput struct {
	Target string
}

func (h *Handler) Dispatch(op string, body []byte) any {
	switch op {
	case "GetSchedule":
		return h.handleGetSchedule(body)
	}
	return nil
}

func (h *Handler) handleGetSchedule(body []byte) any {
	return getScheduleOutput{Target: "x"}
}
`)

	res, err := runCheck(filepath.Join(sdkDir, "deserializers.go"), "awsRestjson1_", svcDir, "")
	require.NoError(t, err)
	assert.True(t, res.NoMapAnyLiterals, "a struct-literal-only service should report N/A, not a false clean")
	assert.Zero(t, res.TotalWritten)
	assert.Empty(t, res.UnresolvedOps)
	assert.Equal(t, exitClean, report(res, svcDir, "awsRestjson1_"))
}
