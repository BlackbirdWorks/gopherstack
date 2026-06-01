package stepfunctions_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// TestRefinement1_ErrNilAppContext verifies the provider nil guard.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &stepfunctions.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrNilAppContext)
}

// TestRefinement1_ProviderInit verifies normal provider init.
func TestRefinement1_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &stepfunctions.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ stepfunctions.StorageBackend = (*stepfunctions.InMemoryBackend)(nil)
}

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations count.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(b)
	assert.Len(t, h.GetSupportedOperations(), h.HandlerOpsLen())
}

// TestRefinement1_HandlerOpsLenHelper verifies the HandlerOpsLen export helper.
func TestRefinement1_HandlerOpsLenHelper(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(b)
	assert.Equal(t, 38, h.HandlerOpsLen())
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestRefinement1_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

// TestRefinement1_AccountID verifies that AccountID returns the configured value.
func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("111222333444", "eu-west-1")
	assert.Equal(t, "111222333444", b.AccountID())
}

// TestRefinement1_Region verifies that Region returns the configured value.
func TestRefinement1_Region(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("111222333444", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

// TestRefinement1_StateMachineCount verifies the StateMachineCount export helper.
func TestRefinement1_StateMachineCount(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	assert.Equal(t, 0, b.StateMachineCount())

	_, err := b.CreateStateMachine("sm1", validPassDef, "arn:role", "STANDARD")
	require.NoError(t, err)
	assert.Equal(t, 1, b.StateMachineCount())
}

// TestRefinement1_ExecutionCount verifies the ExecutionCount export helper.
func TestRefinement1_ExecutionCount(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine("sm1", validPassDef, "arn:role", "STANDARD")
	require.NoError(t, err)

	assert.Equal(t, 0, b.ExecutionCount())

	_, err = b.StartExecution(sm.StateMachineArn, "exec1", `{}`)
	require.NoError(t, err)
	assert.Equal(t, 1, b.ExecutionCount())
}

// TestRefinement1_ActivityCount verifies the ActivityCount export helper.
func TestRefinement1_ActivityCount(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	assert.Equal(t, 0, b.ActivityCount())

	_, err := b.CreateActivity("my-activity")
	require.NoError(t, err)
	assert.Equal(t, 1, b.ActivityCount())
}

// TestRefinement1_ValidateStateMachineDefinition tests the ValidateStateMachineDefinition handler.
func TestRefinement1_ValidateStateMachineDefinition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		def        string
		wantResult string
		wantStatus int
	}{
		{
			name:       "valid_definition",
			def:        validPassDef,
			wantResult: "OK",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_definition",
			def:        `{"StartAt":"Missing"}`,
			wantResult: "FAIL",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newSFNHandler(t)
			body, err := json.Marshal(map[string]string{"definition": tt.def})
			require.NoError(t, err)

			rec := sfnPost(t.Context(), t, h, e, "ValidateStateMachineDefinition", string(body))
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantResult, resp["result"])
		})
	}
}

// TestRefinement1_ListStateMachineVersions tests the ListStateMachineVersions handler.
func TestRefinement1_ListStateMachineVersions(t *testing.T) {
	t.Parallel()

	h, e := newSFNHandler(t)

	// Create a state machine first so ListStateMachineVersions can find it.
	createBody, err := json.Marshal(map[string]string{
		"name":       "ver-sm",
		"definition": validPassDef,
		"roleArn":    "arn:aws:iam::000:role/r",
		"type":       "STANDARD",
	})
	require.NoError(t, err)

	createRec := sfnPost(t.Context(), t, h, e, "CreateStateMachine", string(createBody))
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	smARN := createResp["stateMachineArn"].(string)

	listBody, merr := json.Marshal(map[string]string{"stateMachineArn": smARN})
	require.NoError(t, merr)

	rec := sfnPost(t.Context(), t, h, e, "ListStateMachineVersions", string(listBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions, ok := resp["stateMachineVersions"].([]any)
	require.True(t, ok, "stateMachineVersions should be an array")
	assert.Empty(t, versions)
}

// TestRefinement1_Reset verifies that Reset() clears all backend state.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(b)

	_, err := b.CreateStateMachine("sm1", validPassDef, "arn:role", "STANDARD")
	require.NoError(t, err)
	assert.Equal(t, 1, b.StateMachineCount())

	h.Reset()
	assert.Equal(t, 0, b.StateMachineCount())
}

// TestRefinement1_CreateStateMachineAlreadyExists verifies duplicate state machine creation with
// different definition returns error; same definition is idempotent.
func TestRefinement1_CreateStateMachineAlreadyExists(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.CreateStateMachine("sm1", validPassDef, "arn:role", "STANDARD")
	require.NoError(t, err)

	altDef := `{"StartAt":"T","States":{"T":{"Type":"Succeed"}}}`
	_, err = b.CreateStateMachine("sm1", altDef, "arn:role", "STANDARD")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrStateMachineAlreadyExists)
}

// TestRefinement1_DeleteStateMachineNotFound verifies deleting nonexistent SM returns error.
func TestRefinement1_DeleteStateMachineNotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	err := b.DeleteStateMachine("arn:aws:states:us-east-1:123:stateMachine:nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
}

// TestRefinement1_DescribeStateMachineNotFound verifies describing nonexistent SM returns error.
func TestRefinement1_DescribeStateMachineNotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.DescribeStateMachine("arn:aws:states:us-east-1:123:stateMachine:nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrStateMachineDoesNotExist)
}

// TestRefinement1_UpdateStateMachineInvalidDefinition verifies bad definition is rejected.
func TestRefinement1_UpdateStateMachineInvalidDefinition(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine("sm1", validPassDef, "arn:role", "STANDARD")
	require.NoError(t, err)

	_, err = b.UpdateStateMachine(sm.StateMachineArn, `{"StartAt":"Missing"}`, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrInvalidDefinition)
}

// TestRefinement1_ActivityAlreadyExists verifies creating duplicate activity returns error.
func TestRefinement1_ActivityAlreadyExists(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	_, err := b.CreateActivity("my-act")
	require.NoError(t, err)

	_, err = b.CreateActivity("my-act")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrActivityAlreadyExists)
}

// TestRefinement1_DeleteActivityNotFound verifies deleting nonexistent activity returns error.
func TestRefinement1_DeleteActivityNotFound(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	err := b.DeleteActivity("arn:aws:states:us-east-1:123:activity:nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrActivityDoesNotExist)
}

// TestRefinement1_HandlersErrorResponse verifies that unknown operations return 400.
func TestRefinement1_HandlersErrorResponse(t *testing.T) {
	t.Parallel()

	h, e := newSFNHandler(t)
	rec := sfnPost(t.Context(), t, h, e, "NonExistentOperation", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_CreateStateMachineDefaultType verifies empty type defaults to STANDARD.
func TestRefinement1_CreateStateMachineDefaultType(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	sm, err := b.CreateStateMachine("sm1", validPassDef, "arn:role", "")
	require.NoError(t, err)
	assert.Equal(t, "STANDARD", sm.Type)
}
