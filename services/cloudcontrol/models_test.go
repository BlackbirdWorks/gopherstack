package cloudcontrol_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

// TestProgressEvent_OptionalFieldsOmittedWhenTerminalSuccess verifies that the
// ProgressEvent fields modeled purely for wire-shape parity with the real SDK's
// types.ProgressEvent -- ErrorCode, HooksRequestToken, RetryAfter -- are absent
// from the JSON payload (via `omitempty`) when a request completes synchronously
// to a terminal SUCCESS, matching real AWS: ErrorCode is only ever populated on
// FAILED, RetryAfter only on a non-terminal PENDING/IN_PROGRESS status, and this
// backend has no Hooks concept at all so HooksRequestToken is always empty.
func TestProgressEvent_OptionalFieldsOmittedWhenTerminalSuccess(t *testing.T) {
	t.Parallel()

	h := cloudcontrol.NewHandler(cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1"))

	rec := doRequest(t, h, "CreateResource", map[string]any{
		"TypeName":     "AWS::Logs::LogGroup",
		"DesiredState": `{"LogGroupName":"models-parity-grp"}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	pe, ok := out["ProgressEvent"].(map[string]any)
	require.True(t, ok, "ProgressEvent must be present")

	assert.NotContains(t, pe, "ErrorCode", "ErrorCode must be omitted on a SUCCESS event")
	assert.NotContains(t, pe, "HooksRequestToken", "HooksRequestToken must be omitted (no Hooks concept)")
	assert.NotContains(t, pe, "RetryAfter", "RetryAfter must be omitted on a terminal SUCCESS event")
}

// TestGetResourceRequestStatus_HooksProgressEventOmittedWhenEmpty verifies that
// GetResourceRequestStatusOutput.HooksProgressEvent -- a real field on the SDK's
// GetResourceRequestStatusOutput -- is modeled but always omitted from the
// response, since this backend has no Hooks concept and therefore never has any
// Hook invocations to report.
func TestGetResourceRequestStatus_HooksProgressEventOmittedWhenEmpty(t *testing.T) {
	t.Parallel()

	h := cloudcontrol.NewHandler(cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1"))

	createRec := doRequest(t, h, "CreateResource", map[string]any{
		"TypeName":     "AWS::Logs::LogGroup",
		"DesiredState": `{"LogGroupName":"models-hooks-grp"}`,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	pe, ok := createOut["ProgressEvent"].(map[string]any)
	require.True(t, ok)

	token, tokenOK := pe["RequestToken"].(string)
	require.True(t, tokenOK)
	require.NotEmpty(t, token)

	statusRec := doRequest(t, h, "GetResourceRequestStatus", map[string]any{
		"RequestToken": token,
	})
	require.Equal(t, http.StatusOK, statusRec.Code)

	var statusOut map[string]any
	require.NoError(t, json.Unmarshal(statusRec.Body.Bytes(), &statusOut))

	assert.NotContains(t, statusOut, "HooksProgressEvent",
		"HooksProgressEvent must be omitted since this backend has no Hooks concept")
}
