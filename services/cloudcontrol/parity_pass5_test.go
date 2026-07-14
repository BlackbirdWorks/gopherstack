package cloudcontrol_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

// TestParity_CancelResourceRequest_OnlyAllowsInProgress verifies that AWS Cloud Control only
// allows cancellation of IN_PROGRESS requests; other statuses (SUCCESS, FAILED,
// CANCEL_COMPLETE, CANCEL_IN_PROGRESS, PENDING) must return HTTP 500
// ConcurrentModificationException, per the CancelResourceRequest API reference's
// Errors section.
func TestParity_CancelResourceRequest_OnlyAllowsInProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   string
		wantHTTP int
	}{
		{
			name:     "in_progress_can_be_cancelled",
			status:   "IN_PROGRESS",
			wantHTTP: http.StatusOK,
		},
		{
			name:     "success_cannot_be_cancelled",
			status:   "SUCCESS",
			wantHTTP: http.StatusInternalServerError,
		},
		{
			name:     "failed_cannot_be_cancelled",
			status:   "FAILED",
			wantHTTP: http.StatusInternalServerError,
		},
		{
			name:     "cancel_complete_cannot_be_cancelled",
			status:   "CANCEL_COMPLETE",
			wantHTTP: http.StatusInternalServerError,
		},
		{
			name:     "cancel_in_progress_cannot_be_cancelled",
			status:   "CANCEL_IN_PROGRESS",
			wantHTTP: http.StatusInternalServerError,
		},
		{
			name:     "pending_cannot_be_cancelled",
			status:   "PENDING",
			wantHTTP: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			token := "test-token-" + tt.name
			h.Backend.AddProgressEvent(&cloudcontrol.ProgressEvent{
				RequestToken:    token,
				TypeName:        "AWS::Logs::LogGroup",
				Identifier:      "test-group",
				Operation:       "CREATE",
				OperationStatus: tt.status,
			})

			rec := doRequest(t, h, "CancelResourceRequest", map[string]any{
				"RequestToken": token,
			})
			assert.Equal(t, tt.wantHTTP, rec.Code, "status %s: unexpected HTTP response", tt.status)

			if tt.wantHTTP == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				pe, ok := out["ProgressEvent"].(map[string]any)
				require.True(t, ok, "ProgressEvent must be present")
				assert.Equal(t, "CANCEL_COMPLETE", pe["OperationStatus"])
			}
		})
	}
}

// TestParity_GetResource_PropertiesIsJSONString verifies Properties is returned as a JSON string
// (not an object), matching the AWS Cloud Control API specification.
func TestParity_GetResource_PropertiesIsJSONString(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	props := `{"BucketName":"my-bucket","VersioningConfiguration":{"Status":"Enabled"}}`

	rec := doRequest(t, h, "CreateResource", map[string]any{
		"TypeName":     "AWS::S3::Bucket",
		"DesiredState": props,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	pe := createOut["ProgressEvent"].(map[string]any)
	identifier := pe["Identifier"].(string)

	rec = doRequest(t, h, "GetResource", map[string]any{
		"TypeName":   "AWS::S3::Bucket",
		"Identifier": identifier,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getOut))
	desc, ok := getOut["ResourceDescription"].(map[string]any)
	require.True(t, ok, "ResourceDescription must be present")

	// Properties MUST be a string (JSON-encoded), not an object.
	propertiesRaw, ok := desc["Properties"]
	require.True(t, ok, "Properties must be present")
	_, isString := propertiesRaw.(string)
	assert.True(t, isString, "Properties must be a JSON string, not %T", propertiesRaw)

	// The string must be valid JSON.
	var parsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(propertiesRaw.(string)), &parsed))
	assert.Equal(t, "my-bucket", parsed["BucketName"])
}

// TestParity_ListResourceRequests_FilterByOperationStatus verifies the OperationStatuses filter
// on ListResourceRequests returns only matching events.
func TestParity_ListResourceRequests_FilterByOperationStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	h.Backend.AddProgressEvent(&cloudcontrol.ProgressEvent{
		RequestToken:    "tok-success",
		TypeName:        "AWS::Logs::LogGroup",
		Identifier:      "grp-success",
		Operation:       "CREATE",
		OperationStatus: "SUCCESS",
	})
	h.Backend.AddProgressEvent(&cloudcontrol.ProgressEvent{
		RequestToken:    "tok-inprogress",
		TypeName:        "AWS::Logs::LogGroup",
		Identifier:      "grp-inprogress",
		Operation:       "CREATE",
		OperationStatus: "IN_PROGRESS",
	})
	h.Backend.AddProgressEvent(&cloudcontrol.ProgressEvent{
		RequestToken:    "tok-failed",
		TypeName:        "AWS::Logs::LogGroup",
		Identifier:      "grp-failed",
		Operation:       "CREATE",
		OperationStatus: "FAILED",
	})

	tests := []struct {
		filter  map[string]any
		name    string
		wantLen int
	}{
		{
			name:    "filter_success_only",
			filter:  map[string]any{"OperationStatuses": []string{"SUCCESS"}},
			wantLen: 1,
		},
		{
			name:    "filter_in_progress_only",
			filter:  map[string]any{"OperationStatuses": []string{"IN_PROGRESS"}},
			wantLen: 1,
		},
		{
			name:    "filter_success_and_failed",
			filter:  map[string]any{"OperationStatuses": []string{"SUCCESS", "FAILED"}},
			wantLen: 2,
		},
		{
			name:    "no_filter_returns_all",
			filter:  map[string]any{},
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			body := map[string]any{}
			if len(tt.filter) > 0 {
				body["ResourceRequestStatusFilter"] = tt.filter
			}

			rec := doRequest(t, h, "ListResourceRequests", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			summaries, _ := out["ResourceRequestStatusSummaries"].([]any)
			assert.Len(t, summaries, tt.wantLen, "filter %v", tt.filter)
		})
	}
}

// TestParity_ProgressEvent_RequiredFields verifies the ProgressEvent shape on create/delete/update.
func TestParity_ProgressEvent_RequiredFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		setup     func() map[string]any
		name      string
		operation string
	}{
		{
			name:      "create_returns_progress_event",
			operation: "CreateResource",
			setup: func() map[string]any {
				return map[string]any{
					"TypeName":     "AWS::Logs::LogGroup",
					"DesiredState": `{"LogGroupName":"parity-grp"}`,
				}
			},
		},
		{
			name:      "delete_returns_progress_event",
			operation: "DeleteResource",
			setup: func() map[string]any {
				createRec := doRequest(t, h, "CreateResource", map[string]any{
					"TypeName":     "AWS::Logs::LogGroup",
					"DesiredState": `{"LogGroupName":"del-grp"}`,
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				var out map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &out))
				pe := out["ProgressEvent"].(map[string]any)

				return map[string]any{
					"TypeName":   "AWS::Logs::LogGroup",
					"Identifier": pe["Identifier"],
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tt.operation, tt.setup())
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			pe, ok := out["ProgressEvent"].(map[string]any)
			require.True(t, ok, "ProgressEvent must be present")

			// All required ProgressEvent fields.
			assert.NotEmpty(t, pe["TypeName"], "TypeName required")
			assert.NotEmpty(t, pe["RequestToken"], "RequestToken required")
			assert.NotEmpty(t, pe["Operation"], "Operation required")
			assert.NotEmpty(t, pe["OperationStatus"], "OperationStatus required")
			assert.NotEmpty(t, pe["EventTime"], "EventTime required")
		})
	}
}

// errType decodes the awsjson1.0 "__type" field from an error response body, so tests can
// assert the exact modeled exception name gopherstack put on the wire.
func errType(t *testing.T, body []byte) string {
	t.Helper()

	var env struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(body, &env))

	return env.Type
}

// TestParity_GetResourceRequestStatus_UnknownToken_IsRequestTokenNotFound verifies that an
// unrecognized RequestToken surfaces as RequestTokenNotFoundException -- the ONLY error
// GetResourceRequestStatus declares -- not ResourceNotFoundException, which describes a
// missing *resource*, not a missing *request token*. See the GetResourceRequestStatus
// API reference's Errors section.
func TestParity_GetResourceRequestStatus_UnknownToken_IsRequestTokenNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetResourceRequestStatus", map[string]any{"RequestToken": "nonexistent"})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "RequestTokenNotFoundException", errType(t, rec.Body.Bytes()))
}

// TestParity_CancelResourceRequest_UnknownToken_IsRequestTokenNotFound mirrors the above for
// CancelResourceRequest, which declares RequestTokenNotFoundException alongside
// ConcurrentModificationException.
func TestParity_CancelResourceRequest_UnknownToken_IsRequestTokenNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CancelResourceRequest", map[string]any{"RequestToken": "nonexistent"})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "RequestTokenNotFoundException", errType(t, rec.Body.Bytes()))
}

// TestParity_CancelResourceRequest_TerminalStatus_IsConcurrentModification verifies the exact
// wire error code (not just HTTP status) for cancelling a non-cancellable request.
func TestParity_CancelResourceRequest_TerminalStatus_IsConcurrentModification(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	event, err := h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"terminal"}`, "")
	require.NoError(t, err)

	rec := doRequest(t, h, "CancelResourceRequest", map[string]any{"RequestToken": event.RequestToken})

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, "ConcurrentModificationException", errType(t, rec.Body.Bytes()))
}

// TestParity_MissingRequiredField_IsInvalidRequestException verifies that CloudControl's
// generic input-validation error is InvalidRequestException, not ValidationException --
// CloudControl's error model has no ValidationException shape at all. See
// https://docs.aws.amazon.com/cloudcontrolapi/latest/APIReference/API_CreateResource.html#API_CreateResource_Errors
func TestParity_MissingRequiredField_IsInvalidRequestException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateResource", map[string]any{"DesiredState": `{}`})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "InvalidRequestException", errType(t, rec.Body.Bytes()))
}

// TestParity_GetResource_NotFound_IsResourceNotFoundException verifies both the wire error
// code and the HTTP 400 status (not 404 -- real CloudControl returns 400 for every client
// fault, including ResourceNotFoundException).
func TestParity_GetResource_NotFound_IsResourceNotFoundException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetResource", map[string]any{
		"TypeName":   "AWS::Logs::LogGroup",
		"Identifier": "nonexistent",
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "ResourceNotFoundException", errType(t, rec.Body.Bytes()))
}

// TestParity_CreateResource_Duplicate_IsAlreadyExistsException verifies both the wire error
// code and the HTTP 400 status (not 409).
func TestParity_CreateResource_Duplicate_IsAlreadyExistsException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{
		"TypeName":     "AWS::Logs::LogGroup",
		"DesiredState": `{"LogGroupName":"dup"}`,
	}
	require.Equal(t, http.StatusOK, doRequest(t, h, "CreateResource", body).Code)

	rec := doRequest(t, h, "CreateResource", body)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "AlreadyExistsException", errType(t, rec.Body.Bytes()))
}

// TestParity_ProgressEvent_ResourceModel verifies that CreateResource and UpdateResource
// populate ProgressEvent.ResourceModel with the resource's current properties (a JSON
// string), matching the real ProgressEvent shape -- so callers can read the resource
// straight off the ProgressEvent without a follow-up GetResource call.
func TestParity_ProgressEvent_ResourceModel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateResource", map[string]any{
		"TypeName":     "AWS::Logs::LogGroup",
		"DesiredState": `{"LogGroupName":"model-test","RetentionInDays":7}`,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	createPE := createOut["ProgressEvent"].(map[string]any)

	createModel, ok := createPE["ResourceModel"].(string)
	require.True(t, ok, "ResourceModel must be present as a string on CreateResource")

	var createParsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(createModel), &createParsed))
	assert.Equal(t, "model-test", createParsed["LogGroupName"])

	updateRec := doRequest(t, h, "UpdateResource", map[string]any{
		"TypeName":      "AWS::Logs::LogGroup",
		"Identifier":    "model-test",
		"PatchDocument": `[{"op":"replace","path":"/RetentionInDays","value":30}]`,
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateOut map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateOut))
	updatePE := updateOut["ProgressEvent"].(map[string]any)

	updateModel, ok := updatePE["ResourceModel"].(string)
	require.True(t, ok, "ResourceModel must be present as a string on UpdateResource")

	var updateParsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(updateModel), &updateParsed))
	assert.InEpsilon(t, float64(30), updateParsed["RetentionInDays"], 0, "ResourceModel must reflect the patched value")
}
