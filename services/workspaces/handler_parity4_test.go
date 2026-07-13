package workspaces_test

// Parity-4 tests covering bugs fixed in this pass:
//
//  1. awserr.Newf call sites in backend.go/handler.go had msg/sentinel/args in the
//     wrong order for the shared pkgs/awserr.Newf(msg, sentinel, args...) signature,
//     producing garbled ErrorMessage text like "InvalidParameterValuesException%!(EXTRA
//     ...)" instead of the intended descriptive message.
//  2. CreateWorkspaces aborted the entire batch with a top-level error on the first
//     per-item runtime failure (e.g. an unregistered DirectoryId), instead of AWS's
//     partial-failure semantics: FailedRequests for the bad item, PendingRequests for
//     the rest.
//  3. RestoreWorkspace was a true no-op that never checked whether the WorkspaceId
//     existed, silently succeeding for unknown IDs instead of ResourceNotFoundException.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// 1. awserr.Newf message formatting
// ---------------------------------------------------------------------------

func TestParity4_ErrorMessages_AreNotGarbledByExtraFormatArgs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name    string
		target  string
		body    map[string]any
		wantErr string
	}{
		{
			name:   "CreateWorkspaces empty list",
			target: "CreateWorkspaces",
			body:   map[string]any{"Workspaces": []map[string]any{}},
		},
		{
			name:   "CreateWorkspaces missing UserName",
			target: "CreateWorkspaces",
			body: map[string]any{
				"Workspaces": []map[string]any{
					{"DirectoryId": "d-1234567890", "BundleId": "wsb-bh8rsxt14"},
				},
			},
		},
		{
			name:   "CreateTags empty ResourceId",
			target: "CreateTags",
			body:   map[string]any{"ResourceId": "", "Tags": []map[string]any{}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doTargetRequest(t, h, tc.target, tc.body)
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var resp map[string]string

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotContains(t, resp["message"], "%!(EXTRA",
				"error message must not contain a garbled fmt.Sprintf EXTRA marker")
			assert.NotEmpty(t, resp["message"])
		})
	}
}

func TestParity4_ModifyWorkspaceProperties_InvalidComputeType_MessageIsDescriptive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	rec := doTargetRequest(t, h, "ModifyWorkspaceProperties", map[string]any{
		"WorkspaceId": wsID,
		"WorkspaceProperties": map[string]any{
			"ComputeTypeName": "NOT_A_REAL_TYPE",
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["message"], "NOT_A_REAL_TYPE",
		"message should describe the actual invalid value, not just the error code")
	assert.NotContains(t, resp["message"], "%!(EXTRA")
}

// ---------------------------------------------------------------------------
// 2. CreateWorkspaces partial failure
// ---------------------------------------------------------------------------

func TestParity4_CreateWorkspaces_PartialFailure_DoesNotAbortWholeBatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{
		"DirectoryId": "d-registered000",
	})

	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{
				"UserName":    "gooduser",
				"DirectoryId": "d-registered000",
				"BundleId":    "wsb-bh8rsxt14",
			},
			{
				"UserName":    "baduser",
				"DirectoryId": "d-not-registered",
				"BundleId":    "wsb-bh8rsxt14",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code,
		"a per-item runtime failure must not fail the whole CreateWorkspaces call")

	var resp struct {
		FailedRequests []struct {
			WorkspaceRequest struct {
				DirectoryID string `json:"DirectoryId"`
				UserName    string `json:"UserName"`
			} `json:"WorkspaceRequest"`
			ErrorCode    string `json:"ErrorCode"`
			ErrorMessage string `json:"ErrorMessage"`
		} `json:"FailedRequests"`
		PendingRequests []struct {
			WorkspaceID string `json:"WorkspaceId"`
			UserName    string `json:"UserName"`
		} `json:"PendingRequests"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	require.Len(t, resp.PendingRequests, 1, "the valid spec must succeed")
	assert.Equal(t, "gooduser", resp.PendingRequests[0].UserName)
	assert.NotEmpty(t, resp.PendingRequests[0].WorkspaceID)

	require.Len(t, resp.FailedRequests, 1, "the invalid spec must be reported as a failure")
	assert.Equal(t, "baduser", resp.FailedRequests[0].WorkspaceRequest.UserName)
	assert.Equal(t, "d-not-registered", resp.FailedRequests[0].WorkspaceRequest.DirectoryID)
	assert.Equal(t, "InvalidParameterValuesException", resp.FailedRequests[0].ErrorCode)
	assert.Contains(t, resp.FailedRequests[0].ErrorMessage, "d-not-registered")
}

// ---------------------------------------------------------------------------
// 3. RestoreWorkspace existence validation
// ---------------------------------------------------------------------------

func TestParity4_RestoreWorkspace_UnknownID_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doTargetRequest(t, h, "RestoreWorkspace", map[string]any{
		"WorkspaceId": "ws-doesnotexist",
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestParity4_RestoreWorkspace_KnownID_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	rec := doTargetRequest(t, h, "RestoreWorkspace", map[string]any{
		"WorkspaceId": wsID,
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}
