package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Batch-1 accuracy gap: CreateActionTarget is POST /actionTargets (not POST /action-targets).
func TestCreateActionTargetPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	rec := doRequest(t, h, http.MethodPost, "/actionTargets", map[string]any{
		"Name":        "Send to Slack",
		"Description": "Sends finding to Slack channel",
		"Id":          "send-to-slack",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arn, _ := resp["ActionTargetArn"].(string)
	assert.NotEmpty(t, arn)
	assert.Contains(t, arn, "arn:aws:securityhub:")
	assert.Contains(t, arn, ":action/custom/")
}

// Batch-1 accuracy gap: DescribeActionTargets is POST /actionTargets/get.
func TestDescribeActionTargetsIsPOSTActionTargetsGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)
	doRequest(t, h, http.MethodPost, "/actionTargets", map[string]any{
		"Name":        "My Action",
		"Description": "desc",
		"Id":          "my-action",
	})

	rec := doRequest(t, h, http.MethodPost, "/actionTargets/get", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	targets, _ := resp["ActionTargets"].([]any)
	assert.Len(t, targets, 1)

	t0 := targets[0].(map[string]any)
	assert.Equal(t, "My Action", t0["Name"])
}

// Batch-1 accuracy gap: DeleteActionTarget is DELETE /actionTargets/{ActionTargetArn}.
func TestDeleteActionTargetIsDELETEActionTargetsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	createRec := doRequest(t, h, http.MethodPost, "/actionTargets", map[string]any{
		"Name":        "To Delete",
		"Description": "desc",
		"Id":          "to-delete",
	})

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	arn, _ := createResp["ActionTargetArn"].(string)

	rec := doRequest(t, h, http.MethodDelete, "/actionTargets/"+arn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	deletedArn, _ := resp["ActionTargetArn"].(string)
	assert.Equal(t, arn, deletedArn, "DeleteActionTarget must return the deleted ActionTargetArn")
}

// TestParity_CreateActionTarget_RequiredFields verifies that CreateActionTarget
// rejects requests with missing Name, Description, or Id.
func TestCreateActionTarget_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "all_fields_present",
			body:     map[string]any{"Name": "MyTarget", "Description": "Test action target", "Id": "MyTargetId"},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing_name",
			body:     map[string]any{"Description": "desc", "Id": "id1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_description",
			body:     map[string]any{"Name": "name1", "Id": "id1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_id",
			body:     map[string]any{"Name": "name1", "Description": "desc"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_name",
			body:     map[string]any{"Name": "", "Description": "desc", "Id": "id1"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			enableHub(t, h)
			rec := doRequest(t, h, http.MethodPost, "/actionTargets", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBackend_UpdateActionTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		updateName string
		updateDesc string
		wantErrMsg string
	}{
		{
			name:       "update name only",
			updateName: "NewName",
			updateDesc: "",
		},
		{
			name:       "update description only",
			updateName: "",
			updateDesc: "New desc",
		},
		{
			name:       "not found",
			updateName: "x",
			updateDesc: "",
			wantErrMsg: "not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.EnableHub(false, nil))

			var targetArn string
			if tc.wantErrMsg == "" {
				var err error
				targetArn, err = b.CreateActionTarget("Original", "Orig desc", "my-action")
				require.NoError(t, err)
			} else {
				targetArn = "arn:aws:securityhub:us-east-1:000000000000:action/custom/nonexistent"
			}

			err := b.UpdateActionTarget(targetArn, tc.updateName, tc.updateDesc)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHandler_UpdateActionTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "update existing action target", wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			doRequest(
				t,
				h,
				http.MethodPost,
				"/accounts",
				map[string]any{"EnableDefaultStandards": false},
			)
			doRequest(t, h, http.MethodPost, "/actionTargets", map[string]any{
				"Name":        "MyTarget",
				"Description": "desc",
				"Id":          "my-target",
			})

			rec := doRequest(
				t,
				h,
				http.MethodPatch,
				"/actionTargets/arn:aws:securityhub:us-east-1:000000000000:action/custom/my-target",
				map[string]any{
					"Name":        "Updated",
					"Description": "New desc",
				},
			)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}
