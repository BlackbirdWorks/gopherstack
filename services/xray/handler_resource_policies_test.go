package xray_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

func TestResourcePolicy_MaxFiveCap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 5 policies — should all succeed.
	for i := 1; i <= 5; i++ {
		rec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
			"PolicyName":     fmt.Sprintf("policy-%d", i),
			"PolicyDocument": `{"Version":"2012-10-17"}`,
		})
		require.Equal(t, http.StatusOK, rec.Code, "policy %d should succeed", i)
	}

	// 6th should fail.
	rec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
		"PolicyName":     "policy-6",
		"PolicyDocument": `{"Version":"2012-10-17"}`,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "6th policy should be rejected")
}

func TestResourcePolicy_RevisionIDMismatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a policy.
	createRec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
		"PolicyName":     "rev-policy",
		"PolicyDocument": `{"Version":"2012-10-17"}`,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Update with wrong revision ID — should fail.
	updateRec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
		"PolicyName":       "rev-policy",
		"PolicyDocument":   `{"Version":"2012-10-17","Statement":[]}`,
		"PolicyRevisionId": "wrong-revision-id",
	})
	assert.Equal(t, http.StatusBadRequest, updateRec.Code)
}

// TestPutAndListResourcePolicies verifies the new put/list operations.
func TestPutAndListResourcePolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Initially empty.
	rec := doXrayRequest(t, h, "/ListResourcePolicies", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	policies, ok := resp["ResourcePolicies"].([]any)
	require.True(t, ok)
	assert.Empty(t, policies)

	// Put a policy.
	putBody := map[string]any{
		"PolicyName":     "my-policy",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
	}
	putRec := doXrayRequest(t, h, "/PutResourcePolicy", putBody)
	require.Equal(t, http.StatusOK, putRec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))

	policyObj, ok := putResp["ResourcePolicy"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-policy", policyObj["PolicyName"])
	assert.NotEmpty(t, policyObj["PolicyRevisionId"])

	// List now returns the policy.
	listRec := doXrayRequest(t, h, "/ListResourcePolicies", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	policies, ok = listResp["ResourcePolicies"].([]any)
	require.True(t, ok)
	assert.Len(t, policies, 1)
}

// TestPutResourcePolicyValidation verifies field validation.
func TestPutResourcePolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing PolicyName",
			body:       map[string]any{"PolicyDocument": `{}`},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing PolicyDocument",
			body:       map[string]any{"PolicyName": "pol"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "valid",
			body:       map[string]any{"PolicyName": "pol", "PolicyDocument": `{}`},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/PutResourcePolicy", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestListResourcePolicies_NextTokenInResponse verifies NextToken field is present.
func TestListResourcePolicies_NextTokenInResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/ListResourcePolicies", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, ok := resp["NextToken"]
	assert.True(t, ok, "NextToken must be present in ListResourcePolicies response")
}

func TestHandler_DeleteResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.InMemoryBackend)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing policy",
			setup: func(b *xray.InMemoryBackend) {
				b.AddResourcePolicyInternal(xray.ResourcePolicy{
					PolicyName:       "my-policy",
					PolicyDocument:   `{"Version":"2012-10-17"}`,
					PolicyRevisionID: "rev-1",
				})
			},
			body:       map[string]any{"PolicyName": "my-policy"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing PolicyName returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found returns 400",
			body:       map[string]any{"PolicyName": "no-such-policy"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doXrayRequest(t, h, "/DeleteResourcePolicy", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestResourcePolicy_FivePolicyCap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := 1; i <= 5; i++ {
		rec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
			"PolicyName":     fmt.Sprintf("policy-%d", i),
			"PolicyDocument": `{"Version":"2012-10-17"}`,
		})
		require.Equal(t, http.StatusOK, rec.Code, "policy %d must succeed", i)
	}

	rec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
		"PolicyName":     "policy-6",
		"PolicyDocument": `{"Version":"2012-10-17"}`,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "6th policy must be rejected")
}

func TestResourcePolicy_RevisionIDConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		revisionID string
		wantStatus int
	}{
		{
			name:       "no revision ID always succeeds",
			revisionID: "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "wrong revision ID rejected",
			revisionID: "wrong-revision-id",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
				"PolicyName":     "rev-test-policy",
				"PolicyDocument": `{"Version":"2012-10-17"}`,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			body := map[string]any{
				"PolicyName":     "rev-test-policy",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
			}
			if tt.revisionID != "" {
				body["PolicyRevisionId"] = tt.revisionID
			}

			rec := doXrayRequest(t, h, "/PutResourcePolicy", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestResourcePolicy_JSONValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		policyDocument string
		wantStatus     int
	}{
		{name: "valid JSON object accepted", policyDocument: `{"Version":"2012-10-17"}`, wantStatus: http.StatusOK},
		{name: "valid JSON array accepted", policyDocument: `[]`, wantStatus: http.StatusOK},
		{name: "malformed JSON rejected", policyDocument: `{not valid json`, wantStatus: http.StatusBadRequest},
		{name: "empty string rejected", policyDocument: ``, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
				"PolicyName":     "json-test-policy",
				"PolicyDocument": tt.policyDocument,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestResourcePolicy_ListAfterPut(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := 1; i <= 3; i++ {
		rec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
			"PolicyName":     fmt.Sprintf("list-policy-%d", i),
			"PolicyDocument": `{"Version":"2012-10-17"}`,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doXrayRequest(t, h, "/ListResourcePolicies", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	policies, ok := resp["ResourcePolicies"].([]any)
	require.True(t, ok)
	assert.Len(t, policies, 3)
}

func TestResourcePolicy_DeleteExistingPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
		"PolicyName":     "del-policy",
		"PolicyDocument": `{"Version":"2012-10-17"}`,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	delRec := doXrayRequest(t, h, "/DeleteResourcePolicy", map[string]any{"PolicyName": "del-policy"})
	require.Equal(t, http.StatusOK, delRec.Code)

	listRec := doXrayRequest(t, h, "/ListResourcePolicies", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	policies, _ := resp["ResourcePolicies"].([]any)
	assert.Empty(t, policies, "policy must be removed after delete")
}
