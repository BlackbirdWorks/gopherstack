package wafv2_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

func TestHandler_DeletePermissionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		body       func(arnStr string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *wafv2.Handler) string {
				rg, _ := h.Backend.CreateRuleGroup(context.Background(), "my-rg", "REGIONAL", "", "", 10, nil, nil)
				arnStr := h.Backend.RuleGroupARN(rg.Name, rg.ID, rg.Scope)
				require.NoError(t, h.Backend.PutPermissionPolicy(
					context.Background(), arnStr, `{"Version":"2012-10-17"}`,
				))

				return arnStr
			},
			body: func(arnStr string) map[string]any {
				return map[string]any{"ResourceArn": arnStr}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			setup: func(_ *wafv2.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "arn:aws:wafv2:us-east-1:000000000000:regional/rulegroup/nonexistent/badid"
			},
			body: func(arnStr string) map[string]any {
				return map[string]any{"ResourceArn": arnStr}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arnStr := tt.setup(h)
			rec := doWafv2Request(t, h, "DeletePermissionPolicy", tt.body(arnStr))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestPermissionPolicy_PutGetDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, rgARN := createRuleGroupHelper(t, h, "policy-rg")

	policy := `{"Version":"2012-10-17","Statement":[{` +
		`"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::111122223333:root"},` +
		`"Action":"wafv2:CreateWebACL","Resource":"` + rgARN + `"}]}`

	// Put permission policy.
	putRec := doWafv2Request(t, h, "PutPermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
		"Policy":      policy,
	})
	require.Equal(t, http.StatusOK, putRec.Code, "PutPermissionPolicy: %s", putRec.Body.String())

	// Get permission policy.
	getRec := doWafv2Request(t, h, "GetPermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code, "GetPermissionPolicy: %s", getRec.Body.String())

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	storedPolicy, ok := getResp["Policy"].(string)
	require.True(t, ok, "Policy field should be present")
	assert.Equal(t, policy, storedPolicy)

	// Delete permission policy.
	delRec := doWafv2Request(t, h, "DeletePermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Subsequent Get should return not-found.
	getRec2 := doWafv2Request(t, h, "GetPermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
	})
	assert.Equal(t, http.StatusBadRequest, getRec2.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestPermissionPolicy_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "DeletePermissionPolicy", map[string]any{
		"ResourceArn": "arn:aws:wafv2:us-east-1:000000000000:regional/rulegroup/no-such/xyz",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestPermissionPolicy_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetPermissionPolicy", map[string]any{
		"ResourceArn": "arn:aws:wafv2:us-east-1:000000000000:regional/rulegroup/no-such/xyz",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPermissionPolicy_UpdateReplace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, rgARN := createRuleGroupHelper(t, h, "policy-replace-rg")

	policy1 := `{"Version":"2012-10-17","Statement":[]}`
	policy2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"wafv2:CreateWebACL","Resource":"*"}]}`

	// Put first policy.
	rec := doWafv2Request(t, h, "PutPermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
		"Policy":      policy1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Put second policy — should replace.
	rec = doWafv2Request(t, h, "PutPermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
		"Policy":      policy2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	getRec := doWafv2Request(t, h, "GetPermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	assert.Equal(t, policy2, resp["Policy"])
}

// ---- Rate-based rules with scope-down ----------------------------------------

func TestHandler_PutAndGetPermissionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		putResourceArn string
		putPolicy      string
		getResourceArn string
		wantPutStatus  int
		wantGetStatus  int
	}{
		{
			name:           "put_and_get_success",
			putResourceArn: "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/test/abc",
			putPolicy:      `{"Version":"2012-10-17"}`,
			getResourceArn: "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/test/abc",
			wantPutStatus:  http.StatusOK,
			wantGetStatus:  http.StatusOK,
		},
		{
			name:          "put_missing_resource_arn",
			wantPutStatus: http.StatusBadRequest,
		},
		{
			name:           "get_missing_resource_arn",
			getResourceArn: "",
			wantGetStatus:  http.StatusBadRequest,
		},
		{
			name:           "get_not_found",
			getResourceArn: "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/test/notexist",
			wantGetStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.putResourceArn != "" || tt.wantPutStatus == http.StatusBadRequest {
				var putBody any
				if tt.putResourceArn != "" {
					putBody = map[string]any{
						"ResourceArn": tt.putResourceArn,
						"Policy":      tt.putPolicy,
					}
				} else {
					putBody = map[string]any{}
				}

				rec := doWafv2Request(t, h, "PutPermissionPolicy", putBody)
				assert.Equal(t, tt.wantPutStatus, rec.Code)
			}

			if tt.getResourceArn != "" || tt.wantGetStatus == http.StatusBadRequest {
				var getBody any
				if tt.getResourceArn != "" {
					getBody = map[string]any{"ResourceArn": tt.getResourceArn}
				} else {
					getBody = map[string]any{}
				}

				rec := doWafv2Request(t, h, "GetPermissionPolicy", getBody)
				assert.Equal(t, tt.wantGetStatus, rec.Code)

				if tt.wantGetStatus == http.StatusOK {
					var resp map[string]any
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
					assert.Contains(t, resp, "Policy")
				}
			}
		})
	}
}
