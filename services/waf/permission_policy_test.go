package waf_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPermissionPolicyLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"permission policy CRUD"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)

			rgID := wafCreateRuleGroup(t, h, "SharedGroup")
			rgARN := "arn:aws:waf::123456789012:rulegroup/" + rgID
			policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":"*"},"Action":"waf:GetRuleGroup"}]}`

			// Put
			rec := wafDo(t, h, "PutPermissionPolicy", map[string]any{
				"ResourceArn": rgARN,
				"Policy":      policy,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Get
			rec = wafDo(t, h, "GetPermissionPolicy", map[string]any{"ResourceArn": rgARN})
			require.Equal(t, http.StatusOK, rec.Code)
			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, policy, getResp["Policy"])

			// Delete
			rec = wafDo(t, h, "DeletePermissionPolicy", map[string]any{"ResourceArn": rgARN})
			require.Equal(t, http.StatusOK, rec.Code)

			// Get after delete → not found
			rec = wafDo(t, h, "GetPermissionPolicy", map[string]any{"ResourceArn": rgARN})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestPermissionPolicyNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "GetPermissionPolicy not-found",
			action: "GetPermissionPolicy",
			body:   map[string]any{"ResourceArn": "arn:aws:waf::123:rulegroup/no-such"},
		},
		{
			name:   "DeletePermissionPolicy not-found",
			action: "DeletePermissionPolicy",
			body:   map[string]any{"ResourceArn": "arn:aws:waf::123:rulegroup/no-such"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)
			rec := wafDo(t, h, tc.action, tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}
