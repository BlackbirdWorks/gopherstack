package verifiedpermissions_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVPHandler_CreateOps_ClientTokenIdempotency table-drives the shared
// ClientToken idempotency semantics (see InMemoryBackend.checkClientToken)
// across the three Create* operations besides CreatePolicyStore that also
// accept a ClientToken: CreatePolicy, CreatePolicyTemplate, and
// CreateIdentitySource. For each op: a retry with the same token and the
// same parameters must replay the original resource's ID (no duplicate
// created); a retry with the same token but different parameters must fail
// with ConflictException.
func TestVPHandler_CreateOps_ClientTokenIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       func(storeID, token, variant string) map[string]any
		name       string
		action     string
		idField    string
		listAction string
		listField  string
	}{
		{
			name:   "CreatePolicy",
			action: "CreatePolicy",
			body: func(storeID, token, variant string) map[string]any {
				return map[string]any{
					"policyStoreId": storeID,
					"clientToken":   token,
					"definition": map[string]any{
						"static": map[string]any{
							"statement":   `permit(principal, action, resource);`,
							"description": variant,
						},
					},
				}
			},
			idField:    "policyId",
			listAction: "ListPolicies",
			listField:  "policies",
		},
		{
			name:   "CreatePolicyTemplate",
			action: "CreatePolicyTemplate",
			body: func(storeID, token, variant string) map[string]any {
				return map[string]any{
					"policyStoreId": storeID,
					"clientToken":   token,
					"statement":     `permit(principal == ?principal, action, resource);`,
					"description":   variant,
				}
			},
			idField:    "policyTemplateId",
			listAction: "ListPolicyTemplates",
			listField:  "policyTemplates",
		},
		{
			name:   "CreateIdentitySource",
			action: "CreateIdentitySource",
			body: func(storeID, token, variant string) map[string]any {
				return map[string]any{
					"policyStoreId":       storeID,
					"clientToken":         token,
					"principalEntityType": variant,
					"configuration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_test",
						},
					},
				}
			},
			idField:    "identitySourceId",
			listAction: "ListIdentitySources",
			listField:  "identitySources",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			storeID := createTestPolicyStore(t, h)

			rec1 := doVPRequest(t, h, tt.action, tt.body(storeID, "fixed-token", "v1"))
			require.Equal(t, http.StatusOK, rec1.Code, "body: %s", rec1.Body.String())
			var resp1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

			// Same token, same parameters: replays the original resource.
			rec2 := doVPRequest(t, h, tt.action, tt.body(storeID, "fixed-token", "v1"))
			require.Equal(t, http.StatusOK, rec2.Code, "body: %s", rec2.Body.String())
			var resp2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
			assert.Equal(t, resp1[tt.idField], resp2[tt.idField])

			listRec := doVPRequest(t, h, tt.listAction, map[string]any{"policyStoreId": storeID})
			require.Equal(t, http.StatusOK, listRec.Code)
			var listResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			items, _ := listResp[tt.listField].([]any)
			assert.Len(t, items, 1, "a replayed ClientToken call must not create a duplicate resource")

			// Same token, different parameters: ConflictException.
			rec3 := doVPRequest(t, h, tt.action, tt.body(storeID, "fixed-token", "v2-different"))
			assert.Equal(t, http.StatusBadRequest, rec3.Code)
			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &errResp))
			assert.Equal(t, "ConflictException", errResp["__type"])
		})
	}
}
