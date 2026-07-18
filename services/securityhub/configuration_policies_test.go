package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_GetConfigurationPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		useByARN   bool
		useByName  bool
		preCreate  bool
	}{
		{
			name:      "get by ID",
			preCreate: true,
		},
		{
			name:      "get by ARN",
			preCreate: true,
			useByARN:  true,
		},
		{
			name:      "get by name",
			preCreate: true,
			useByName: true,
		},
		{
			name:       "not found",
			preCreate:  false,
			wantErrMsg: "not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			var identifier string
			if tc.preCreate {
				cp, err := b.CreateConfigurationPolicy("TestPolicy", "desc", map[string]any{}, nil)
				require.NoError(t, err)
				if tc.useByARN { //nolint:gocritic // existing issue.
					identifier = cp.Arn
				} else if tc.useByName {
					identifier = cp.Name
				} else {
					identifier = cp.Id
				}
			} else {
				identifier = "nonexistent-id"
			}

			result, err := b.GetConfigurationPolicy(identifier)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, "TestPolicy", result.Name)
			}
		})
	}
}

func TestBackend_UpdateConfigurationPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		updateField string
		wantErrMsg  string
	}{
		{name: "update name", updateField: "name"},
		{name: "update description", updateField: "desc"},
		{name: "update policy", updateField: "policy"},
		{name: "not found", wantErrMsg: "not found"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			var identifier string
			if tc.wantErrMsg == "" {
				cp, err := b.CreateConfigurationPolicy(
					"Original",
					"orig desc",
					map[string]any{},
					nil,
				)
				require.NoError(t, err)
				identifier = cp.Id
			} else {
				identifier = "nonexistent"
			}

			var newName, newDesc string
			var newPolicy map[string]any

			switch tc.updateField {
			case "name":
				newName = "UpdatedName"
			case "desc":
				newDesc = "Updated desc"
			case "policy":
				newPolicy = map[string]any{"SecurityHub": map[string]any{"ServiceEnabled": false}}
			}

			result, err := b.UpdateConfigurationPolicy(identifier, newName, newDesc, newPolicy)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}

func TestBackend_DeleteConfigurationPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		useByARN   bool
		preCreate  bool
	}{
		{name: "delete by ID", preCreate: true},
		{name: "delete by ARN", preCreate: true, useByARN: true},
		{name: "not found", preCreate: false, wantErrMsg: "not found"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			var identifier string
			if tc.preCreate {
				cp, err := b.CreateConfigurationPolicy("ToDelete", "desc", map[string]any{}, nil)
				require.NoError(t, err)
				if tc.useByARN {
					identifier = cp.Arn
				} else {
					identifier = cp.Id
				}
			} else {
				identifier = "nonexistent"
			}

			err := b.DeleteConfigurationPolicy(identifier)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfigurationPolicy(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, resp map[string]any) string
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Create Get List Update Delete ConfigurationPolicy",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/configurationPolicy/create",
					body: map[string]any{
						"Name":        "TestPolicy",
						"Description": "A test policy",
						"ConfigurationPolicy": map[string]any{
							"SecurityHub": map[string]any{"ServiceEnabled": true},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						id, _ := resp["Id"].(string)
						assert.NotEmpty(t, id)
						assert.Equal(t, "TestPolicy", resp["Name"])

						return id
					},
				},
				{
					name:   "list",
					method: http.MethodGet,
					path:   "/configurationPolicy/list",
					body:   nil,
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						policies, _ := resp["ConfigurationPolicySummaryList"].([]any)
						assert.Len(t, policies, 1)

						return ""
					},
				},
			},
		},
		{
			name: "ConfigurationPolicy association lifecycle",
			steps: []step{
				{
					name:   "create policy",
					method: http.MethodPost,
					path:   "/configurationPolicy/create",
					body: map[string]any{
						"Name":        "AssocPolicy",
						"Description": "for assoc test",
						"ConfigurationPolicy": map[string]any{
							"SecurityHub": map[string]any{"ServiceEnabled": true},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)

						return resp["Id"].(string)
					},
				},
				{
					name:   "associate",
					method: http.MethodPost,
					path:   "/configurationPolicyAssociation/associate",
					body: map[string]any{
						"ConfigurationPolicyIdentifier": "policy-1",
						"Target": map[string]any{
							"AccountId": "123456789012",
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) string { //nolint:revive // existing issue.
						t.Helper()
						assert.Equal(t, http.StatusOK, code)

						return ""
					},
				},
				{
					name:   "get association",
					method: http.MethodPost,
					path:   "/configurationPolicyAssociation/get",
					body: map[string]any{
						"Target": map[string]any{
							"AccountId": "123456789012",
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.Equal(t, "123456789012", resp["TargetId"])
						assert.Equal(t, "SUCCESS", resp["AssociationStatus"])

						return ""
					},
				},
				{
					name:   "list associations",
					method: http.MethodPost,
					path:   "/configurationPolicyAssociation/list",
					body:   map[string]any{},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assocs, _ := resp["ConfigurationPolicyAssociationSummaryList"].([]any)
						assert.Len(t, assocs, 1)

						return ""
					},
				},
				{
					name:   "batch get",
					method: http.MethodPost,
					path:   "/configurationPolicyAssociation/batchget",
					body: map[string]any{
						"ConfigurationPolicyAssociationIdentifiers": []any{
							map[string]any{"TargetId": "123456789012"},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						found, _ := resp["ConfigurationPolicyAssociations"].([]any)
						assert.Len(t, found, 1)

						return ""
					},
				},
				{
					name:   "disassociate",
					method: http.MethodPost,
					path:   "/configurationPolicyAssociation/disassociate",
					body: map[string]any{
						"ConfigurationPolicyIdentifier": "policy-1",
						"Target": map[string]any{
							"AccountId": "123456789012",
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)

						return ""
					},
				},
				{
					name:   "list after disassociate is empty",
					method: http.MethodPost,
					path:   "/configurationPolicyAssociation/list",
					body:   map[string]any{},
					check: func(t *testing.T, code int, resp map[string]any) string {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assocs, _ := resp["ConfigurationPolicyAssociationSummaryList"].([]any)
						assert.Empty(t, assocs)

						return ""
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

// TestParity_ConfigurationPolicyAssociation_TargetTypeDerived verifies that
// TargetType in the association response is derived from which key is
// present in the request's Target union (AccountId/OrganizationalUnitId/
// RootId), matching aws-sdk-go-v2's types.Target wire shape. The real
// request never sends a "TargetType" field alongside Target -- reading one
// left TargetType permanently empty in the response.
func TestConfigurationPolicyAssociation_TargetTypeDerived(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target         map[string]any
		name           string
		wantTargetType string
	}{
		{name: "account", target: map[string]any{"AccountId": "111111111111"}, wantTargetType: "ACCOUNT"},
		{
			name:           "organizational_unit",
			target:         map[string]any{"OrganizationalUnitId": "ou-abcd-12345678"},
			wantTargetType: "ORGANIZATIONAL_UNIT",
		},
		{name: "root", target: map[string]any{"RootId": "r-abcd"}, wantTargetType: "ROOT"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, http.MethodPost, "/configurationPolicy/create", map[string]any{
				"Name":                "test-policy",
				"Description":         "test",
				"ConfigurationPolicy": map[string]any{},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			policyID, _ := createResp["Id"].(string)
			require.NotEmpty(t, policyID)

			assocRec := doRequest(t, h, http.MethodPost, "/configurationPolicyAssociation/associate", map[string]any{
				"ConfigurationPolicyIdentifier": policyID,
				"Target":                        tc.target,
			})
			require.Equal(t, http.StatusOK, assocRec.Code)

			var assocResp map[string]any
			require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
			assert.Equal(t, tc.wantTargetType, assocResp["TargetType"],
				"TargetType must be derived from the Target union key, not read from a nonexistent request field")

			// GetConfigurationPolicyAssociation must derive the same TargetType
			// when looking the association back up.
			getRec := doRequest(t, h, http.MethodPost, "/configurationPolicyAssociation/get", map[string]any{
				"Target": tc.target,
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			assert.Equal(t, tc.wantTargetType, getResp["TargetType"])
		})
	}
}
