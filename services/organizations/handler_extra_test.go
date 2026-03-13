package organizations_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// newHandlerWithOrg creates a handler that already has an organization.
func newHandlerWithOrg(t *testing.T) (*organizations.Handler, string) {
	t.Helper()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListRoots", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var rootsResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&rootsResp))
	roots := rootsResp["Roots"].([]any)
	rootID := roots[0].(map[string]any)["Id"].(string)

	return h, rootID
}

// createAccountViaHandler creates an account and returns its ID.
func createAccountViaHandler(t *testing.T, h *organizations.Handler, name, email string) string {
	t.Helper()

	rec := doRequest(t, h, "CreateAccount", map[string]any{
		"AccountName": name,
		"Email":       email,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	status := resp["CreateAccountStatus"].(map[string]any)

	return status["AccountId"].(string)
}

// createPolicyViaHandler creates a policy and returns its ID.
func createPolicyViaHandler(t *testing.T, h *organizations.Handler, name string) string {
	t.Helper()

	rec := doRequest(t, h, "CreatePolicy", map[string]any{
		"Name":        name,
		"Description": "test policy",
		"Content":     `{"Version":"2012-10-17","Statement":[]}`,
		"Type":        "SERVICE_CONTROL_POLICY",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	policy := resp["Policy"].(map[string]any)
	summary := policy["PolicySummary"].(map[string]any)

	return summary["Id"].(string)
}

// TestHandler_DeleteOrganization tests the HTTP handler for DeleteOrganization.
func TestHandler_DeleteOrganization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "deletes_org",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			rec := doRequest(t, h, "DeleteOrganization", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_EnableAllFeatures tests the HTTP handler for EnableAllFeatures.
func TestHandler_EnableAllFeatures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "enables_all_features",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			rec := doRequest(t, h, "EnableAllFeatures", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ListAccounts tests the HTTP handler for ListAccounts.
func TestHandler_ListAccounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "lists_accounts",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			createAccountViaHandler(t, h, "test-account", "test@example.com")

			rec := doRequest(t, h, "ListAccounts", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			accounts, ok := resp["Accounts"].([]any)
			require.True(t, ok)
			assert.NotEmpty(t, accounts)
		})
	}
}

// TestHandler_DescribeAccount tests the HTTP handler for DescribeAccount.
func TestHandler_DescribeAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		notFound   bool
	}{
		{
			name:       "describes_account",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			wantStatus: http.StatusBadRequest,
			notFound:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			accountID := "000000000099"
			if !tt.notFound {
				accountID = createAccountViaHandler(t, h, "test-account", "test@example.com")
			}

			rec := doRequest(t, h, "DescribeAccount", map[string]any{"AccountId": accountID})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeCreateAccountStatus tests the HTTP handler for DescribeCreateAccountStatus.
func TestHandler_DescribeCreateAccountStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "describes_status",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			// Create account and capture status.
			rec := doRequest(t, h, "CreateAccount", map[string]any{
				"AccountName": "test-account",
				"Email":       "test@example.com",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
			status := createResp["CreateAccountStatus"].(map[string]any)
			statusID := status["Id"].(string)

			rec = doRequest(t, h, "DescribeCreateAccountStatus", map[string]any{
				"CreateAccountRequestId": statusID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_RemoveAccountFromOrganization tests the HTTP handler.
func TestHandler_RemoveAccountFromOrganization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "removes_account",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)
			accountID := createAccountViaHandler(t, h, "test-account", "test@example.com")

			rec := doRequest(t, h, "RemoveAccountFromOrganization", map[string]any{"AccountId": accountID})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_MoveAccount tests the HTTP handler for MoveAccount.
func TestHandler_MoveAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "moves_account",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)
			accountID := createAccountViaHandler(t, h, "test-account", "test@example.com")

			// Create target OU.
			ouRec := doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
				"ParentId": rootID,
				"Name":     "target-ou",
			})
			require.Equal(t, http.StatusOK, ouRec.Code)

			var ouResp map[string]any
			require.NoError(t, json.NewDecoder(ouRec.Body).Decode(&ouResp))
			ou := ouResp["OrganizationalUnit"].(map[string]any)
			ouID := ou["Id"].(string)

			rec := doRequest(t, h, "MoveAccount", map[string]any{
				"AccountId":           accountID,
				"SourceParentId":      rootID,
				"DestinationParentId": ouID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeOrganizationalUnit tests the HTTP handler.
func TestHandler_DescribeOrganizationalUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "describes_ou",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)

			ouRec := doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
				"ParentId": rootID,
				"Name":     "test-ou",
			})
			require.Equal(t, http.StatusOK, ouRec.Code)

			var ouResp map[string]any
			require.NoError(t, json.NewDecoder(ouRec.Body).Decode(&ouResp))
			ou := ouResp["OrganizationalUnit"].(map[string]any)
			ouID := ou["Id"].(string)

			rec := doRequest(t, h, "DescribeOrganizationalUnit", map[string]any{"OrganizationalUnitId": ouID})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DeleteOrganizationalUnit tests the HTTP handler.
func TestHandler_DeleteOrganizationalUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "deletes_ou",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)

			ouRec := doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
				"ParentId": rootID,
				"Name":     "test-ou",
			})
			require.Equal(t, http.StatusOK, ouRec.Code)

			var ouResp map[string]any
			require.NoError(t, json.NewDecoder(ouRec.Body).Decode(&ouResp))
			ou := ouResp["OrganizationalUnit"].(map[string]any)
			ouID := ou["Id"].(string)

			rec := doRequest(t, h, "DeleteOrganizationalUnit", map[string]any{"OrganizationalUnitId": ouID})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_UpdateOrganizationalUnit tests the HTTP handler.
func TestHandler_UpdateOrganizationalUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "updates_ou",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)

			ouRec := doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
				"ParentId": rootID,
				"Name":     "test-ou",
			})
			require.Equal(t, http.StatusOK, ouRec.Code)

			var ouResp map[string]any
			require.NoError(t, json.NewDecoder(ouRec.Body).Decode(&ouResp))
			ou := ouResp["OrganizationalUnit"].(map[string]any)
			ouID := ou["Id"].(string)

			rec := doRequest(t, h, "UpdateOrganizationalUnit", map[string]any{
				"OrganizationalUnitId": ouID,
				"Name":                 "renamed-ou",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ListOrganizationalUnitsForParent tests the HTTP handler.
func TestHandler_ListOrganizationalUnitsForParent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "lists_ous",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)

			doRequest(t, h, "CreateOrganizationalUnit", map[string]any{"ParentId": rootID, "Name": "test-ou"})

			rec := doRequest(t, h, "ListOrganizationalUnitsForParent", map[string]any{"ParentId": rootID})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ListAccountsForParent tests the HTTP handler.
func TestHandler_ListAccountsForParent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "lists_accounts",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)

			createAccountViaHandler(t, h, "test-account", "test@example.com")

			rec := doRequest(t, h, "ListAccountsForParent", map[string]any{"ParentId": rootID})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ListChildren tests the HTTP handler.
func TestHandler_ListChildren(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		childType  string
		wantStatus int
	}{
		{
			name:       "lists_account_children",
			childType:  "ACCOUNT",
			wantStatus: http.StatusOK,
		},
		{
			name:       "lists_ou_children",
			childType:  "ORGANIZATIONAL_UNIT",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)

			rec := doRequest(t, h, "ListChildren", map[string]any{
				"ParentId":  rootID,
				"ChildType": tt.childType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_PolicyOperations tests create, describe, update, delete, list, attach, detach.
func TestHandler_PolicyOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "full_policy_lifecycle",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)
			policyID := createPolicyViaHandler(t, h, "test-policy")

			// DescribePolicy.
			rec := doRequest(t, h, "DescribePolicy", map[string]any{"PolicyId": policyID})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// UpdatePolicy.
			rec = doRequest(t, h, "UpdatePolicy", map[string]any{
				"PolicyId":    policyID,
				"Name":        "updated-policy",
				"Description": "updated",
				"Content":     `{"Version":"2012-10-17"}`,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// ListPolicies.
			rec = doRequest(t, h, "ListPolicies", map[string]any{"Filter": "SERVICE_CONTROL_POLICY"})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// AttachPolicy.
			rec = doRequest(t, h, "AttachPolicy", map[string]any{
				"PolicyId": policyID,
				"TargetId": rootID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// ListPoliciesForTarget.
			rec = doRequest(t, h, "ListPoliciesForTarget", map[string]any{
				"TargetId": rootID,
				"Filter":   "SERVICE_CONTROL_POLICY",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// ListTargetsForPolicy.
			rec = doRequest(t, h, "ListTargetsForPolicy", map[string]any{"PolicyId": policyID})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// DetachPolicy.
			rec = doRequest(t, h, "DetachPolicy", map[string]any{
				"PolicyId": policyID,
				"TargetId": rootID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// DeletePolicy.
			rec = doRequest(t, h, "DeletePolicy", map[string]any{"PolicyId": policyID})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DisablePolicyType tests the HTTP handler.
func TestHandler_DisablePolicyType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "disables_policy_type",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)

			// Enable first.
			doRequest(t, h, "EnablePolicyType", map[string]any{
				"RootId":     rootID,
				"PolicyType": "SERVICE_CONTROL_POLICY",
			})

			rec := doRequest(t, h, "DisablePolicyType", map[string]any{
				"RootId":     rootID,
				"PolicyType": "SERVICE_CONTROL_POLICY",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_TagOperations tests tag CRUD operations via handler.
func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "tag_untag_list",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			// Get org ID via DescribeOrganization.
			rec := doRequest(t, h, "DescribeOrganization", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&descResp))
			org := descResp["Organization"].(map[string]any)
			orgID := org["Id"].(string)

			// TagResource.
			rec = doRequest(t, h, "TagResource", map[string]any{
				"ResourceId": orgID,
				"Tags":       []map[string]string{{"Key": "env", "Value": "test"}},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// ListTagsForResource.
			rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": orgID})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&tagsResp))
			tags, ok := tagsResp["Tags"].([]any)
			require.True(t, ok)
			assert.NotEmpty(t, tags)

			// UntagResource.
			rec = doRequest(t, h, "UntagResource", map[string]any{
				"ResourceId": orgID,
				"TagKeys":    []string{"env"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ServiceAccess tests service access operations via handler.
func TestHandler_ServiceAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "enable_disable_list",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			// EnableAWSServiceAccess.
			rec := doRequest(t, h, "EnableAWSServiceAccess", map[string]any{
				"ServicePrincipal": "ssm.amazonaws.com",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// ListAWSServiceAccessForOrganization.
			rec = doRequest(t, h, "ListAWSServiceAccessForOrganization", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// DisableAWSServiceAccess.
			rec = doRequest(t, h, "DisableAWSServiceAccess", map[string]any{
				"ServicePrincipal": "ssm.amazonaws.com",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DelegatedAdminOperations tests delegated admin via handler.
func TestHandler_DelegatedAdminOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "register_list_deregister",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)
			accountID := createAccountViaHandler(t, h, "delegate-account", "delegate@example.com")

			// RegisterDelegatedAdministrator.
			rec := doRequest(t, h, "RegisterDelegatedAdministrator", map[string]any{
				"AccountId":        accountID,
				"ServicePrincipal": "ssm.amazonaws.com",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// ListDelegatedAdministrators.
			rec = doRequest(t, h, "ListDelegatedAdministrators", map[string]any{
				"ServicePrincipal": "ssm.amazonaws.com",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// DeregisterDelegatedAdministrator.
			rec = doRequest(t, h, "DeregisterDelegatedAdministrator", map[string]any{
				"AccountId":        accountID,
				"ServicePrincipal": "ssm.amazonaws.com",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ServiceMetadata tests the service metadata methods.
func TestHandler_ServiceMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "metadata"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			assert.Equal(t, "Organizations", h.Name())
			assert.NotEmpty(t, h.GetSupportedOperations())
			assert.Equal(t, "organizations", h.ChaosServiceName())
			assert.NotNil(t, h.ChaosOperations())
			assert.NotNil(t, h.ChaosRegions())
			assert.NotNil(t, h.RouteMatcher())
			assert.Positive(t, h.MatchPriority())
		})
	}
}

// TestBackend_ListParents_OU tests ListParents for an OU.
func TestBackend_ListParents_OU(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "ou_parent_is_root"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			ou, err := b.CreateOrganizationalUnit(rootID, "test-ou", nil)
			require.NoError(t, err)

			parents, err := b.ListParents(ou.ID)
			require.NoError(t, err)
			require.Len(t, parents, 1)
			assert.Equal(t, rootID, parents[0].ID)
		})
	}
}

// TestBackend_ListParents_Error tests ListParents with invalid child.
func TestBackend_ListParents_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "child_not_found",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			_, err := b.ListParents("ou-doesnotexist")

			if tt.wantErr {
				require.Error(t, err)
			}
		})
	}
}

// TestHandler_ErrorPaths tests handler error paths.
func TestHandler_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "list_roots_no_org",
			op:         "ListRoots",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "list_accounts_no_org",
			op:         "ListAccounts",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_org_no_org",
			op:         "DeleteOrganization",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_BadBodyReturns400 tests that all handler operations return 400 for bad JSON.
func TestHandler_BadBodyReturns400(t *testing.T) {
	t.Parallel()

	ops := []string{
		"CreateOrganization", "DescribeOrganization", "DeleteOrganization", "EnableAllFeatures",
		"ListAccounts", "CreateAccount", "DescribeCreateAccountStatus", "DescribeAccount",
		"RemoveAccountFromOrganization", "MoveAccount",
		"ListRoots", "CreateOrganizationalUnit", "DescribeOrganizationalUnit",
		"DeleteOrganizationalUnit", "UpdateOrganizationalUnit", "ListOrganizationalUnitsForParent",
		"ListAccountsForParent", "ListParents", "ListChildren",
		"CreatePolicy", "DescribePolicy", "UpdatePolicy", "DeletePolicy", "ListPolicies",
		"AttachPolicy", "DetachPolicy", "ListPoliciesForTarget", "ListTargetsForPolicy",
		"EnablePolicyType", "DisablePolicyType",
		"TagResource", "UntagResource", "ListTagsForResource",
		"EnableAWSServiceAccess", "DisableAWSServiceAccess",
		"ListAWSServiceAccessForOrganization",
		"RegisterDelegatedAdministrator", "DeregisterDelegatedAdministrator", "ListDelegatedAdministrators",
	}

	for _, op := range ops {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("not-json{")))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "AWSOrganizationsV20161128."+op)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			_ = h.Handler()(c)
			// Bad JSON should result in 400.
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestHandler_UnknownOperation tests routing with an unknown operation.
func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "unknown_op", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "NonExistentOperation", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
