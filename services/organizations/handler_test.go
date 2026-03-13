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

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"
)

func newTestBackend() *organizations.InMemoryBackend {
	return organizations.NewInMemoryBackend(testAccountID, testRegion)
}

func newTestHandler(t *testing.T) *organizations.Handler {
	t.Helper()

	b := newTestBackend()

	return organizations.NewHandler(b)
}

func doRequest(t *testing.T, h *organizations.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSOrganizationsV20161128."+op)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestBackend_OrgLifecycle tests organization creation, describe, and deletion.
func TestBackend_OrgLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		featureSet   string
		wantErr      bool
		doubleCreate bool
	}{
		{
			name:       "create_ALL",
			featureSet: "ALL",
		},
		{
			name:       "create_CONSOLIDATED_BILLING",
			featureSet: "CONSOLIDATED_BILLING",
		},
		{
			name:         "duplicate_create_fails",
			featureSet:   "ALL",
			doubleCreate: true,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			org, root, err := b.CreateOrganization(tt.featureSet)

			if tt.wantErr && !tt.doubleCreate {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, org)
			require.NotNil(t, root)
			assert.NotEmpty(t, org.ID)
			assert.NotEmpty(t, org.ARN)
			assert.Equal(t, tt.featureSet, org.FeatureSet)
			assert.NotEmpty(t, root.ID)

			if tt.doubleCreate {
				_, _, err2 := b.CreateOrganization(tt.featureSet)
				require.Error(t, err2)

				return
			}

			// DescribeOrganization.
			descOrg, err := b.DescribeOrganization()
			require.NoError(t, err)
			assert.Equal(t, org.ID, descOrg.ID)

			// DeleteOrganization.
			err = b.DeleteOrganization()
			require.NoError(t, err)

			// After deletion, describe should fail.
			_, err = b.DescribeOrganization()
			require.Error(t, err)
		})
	}
}

// TestBackend_AccountLifecycle tests account creation and listing.
func TestBackend_AccountLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		accountName string
		email       string
		wantErr     bool
		noOrg       bool
	}{
		{
			name:        "create_and_list",
			accountName: "dev-account",
			email:       "dev@example.com",
		},
		{
			name:        "no_org_returns_error",
			accountName: "dev-account",
			email:       "dev@example.com",
			noOrg:       true,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if !tt.noOrg {
				_, _, err := b.CreateOrganization("ALL")
				require.NoError(t, err)
			}

			status, err := b.CreateAccount(tt.accountName, tt.email, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, status)
			assert.NotEmpty(t, status.AccountID)
			assert.Equal(t, "SUCCEEDED", status.State)

			// DescribeAccount.
			acct, err := b.DescribeAccount(status.AccountID)
			require.NoError(t, err)
			assert.Equal(t, tt.accountName, acct.Name)
			assert.Equal(t, tt.email, acct.Email)

			// ListAccounts.
			accts, err := b.ListAccounts()
			require.NoError(t, err)
			assert.NotEmpty(t, accts)

			found := false

			for _, a := range accts {
				if a.ID == status.AccountID {
					found = true

					break
				}
			}

			assert.True(t, found, "created account should appear in ListAccounts")
		})
	}
}

// TestBackend_OULifecycle tests OU create, describe, and delete.
func TestBackend_OULifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ouName  string
		wantErr bool
	}{
		{
			name:   "create_describe_delete",
			ouName: "dev-ou",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			_, _, err := b.CreateOrganization("ALL")
			require.NoError(t, err)

			roots, err := b.ListRoots()
			require.NoError(t, err)
			require.NotEmpty(t, roots)

			rootID := roots[0].ID

			// CreateOrganizationalUnit.
			ou, err := b.CreateOrganizationalUnit(rootID, tt.ouName, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, ou.ID)
			assert.Equal(t, tt.ouName, ou.Name)

			// DescribeOrganizationalUnit.
			desc, err := b.DescribeOrganizationalUnit(ou.ID)
			require.NoError(t, err)
			assert.Equal(t, ou.ID, desc.ID)
			assert.Equal(t, tt.ouName, desc.Name)

			// ListOrganizationalUnitsForParent.
			ous, err := b.ListOrganizationalUnitsForParent(rootID)
			require.NoError(t, err)

			found := false

			for _, o := range ous {
				if o.ID == ou.ID {
					found = true

					break
				}
			}

			assert.True(t, found, "OU should appear in list")

			// UpdateOrganizationalUnit.
			updated, err := b.UpdateOrganizationalUnit(ou.ID, "renamed-ou")
			require.NoError(t, err)
			assert.Equal(t, "renamed-ou", updated.Name)

			// DeleteOrganizationalUnit.
			err = b.DeleteOrganizationalUnit(ou.ID)
			require.NoError(t, err)

			// After deletion, describe should fail.
			_, err = b.DescribeOrganizationalUnit(ou.ID)
			require.Error(t, err)
		})
	}
}

// TestBackend_PolicyLifecycle tests policy CRUD and attachment.
func TestBackend_PolicyLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policyName string
		policyType string
		content    string
	}{
		{
			name:       "scp_lifecycle",
			policyName: "deny-all",
			policyType: "SERVICE_CONTROL_POLICY",
			content:    `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			_, _, err := b.CreateOrganization("ALL")
			require.NoError(t, err)

			// CreatePolicy.
			policy, err := b.CreatePolicy(tt.policyName, "test policy", tt.content, tt.policyType, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, policy.PolicySummary.ID)
			assert.Equal(t, tt.policyName, policy.PolicySummary.Name)

			// DescribePolicy.
			desc, err := b.DescribePolicy(policy.PolicySummary.ID)
			require.NoError(t, err)
			assert.Equal(t, policy.PolicySummary.ID, desc.PolicySummary.ID)

			// ListPolicies.
			policies, err := b.ListPolicies(tt.policyType)
			require.NoError(t, err)
			assert.NotEmpty(t, policies)

			// DeletePolicy.
			err = b.DeletePolicy(policy.PolicySummary.ID)
			require.NoError(t, err)

			// After deletion, describe should fail.
			_, err = b.DescribePolicy(policy.PolicySummary.ID)
			require.Error(t, err)
		})
	}
}

// TestBackend_TagOperations tests tagging resources.
func TestBackend_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name: "tag_and_untag",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			org, _, err := b.CreateOrganization("ALL")
			require.NoError(t, err)

			// TagResource.
			tags := []organizations.Tag{{Key: "env", Value: "test"}}
			err = b.TagResource(org.ID, tags)
			require.NoError(t, err)

			// ListTagsForResource.
			listed, err := b.ListTagsForResource(org.ID)
			require.NoError(t, err)
			assert.Len(t, listed, 1)
			assert.Equal(t, "env", listed[0].Key)
			assert.Equal(t, "test", listed[0].Value)

			// UntagResource.
			err = b.UntagResource(org.ID, []string{"env"})
			require.NoError(t, err)

			// After untag, tags should be empty.
			listed, err = b.ListTagsForResource(org.ID)
			require.NoError(t, err)
			assert.Empty(t, listed)
		})
	}
}

// TestHandler_CreateOrganization tests the HTTP handler for CreateOrganization.
func TestHandler_CreateOrganization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantOrgID  bool
	}{
		{
			name:       "creates_org_ALL",
			body:       map[string]any{"FeatureSet": "ALL"},
			wantStatus: http.StatusOK,
			wantOrgID:  true,
		},
		{
			name:       "creates_org_no_feature_set",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantOrgID:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateOrganization", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantOrgID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				org, ok := resp["Organization"].(map[string]any)
				require.True(t, ok, "response should contain Organization")
				assert.NotEmpty(t, org["Id"])
			}
		})
	}
}

// TestHandler_DescribeOrganization tests the HTTP handler for DescribeOrganization.
func TestHandler_DescribeOrganization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantStatus  int
		createFirst bool
	}{
		{
			name:        "no_org_returns_error",
			wantStatus:  http.StatusBadRequest,
			createFirst: false,
		},
		{
			name:        "describes_existing_org",
			wantStatus:  http.StatusOK,
			createFirst: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.createFirst {
				rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DescribeOrganization", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ListRoots tests the HTTP handler for ListRoots.
func TestHandler_ListRoots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantRoots  bool
	}{
		{
			name:       "lists_roots_after_org_creation",
			wantStatus: http.StatusOK,
			wantRoots:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			rec := doRequest(t, h, "ListRoots", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantRoots {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				roots, ok := resp["Roots"].([]any)
				require.True(t, ok, "response should contain Roots")
				assert.NotEmpty(t, roots)
			}
		})
	}
}

// TestHandler_CreateAccount tests the HTTP handler for CreateAccount.
func TestHandler_CreateAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "creates_account",
			body: map[string]any{
				"AccountName": "test-account",
				"Email":       "test@example.com",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_name_fails",
			body:       map[string]any{"Email": "test@example.com"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_email_fails",
			body:       map[string]any{"AccountName": "test-account"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			rec := doRequest(t, h, "CreateAccount", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_CreateOrganizationalUnit tests the HTTP handler for CreateOrganizationalUnit.
func TestHandler_CreateOrganizationalUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantOU     bool
	}{
		{
			name:       "creates_ou",
			wantStatus: http.StatusOK,
			wantOU:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			rootsRec := doRequest(t, h, "ListRoots", map[string]any{})
			require.Equal(t, http.StatusOK, rootsRec.Code)

			var rootsResp map[string]any
			require.NoError(t, json.NewDecoder(rootsRec.Body).Decode(&rootsResp))
			roots := rootsResp["Roots"].([]any)
			rootID := roots[0].(map[string]any)["Id"].(string)

			rec := doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
				"ParentId": rootID,
				"Name":     "dev-ou",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantOU {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				ou, ok := resp["OrganizationalUnit"].(map[string]any)
				require.True(t, ok, "response should contain OrganizationalUnit")
				assert.NotEmpty(t, ou["Id"])
				assert.Equal(t, "dev-ou", ou["Name"])
			}
		})
	}
}

// TestHandler_EnablePolicyType tests the HTTP handler for EnablePolicyType.
func TestHandler_EnablePolicyType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "enables_policy_type",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			rootsRec := doRequest(t, h, "ListRoots", map[string]any{})
			var rootsResp map[string]any
			require.NoError(t, json.NewDecoder(rootsRec.Body).Decode(&rootsResp))
			roots := rootsResp["Roots"].([]any)
			rootID := roots[0].(map[string]any)["Id"].(string)

			rec := doRequest(t, h, "EnablePolicyType", map[string]any{
				"RootId":     rootID,
				"PolicyType": "SERVICE_CONTROL_POLICY",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ListParents tests the HTTP handler for ListParents.
func TestHandler_ListParents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantStatus  int
		wantParents bool
	}{
		{
			name:        "lists_parents_of_account",
			wantStatus:  http.StatusOK,
			wantParents: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			acctRec := doRequest(t, h, "CreateAccount", map[string]any{
				"AccountName": "test-account",
				"Email":       "test@example.com",
			})
			require.Equal(t, http.StatusOK, acctRec.Code)

			var acctResp map[string]any
			require.NoError(t, json.NewDecoder(acctRec.Body).Decode(&acctResp))
			status := acctResp["CreateAccountStatus"].(map[string]any)
			accountID := status["AccountId"].(string)

			rec := doRequest(t, h, "ListParents", map[string]any{"ChildId": accountID})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantParents {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				parents, ok := resp["Parents"].([]any)
				require.True(t, ok, "response should contain Parents")
				assert.NotEmpty(t, parents)
			}
		})
	}
}
