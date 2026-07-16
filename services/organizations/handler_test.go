package organizations_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/organizations"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// newOrgBackend creates a backend with a pre-created organization for convenience.
func newOrgBackend(t *testing.T) (*organizations.InMemoryBackend, string) {
	t.Helper()

	b := newTestBackend()

	_, root, err := b.CreateOrganization("ALL")
	require.NoError(t, err)

	return b, root.ID
}

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

func createOrgOn(t *testing.T, b *organizations.InMemoryBackend) {
	t.Helper()

	_, _, err := b.CreateOrganization("ALL")
	require.NoError(t, err)
}

// TestHandler_Reset verifies Handler.Reset() clears backend state.
func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupOrg   bool
		wantAfter  int
		wantBefore int
	}{
		{
			name:       "clears_org_and_accounts",
			setupOrg:   true,
			wantBefore: 1, // management account
			wantAfter:  0,
		},
		{
			name:      "reset_on_empty_backend_is_safe",
			setupOrg:  false,
			wantAfter: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			h := organizations.NewHandler(b)

			if tt.setupOrg {
				createOrgOn(t, b)
				require.Equal(t, tt.wantBefore, organizations.AccountCount(b))
			}

			h.Reset()

			assert.Equal(t, tt.wantAfter, organizations.AccountCount(b))
		})
	}
}

// TestHandlerOpsLen verifies GetSupportedOperations has 50 entries.
func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantLen int
	}{
		{name: "fifty_ops", wantLen: 63},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			assert.Equal(t, tt.wantLen, organizations.HandlerOpsLen(h))
		})
	}
}
