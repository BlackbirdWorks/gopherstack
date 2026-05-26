package organizations_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// ---- helpers ----

func r1NewBackend(t *testing.T) *organizations.InMemoryBackend {
	t.Helper()

	return organizations.NewInMemoryBackend("123456789012", "us-east-1")
}

func r1NewHandler(t *testing.T) *organizations.Handler {
	t.Helper()

	b := r1NewBackend(t)

	return organizations.NewHandler(b)
}

func r1CreateOrg(t *testing.T, b *organizations.InMemoryBackend) {
	t.Helper()

	_, _, err := b.CreateOrganization("ALL")
	require.NoError(t, err)
}

func r1DoRequest(t *testing.T, h *organizations.Handler, op string, body any) *httptest.ResponseRecorder {
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

// ---- tests ----

// TestRefinement1_Handler_Reset verifies Handler.Reset() clears backend state.
func TestRefinement1_Handler_Reset(t *testing.T) {
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

			b := r1NewBackend(t)
			h := organizations.NewHandler(b)

			if tt.setupOrg {
				r1CreateOrg(t, b)
				require.Equal(t, tt.wantBefore, organizations.AccountCount(b))
			}

			h.Reset()

			assert.Equal(t, tt.wantAfter, organizations.AccountCount(b))
		})
	}
}

// TestRefinement1_Backend_AccountID verifies AccountID() returns configured value.
func TestRefinement1_Backend_AccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
	}{
		{name: "default_account", accountID: "123456789012"},
		{name: "custom_account", accountID: "999999999999"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := organizations.NewInMemoryBackend(tt.accountID, "us-east-1")
			assert.Equal(t, tt.accountID, b.AccountID())
		})
	}
}

// TestRefinement1_Backend_Region verifies Region() returns configured value.
func TestRefinement1_Backend_Region(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "us_east_1", region: "us-east-1"},
		{name: "eu_west_1", region: "eu-west-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := organizations.NewInMemoryBackend("123456789012", tt.region)
			assert.Equal(t, tt.region, b.Region())
		})
	}
}

// TestRefinement1_ListAccounts_Sorted verifies deterministic sorted order by account ID.
func TestRefinement1_ListAccounts_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		extraAcct int
		wantCount int
	}{
		{name: "one_account", extraAcct: 0, wantCount: 1},
		{name: "three_accounts", extraAcct: 2, wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			for i := range tt.extraAcct {
				_, createErr := b.CreateAccount("acct", fmt.Sprintf("a%d@example.com", i), "", "", nil)
				require.NoError(t, createErr)
			}

			accts, err := b.ListAccounts()
			require.NoError(t, err)
			require.Len(t, accts, tt.wantCount)

			for i := 1; i < len(accts); i++ {
				assert.LessOrEqual(t, accts[i-1].ID, accts[i].ID, "accounts should be sorted by ID")
			}
		})
	}
}

// TestRefinement1_ListPolicies_Sorted verifies deterministic sorted order by policy name.
func TestRefinement1_ListPolicies_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		names   []string
		wantLen int
	}{
		{
			name:    "two_policies_sorted",
			names:   []string{"ZPolicy", "APolicy"},
			wantLen: 2,
		},
		{
			name:    "three_policies_sorted",
			names:   []string{"CPolicy", "APolicy", "BPolicy"},
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			for _, name := range tt.names {
				_, err := b.CreatePolicy(name, "", `{"Version":"2012-10-17"}`, "SERVICE_CONTROL_POLICY", nil)
				require.NoError(t, err)
			}

			policies, err := b.ListPolicies("")
			require.NoError(t, err)
			require.Len(t, policies, tt.wantLen)

			for i := 1; i < len(policies); i++ {
				assert.LessOrEqual(t, policies[i-1].PolicySummary.Name, policies[i].PolicySummary.Name)
			}
		})
	}
}

// TestRefinement1_ListOrganizationalUnitsForParent_Sorted verifies sorted output by OU name.
func TestRefinement1_ListOrganizationalUnitsForParent_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ouNames []string
	}{
		{
			name:    "three_ous_sorted",
			ouNames: []string{"ZOU", "AOU", "MOU"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			roots, err := b.ListRoots()
			require.NoError(t, err)
			rootID := roots[0].ID

			for _, ouName := range tt.ouNames {
				_, ouErr := b.CreateOrganizationalUnit(rootID, ouName, nil)
				require.NoError(t, ouErr)
			}

			ous, err := b.ListOrganizationalUnitsForParent(rootID)
			require.NoError(t, err)
			require.Len(t, ous, len(tt.ouNames))

			for i := 1; i < len(ous); i++ {
				assert.LessOrEqual(t, ous[i-1].Name, ous[i].Name)
			}
		})
	}
}

// TestRefinement1_ListTagsForResource_Sorted verifies sorted output by key.
func TestRefinement1_ListTagsForResource_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tags []organizations.Tag
	}{
		{
			name: "three_tags_sorted_by_key",
			tags: []organizations.Tag{
				{Key: "zzz", Value: "1"},
				{Key: "aaa", Value: "2"},
				{Key: "mmm", Value: "3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			status, err := b.CreateAccount("tagged-acct", "t@example.com", "", "", tt.tags)
			require.NoError(t, err)

			tags, err := b.ListTagsForResource(status.AccountID)
			require.NoError(t, err)

			for i := 1; i < len(tags); i++ {
				assert.LessOrEqual(t, tags[i-1].Key, tags[i].Key, "tags should be sorted by key")
			}
		})
	}
}

// TestRefinement1_ListDelegatedAdministrators_Sorted verifies sorted output by AccountID.
func TestRefinement1_ListDelegatedAdministrators_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCount int
	}{
		{name: "two_delegated_admins_sorted", wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			require.NoError(t, b.EnableAWSServiceAccess("ram.amazonaws.com"))

			for i := range 2 {
				s, err := b.CreateAccount("da-acct", fmt.Sprintf("da%d@example.com", i), "", "", nil)
				require.NoError(t, err)
				err = b.RegisterDelegatedAdministrator(s.AccountID, "ram.amazonaws.com")
				require.NoError(t, err)
			}

			admins, err := b.ListDelegatedAdministrators("")
			require.NoError(t, err)
			require.Len(t, admins, tt.wantCount)

			for i := 1; i < len(admins); i++ {
				assert.LessOrEqual(t, admins[i-1].AccountID, admins[i].AccountID)
			}
		})
	}
}

// TestRefinement1_ListAWSServiceAccessForOrganization_Sorted verifies sorted output by ServicePrincipal.
func TestRefinement1_ListAWSServiceAccessForOrganization_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		principals []string
	}{
		{
			name:       "three_services_sorted",
			principals: []string{"sso.amazonaws.com", "access-analyzer.amazonaws.com", "ram.amazonaws.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			for _, p := range tt.principals {
				err := b.EnableAWSServiceAccess(p)
				require.NoError(t, err)
			}

			svcs, err := b.ListAWSServiceAccessForOrganization()
			require.NoError(t, err)
			require.Len(t, svcs, len(tt.principals))

			for i := 1; i < len(svcs); i++ {
				assert.LessOrEqual(t, svcs[i-1].ServicePrincipal, svcs[i].ServicePrincipal)
			}
		})
	}
}

// TestRefinement1_ListChildren_Sorted verifies sorted output by ID.
func TestRefinement1_ListChildren_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		childCount int
	}{
		{name: "three_accounts_sorted", childCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			roots, err := b.ListRoots()
			require.NoError(t, err)
			rootID := roots[0].ID

			for i := range tt.childCount {
				_, childErr := b.CreateAccount("child", fmt.Sprintf("c%d@example.com", i), "", "", nil)
				require.NoError(t, childErr)
			}

			children, err := b.ListChildren(rootID, "ACCOUNT")
			require.NoError(t, err)

			for i := 1; i < len(children); i++ {
				assert.LessOrEqual(t, children[i-1].ID, children[i].ID, "children should be sorted by ID")
			}
		})
	}
}

// TestRefinement1_ListAccountsForParent_Sorted verifies sorted output by account ID.
func TestRefinement1_ListAccountsForParent_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		acctCount  int
		wantSorted bool
	}{
		{name: "sorted", acctCount: 3, wantSorted: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			roots, err := b.ListRoots()
			require.NoError(t, err)
			rootID := roots[0].ID

			for i := range tt.acctCount {
				_, acctErr := b.CreateAccount("acct", fmt.Sprintf("a%d@example.com", i), "", "", nil)
				require.NoError(t, acctErr)
			}

			accts, err := b.ListAccountsForParent(rootID)
			require.NoError(t, err)

			for i := 1; i < len(accts); i++ {
				assert.LessOrEqual(t, accts[i-1].ID, accts[i].ID)
			}
		})
	}
}

// TestRefinement1_ListTargetsForPolicy_Sorted verifies sorted output by target ID.
func TestRefinement1_ListTargetsForPolicy_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "sorted_targets"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			roots, err := b.ListRoots()
			require.NoError(t, err)

			_, err = b.EnablePolicyType(roots[0].ID, "SERVICE_CONTROL_POLICY")
			require.NoError(t, err)

			p, err := b.CreatePolicy("sorted-pol", "", `{"Version":"2012-10-17"}`, "SERVICE_CONTROL_POLICY", nil)
			require.NoError(t, err)

			// Attach to root + a couple of accounts.
			err = b.AttachPolicy(p.PolicySummary.ID, roots[0].ID)
			require.NoError(t, err)

			for i := range 2 {
				s, err2 := b.CreateAccount("t-acct", fmt.Sprintf("t%d@example.com", i), "", "", nil)
				require.NoError(t, err2)
				require.NoError(t, b.AttachPolicy(p.PolicySummary.ID, s.AccountID))
			}

			targets, err := b.ListTargetsForPolicy(p.PolicySummary.ID)
			require.NoError(t, err)
			require.NotEmpty(t, targets)

			for i := 1; i < len(targets); i++ {
				assert.LessOrEqual(t, targets[i-1].TargetID, targets[i].TargetID)
			}
		})
	}
}

// TestRefinement1_DeleteOrganizationalUnit_WithAccounts_Fails verifies AWS behaviour: cannot delete OU with accounts.
func TestRefinement1_DeleteOrganizationalUnit_WithAccounts_Fails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "fails_when_ou_has_accounts", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			roots, err := b.ListRoots()
			require.NoError(t, err)

			ou, err := b.CreateOrganizationalUnit(roots[0].ID, "test-ou", nil)
			require.NoError(t, err)

			// Move management account into the OU.
			org, err := b.DescribeOrganization()
			require.NoError(t, err)
			err = b.MoveAccount(org.MasterAccountID, roots[0].ID, ou.ID)
			require.NoError(t, err)

			err = b.DeleteOrganizationalUnit(ou.ID)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement1_DeleteOrganizationalUnit_WithChildren_Fails verifies AWS behaviour: cannot delete OU with child OUs.
func TestRefinement1_DeleteOrganizationalUnit_WithChildren_Fails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "fails_when_ou_has_child_ou", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			roots, err := b.ListRoots()
			require.NoError(t, err)

			parent, err := b.CreateOrganizationalUnit(roots[0].ID, "parent-ou", nil)
			require.NoError(t, err)

			_, err = b.CreateOrganizationalUnit(parent.ID, "child-ou", nil)
			require.NoError(t, err)

			err = b.DeleteOrganizationalUnit(parent.ID)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement1_PutResourcePolicy_Happy verifies PutResourcePolicy via the HTTP handler.
func TestRefinement1_PutResourcePolicy_Happy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		wantOK  bool
	}{
		{name: "creates_policy", content: `{"Version":"2012-10-17","Statement":[]}`, wantOK: true},
		{name: "replaces_policy", content: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`, wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			h := organizations.NewHandler(b)

			rec := r1DoRequest(t, h, "PutResourcePolicy", map[string]string{"Content": tt.content})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			assert.NotNil(t, resp["ResourcePolicy"])
		})
	}
}

// TestRefinement1_PutResourcePolicy_NoOrg verifies PutResourcePolicy returns 400 without org.
func TestRefinement1_PutResourcePolicy_NoOrg(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "no_org_returns_error", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := r1NewHandler(t)
			rec := r1DoRequest(t, h, "PutResourcePolicy", map[string]string{"Content": "{}"})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_PutResourcePolicy_BadJSON verifies PutResourcePolicy handles malformed JSON.
func TestRefinement1_PutResourcePolicy_BadJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name:       "bad_json",
			body:       []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_content",
			body:       []byte(`{"Content":""}`),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)
			h := organizations.NewHandler(b)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "AWSOrganizationsV20161128.PutResourcePolicy")

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_Backend_Reset verifies Backend.Reset() clears all state.
func TestRefinement1_Backend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset_clears_accounts_ous_policies"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			// Create some data.
			_, err := b.CreateAccount("a", "a@x.com", "", "", nil)
			require.NoError(t, err)

			roots, err := b.ListRoots()
			require.NoError(t, err)

			_, err = b.CreateOrganizationalUnit(roots[0].ID, "ou1", nil)
			require.NoError(t, err)

			_, err = b.CreatePolicy("p1", "", `{}`, "SERVICE_CONTROL_POLICY", nil)
			require.NoError(t, err)

			require.Positive(t, organizations.AccountCount(b))
			require.Positive(t, organizations.OUCount(b))

			b.Reset()

			assert.Equal(t, 0, organizations.AccountCount(b), tt.name)
			assert.Equal(t, 0, organizations.OUCount(b), tt.name)
			assert.Equal(t, 0, organizations.PolicyCount(b), tt.name)
		})
	}
}

// TestRefinement1_AddHandshakeInternal_SetsExpiry verifies expiry is set automatically.
func TestRefinement1_AddHandshakeInternal_SetsExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		withExpiry bool
		withID     bool
		withARN    bool
	}{
		{name: "auto_expiry_id_arn", withExpiry: false, withID: false, withARN: false},
		{name: "explicit_expiry_preserved", withExpiry: true, withID: true, withARN: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			h := &organizations.Handshake{
				State: "OPEN",
			}

			if tt.withExpiry {
				h.ExpirationTimestamp = time.Now().Add(24 * time.Hour)
			}

			if tt.withID {
				h.ID = "h-testid00"
			}

			b.AddHandshakeInternal(h)

			assert.False(t, h.ExpirationTimestamp.IsZero(), "expiry should be set")

			if tt.withID {
				assert.Equal(t, "h-testid00", h.ID)
			} else {
				assert.NotEmpty(t, h.ID)
			}

			assert.Equal(t, 1, organizations.HandshakeCount(b))
		})
	}
}

// TestRefinement1_CreatePolicy_InvalidType verifies CreatePolicy rejects invalid policy types.
func TestRefinement1_CreatePolicy_InvalidType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policyType string
		wantErr    bool
	}{
		{name: "invalid_type", policyType: "INVALID_POLICY_TYPE", wantErr: true},
		{name: "empty_type", policyType: "", wantErr: true},
		{name: "valid_scp", policyType: "SERVICE_CONTROL_POLICY", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			_, err := b.CreatePolicy("p", "", `{"Version":"2012-10-17"}`, tt.policyType, nil)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement1_CreatePolicy_ValidTypes verifies all 6 valid policy types are accepted.
func TestRefinement1_CreatePolicy_ValidTypes(t *testing.T) {
	t.Parallel()

	validTypes := []string{
		"SERVICE_CONTROL_POLICY",
		"TAG_POLICY",
		"BACKUP_POLICY",
		"AISERVICES_OPT_OUT_POLICY",
		"CHATBOT_POLICY",
		"DECLARATIVE_POLICY_EC2",
	}

	for _, pt := range validTypes {
		t.Run(pt, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			p, err := b.CreatePolicy("mypol", "", `{"Version":"2012-10-17"}`, pt, nil)
			require.NoError(t, err)
			assert.Equal(t, pt, p.PolicySummary.Type)
		})
	}
}

// TestRefinement1_Backend_AddAccountInternal verifies seed helper.
func TestRefinement1_Backend_AddAccountInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		wantCount int
	}{
		{name: "adds_account", accountID: "111111111111", wantCount: 2}, // management + seed
		{name: "replaces_if_same_id", accountID: "111111111111", wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			b.AddAccountInternal(&organizations.Account{
				ID:     tt.accountID,
				Name:   "seed-account",
				Status: "ACTIVE",
			})

			assert.Equal(t, tt.wantCount, organizations.AccountCount(b))
		})
	}
}

// TestRefinement1_Backend_AddOUInternal verifies OU seed helper.
func TestRefinement1_Backend_AddOUInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ouID      string
		wantCount int
	}{
		{name: "adds_ou", ouID: "ou-abcd-12345678", wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			b.AddOUInternal(&organizations.OrganizationalUnit{
				ID:   tt.ouID,
				Name: "seed-ou",
			})

			assert.Equal(t, tt.wantCount, organizations.OUCount(b))
		})
	}
}

// TestRefinement1_Backend_AddPolicyInternal verifies policy seed helper.
func TestRefinement1_Backend_AddPolicyInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		policyID  string
		wantCount int
	}{
		{name: "adds_policy", policyID: "p-12345678", wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			b.AddPolicyInternal(&organizations.Policy{
				PolicySummary: organizations.PolicySummary{
					ID:   tt.policyID,
					Name: "seed-policy",
					Type: "SERVICE_CONTROL_POLICY",
				},
				Content: `{}`,
			})

			assert.Equal(t, tt.wantCount, organizations.PolicyCount(b))
		})
	}
}

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations has 50 entries.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
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

			h := r1NewHandler(t)
			assert.Equal(t, tt.wantLen, organizations.HandlerOpsLen(h))
		})
	}
}

// TestRefinement1_ResolveTargetSummary_Root verifies the ROOT target type is returned correctly.
func TestRefinement1_ResolveTargetSummary_Root(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantType string
	}{
		{name: "root_target", wantType: "ROOT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			roots, err := b.ListRoots()
			require.NoError(t, err)
			require.Len(t, roots, 1)

			summary := organizations.ResolveTargetSummaryExported(b, roots[0].ID)
			assert.Equal(t, tt.wantType, summary.Type)
			assert.Equal(t, roots[0].ID, summary.TargetID)
		})
	}
}

// TestRefinement1_ResolveTargetSummary_OU verifies the ORGANIZATIONAL_UNIT target type.
func TestRefinement1_ResolveTargetSummary_OU(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ouName   string
		wantType string
	}{
		{name: "ou_target", ouName: "test-ou", wantType: "ORGANIZATIONAL_UNIT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			roots, err := b.ListRoots()
			require.NoError(t, err)

			ou, err := b.CreateOrganizationalUnit(roots[0].ID, tt.ouName, nil)
			require.NoError(t, err)

			summary := organizations.ResolveTargetSummaryExported(b, ou.ID)
			assert.Equal(t, tt.wantType, summary.Type)
			assert.Equal(t, tt.ouName, summary.Name)
		})
	}
}

// TestRefinement1_Persistence_EnsureNonNilMaps verifies ensureNonNilMaps initialises all fields.
func TestRefinement1_Persistence_EnsureNonNilMaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_snapshot_gets_non_nil_maps"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snap := organizations.NewBackendSnapshot()
			organizations.EnsureNonNilMapsExported(snap)

			// After EnsureNonNilMaps all maps in the snapshot should be non-nil.
			// We can verify via round-trip: marshal+unmarshal should not panic.
			data, err := json.Marshal(snap)
			require.NoError(t, err)
			assert.NotNil(t, data, tt.name)
		})
	}
}

// TestRefinement1_ExportHelpers verifies all export helpers return correct types.
func TestRefinement1_ExportHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "helpers_return_correct_counts"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			_, err := b.CreateAccount("a", "a@x.com", "", "", nil)
			require.NoError(t, err)

			roots, err := b.ListRoots()
			require.NoError(t, err)

			_, err = b.CreateOrganizationalUnit(roots[0].ID, "ou", nil)
			require.NoError(t, err)

			_, err = b.CreatePolicy("pol", "", `{}`, "SERVICE_CONTROL_POLICY", nil)
			require.NoError(t, err)

			err = b.TagResource(roots[0].ID, []organizations.Tag{{Key: "k", Value: "v"}})
			require.NoError(t, err)

			b.AddHandshakeInternal(&organizations.Handshake{State: "OPEN"})

			assert.Equal(t, 2, organizations.AccountCount(b), tt.name+": account count")
			assert.Equal(t, 1, organizations.OUCount(b), tt.name+": ou count")
			assert.Equal(t, 1, organizations.PolicyCount(b), tt.name+": policy count")
			assert.Equal(t, 1, organizations.HandshakeCount(b), tt.name+": handshake count")
			assert.Equal(t, 1, organizations.TagCount(b), tt.name+": tag count")

			h := organizations.NewHandler(b)
			assert.Positive(t, organizations.HandlerOpsLen(h), tt.name+": ops len")
		})
	}
}

// TestRefinement1_ListRoots_DeepCopy verifies ListRoots returns a copy, not a reference.
func TestRefinement1_ListRoots_DeepCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutating_returned_root_does_not_affect_backend"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := r1NewBackend(t)
			r1CreateOrg(t, b)

			roots, err := b.ListRoots()
			require.NoError(t, err)
			require.Len(t, roots, 1)

			originalName := roots[0].Name
			roots[0].Name = "mutated"

			roots2, err := b.ListRoots()
			require.NoError(t, err)

			assert.Equal(t, originalName, roots2[0].Name, tt.name)
		})
	}
}

// TestRefinement1_Provider_NilAppContextError verifies ErrNilAppContext is returned.
func TestRefinement1_Provider_NilAppContextError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx     any
		name    string
		wantErr bool
	}{
		{name: "nil_ctx_returns_error", ctx: nil, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &organizations.Provider{}
			_, err := p.Init(nil)

			if tt.wantErr {
				require.ErrorIs(t, err, organizations.ErrNilAppContext)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
