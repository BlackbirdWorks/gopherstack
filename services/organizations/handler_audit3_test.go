package organizations_test

import (
	"encoding/json"
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// ---------------------------------------------------------------------------
// Item 12: ListPolicies / ListPoliciesForTarget require non-empty Filter
// ---------------------------------------------------------------------------

// TestAudit3_ListPolicies_EmptyFilter verifies that ListPolicies rejects empty Filter.
func TestAudit3_ListPolicies_EmptyFilter(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	_, err := b.ListPolicies("")
	require.Error(t, err, "empty Filter must be rejected")

	// Valid filter works fine (empty result is OK, nil error is required).
	_, err = b.ListPolicies("SERVICE_CONTROL_POLICY")
	require.NoError(t, err)
}

// TestAudit3_ListPoliciesForTarget_EmptyFilter verifies that ListPoliciesForTarget rejects empty Filter.
func TestAudit3_ListPoliciesForTarget_EmptyFilter(t *testing.T) {
	t.Parallel()

	b, rootID := newOrgBackend(t)

	_, err := b.ListPoliciesForTarget(rootID, "")
	require.Error(t, err, "empty Filter must be rejected")

	// Valid filter works fine.
	_, err = b.ListPoliciesForTarget(rootID, "TAG_POLICY")
	require.NoError(t, err)
}

// TestAudit3_ListPolicies_EmptyFilter_ViaHandler verifies HTTP 400 when Filter is empty.
func TestAudit3_ListPolicies_EmptyFilter_ViaHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "ListPolicies_empty_filter",
			op:         "ListPolicies",
			body:       map[string]any{"Filter": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "ListPolicies_missing_filter",
			op:         "ListPolicies",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "ListPoliciesForTarget_empty_filter",
			op:         "ListPoliciesForTarget",
			body:       map[string]any{"TargetId": "r-test", "Filter": ""},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code, tt.name)
		})
	}
}

// ---------------------------------------------------------------------------
// Item 26: Policy IDs use lowercase hex chars only
// ---------------------------------------------------------------------------

var policyIDHexRe = regexp.MustCompile(`^p-[0-9a-f]{8}$`)

// TestAudit3_PolicyID_HexFormat verifies that generated policy IDs use only
// lowercase hex chars (0-9 a-f) — matching real AWS policy ID format.
func TestAudit3_PolicyID_HexFormat(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	for i := range 20 {
		p, err := b.CreatePolicy(
			"hex-test-"+string(rune('a'+i%26)),
			"",
			`{"Version":"2012-10-17"}`,
			"SERVICE_CONTROL_POLICY",
			nil,
		)
		require.NoError(t, err)
		assert.Regexp(t, policyIDHexRe, p.PolicySummary.ID,
			"policy ID must match p-[0-9a-f]{8}, got %s", p.PolicySummary.ID)
	}
}

// ---------------------------------------------------------------------------
// Item 8: RemoveAccountFromOrganization cleans policyTargets reverse mapping
// and generates LEAVE_ORGANIZATION handshake for INVITED accounts
// ---------------------------------------------------------------------------

// TestAudit3_RemoveAccount_CleansPolicyTargets verifies that removing an account
// also removes it from every attached policy's target list (no dangling references).
func TestAudit3_RemoveAccount_CleansPolicyTargets(t *testing.T) {
	t.Parallel()

	b, rootID := newOrgBackend(t)

	// Enable SCP and create an account.
	_, err := b.EnablePolicyType(rootID, "SERVICE_CONTROL_POLICY")
	require.NoError(t, err)

	status, err := b.CreateAccount("to-remove", "remove@example.com", "", "", nil)
	require.NoError(t, err)
	accountID := status.AccountID

	// Create a policy and attach it to the account.
	p, err := b.CreatePolicy("test-scp", "", `{"Version":"2012-10-17"}`, "SERVICE_CONTROL_POLICY", nil)
	require.NoError(t, err)
	require.NoError(t, b.AttachPolicy(p.PolicySummary.ID, accountID))

	// Verify the account appears in ListTargetsForPolicy before removal.
	targets, err := b.ListTargetsForPolicy(p.PolicySummary.ID)
	require.NoError(t, err)

	found := false
	for _, tgt := range targets {
		if tgt.TargetID == accountID {
			found = true

			break
		}
	}
	assert.True(t, found, "account should appear in ListTargetsForPolicy before removal")

	// Remove the account.
	require.NoError(t, b.RemoveAccountFromOrganization(accountID))

	// Verify the account no longer appears in ListTargetsForPolicy.
	targets, err = b.ListTargetsForPolicy(p.PolicySummary.ID)
	require.NoError(t, err)

	for _, tgt := range targets {
		assert.NotEqual(t, accountID, tgt.TargetID,
			"removed account must not appear in ListTargetsForPolicy")
	}
}

// TestAudit3_RemoveAccount_InvitedCreatesLeaveHandshake verifies that removing
// an INVITED account generates a LEAVE_ORGANIZATION handshake record.
func TestAudit3_RemoveAccount_InvitedCreatesLeaveHandshake(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	// Invite an account (this creates an INVITED account after accepting).
	h, err := b.InviteAccountToOrganization(organizations.HandshakeParty{
		ID:   "999999999999",
		Type: "ACCOUNT",
	}, "")
	require.NoError(t, err)

	_, err = b.AcceptHandshake(h.ID)
	require.NoError(t, err)

	// Verify the account is now in the org.
	accts, err := b.ListAccounts()
	require.NoError(t, err)

	var invitedID string
	for _, a := range accts {
		if a.ID == "999999999999" {
			invitedID = a.ID

			break
		}
	}
	require.NotEmpty(t, invitedID, "invited account should be present after AcceptHandshake")

	// Count handshakes before removal.
	countBefore := organizations.HandshakeCount(b)

	// Remove the invited account.
	require.NoError(t, b.RemoveAccountFromOrganization(invitedID))

	// A LEAVE_ORGANIZATION handshake should have been created.
	countAfter := organizations.HandshakeCount(b)
	assert.Greater(t, countAfter, countBefore,
		"removing an INVITED account should create a LEAVE_ORGANIZATION handshake")

	// Verify we can list it.
	handshakes, err := b.ListHandshakesForOrganization("")
	require.NoError(t, err)

	found := false
	for _, hs := range handshakes {
		if hs.Action == "LEAVE_ORGANIZATION" {
			found = true

			break
		}
	}
	assert.True(t, found, "LEAVE_ORGANIZATION handshake should appear in ListHandshakesForOrganization")
}

// TestAudit3_RemoveAccount_CreatedNoLeaveHandshake verifies that removing
// a CREATED account does NOT generate a LEAVE_ORGANIZATION handshake.
func TestAudit3_RemoveAccount_CreatedNoLeaveHandshake(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	// CreateAccount produces a CREATED account.
	status, err := b.CreateAccount("created-acct", "created@example.com", "", "", nil)
	require.NoError(t, err)
	accountID := status.AccountID

	countBefore := organizations.HandshakeCount(b)

	require.NoError(t, b.RemoveAccountFromOrganization(accountID))

	countAfter := organizations.HandshakeCount(b)
	assert.Equal(t, countBefore, countAfter,
		"removing a CREATED account must not create any handshake")
}

// ---------------------------------------------------------------------------
// Item 19: DescribeEffectivePolicy walks full hierarchy chain
// ---------------------------------------------------------------------------

// TestAudit3_DescribeEffectivePolicy_FullChain verifies that DescribeEffectivePolicy
// returns the effective policy even when the policy is attached to a parent (OU or root),
// not just the direct target.
func TestAudit3_DescribeEffectivePolicy_FullChain(t *testing.T) {
	t.Parallel()

	b, rootID := newOrgBackend(t)

	// Enable TAG_POLICY so we can use it.
	_, err := b.EnablePolicyType(rootID, "TAG_POLICY")
	require.NoError(t, err)

	// Create a TAG_POLICY attached at root level.
	tagContent := `{"tags":{"env":{"tag_value":{"@@assign":"prod"}}}}`
	rootPolicy, err := b.CreatePolicy("root-tag", "", tagContent, "TAG_POLICY", nil)
	require.NoError(t, err)
	require.NoError(t, b.AttachPolicy(rootPolicy.PolicySummary.ID, rootID))

	// Create an OU with no direct policy.
	ou, err := b.CreateOrganizationalUnit(rootID, "prod-ou", nil)
	require.NoError(t, err)

	// Create an account inside the OU with no direct policy.
	status, err := b.CreateAccount("leaf-acct", "leaf@example.com", "", "", nil)
	require.NoError(t, err)
	require.NoError(t, b.MoveAccount(status.AccountID, rootID, ou.ID))

	// DescribeEffectivePolicy on the account should find the root-level policy via chain walk.
	ep, err := b.DescribeEffectivePolicy("TAG_POLICY", status.AccountID)
	require.NoError(t, err, "effective policy should be found via hierarchy chain")
	assert.NotEmpty(t, ep.PolicyContent)
	assert.Equal(t, "TAG_POLICY", ep.PolicyType)
}

// TestAudit3_DescribeEffectivePolicy_MergeTagPolicy verifies that TAG_POLICY
// content from multiple levels in the chain is deep-merged, with child overriding parent.
func TestAudit3_DescribeEffectivePolicy_MergeTagPolicy(t *testing.T) {
	t.Parallel()

	b, rootID := newOrgBackend(t)

	_, err := b.EnablePolicyType(rootID, "TAG_POLICY")
	require.NoError(t, err)

	// Attach a TAG_POLICY at root that sets "env" and "owner".
	parentContent := `{"tags":{` +
		`"env":{"tag_value":{"@@assign":"prod"}},` +
		`"owner":{"tag_value":{"@@assign":"platform"}}}}`
	parentPolicy, err := b.CreatePolicy("parent-tag", "", parentContent, "TAG_POLICY", nil)
	require.NoError(t, err)
	require.NoError(t, b.AttachPolicy(parentPolicy.PolicySummary.ID, rootID))

	// Create account and attach a TAG_POLICY that overrides "env" but keeps "owner" from parent.
	childContent := `{"tags":{"env":{"tag_value":{"@@assign":"dev"}}}}`
	childPolicy, err := b.CreatePolicy("child-tag", "", childContent, "TAG_POLICY", nil)
	require.NoError(t, err)

	status, err := b.CreateAccount("merge-acct", "merge@example.com", "", "", nil)
	require.NoError(t, err)
	require.NoError(t, b.AttachPolicy(childPolicy.PolicySummary.ID, status.AccountID))

	ep, err := b.DescribeEffectivePolicy("TAG_POLICY", status.AccountID)
	require.NoError(t, err)

	// Merged content should have both "env" (from child) and "owner" (from parent).
	var merged map[string]any
	require.NoError(t, json.Unmarshal([]byte(ep.PolicyContent), &merged))

	tags, hasTags := merged["tags"].(map[string]any)
	require.True(t, hasTags, "merged policy should have 'tags' object")
	assert.Contains(t, tags, "env", "merged policy should contain child 'env' key")
	assert.Contains(t, tags, "owner", "merged policy should contain parent 'owner' key")

	// Child's "env" value should override parent's.
	envTag, hasEnvTag := tags["env"].(map[string]any)
	if hasEnvTag {
		tagVal, hasTagVal := envTag["tag_value"].(map[string]any)
		if hasTagVal {
			assert.Equal(t, "dev", tagVal["@@assign"], "child 'env' should override parent")
		}
	}
}

// TestAudit3_DescribeEffectivePolicy_OUChain verifies the chain walk through multiple OU levels.
func TestAudit3_DescribeEffectivePolicy_OUChain(t *testing.T) {
	t.Parallel()

	b, rootID := newOrgBackend(t)

	_, err := b.EnablePolicyType(rootID, "TAG_POLICY")
	require.NoError(t, err)

	// Attach policy at root.
	rootPolicy, err := b.CreatePolicy("chain-tag", "", `{"Version":"2012-10-17"}`, "TAG_POLICY", nil)
	require.NoError(t, err)
	require.NoError(t, b.AttachPolicy(rootPolicy.PolicySummary.ID, rootID))

	// Build OU1 > OU2 chain.
	ou1, err := b.CreateOrganizationalUnit(rootID, "ou1", nil)
	require.NoError(t, err)
	ou2, err := b.CreateOrganizationalUnit(ou1.ID, "ou2", nil)
	require.NoError(t, err)

	// Account inside OU2 with no direct policy.
	status, err := b.CreateAccount("deep-acct", "deep@example.com", "", "", nil)
	require.NoError(t, err)
	require.NoError(t, b.MoveAccount(status.AccountID, rootID, ou2.ID))

	ep, err := b.DescribeEffectivePolicy("TAG_POLICY", status.AccountID)
	require.NoError(t, err, "root-level policy should be found via deep OU chain")
	assert.NotEmpty(t, ep.PolicyContent)
}

// ---------------------------------------------------------------------------
// Item 22: CreateAccount response must NOT include GovCloudAccountId
// ---------------------------------------------------------------------------

// TestAudit3_CreateAccount_NoGovCloudID verifies that CreateAccount followed by
// DescribeCreateAccountStatus returns a payload without GovCloudAccountId.
func TestAudit3_CreateAccount_NoGovCloudID(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	status, err := b.CreateAccount("commercial-acct", "commercial@example.com", "", "", nil)
	require.NoError(t, err)

	described, err := b.DescribeCreateAccountStatus(status.ID)
	require.NoError(t, err)
	assert.Empty(t, described.GovCloudAccountID,
		"CreateAccount DescribeCreateAccountStatus must not include GovCloudAccountId")
}

// TestAudit3_CreateAccount_NoGovCloudID_ViaHandler verifies that the HTTP response
// for CreateAccount does not include the GovCloudAccountId key.
func TestAudit3_CreateAccount_NoGovCloudID_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

	rec := doRequest(t, h, "CreateAccount", map[string]any{
		"AccountName": "commercial-only",
		"Email":       "commercial-only@example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	statusObj, ok := resp["CreateAccountStatus"].(map[string]any)
	require.True(t, ok, "response must have CreateAccountStatus")

	_, hasGovCloud := statusObj["GovCloudAccountId"]
	assert.False(t, hasGovCloud,
		"CreateAccount response must not include GovCloudAccountId; got %v", statusObj)
}

// TestAudit3_GovCloudAccount_HasGovCloudID verifies that CreateGovCloudAccount
// response DOES include GovCloudAccountId.
func TestAudit3_GovCloudAccount_HasGovCloudID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

	rec := doRequest(t, h, "CreateGovCloudAccount", map[string]any{
		"AccountName": "gov-only",
		"Email":       "gov-only@example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	statusObj, ok := resp["CreateAccountStatus"].(map[string]any)
	require.True(t, ok, "response must have CreateAccountStatus")

	govID, hasGovCloud := statusObj["GovCloudAccountId"]
	assert.True(t, hasGovCloud, "CreateGovCloudAccount response must include GovCloudAccountId")
	assert.NotEmpty(t, govID, "GovCloudAccountId must not be empty")
}
