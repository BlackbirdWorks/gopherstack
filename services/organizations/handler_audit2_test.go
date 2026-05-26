package organizations_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// ---------------------------------------------------------------------------
// Item 1 & 2: AvailablePolicyTypes in DescribeOrganization + new policy types
// ---------------------------------------------------------------------------

// TestAudit2_DescribeOrganization_AvailablePolicyTypes verifies that DescribeOrganization
// returns all 8 valid policy types in AvailablePolicyTypes, with correct ENABLED/DISABLED status.
func TestAudit2_DescribeOrganization_AvailablePolicyTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		enableTypes  []string
		wantEnabled  []string
		wantDisabled []string
		wantTotalLen int
	}{
		{
			name:         "all_disabled_initially",
			enableTypes:  nil,
			wantEnabled:  nil,
			wantTotalLen: 8,
			wantDisabled: []string{
				"SERVICE_CONTROL_POLICY",
				"RESOURCE_CONTROL_POLICY",
				"TAG_POLICY",
				"BACKUP_POLICY",
				"AISERVICES_OPT_OUT_POLICY",
				"CHATBOT_POLICY",
				"DECLARATIVE_POLICY_EC2",
				"SECURITYHUB_POLICY",
			},
		},
		{
			name:        "one_enabled",
			enableTypes: []string{"SERVICE_CONTROL_POLICY"},
			wantEnabled: []string{"SERVICE_CONTROL_POLICY"},
			wantDisabled: []string{
				"RESOURCE_CONTROL_POLICY",
				"TAG_POLICY",
				"BACKUP_POLICY",
				"AISERVICES_OPT_OUT_POLICY",
				"CHATBOT_POLICY",
				"DECLARATIVE_POLICY_EC2",
				"SECURITYHUB_POLICY",
			},
			wantTotalLen: 8,
		},
		{
			name: "multiple_enabled",
			enableTypes: []string{
				"SERVICE_CONTROL_POLICY",
				"RESOURCE_CONTROL_POLICY",
				"SECURITYHUB_POLICY",
			},
			wantEnabled: []string{
				"SERVICE_CONTROL_POLICY",
				"RESOURCE_CONTROL_POLICY",
				"SECURITYHUB_POLICY",
			},
			wantTotalLen: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			for _, pt := range tt.enableTypes {
				_, err := b.EnablePolicyType(rootID, pt)
				require.NoError(t, err)
			}

			org, err := b.DescribeOrganization()
			require.NoError(t, err)

			assert.Len(t, org.AvailablePolicyTypes, tt.wantTotalLen, "should have 8 policy types")

			// Build a map for easy lookup.
			statusMap := make(map[string]string)
			for _, pt := range org.AvailablePolicyTypes {
				statusMap[pt.Type] = pt.Status
			}

			for _, enabled := range tt.wantEnabled {
				assert.Equal(t, "ENABLED", statusMap[enabled], "type %s should be ENABLED", enabled)
			}

			for _, disabled := range tt.wantDisabled {
				assert.Equal(t, "DISABLED", statusMap[disabled], "type %s should be DISABLED", disabled)
			}
		})
	}
}

// TestAudit2_AvailablePolicyTypes_ViaHandler verifies DescribeOrganization via HTTP handler.
func TestAudit2_AvailablePolicyTypes_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create org.
	rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe org.
	rec = doRequest(t, h, "DescribeOrganization", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	org := resp["Organization"].(map[string]any)
	pts, ok := org["AvailablePolicyTypes"].([]any)
	require.True(t, ok, "AvailablePolicyTypes should be present")
	assert.Len(t, pts, 8, "should have 8 available policy types")
}

// ---------------------------------------------------------------------------
// Item 2: Validate all 8 policy types in validPolicyTypes
// ---------------------------------------------------------------------------

// TestAudit2_ValidPolicyTypes_AllEight ensures all 8 policy types can be used in CreatePolicy.
func TestAudit2_ValidPolicyTypes_AllEight(t *testing.T) {
	t.Parallel()

	allTypes := []string{
		"SERVICE_CONTROL_POLICY",
		"RESOURCE_CONTROL_POLICY",
		"TAG_POLICY",
		"BACKUP_POLICY",
		"AISERVICES_OPT_OUT_POLICY",
		"CHATBOT_POLICY",
		"DECLARATIVE_POLICY_EC2",
		"SECURITYHUB_POLICY",
	}

	for _, pt := range allTypes {
		t.Run(pt, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			p, err := b.CreatePolicy("test", "desc", `{"Version":"2012-10-17"}`, pt, nil)
			require.NoError(t, err, "CreatePolicy should succeed for %s", pt)
			assert.Equal(t, pt, p.PolicySummary.Type)
		})
	}
}

// TestAudit2_NewPolicyTypes_ResourceControl_SecurityHub specifically tests the two new ones.
func TestAudit2_NewPolicyTypes_ResourceControl_SecurityHub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policyType string
	}{
		{name: "resource_control_policy", policyType: "RESOURCE_CONTROL_POLICY"},
		{name: "securityhub_policy", policyType: "SECURITYHUB_POLICY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			// CreatePolicy.
			p, err := b.CreatePolicy("test", "desc", `{"Version":"2012-10-17"}`, tt.policyType, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.policyType, p.PolicySummary.Type)

			// EnablePolicyType.
			root, err := b.EnablePolicyType(rootID, tt.policyType)
			require.NoError(t, err)

			found := false
			for _, pt := range root.PolicyTypes {
				if pt.Type == tt.policyType && pt.Status == "ENABLED" {
					found = true
				}
			}
			assert.True(t, found, "%s should be ENABLED", tt.policyType)
		})
	}
}

// ---------------------------------------------------------------------------
// Item 3: FeatureSet validation in CreateOrganization
// ---------------------------------------------------------------------------

// TestAudit2_CreateOrganization_FeatureSetValidation verifies that invalid FeatureSets are rejected.
func TestAudit2_CreateOrganization_FeatureSetValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		featureSet string
		wantErr    bool
	}{
		{name: "ALL_valid", featureSet: "ALL", wantErr: false},
		{name: "CONSOLIDATED_BILLING_valid", featureSet: "CONSOLIDATED_BILLING", wantErr: false},
		{name: "empty_defaults_to_ALL", featureSet: "", wantErr: false},
		{name: "invalid_FULL", featureSet: "FULL", wantErr: true},
		{name: "invalid_NONE", featureSet: "NONE", wantErr: true},
		{name: "invalid_lowercase_all", featureSet: "all", wantErr: true},
		{name: "invalid_gibberish", featureSet: "SOMETHING_ELSE", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			_, _, err := b.CreateOrganization(tt.featureSet)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestAudit2_CreateOrganization_FeatureSetViaHandler tests FeatureSet validation via handler.
func TestAudit2_CreateOrganization_FeatureSetViaHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		featureSet string
		wantStatus int
	}{
		{name: "ALL", featureSet: "ALL", wantStatus: http.StatusOK},
		{name: "CONSOLIDATED_BILLING", featureSet: "CONSOLIDATED_BILLING", wantStatus: http.StatusOK},
		{name: "invalid", featureSet: "BOGUS", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": tt.featureSet})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Item 4: RoleName and IamUserAccessToBilling in CreateAccount
// ---------------------------------------------------------------------------

// TestAudit2_CreateAccount_RoleName verifies RoleName defaults to OrganizationAccountAccessRole.
func TestAudit2_CreateAccount_RoleName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		roleName     string
		wantRoleName string
	}{
		{name: "default_role", roleName: "", wantRoleName: "OrganizationAccountAccessRole"},
		{name: "custom_role", roleName: "MyCustomRole", wantRoleName: "MyCustomRole"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create org first.
			rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
			require.Equal(t, http.StatusOK, rec.Code)

			// Create account.
			body := map[string]any{
				"AccountName": "test-account",
				"Email":       "test@example.com",
			}
			if tt.roleName != "" {
				body["RoleName"] = tt.roleName
			}

			rec = doRequest(t, h, "CreateAccount", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			// Describe account.
			status := resp["CreateAccountStatus"].(map[string]any)
			accountID := status["AccountId"].(string)

			rec = doRequest(t, h, "DescribeAccount", map[string]any{"AccountId": accountID})
			require.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&descResp))

			acct := descResp["Account"].(map[string]any)
			roleName, _ := acct["RoleName"].(string)
			assert.Equal(t, tt.wantRoleName, roleName)
		})
	}
}

// TestAudit2_CreateAccount_IamUserAccessToBilling verifies IamUserAccessToBilling validation.
func TestAudit2_CreateAccount_IamUserAccessToBilling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		iamAccess     string
		wantIamAccess string
		wantStatus    int
	}{
		{name: "default_allow", iamAccess: "", wantStatus: http.StatusOK, wantIamAccess: "ALLOW"},
		{name: "explicit_allow", iamAccess: "ALLOW", wantStatus: http.StatusOK, wantIamAccess: "ALLOW"},
		{name: "explicit_deny", iamAccess: "DENY", wantStatus: http.StatusOK, wantIamAccess: "DENY"},
		{name: "invalid_value", iamAccess: "MAYBE", wantStatus: http.StatusBadRequest},
		{name: "invalid_lowercase", iamAccess: "allow", wantStatus: http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
			require.Equal(t, http.StatusOK, rec.Code)

			body := map[string]any{
				"AccountName": "test-account",
				"Email":       fmt.Sprintf("test%d@example.com", i),
			}
			if tt.iamAccess != "" {
				body["IamUserAccessToBilling"] = tt.iamAccess
			}

			rec = doRequest(t, h, "CreateAccount", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.wantIamAccess != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

				status := resp["CreateAccountStatus"].(map[string]any)
				accountID := status["AccountId"].(string)

				rec = doRequest(t, h, "DescribeAccount", map[string]any{"AccountId": accountID})
				require.Equal(t, http.StatusOK, rec.Code)

				var descResp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&descResp))

				acct := descResp["Account"].(map[string]any)
				iamAccess, _ := acct["IamUserAccessToBilling"].(string)
				assert.Equal(t, tt.wantIamAccess, iamAccess)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Item 5: CloseAccount sets PENDING_CLOSURE and rejects double-close
// ---------------------------------------------------------------------------

// TestAudit2_CloseAccount_PendingClosure verifies CloseAccount sets status to PENDING_CLOSURE.
func TestAudit2_CloseAccount_PendingClosure(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	status, err := b.CreateAccount("close-me", "close@example.com", "", "", nil)
	require.NoError(t, err)

	err = b.CloseAccount(status.AccountID)
	require.NoError(t, err)

	acct, err := b.DescribeAccount(status.AccountID)
	require.NoError(t, err)
	assert.Equal(t, "PENDING_CLOSURE", acct.Status)
}

// TestAudit2_CloseAccount_DoubleCloseRejected verifies that closing an already-closed account fails.
func TestAudit2_CloseAccount_DoubleCloseRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupStatus string // "PENDING_CLOSURE" or "SUSPENDED" (via direct internal method)
		wantErr     bool
	}{
		{name: "double_pending_closure", setupStatus: "PENDING_CLOSURE", wantErr: true},
		{name: "suspended_also_rejected", setupStatus: "SUSPENDED", wantErr: true},
		{name: "active_succeeds", setupStatus: "", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			status, err := b.CreateAccount("close-me", "close@example.com", "", "", nil)
			require.NoError(t, err)
			accountID := status.AccountID

			switch tt.setupStatus {
			case "PENDING_CLOSURE":
				// Close once to get to PENDING_CLOSURE.
				err = b.CloseAccount(accountID)
				require.NoError(t, err)
			case "SUSPENDED":
				// Seed a SUSPENDED account directly.
				b.AddAccountInternal(&organizations.Account{
					ID:     "999999999999",
					ARN:    "arn:aws:organizations::123456789012:account/o-test/999999999999",
					Name:   "suspended",
					Email:  "suspended@example.com",
					Status: "SUSPENDED",
				})
				accountID = "999999999999"
			}

			err = b.CloseAccount(accountID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestAudit2_CloseAccount_ViaHandler tests CloseAccount via HTTP handler.
func TestAudit2_CloseAccount_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateAccount", map[string]any{
		"AccountName": "close-test",
		"Email":       "close@example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	statusObj := createResp["CreateAccountStatus"].(map[string]any)
	accountID := statusObj["AccountId"].(string)

	// First close succeeds.
	rec = doRequest(t, h, "CloseAccount", map[string]any{"AccountId": accountID})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Second close should fail.
	rec = doRequest(t, h, "CloseAccount", map[string]any{"AccountId": accountID})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Item 6: OU depth limit
// ---------------------------------------------------------------------------

// TestAudit2_OU_DepthLimit verifies that OUs can only be nested 5 levels deep.
func TestAudit2_OU_DepthLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		depth   int
		wantErr bool
	}{
		{name: "depth_1_ok", depth: 1, wantErr: false},
		{name: "depth_2_ok", depth: 2, wantErr: false},
		{name: "depth_3_ok", depth: 3, wantErr: false},
		{name: "depth_4_ok", depth: 4, wantErr: false},
		{name: "depth_5_ok", depth: 5, wantErr: false},
		{name: "depth_6_rejected", depth: 6, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			// Build a chain of OUs up to depth tt.depth.
			parentID := rootID
			var err error

			for d := 1; d <= tt.depth; d++ {
				var ou *organizations.OrganizationalUnit
				ou, err = b.CreateOrganizationalUnit(parentID, fmt.Sprintf("ou-depth-%d", d), nil)
				if err != nil {
					break
				}
				parentID = ou.ID
			}

			if tt.wantErr {
				require.Error(t, err, "depth %d should be rejected", tt.depth)
			} else {
				require.NoError(t, err, "depth %d should be allowed", tt.depth)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Item 7: OU name uniqueness
// ---------------------------------------------------------------------------

// TestAudit2_OU_NameUniqueness verifies that sibling OUs cannot have the same name.
func TestAudit2_OU_NameUniqueness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *organizations.InMemoryBackend, rootID string)
		create  func(b *organizations.InMemoryBackend, rootID string) error
		name    string
		wantErr bool
	}{
		{
			name: "duplicate_sibling_rejected",
			setup: func(b *organizations.InMemoryBackend, rootID string) {
				_, err := b.CreateOrganizationalUnit(rootID, "Engineering", nil)
				if err != nil {
					panic(err)
				}
			},
			create: func(b *organizations.InMemoryBackend, rootID string) error {
				_, err := b.CreateOrganizationalUnit(rootID, "Engineering", nil)

				return err
			},
			wantErr: true,
		},
		{
			name:  "different_name_same_parent_ok",
			setup: func(_ *organizations.InMemoryBackend, _ string) {},
			create: func(b *organizations.InMemoryBackend, rootID string) error {
				_, err := b.CreateOrganizationalUnit(rootID, "Finance", nil)
				if err != nil {
					return err
				}
				_, err = b.CreateOrganizationalUnit(rootID, "Engineering", nil)

				return err
			},
			wantErr: false,
		},
		{
			name: "same_name_different_parent_ok",
			setup: func(b *organizations.InMemoryBackend, rootID string) {
				ou, err := b.CreateOrganizationalUnit(rootID, "ParentA", nil)
				if err != nil {
					panic(err)
				}
				_, err = b.CreateOrganizationalUnit(ou.ID, "Shared", nil)
				if err != nil {
					panic(err)
				}
			},
			create: func(b *organizations.InMemoryBackend, rootID string) error {
				ou, err := b.CreateOrganizationalUnit(rootID, "ParentB", nil)
				if err != nil {
					return err
				}
				_, err = b.CreateOrganizationalUnit(ou.ID, "Shared", nil)

				return err
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)
			tt.setup(b, rootID)

			err := tt.create(b, rootID)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestAudit2_UpdateOU_NameUniqueness verifies that UpdateOrganizationalUnit also enforces uniqueness.
func TestAudit2_UpdateOU_NameUniqueness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rename  string
		wantErr bool
	}{
		{
			name:    "rename_to_sibling_name_rejected",
			rename:  "Finance",
			wantErr: true,
		},
		{
			name:    "rename_to_unique_name_ok",
			rename:  "UniqueNewName",
			wantErr: false,
		},
		{
			name:    "rename_to_same_name_ok",
			rename:  "Engineering",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			// Create two sibling OUs.
			ouA, err := b.CreateOrganizationalUnit(rootID, "Finance", nil)
			require.NoError(t, err)

			ouB, err := b.CreateOrganizationalUnit(rootID, "Engineering", nil)
			require.NoError(t, err)
			_ = ouA

			// Try to rename Engineering.
			_, err = b.UpdateOrganizationalUnit(ouB.ID, tt.rename)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Item 8: Account email uniqueness
// ---------------------------------------------------------------------------

// TestAudit2_CreateAccount_EmailUniqueness verifies that duplicate email addresses are rejected.
func TestAudit2_CreateAccount_EmailUniqueness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		email1  string
		email2  string
		wantErr bool
	}{
		{
			name:    "duplicate_email_rejected",
			email1:  "same@example.com",
			email2:  "same@example.com",
			wantErr: true,
		},
		{
			name:    "unique_emails_ok",
			email1:  "first@example.com",
			email2:  "second@example.com",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			_, err := b.CreateAccount("Account1", tt.email1, "", "", nil)
			require.NoError(t, err, "first account creation should succeed")

			_, err = b.CreateAccount("Account2", tt.email2, "", "", nil)
			if tt.wantErr {
				require.Error(t, err, "duplicate email should fail")
			} else {
				require.NoError(t, err, "unique email should succeed")
			}
		})
	}
}

// TestAudit2_CreateAccount_EmailUniqueness_AfterReset verifies that Reset clears email index.
func TestAudit2_CreateAccount_EmailUniqueness_AfterReset(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, _, err := b.CreateOrganization("ALL")
	require.NoError(t, err)

	_, err = b.CreateAccount("Account1", "reuse@example.com", "", "", nil)
	require.NoError(t, err)

	b.Reset()

	_, _, err = b.CreateOrganization("ALL")
	require.NoError(t, err)

	// After reset, the same email should be usable again.
	_, err = b.CreateAccount("Account2", "reuse@example.com", "", "", nil)
	require.NoError(t, err, "email should be available after Reset()")
}

// ---------------------------------------------------------------------------
// Item 9: Handshake ARN contains lowercase action
// ---------------------------------------------------------------------------

// TestAudit2_HandshakeARN_LowercaseAction verifies that handshake ARNs use lowercase action.
func TestAudit2_HandshakeARN_LowercaseAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupFn      func(b *organizations.InMemoryBackend) (*organizations.Handshake, error)
		wantFragment string
	}{
		{
			name: "invite_action_lowercase",
			setupFn: func(b *organizations.InMemoryBackend) (*organizations.Handshake, error) {
				return b.InviteAccountToOrganization(organizations.HandshakeParty{
					ID:   "123456789012",
					Type: "ACCOUNT",
				}, "")
			},
			wantFragment: "/invite/",
		},
		{
			name: "enable_all_features_lowercase",
			setupFn: func(b *organizations.InMemoryBackend) (*organizations.Handshake, error) {
				return b.EnableAllFeatures()
			},
			wantFragment: "/enable_all_features/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			hs, err := tt.setupFn(b)
			require.NoError(t, err)

			assert.Contains(t, hs.ARN, tt.wantFragment,
				"handshake ARN %q should contain lowercase action %q", hs.ARN, tt.wantFragment)

			// Verify no uppercase in the action portion.
			assert.True(t, strings.ToLower(hs.ARN) == hs.ARN || !strings.Contains(hs.ARN, "/INVITE/"),
				"ARN should not contain uppercase action")
		})
	}
}

// ---------------------------------------------------------------------------
// Item 10: Lazy handshake expiration
// ---------------------------------------------------------------------------

// TestAudit2_HandshakeExpiration verifies that expired handshakes are transitioned to EXPIRED.
func TestAudit2_HandshakeExpiration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantState   string
		checkMethod string
		expired     bool
	}{
		{
			name:        "describe_transitions_expired",
			expired:     true,
			wantState:   "EXPIRED",
			checkMethod: "describe",
		},
		{
			name:        "list_for_org_transitions_expired",
			expired:     true,
			wantState:   "EXPIRED",
			checkMethod: "list_org",
		},
		{
			name:        "list_for_account_transitions_expired",
			expired:     true,
			wantState:   "EXPIRED",
			checkMethod: "list_acct",
		},
		{
			name:        "not_yet_expired_stays_open",
			expired:     false,
			wantState:   "OPEN",
			checkMethod: "describe",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			// Seed a handshake with expiration in the past or future.
			expTime := time.Now().Add(24 * time.Hour)
			if tt.expired {
				expTime = time.Now().Add(-1 * time.Hour)
			}

			hs := &organizations.Handshake{
				ID:                  "h-test001",
				Action:              "INVITE",
				State:               "OPEN",
				ExpirationTimestamp: expTime,
				RequestedTimestamp:  time.Now().Add(-2 * time.Hour),
			}
			b.AddHandshakeInternal(hs)

			var gotState string

			switch tt.checkMethod {
			case "describe":
				result, err := b.DescribeHandshake("h-test001")
				require.NoError(t, err)
				gotState = result.State
			case "list_org":
				results, err := b.ListHandshakesForOrganization("")
				require.NoError(t, err)
				for _, h := range results {
					if h.ID == "h-test001" {
						gotState = h.State
					}
				}
			case "list_acct":
				results, err := b.ListHandshakesForAccount("")
				require.NoError(t, err)
				for _, h := range results {
					if h.ID == "h-test001" {
						gotState = h.State
					}
				}
			}

			assert.Equal(t, tt.wantState, gotState)
		})
	}
}

// ---------------------------------------------------------------------------
// Item 11: AttachPolicy target validation
// ---------------------------------------------------------------------------

// TestAudit2_AttachPolicy_TargetValidation verifies that AttachPolicy validates the target.
func TestAudit2_AttachPolicy_TargetValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		targetFn func(b *organizations.InMemoryBackend, rootID string) string
		name     string
		wantErr  bool
	}{
		{
			name: "root_target_valid",
			targetFn: func(_ *organizations.InMemoryBackend, rootID string) string {
				return rootID
			},
			wantErr: false,
		},
		{
			name: "ou_target_valid",
			targetFn: func(b *organizations.InMemoryBackend, rootID string) string {
				ou, err := b.CreateOrganizationalUnit(rootID, "TestOU", nil)
				if err != nil {
					panic(err)
				}

				return ou.ID
			},
			wantErr: false,
		},
		{
			name: "account_target_valid",
			targetFn: func(b *organizations.InMemoryBackend, _ string) string {
				s, err := b.CreateAccount("target-acct", "target@example.com", "", "", nil)
				if err != nil {
					panic(err)
				}

				return s.AccountID
			},
			wantErr: false,
		},
		{
			name: "unknown_target_rejected",
			targetFn: func(_ *organizations.InMemoryBackend, _ string) string {
				return "ou-nonexistent-12345678"
			},
			wantErr: true,
		},
		{
			name: "random_string_rejected",
			targetFn: func(_ *organizations.InMemoryBackend, _ string) string {
				return "not-a-real-target"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			p, err := b.CreatePolicy("test-pol", "desc", `{"Version":"2012-10-17"}`, "SERVICE_CONTROL_POLICY", nil)
			require.NoError(t, err)

			targetID := tt.targetFn(b, rootID)

			err = b.AttachPolicy(p.PolicySummary.ID, targetID)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Item 12: Tag resource existence validation
// ---------------------------------------------------------------------------

// TestAudit2_TagResource_ExistenceValidation verifies that TagResource/UntagResource/
// ListTagsForResource validate that the resource exists.
func TestAudit2_TagResource_ExistenceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resourceFn func(b *organizations.InMemoryBackend, rootID string) string
		name       string
		wantErr    bool
	}{
		{
			name: "root_id_valid",
			resourceFn: func(_ *organizations.InMemoryBackend, rootID string) string {
				return rootID
			},
			wantErr: false,
		},
		{
			name: "account_id_valid",
			resourceFn: func(b *organizations.InMemoryBackend, _ string) string {
				s, err := b.CreateAccount("tag-acct", "tag@example.com", "", "", nil)
				if err != nil {
					panic(err)
				}

				return s.AccountID
			},
			wantErr: false,
		},
		{
			name: "ou_id_valid",
			resourceFn: func(b *organizations.InMemoryBackend, rootID string) string {
				ou, err := b.CreateOrganizationalUnit(rootID, "TagOU", nil)
				if err != nil {
					panic(err)
				}

				return ou.ID
			},
			wantErr: false,
		},
		{
			name: "policy_id_valid",
			resourceFn: func(b *organizations.InMemoryBackend, _ string) string {
				p, err := b.CreatePolicy("tag-pol", "", `{}`, "TAG_POLICY", nil)
				if err != nil {
					panic(err)
				}

				return p.PolicySummary.ID
			},
			wantErr: false,
		},
		{
			name: "unknown_id_rejected",
			resourceFn: func(_ *organizations.InMemoryBackend, _ string) string {
				return "ou-nonexistent-00000000"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)
			resourceID := tt.resourceFn(b, rootID)

			// Test TagResource.
			err := b.TagResource(resourceID, []organizations.Tag{{Key: "k", Value: "v"}})
			if tt.wantErr {
				require.Error(t, err, "TagResource should fail for unknown resource")
			} else {
				require.NoError(t, err, "TagResource should succeed")
			}

			// Test UntagResource.
			err = b.UntagResource(resourceID, []string{"k"})
			if tt.wantErr {
				require.Error(t, err, "UntagResource should fail for unknown resource")
			} else {
				require.NoError(t, err, "UntagResource should succeed")
			}

			// Test ListTagsForResource.
			_, err = b.ListTagsForResource(resourceID)
			if tt.wantErr {
				require.Error(t, err, "ListTagsForResource should fail for unknown resource")
			} else {
				require.NoError(t, err, "ListTagsForResource should succeed")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Item 13: RegisterDelegatedAdministrator service access check
// ---------------------------------------------------------------------------

// TestAudit2_RegisterDelegatedAdmin_ServiceAccessCheck verifies service access must be enabled.
func TestAudit2_RegisterDelegatedAdmin_ServiceAccessCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		servicePrincipal  string
		enableService     bool
		useManagementAcct bool
		wantErr           bool
	}{
		{
			name:             "service_access_not_enabled_fails",
			enableService:    false,
			servicePrincipal: "ssm.amazonaws.com",
			wantErr:          true,
		},
		{
			name:             "service_access_enabled_succeeds",
			enableService:    true,
			servicePrincipal: "ssm.amazonaws.com",
			wantErr:          false,
		},
		{
			name:              "management_account_rejected",
			enableService:     true,
			servicePrincipal:  "ssm.amazonaws.com",
			useManagementAcct: true,
			wantErr:           true,
		},
		{
			name:             "different_service_not_enabled",
			enableService:    true,
			servicePrincipal: "guardduty.amazonaws.com",
			wantErr:          true, // enabled ssm but not guardduty
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			if tt.enableService {
				err := b.EnableAWSServiceAccess("ssm.amazonaws.com")
				require.NoError(t, err)
			}

			var accountID string

			if tt.useManagementAcct {
				org, err := b.DescribeOrganization()
				require.NoError(t, err)
				accountID = org.MasterAccountID
			} else {
				s, err := b.CreateAccount("da-test", "da@example.com", "", "", nil)
				require.NoError(t, err)
				accountID = s.AccountID
			}

			err := b.RegisterDelegatedAdministrator(accountID, tt.servicePrincipal)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Item 14: InviteAccountToOrganization target validation
// ---------------------------------------------------------------------------

// TestAudit2_InviteAccount_PartyValidation verifies handshake party validation.
func TestAudit2_InviteAccount_PartyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		target  organizations.HandshakeParty
		wantErr bool
	}{
		{
			name:    "account_type_12_digits_ok",
			target:  organizations.HandshakeParty{ID: "123456789012", Type: "ACCOUNT"},
			wantErr: false,
		},
		{
			name:    "account_type_11_digits_rejected",
			target:  organizations.HandshakeParty{ID: "12345678901", Type: "ACCOUNT"},
			wantErr: true,
		},
		{
			name:    "account_type_13_digits_rejected",
			target:  organizations.HandshakeParty{ID: "1234567890123", Type: "ACCOUNT"},
			wantErr: true,
		},
		{
			name:    "email_type_with_at_ok",
			target:  organizations.HandshakeParty{ID: "user@example.com", Type: "EMAIL"},
			wantErr: false,
		},
		{
			name:    "email_type_without_at_rejected",
			target:  organizations.HandshakeParty{ID: "userexample.com", Type: "EMAIL"},
			wantErr: true,
		},
		{
			name:    "empty_id_rejected",
			target:  organizations.HandshakeParty{ID: "", Type: "ACCOUNT"},
			wantErr: true,
		},
		{
			name:    "invalid_type_rejected",
			target:  organizations.HandshakeParty{ID: "123456789012", Type: "ORGANIZATION"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			_, err := b.InviteAccountToOrganization(tt.target, "")
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Combined / integration tests
// ---------------------------------------------------------------------------

// TestAudit2_DescribeOrganization_AllTypesPresent verifies that all 8 policy types
// appear in AvailablePolicyTypes even after enabling/disabling some.
func TestAudit2_DescribeOrganization_AllTypesAfterEnableDisable(t *testing.T) {
	t.Parallel()

	b, rootID := newOrgBackend(t)

	// Enable two types.
	_, err := b.EnablePolicyType(rootID, "SERVICE_CONTROL_POLICY")
	require.NoError(t, err)

	_, err = b.EnablePolicyType(rootID, "TAG_POLICY")
	require.NoError(t, err)

	// Disable one.
	_, err = b.DisablePolicyType(rootID, "SERVICE_CONTROL_POLICY")
	require.NoError(t, err)

	org, err := b.DescribeOrganization()
	require.NoError(t, err)

	assert.Len(t, org.AvailablePolicyTypes, 8)

	statusMap := make(map[string]string)
	for _, pt := range org.AvailablePolicyTypes {
		statusMap[pt.Type] = pt.Status
	}

	assert.Equal(t, "DISABLED", statusMap["SERVICE_CONTROL_POLICY"])
	assert.Equal(t, "ENABLED", statusMap["TAG_POLICY"])
	assert.Equal(t, "DISABLED", statusMap["RESOURCE_CONTROL_POLICY"])
	assert.Equal(t, "DISABLED", statusMap["SECURITYHUB_POLICY"])
}

// TestAudit2_OUDepth_ViaHandler tests OU depth limit enforcement through the handler.
func TestAudit2_OUDepth_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get root ID.
	rec = doRequest(t, h, "ListRoots", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var rootsResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&rootsResp))
	roots := rootsResp["Roots"].([]any)
	rootID := roots[0].(map[string]any)["Id"].(string)

	// Create 5 levels of OUs (should succeed).
	parentID := rootID

	for d := 1; d <= 5; d++ {
		rec = doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
			"ParentId": parentID,
			"Name":     fmt.Sprintf("OU-Level-%d", d),
		})
		require.Equal(t, http.StatusOK, rec.Code, "depth %d should succeed", d)

		var ouResp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&ouResp))
		parentID = ouResp["OrganizationalUnit"].(map[string]any)["Id"].(string)
	}

	// Attempt to create level 6 (should fail).
	rec = doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
		"ParentId": parentID,
		"Name":     "OU-Level-6",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "depth 6 should be rejected")
}

// TestAudit2_EmailUniqueness_ViaHandler tests email uniqueness through the handler.
func TestAudit2_EmailUniqueness_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	// First account with email.
	rec = doRequest(t, h, "CreateAccount", map[string]any{
		"AccountName": "AccountA",
		"Email":       "shared@example.com",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Second account with same email should fail.
	rec = doRequest(t, h, "CreateAccount", map[string]any{
		"AccountName": "AccountB",
		"Email":       "shared@example.com",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestAudit2_AttachPolicy_UnknownTarget_ViaHandler tests AttachPolicy target validation via handler.
func TestAudit2_AttachPolicy_UnknownTarget_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreatePolicy", map[string]any{
		"Name":    "test-pol",
		"Type":    "SERVICE_CONTROL_POLICY",
		"Content": `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var polResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&polResp))
	policyID := polResp["Policy"].(map[string]any)["PolicySummary"].(map[string]any)["Id"].(string)

	// Attempt to attach to a non-existent target.
	rec = doRequest(t, h, "AttachPolicy", map[string]any{
		"PolicyId": policyID,
		"TargetId": "ou-nonexistent-12345678",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestAudit2_TagResource_InvalidResource_ViaHandler tests tag resource validation via handler.
func TestAudit2_TagResource_InvalidResource_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Tag a non-existent resource.
	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceId": "ou-nonexistent-12345678",
		"Tags":       []map[string]string{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestAudit2_InviteAccount_ViaHandler tests party validation via handler.
func TestAudit2_InviteAccount_ViaHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target     map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "valid_account_id",
			target:     map[string]any{"Id": "123456789012", "Type": "ACCOUNT"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_account_id_short",
			target:     map[string]any{"Id": "12345", "Type": "ACCOUNT"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "valid_email",
			target:     map[string]any{"Id": "user@example.com", "Type": "EMAIL"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_email_no_at",
			target:     map[string]any{"Id": "notanemail", "Type": "EMAIL"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, "InviteAccountToOrganization", map[string]any{
				"Target": tt.target,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit2_RegisterDelegatedAdmin_ViaHandler tests service access requirement via handler.
func TestAudit2_RegisterDelegatedAdmin_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateAccount", map[string]any{
		"AccountName": "delegate-acct",
		"Email":       "da@example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	statusObj := createResp["CreateAccountStatus"].(map[string]any)
	accountID := statusObj["AccountId"].(string)

	// Attempt to register without enabling service access - should fail.
	rec = doRequest(t, h, "RegisterDelegatedAdministrator", map[string]any{
		"AccountId":        accountID,
		"ServicePrincipal": "ssm.amazonaws.com",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Enable service access.
	rec = doRequest(t, h, "EnableAWSServiceAccess", map[string]any{
		"ServicePrincipal": "ssm.amazonaws.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Now registration should succeed.
	rec = doRequest(t, h, "RegisterDelegatedAdministrator", map[string]any{
		"AccountId":        accountID,
		"ServicePrincipal": "ssm.amazonaws.com",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestAudit2_OUNameUniqueness_ViaHandler tests OU name uniqueness via handler.
func TestAudit2_OUNameUniqueness_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get root ID.
	rec = doRequest(t, h, "ListRoots", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var rootsResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&rootsResp))
	roots := rootsResp["Roots"].([]any)
	rootID := roots[0].(map[string]any)["Id"].(string)

	// Create OU.
	rec = doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
		"ParentId": rootID,
		"Name":     "Engineering",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Attempt to create another OU with the same name.
	rec = doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
		"ParentId": rootID,
		"Name":     "Engineering",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Create with a different name - should succeed.
	rec = doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
		"ParentId": rootID,
		"Name":     "Finance",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestAudit2_HandshakeExpiration_ViaHandler tests handshake expiration via handler.
func TestAudit2_HandshakeExpiration_ViaHandler(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)
	h := organizations.NewHandler(b)

	// Seed an expired handshake.
	expiredHandshake := &organizations.Handshake{
		ID:                  "h-expired01",
		Action:              "INVITE",
		State:               "OPEN",
		ExpirationTimestamp: time.Now().Add(-2 * time.Hour),
		RequestedTimestamp:  time.Now().Add(-3 * time.Hour),
	}
	b.AddHandshakeInternal(expiredHandshake)

	// DescribeHandshake should show EXPIRED.
	rec := doRequest(t, h, "DescribeHandshake", map[string]any{
		"HandshakeId": "h-expired01",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	handshake := resp["Handshake"].(map[string]any)
	assert.Equal(t, "EXPIRED", handshake["State"].(string))
}
