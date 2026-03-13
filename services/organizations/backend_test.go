package organizations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// newOrgBackend creates a backend with a pre-created organization for convenience.
func newOrgBackend(t *testing.T) (*organizations.InMemoryBackend, string) {
	t.Helper()

	b := newTestBackend()

	_, root, err := b.CreateOrganization("ALL")
	require.NoError(t, err)

	return b, root.ID
}

// TestBackend_DescribeCreateAccountStatus tests the DescribeCreateAccountStatus operation.
func TestBackend_DescribeCreateAccountStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		requestID string
		wantErr   bool
	}{
		{
			name:    "found",
			wantErr: false,
		},
		{
			name:      "not_found",
			requestID: "car-doesnotexist",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			requestID := tt.requestID

			if requestID == "" {
				status, err := b.CreateAccount("test-account", "test@example.com", nil)
				require.NoError(t, err)
				requestID = status.ID
			}

			desc, err := b.DescribeCreateAccountStatus(requestID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, desc.AccountID)
		})
	}
}

// TestBackend_RemoveAccountFromOrganization tests removing an account.
func TestBackend_RemoveAccountFromOrganization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		wantErr   bool
	}{
		{
			name:    "removes_account",
			wantErr: false,
		},
		{
			name:      "not_found",
			accountID: "999999999999",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			accountID := tt.accountID

			if accountID == "" {
				status, err := b.CreateAccount("test-account", "test@example.com", nil)
				require.NoError(t, err)
				accountID = status.AccountID
			}

			err := b.RemoveAccountFromOrganization(accountID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			// Account should not be in list any more.
			_, descErr := b.DescribeAccount(accountID)
			require.Error(t, descErr, "removed account should no longer be describable")
		})
	}
}

// TestBackend_MoveAccount tests moving an account between OUs.
func TestBackend_MoveAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name: "moves_account",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			// Create target OU.
			ou, err := b.CreateOrganizationalUnit(rootID, "target-ou", nil)
			require.NoError(t, err)

			// Create account (placed in root by default).
			status, err := b.CreateAccount("move-account", "move@example.com", nil)
			require.NoError(t, err)

			// Move account from root to OU.
			err = b.MoveAccount(status.AccountID, rootID, ou.ID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			// Verify account is in the target OU.
			parents, parentsErr := b.ListParents(status.AccountID)
			require.NoError(t, parentsErr)
			require.Len(t, parents, 1)
			assert.Equal(t, ou.ID, parents[0].ID)
		})
	}
}

// TestBackend_ListAccountsForParent tests listing accounts under a parent.
func TestBackend_ListAccountsForParent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		expectCount int
	}{
		{
			name:        "lists_accounts_in_root",
			expectCount: 2, // management account + created account
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			// Create an account (goes to root).
			_, err := b.CreateAccount("child-account", "child@example.com", nil)
			require.NoError(t, err)

			accts, err := b.ListAccountsForParent(rootID)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(accts), tt.expectCount)
		})
	}
}

// TestBackend_ListChildren tests listing children of a parent.
func TestBackend_ListChildren(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		childType string
		wantErr   bool
	}{
		{
			name:      "lists_account_children",
			childType: "ACCOUNT",
		},
		{
			name:      "lists_ou_children",
			childType: "ORGANIZATIONAL_UNIT",
		},
		{
			name:      "invalid_child_type",
			childType: "INVALID",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			_, err := b.CreateAccount("child-account", "child@example.com", nil)
			require.NoError(t, err)

			_, err = b.CreateOrganizationalUnit(rootID, "child-ou", nil)
			require.NoError(t, err)

			children, err := b.ListChildren(rootID, tt.childType)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, children)
		})
	}
}

// TestBackend_PolicyAttachment tests attaching and detaching policies.
func TestBackend_PolicyAttachment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name: "attach_and_detach",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			// Create a policy.
			policy, err := b.CreatePolicy(
				"test-scp",
				"test",
				`{"Version":"2012-10-17","Statement":[]}`,
				"SERVICE_CONTROL_POLICY",
				nil,
			)
			require.NoError(t, err)

			// AttachPolicy.
			err = b.AttachPolicy(policy.PolicySummary.ID, rootID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			// Duplicate attach should fail.
			err = b.AttachPolicy(policy.PolicySummary.ID, rootID)
			require.Error(t, err)

			// ListPoliciesForTarget.
			policies, err := b.ListPoliciesForTarget(rootID, "SERVICE_CONTROL_POLICY")
			require.NoError(t, err)

			found := false

			for _, p := range policies {
				if p.PolicySummary.ID == policy.PolicySummary.ID {
					found = true

					break
				}
			}

			assert.True(t, found, "attached policy should appear in ListPoliciesForTarget")

			// ListTargetsForPolicy.
			targets, err := b.ListTargetsForPolicy(policy.PolicySummary.ID)
			require.NoError(t, err)
			assert.NotEmpty(t, targets)

			// DetachPolicy.
			err = b.DetachPolicy(policy.PolicySummary.ID, rootID)
			require.NoError(t, err)

			// Double detach should fail.
			err = b.DetachPolicy(policy.PolicySummary.ID, rootID)
			require.Error(t, err)
		})
	}
}

// TestBackend_UpdatePolicy tests updating a policy.
func TestBackend_UpdatePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name: "updates_policy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			policy, err := b.CreatePolicy("original", "desc", `{}`, "SERVICE_CONTROL_POLICY", nil)
			require.NoError(t, err)

			updated, err := b.UpdatePolicy(policy.PolicySummary.ID, "updated-name", "updated-desc", `{"Version":"2012-10-17"}`)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "updated-name", updated.PolicySummary.Name)
		})
	}
}

// TestBackend_DisablePolicyType tests disabling a policy type.
func TestBackend_DisablePolicyType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name: "enable_then_disable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			// Enable first.
			_, err := b.EnablePolicyType(rootID, "SERVICE_CONTROL_POLICY")
			require.NoError(t, err)

			// Now disable.
			_, err = b.DisablePolicyType(rootID, "SERVICE_CONTROL_POLICY")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestBackend_ServiceAccess tests enabling/disabling AWS service access.
func TestBackend_ServiceAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		service string
	}{
		{
			name:    "enable_disable_service_access",
			service: "ssm.amazonaws.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			// Enable service access.
			err := b.EnableAWSServiceAccess(tt.service)
			require.NoError(t, err)

			// List service access.
			services, err := b.ListAWSServiceAccessForOrganization()
			require.NoError(t, err)

			found := false

			for _, s := range services {
				if s.ServicePrincipal == tt.service {
					found = true

					break
				}
			}

			assert.True(t, found, "enabled service should appear in list")

			// Disable service access.
			err = b.DisableAWSServiceAccess(tt.service)
			require.NoError(t, err)

			// List again - should be gone.
			services, err = b.ListAWSServiceAccessForOrganization()
			require.NoError(t, err)

			for _, s := range services {
				assert.NotEqual(t, tt.service, s.ServicePrincipal)
			}
		})
	}
}

// TestBackend_DelegatedAdministrators tests delegated admin lifecycle.
func TestBackend_DelegatedAdministrators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		service string
	}{
		{
			name:    "register_and_deregister",
			service: "ssm.amazonaws.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			// Create an account to register.
			status, err := b.CreateAccount("delegate-account", "delegate@example.com", nil)
			require.NoError(t, err)

			accountID := status.AccountID

			// RegisterDelegatedAdministrator.
			err = b.RegisterDelegatedAdministrator(accountID, tt.service)
			require.NoError(t, err)

			// Duplicate register should fail.
			err = b.RegisterDelegatedAdministrator(accountID, tt.service)
			require.Error(t, err)

			// ListDelegatedAdministrators.
			admins, err := b.ListDelegatedAdministrators(tt.service)
			require.NoError(t, err)

			found := false

			for _, a := range admins {
				if a.AccountID == accountID {
					found = true

					break
				}
			}

			assert.True(t, found, "registered admin should appear in list")

			// DeregisterDelegatedAdministrator.
			err = b.DeregisterDelegatedAdministrator(accountID, tt.service)
			require.NoError(t, err)

			// Deregister again should fail.
			err = b.DeregisterDelegatedAdministrator(accountID, tt.service)
			require.Error(t, err)
		})
	}
}

// TestBackend_EnsureOrgExists tests the EnsureOrgExists helper.
func TestBackend_EnsureOrgExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		hasOrg  bool
		wantErr bool
	}{
		{
			name:   "org_exists",
			hasOrg: true,
		},
		{
			name:    "no_org",
			hasOrg:  false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.hasOrg {
				_, _, err := b.CreateOrganization("ALL")
				require.NoError(t, err)
			}

			err := b.EnsureOrgExists()

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
