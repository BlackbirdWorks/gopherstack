package organizations_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/organizations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			status, err := b.CreateAccount("delegate-account", "delegate@example.com", "", "", nil)
			require.NoError(t, err)

			accountID := status.AccountID

			// Enable service access first (required by new validation).
			err = b.EnableAWSServiceAccess(tt.service)
			require.NoError(t, err)

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

func TestDelegatedServices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateOrganization", map[string]any{"featureSet": "ALL"})

	// ListDelegatedServicesForAccount
	rec := doRequest(t, h, "ListDelegatedServicesForAccount", map[string]any{
		"accountId": "123456789012",
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
}

// TestDelegatedAdmin_MultiService tests registering one account across multiple services.
func TestDelegatedAdmin_MultiService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filter   string
		services []string
		wantLen  int
	}{
		{
			name:     "single_service",
			services: []string{"ssm.amazonaws.com"},
			filter:   "ssm.amazonaws.com",
			wantLen:  1,
		},
		{
			name: "multi_service_no_filter",
			services: []string{
				"ssm.amazonaws.com",
				"cloudtrail.amazonaws.com",
				"guardduty.amazonaws.com",
			},
			filter:  "",
			wantLen: 3,
		},
		{
			name: "multi_service_with_filter",
			services: []string{
				"ssm.amazonaws.com",
				"cloudtrail.amazonaws.com",
			},
			filter:  "ssm.amazonaws.com",
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			status, err := b.CreateAccount("admin-account", "admin@example.com", "", "", nil)
			require.NoError(t, err)

			for _, svc := range tt.services {
				require.NoError(t, b.EnableAWSServiceAccess(svc))
				require.NoError(t, b.RegisterDelegatedAdministrator(status.AccountID, svc))
			}

			admins, err := b.ListDelegatedAdministrators(tt.filter)
			require.NoError(t, err)
			assert.Len(t, admins, tt.wantLen)
		})
	}
}

// TestDelegatedAdmin_ListServices tests ListDelegatedServicesForAccount.
func TestDelegatedAdmin_ListServices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		registerSvcs []string
		wantSvcCount int
		wantSorted   bool
	}{
		{
			name:         "no_delegations",
			registerSvcs: nil,
			wantSvcCount: 0,
		},
		{
			name:         "one_delegation",
			registerSvcs: []string{"ssm.amazonaws.com"},
			wantSvcCount: 1,
		},
		{
			name: "multiple_delegations_sorted",
			registerSvcs: []string{
				"ssm.amazonaws.com",
				"cloudtrail.amazonaws.com",
				"guardduty.amazonaws.com",
			},
			wantSvcCount: 3,
			wantSorted:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			status, err := b.CreateAccount("svc-account", "svc@example.com", "", "", nil)
			require.NoError(t, err)

			for _, svc := range tt.registerSvcs {
				require.NoError(t, b.EnableAWSServiceAccess(svc))
				require.NoError(t, b.RegisterDelegatedAdministrator(status.AccountID, svc))
			}

			svcs, err := b.ListDelegatedServicesForAccount(status.AccountID)
			require.NoError(t, err)
			assert.Len(t, svcs, tt.wantSvcCount)

			if tt.wantSorted && len(svcs) > 1 {
				for i := 1; i < len(svcs); i++ {
					assert.LessOrEqual(t, svcs[i-1].ServicePrincipal, svcs[i].ServicePrincipal,
						"services must be sorted")
				}
			}
		})
	}
}

// TestDelegatedAdmin_ErrorCases tests error conditions.
func TestDelegatedAdmin_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *organizations.InMemoryBackend, accountID string)
		name      string
		op        string // "register", "deregister"
		accountID string
		service   string
		wantErr   bool
	}{
		{
			name:      "register_unknown_account",
			op:        "register",
			accountID: "999999999999",
			service:   "ssm.amazonaws.com",
			wantErr:   true,
		},
		{
			name:    "register_duplicate_fails",
			op:      "register",
			service: "ssm.amazonaws.com",
			setup: func(b *organizations.InMemoryBackend, accountID string) {
				require.NoError(t, b.EnableAWSServiceAccess("ssm.amazonaws.com"))
				require.NoError(t, b.RegisterDelegatedAdministrator(accountID, "ssm.amazonaws.com"))
			},
			wantErr: true,
		},
		{
			name:    "deregister_not_registered_fails",
			op:      "deregister",
			service: "ssm.amazonaws.com",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			accountID := tt.accountID
			if accountID == "" {
				status, err := b.CreateAccount("test-acct", "test@example.com", "", "", nil)
				require.NoError(t, err)
				accountID = status.AccountID
			}

			if tt.setup != nil {
				tt.setup(b, accountID)
			}

			var err error
			switch tt.op {
			case "register":
				// Enable service access so the check passes (account-not-found and duplicate tests still get correct errors).
				_ = b.EnableAWSServiceAccess(tt.service)
				err = b.RegisterDelegatedAdministrator(accountID, tt.service)
			case "deregister":
				err = b.DeregisterDelegatedAdministrator(accountID, tt.service)
			}

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ResourcePolicy
// ---------------------------------------------------------------------------

// TestRegisterDelegatedAdmin_ServiceAccessCheck verifies service access must be enabled.
func TestRegisterDelegatedAdmin_ServiceAccessCheck(t *testing.T) {
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

// TestRegisterDelegatedAdmin_ViaHandler tests service access requirement via handler.
func TestRegisterDelegatedAdmin_ViaHandler(t *testing.T) {
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

			// Enable service access first (required by RegisterDelegatedAdministrator).
			rec := doRequest(t, h, "EnableAWSServiceAccess", map[string]any{
				"ServicePrincipal": "ssm.amazonaws.com",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// RegisterDelegatedAdministrator.
			rec = doRequest(t, h, "RegisterDelegatedAdministrator", map[string]any{
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

// TestListDelegatedAdministrators_Sorted verifies sorted output by AccountID.
func TestListDelegatedAdministrators_Sorted(t *testing.T) {
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

			b := newTestBackend()
			createOrgOn(t, b)

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
