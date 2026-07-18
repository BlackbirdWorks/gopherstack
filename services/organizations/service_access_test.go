package organizations_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestServiceAccess_MultiService tests enabling multiple service principals.
func TestServiceAccess_MultiService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		disable  string
		services []string
		wantLeft int
	}{
		{
			name:     "enable_one",
			services: []string{"cloudtrail.amazonaws.com"},
			wantLeft: 1,
		},
		{
			name:     "enable_three",
			services: []string{"cloudtrail.amazonaws.com", "ssm.amazonaws.com", "guardduty.amazonaws.com"},
			wantLeft: 3,
		},
		{
			name:     "enable_three_disable_one",
			services: []string{"cloudtrail.amazonaws.com", "ssm.amazonaws.com", "guardduty.amazonaws.com"},
			disable:  "ssm.amazonaws.com",
			wantLeft: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			for _, svc := range tt.services {
				require.NoError(t, b.EnableAWSServiceAccess(svc))
			}

			if tt.disable != "" {
				require.NoError(t, b.DisableAWSServiceAccess(tt.disable))
			}

			enabled, err := b.ListAWSServiceAccessForOrganization()
			require.NoError(t, err)
			assert.Len(t, enabled, tt.wantLeft)

			if tt.disable != "" {
				for _, e := range enabled {
					assert.NotEqual(t, tt.disable, e.ServicePrincipal)
				}
			}
		})
	}
}

// TestServiceAccess_Idempotent tests enabling same service twice is idempotent.
func TestServiceAccess_Idempotent(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	svc := "cloudtrail.amazonaws.com"
	require.NoError(t, b.EnableAWSServiceAccess(svc))
	require.NoError(t, b.EnableAWSServiceAccess(svc)) // second enable overwrites timestamp

	enabled, err := b.ListAWSServiceAccessForOrganization()
	require.NoError(t, err)
	assert.Len(t, enabled, 1, "enabling same service twice should result in one entry")
}

// TestServiceAccess_SortedOutput tests that ListAWSServiceAccessForOrganization
// returns entries sorted by ServicePrincipal.
func TestServiceAccess_SortedOutput(t *testing.T) {
	t.Parallel()

	b, _ := newOrgBackend(t)

	services := []string{
		"ssm.amazonaws.com",
		"cloudtrail.amazonaws.com",
		"guardduty.amazonaws.com",
		"access-analyzer.amazonaws.com",
	}

	for _, svc := range services {
		require.NoError(t, b.EnableAWSServiceAccess(svc))
	}

	enabled, err := b.ListAWSServiceAccessForOrganization()
	require.NoError(t, err)
	require.Len(t, enabled, len(services))

	for i := 1; i < len(enabled); i++ {
		assert.LessOrEqual(t, enabled[i-1].ServicePrincipal, enabled[i].ServicePrincipal,
			"output must be sorted by ServicePrincipal")
	}
}

// TestServiceAccess_Handler tests the HTTP handler for service access operations.
// TestServiceAccess_Handler tests enable → disable → list as a sequential flow.
func TestServiceAccess_Handler(t *testing.T) {
	t.Parallel()

	const svc = "cloudtrail.amazonaws.com"

	h, _ := newHandlerWithOrg(t)

	enableRec := doRequest(t, h, "EnableAWSServiceAccess", map[string]any{"ServicePrincipal": svc})
	require.Equal(t, http.StatusOK, enableRec.Code)

	disableRec := doRequest(t, h, "DisableAWSServiceAccess", map[string]any{"ServicePrincipal": svc})
	require.Equal(t, http.StatusOK, disableRec.Code)

	listRec := doRequest(t, h, "ListAWSServiceAccessForOrganization", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&resp))

	principals := resp["EnabledServicePrincipals"].([]any)
	assert.Empty(t, principals, "disabled service should not appear")
}

// ---------------------------------------------------------------------------
// DelegatedAdministrator
// ---------------------------------------------------------------------------

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

// TestListAWSServiceAccessForOrganization_Sorted verifies sorted output by ServicePrincipal.
func TestListAWSServiceAccessForOrganization_Sorted(t *testing.T) {
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

			b := newTestBackend()
			createOrgOn(t, b)

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
