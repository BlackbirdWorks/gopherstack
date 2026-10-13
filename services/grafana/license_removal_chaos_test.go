package grafana_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/grafana"
)

type chaosSiblings struct{ store *chaos.FaultStore }

func (chaosSiblings) GetIAMHandler() service.Registerable           { return nil }
func (chaosSiblings) GetEC2Handler() service.Registerable           { return nil }
func (chaosSiblings) GetOrganizationsHandler() service.Registerable { return nil }
func (chaosSiblings) GetSsoAdminHandler() service.Registerable      { return nil }
func (chaosSiblings) GetIdentityStoreHandler() service.Registerable { return nil }
func (c chaosSiblings) GetFaultStore() *chaos.FaultStore            { return c.store }

func TestDisassociateLicenseChaos(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		rule        *chaos.FaultRule
		wantStatus  types.WorkspaceStatus
		wantLicense types.LicenseType
	}{
		{name: "no_rule", wantStatus: types.WorkspaceStatusActive},
		{
			name: "failure_rule",
			rule: &chaos.FaultRule{
				Service:   "grafana",
				Operation: "WorkspaceTransition",
				Error:     &chaos.FaultError{Code: "X", StatusCode: 500},
			},
			wantStatus:  types.WorkspaceStatusLicenseRemovalFailed,
			wantLicense: types.LicenseTypeEnterprise,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := grafana.NewInMemoryBackend(t.Context(), "000000000000", rtTestRegion)
			t.Cleanup(backend.Close)

			store := chaos.NewFaultStore()
			backend.SetAppConfig(chaosSiblings{store: store})

			client := newRoundTripClient(t, grafana.NewHandler(backend))
			id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

			_, err := client.AssociateLicense(t.Context(), &grafanasdk.AssociateLicenseInput{
				WorkspaceId: aws.String(id), LicenseType: types.LicenseTypeEnterprise,
			})
			require.NoError(t, err)
			waitForWorkspaceActive(t, client, id)

			if tt.rule != nil {
				store.SetRules([]chaos.FaultRule{*tt.rule})
			}

			out, err := client.DisassociateLicense(t.Context(), &grafanasdk.DisassociateLicenseInput{
				WorkspaceId: aws.String(id), LicenseType: types.LicenseTypeEnterprise,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantLicense, out.Workspace.LicenseType)

			desc, err := client.DescribeWorkspace(
				t.Context(),
				&grafanasdk.DescribeWorkspaceInput{WorkspaceId: aws.String(id)},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, desc.Workspace.Status)
		})
	}
}
