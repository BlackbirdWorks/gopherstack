package cloudformation_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/blackbirdworks/gopherstack/services/organizations"
)

func TestImportStacksToStackSet_OrganizationalUnitStamping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		permission  string
		wantStatus  int
		inOU        bool
		activate    bool
		wantStamped bool
	}{
		{
			name:        "member_account",
			permission:  "SERVICE_MANAGED",
			inOU:        true,
			activate:    true,
			wantStatus:  200,
			wantStamped: true,
		},
		{name: "outside_account", permission: "SERVICE_MANAGED", inOU: false, activate: true, wantStatus: 400},
		{name: "self_managed", permission: "SELF_MANAGED", inOU: true, activate: true, wantStatus: 400},
		{name: "no_trusted_access", permission: "SERVICE_MANAGED", inOU: true, activate: false, wantStatus: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			orgBackend := organizations.NewInMemoryBackend("000000000000", "us-east-1")
			_, root, err := orgBackend.CreateOrganization("ALL")
			require.NoError(t, err)

			ou, err := orgBackend.CreateOrganizationalUnit(root.ID, "Workloads", nil)
			require.NoError(t, err)

			status, err := orgBackend.CreateAccount("a@example.com", "a", "OrganizationAccountAccessRole", "ALLOW", nil)
			require.NoError(t, err)

			if tt.inOU {
				require.NoError(t, orgBackend.MoveAccount(status.AccountID, root.ID, ou.ID))
			}

			cfnBackend := cloudformation.NewInMemoryBackendWithConfig(
				"000000000000", "us-east-1", cloudformation.NewResourceCreator(nil),
			)
			cfnBackend.SetOrganizationsDirectory(orgBackend)
			h := cloudformation.NewHandler(cfnBackend)

			if tt.activate {
				postForm(t, h, url.Values{"Action": {"ActivateOrganizationsAccess"}}.Encode())
			}

			postForm(t, h, url.Values{
				"Action": {"CreateStackSet"}, "StackSetName": {"imp"}, "TemplateBody": {simpleTemplate},
				"PermissionModel": {tt.permission},
			}.Encode())

			stackID := "arn:aws:cloudformation:us-east-1:" + status.AccountID + ":stack/s/abc"
			rec := postForm(t, h, url.Values{
				"Action": {"ImportStacksToStackSet"}, "StackSetName": {"imp"},
				"StackIds.member.1": {stackID}, "OrganizationalUnitIds.member.1": {ou.ID},
			}.Encode())
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if !tt.wantStamped {
				return
			}

			rec = postForm(t, h, url.Values{"Action": {"ListStackInstances"}, "StackSetName": {"imp"}}.Encode())
			assert.Contains(t, rec.Body.String(), "<OrganizationalUnitId>"+ou.ID+"</OrganizationalUnitId>")
		})
	}
}
