package grafana_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/stretchr/testify/require"
)

func TestAssociateAndDisassociateLicense(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	assoc, err := client.AssociateLicense(t.Context(), &grafanasdk.AssociateLicenseInput{
		WorkspaceId:  aws.String(id),
		LicenseType:  types.LicenseTypeEnterprise,
		GrafanaToken: aws.String("glabs-token-123"),
	})
	require.NoError(t, err)
	require.Equal(t, types.LicenseTypeEnterprise, assoc.Workspace.LicenseType)
	require.Equal(t, types.WorkspaceStatusUpgrading, assoc.Workspace.Status)
	require.NotNil(t, assoc.Workspace.LicenseExpiration)
	require.Equal(t, "glabs-token-123", aws.ToString(assoc.Workspace.GrafanaToken))

	time.Sleep(workspaceTransitionWait)

	desc, err := client.DescribeWorkspace(t.Context(), &grafanasdk.DescribeWorkspaceInput{WorkspaceId: aws.String(id)})
	require.NoError(t, err)
	require.Equal(t, types.WorkspaceStatusActive, desc.Workspace.Status)
	require.Equal(t, types.LicenseTypeEnterprise, desc.Workspace.LicenseType)

	disassoc, err := client.DisassociateLicense(t.Context(), &grafanasdk.DisassociateLicenseInput{
		WorkspaceId: aws.String(id),
		LicenseType: types.LicenseTypeEnterprise,
	})
	require.NoError(t, err)
	require.Empty(t, disassoc.Workspace.LicenseType)
}

func TestAssociateLicense_FreeTrialRejected(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	_, err := client.AssociateLicense(t.Context(), &grafanasdk.AssociateLicenseInput{
		WorkspaceId: aws.String(id),
		LicenseType: types.LicenseTypeEnterpriseFreeTrial,
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve,
		"Amazon Managed Grafana workspaces no longer support Grafana Enterprise free trials")
}

// TestDisassociateLicense_NoneAssociated_Idempotent proves disassociating a
// license type the workspace never had is a no-op success, not an error:
// DisassociateLicense's own deserializeOpErrorDisassociateLicense in the
// real SDK does not recognize ConflictException for this operation, so
// there is no wire-accurate error shape for "nothing to remove" -- see
// license.go's DisassociateLicense doc comment.
func TestDisassociateLicense_NoneAssociated_Idempotent(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	out, err := client.DisassociateLicense(t.Context(), &grafanasdk.DisassociateLicenseInput{
		WorkspaceId: aws.String(id),
		LicenseType: types.LicenseTypeEnterprise,
	})
	require.NoError(t, err)
	require.Empty(t, out.Workspace.LicenseType)
}
