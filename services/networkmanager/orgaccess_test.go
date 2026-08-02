package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/stretchr/testify/require"
)

// TestRoundTrip_OrganizationServiceAccess drives family V. This is also the
// only op in the whole 95-op surface with zero typed exception cases
// (ListOrganizationServiceAccessStatus, PARITY.md) -- confirmed here by the
// call simply succeeding with no special error-handling path needed.
func TestRoundTrip_OrganizationServiceAccess(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	initial, err := client.ListOrganizationServiceAccessStatus(
		ctx,
		&networkmanagersdk.ListOrganizationServiceAccessStatusInput{},
	)
	require.NoError(t, err)
	require.Equal(t, "DISABLED", aws.ToString(initial.OrganizationStatus.OrganizationAwsServiceAccessStatus))

	enabled, err := client.StartOrganizationServiceAccessUpdate(
		ctx,
		&networkmanagersdk.StartOrganizationServiceAccessUpdateInput{
			Action: aws.String("ENABLE"),
		},
	)
	require.NoError(t, err)
	require.Equal(t, "ENABLED", aws.ToString(enabled.OrganizationStatus.OrganizationAwsServiceAccessStatus))
	require.NotEmpty(t, aws.ToString(enabled.OrganizationStatus.OrganizationId))

	status, err := client.ListOrganizationServiceAccessStatus(
		ctx,
		&networkmanagersdk.ListOrganizationServiceAccessStatusInput{},
	)
	require.NoError(t, err)
	require.Equal(t, "ENABLED", aws.ToString(status.OrganizationStatus.OrganizationAwsServiceAccessStatus))
	require.Equal(
		t,
		aws.ToString(enabled.OrganizationStatus.OrganizationId),
		aws.ToString(status.OrganizationStatus.OrganizationId),
	)

	disabled, err := client.StartOrganizationServiceAccessUpdate(
		ctx,
		&networkmanagersdk.StartOrganizationServiceAccessUpdateInput{
			Action: aws.String("DISABLE"),
		},
	)
	require.NoError(t, err)
	require.Equal(t, "DISABLED", aws.ToString(disabled.OrganizationStatus.OrganizationAwsServiceAccessStatus))
}
