package ssoadmin_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssoadminsdk "github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	ssoadmintypes "github.com/aws/aws-sdk-go-v2/service/ssoadmin/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// TestListApplications_PortalOptions_RealClient covers gopherstack-21my:
// ListApplicationsOutput's per-item Application type (ssoadmin@v1.43.1
// api_op_ListApplications.go) declares PortalOptions, the same field
// DescribeApplicationOutput already returns, but the handler's
// applicationView (handler_applications.go) never carried it, so a real
// client's PortalOptions was always nil on every entry even though the
// backend fully populates it from CreateApplication.
func TestListApplications_PortalOptions_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssoadmin.NewInMemoryBackend("000000000000", tagsRTRegion)
	client := newTestSSOAdminClient(t, ssoadmin.NewHandler(backend))
	ctx := t.Context()

	inst, err := client.CreateInstance(ctx, &ssoadminsdk.CreateInstanceInput{Name: aws.String("s21my-inst")})
	require.NoError(t, err)

	created, err := client.CreateApplication(ctx, &ssoadminsdk.CreateApplicationInput{
		InstanceArn:            inst.InstanceArn,
		Name:                   aws.String("s21my-app"),
		ApplicationProviderArn: aws.String("arn:aws:sso::aws:applicationProvider/custom"),
		PortalOptions: &ssoadmintypes.PortalOptions{
			Visibility: ssoadmintypes.ApplicationVisibilityEnabled,
			SignInOptions: &ssoadmintypes.SignInOptions{
				Origin: ssoadmintypes.SignInOriginIdentityCenter,
			},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListApplications(ctx, &ssoadminsdk.ListApplicationsInput{InstanceArn: inst.InstanceArn})
	require.NoError(t, err)
	require.Len(t, listOut.Applications, 1)

	item := listOut.Applications[0]
	require.Equal(t, aws.ToString(created.ApplicationArn), aws.ToString(item.ApplicationArn))
	require.NotNil(t, item.PortalOptions, "per-item PortalOptions must round-trip through ListApplications")
	assert.Equal(t, ssoadmintypes.ApplicationVisibilityEnabled, item.PortalOptions.Visibility)
	require.NotNil(t, item.PortalOptions.SignInOptions)
	assert.Equal(t, ssoadmintypes.SignInOriginIdentityCenter, item.PortalOptions.SignInOptions.Origin)
}
